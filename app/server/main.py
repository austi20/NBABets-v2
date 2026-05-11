from __future__ import annotations

import argparse
import logging
import secrets
import sys
import threading
import traceback
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import date
from importlib import metadata
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, cast

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import Table
from sqlalchemy.engine import make_url

from app.config.settings import get_settings
from app.db.base import Base
from app.db.models.trading import (
    TradingDailyPnL,
    TradingFill,
    TradingKillSwitch,
    TradingOrder,
    TradingPosition,
)
from app.db.session import SessionLocal, get_engine
from app.server.auth import AppTokenMiddleware
from app.server.routers.board import router as board_router
from app.server.routers.insights import router as insights_router
from app.server.routers.local_agent import router as local_agent_router
from app.server.routers.parlays import router as parlays_router
from app.server.routers.props import router as props_router
from app.server.routers.startup import router as startup_router
from app.server.routers.trading import router as trading_router
from app.server.services.board_cache import BoardCache
from app.services.startup import StartupCoordinator
from app.trading.brain_auto_resync import BrainAutoResync
from app.trading.decision_brain import sync_decision_brain, write_blocked_decision_pack
from app.trading.factory import build_exchange_adapter
from app.trading.ledger import InMemoryPortfolioLedger
from app.trading.live_limits import LimitsConfigError, load_live_limits
from app.trading.loop_controller import TradingLoopController
from app.trading.market_book import MarketBook
from app.trading.risk import ExposureRiskEngine
from app.trading.snapshot_service import TradingSnapshotService
from app.trading.stream_publisher import TradingStreamPublisher
from app.trading.ws_consumer import KalshiWsCredentials
from app.trading.ws_service import KalshiMarketService

_ALLOWED_ORIGINS = ("*",)


def _build_market_service() -> KalshiMarketService:
    settings = get_settings()
    creds = KalshiWsCredentials(
        api_key_id=settings.kalshi_api_key_id or "",
        private_key_path=Path(settings.kalshi_private_key_path)
        if settings.kalshi_private_key_path
        else Path(""),
    )
    return KalshiMarketService(
        symbols_path=Path(settings.kalshi_symbols_path),
        credentials=creds,
        book=MarketBook(),
        base_url=settings.kalshi_ws_base_url,
        ping_interval_seconds=settings.kalshi_ws_ping_interval_seconds,
        max_backoff_seconds=settings.kalshi_ws_max_backoff_seconds,
        max_consecutive_auth_failures=settings.kalshi_ws_max_consecutive_auth_failures,
    )


def _ensure_trading_tables() -> None:
    Base.metadata.create_all(
        get_engine(),
        tables=[
            cast(Table, TradingOrder.__table__),
            cast(Table, TradingFill.__table__),
            cast(Table, TradingPosition.__table__),
            cast(Table, TradingKillSwitch.__table__),
            cast(Table, TradingDailyPnL.__table__),
        ],
    )


def _build_trading_risk() -> ExposureRiskEngine:
    settings = get_settings()
    try:
        return ExposureRiskEngine(load_live_limits(settings.trading_limits_path))
    except LimitsConfigError:
        return ExposureRiskEngine()


def _app_version() -> str:
    try:
        return metadata.version("nba-prop-probability-engine")
    except metadata.PackageNotFoundError:
        return "0.1.0"


def _sanitize_settings() -> dict[str, Any]:
    settings = get_settings()
    database_url = settings.database_url
    if database_url.startswith("sqlite"):
        safe_database_url = database_url
    else:
        parsed = make_url(database_url)
        if parsed.username:
            parsed = parsed.set(username="***")
        if parsed.password is not None:
            parsed = parsed.set(password="***")
        safe_database_url = parsed.render_as_string(hide_password=False)
    return {
        "app_env": settings.app_env,
        "database_url": safe_database_url,
        "app_data_dir": str(settings.app_data_dir),
        "logs_dir": str(settings.logs_dir),
        "providers": {
            "stats": settings.stats_provider,
            "odds": settings.odds_provider,
            "injury": settings.injury_provider,
        },
        "agent_mode": settings.agent_mode,
        "local_autonomy_enabled": settings.local_autonomy_enabled,
        "model_version": settings.model_version,
        "feature_version": settings.feature_version,
    }


def _configure_sidecar_logging() -> None:
    settings = get_settings()
    log_path = settings.logs_dir / "sidecar.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.INFO)

    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)


def _install_exception_logging() -> None:
    logger = logging.getLogger("nba.sidecar")

    def _sys_hook(exc_type, exc, tb) -> None:
        logger.error("Uncaught exception in sidecar main thread")
        logger.error("".join(traceback.format_exception(exc_type, exc, tb)).rstrip())
        sys.__excepthook__(exc_type, exc, tb)

    def _thread_hook(args: threading.ExceptHookArgs) -> None:
        thread_name = args.thread.name if args.thread is not None else "<unknown>"
        logger.error("Uncaught exception in thread %s", thread_name)
        logger.error(
            "".join(traceback.format_exception(args.exc_type, args.exc_value, args.exc_traceback)).rstrip()
        )

    sys.excepthook = _sys_hook
    threading.excepthook = _thread_hook


def create_app(
    *,
    startup_coordinator: StartupCoordinator | None = None,
    board_cache: BoardCache | None = None,
    app_token: str | None = None,
) -> FastAPI:
    market_service = _build_market_service()

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        settings = get_settings()
        _app.state.market_service = market_service
        if settings.kalshi_ws_enabled:
            await market_service.start()
        if (
            settings.auto_init_budget_from_wallet
            and settings.kalshi_api_key_id
            and settings.kalshi_private_key_path
        ):
            try:
                from app.providers.exchanges.kalshi_client import KalshiClient
                from app.trading.wallet_init import init_budget_from_wallet

                _wc = KalshiClient(
                    api_key_id=settings.kalshi_api_key_id,
                    private_key_path=Path(str(settings.kalshi_private_key_path)),
                    base_url=settings.kalshi_base_url,
                )
                try:
                    init_budget_from_wallet(
                        client=_wc,
                        path=Path(settings.trading_limits_path),
                    )
                finally:
                    _wc.close()
            except Exception as exc:  # noqa: BLE001
                logging.getLogger("nba.sidecar").warning("wallet-init failed: %s", exc)
        await brain_resync.start()
        try:
            yield
        finally:
            await brain_resync.stop()
            await market_service.stop()

    app = FastAPI(title="NBA Prop Probability Engine API", version=_app_version(), lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(_ALLOWED_ORIGINS),
        allow_credentials=False,
        allow_methods=["GET", "POST"],
        allow_headers=["*"],
    )
    resolved_board_cache = board_cache or BoardCache()

    def _on_startup_start() -> None:
        resolved_board_cache.clear()
        settings = get_settings()
        if settings.kalshi_decision_brain_enabled:
            try:
                write_blocked_decision_pack(
                    settings=settings,
                    board_date=date.today(),
                    mode="startup",
                    reason="startup data refresh in progress",
                )
            except Exception as exc:  # noqa: BLE001 - startup should continue while surfacing stale-pack risk
                logging.getLogger("nba.sidecar").warning("Could not clear decision pack at startup: %s", exc)

    def _on_startup_success(result) -> None:  # noqa: ANN001
        resolved_board_cache.clear()
        entry = resolved_board_cache.populate(result.board_date)
        settings = get_settings()
        if settings.kalshi_decision_brain_enabled and settings.kalshi_decision_brain_auto_sync_on_startup:
            try:
                brain_result = sync_decision_brain(
                    settings=settings,
                    board_entry=entry,
                    board_date=result.board_date,
                    mode="observe",
                    resolve_markets=True,
                    build_pack=True,
                )
                logging.getLogger("nba.sidecar").info(
                    "Decision brain startup sync: state=%s selected=%s ticker=%s",
                    brain_result.state,
                    brain_result.selected_candidate_id,
                    brain_result.selected_ticker,
                )
            except Exception as exc:  # noqa: BLE001 - startup must still finish if brain sync blocks
                logging.getLogger("nba.sidecar").warning("Decision brain startup sync failed: %s", exc)

    resolved_startup_coordinator = startup_coordinator or StartupCoordinator(
        on_start=_on_startup_start,
        on_success=_on_startup_success,
    )

    app.state.startup_coordinator = resolved_startup_coordinator
    app.state.board_cache = resolved_board_cache
    _ensure_trading_tables()
    app.state.trading_session_factory = SessionLocal
    app.state.trading_ledger = InMemoryPortfolioLedger()
    app.state.trading_risk = _build_trading_risk()
    app.state.trading_loop_controller = TradingLoopController()
    exchange_config = build_exchange_adapter()
    app.state.trading_exchange = exchange_config.exchange
    app.state.trading_adapter = exchange_config.adapter
    stream_publisher = TradingStreamPublisher()
    snapshot_service = TradingSnapshotService(
        settings=get_settings(),
        market_book=market_service.book,
        selections_path=Path(get_settings().app_data_dir) / "trading_selections.json",
        publisher=stream_publisher,
    )
    app.state.trading_stream_publisher = stream_publisher
    app.state.trading_snapshot_service = snapshot_service
    stream_publisher.log_event(level="info", message="trading stream publisher started")

    def _do_brain_sync() -> object:
        # Stub - will be replaced when decision_brain is fully wired for auto-resync
        return None

    def _current_mode() -> str:
        import json as _json

        decisions_path_str = getattr(get_settings(), "kalshi_decisions_path", None)
        if not decisions_path_str:
            return "observe"
        decisions_path = Path(str(decisions_path_str))
        if not decisions_path.is_file():
            return "observe"
        try:
            data = _json.loads(decisions_path.read_text(encoding="utf-8"))
            rows = data if isinstance(data, list) else (data.get("decisions") or [{}])
            first = rows[0] if rows else {}
            return "supervised-live" if first.get("mode") == "live" else "observe"
        except Exception:  # noqa: BLE001
            return "observe"

    brain_resync = BrainAutoResync(
        interval_seconds=float(get_settings().brain_auto_resync_seconds),
        sync_fn=_do_brain_sync,
        mode_fn=_current_mode,
        publisher=stream_publisher,
    )
    app.state.trading_brain_resync = brain_resync
    app.state.app_token = app_token or secrets.token_urlsafe(24)
    app.add_middleware(
        AppTokenMiddleware,
        protected_prefixes=("/api/local-agent", "/api/trading"),
        exempt_paths=("/api/startup/run",),
    )
    app.include_router(startup_router)
    app.include_router(board_router)
    app.include_router(props_router)
    app.include_router(parlays_router)
    app.include_router(insights_router)
    app.include_router(local_agent_router)
    app.include_router(trading_router)

    @app.get("/api/health")
    def health() -> dict[str, Any]:
        settings = get_settings()
        return {
            "ok": True,
            "version": _app_version(),
            "db_path": settings.database_url,
        }

    @app.get("/api/settings")
    def settings_summary() -> dict[str, Any]:
        return _sanitize_settings()

    return app


def cli() -> None:
    parser = argparse.ArgumentParser(description="Run NBA sidecar API server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--app-token", default=None)
    parser.add_argument("--reload", action="store_true", default=False)
    args = parser.parse_args()
    _configure_sidecar_logging()
    _install_exception_logging()
    logging.getLogger("nba.sidecar").info("Starting sidecar on %s:%s", args.host, args.port)

    uvicorn.run(
        create_app(app_token=args.app_token),
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_config=None,
    )


if __name__ == "__main__":
    cli()


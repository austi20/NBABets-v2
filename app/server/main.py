from __future__ import annotations

import argparse
import logging
import os
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
from typing import Any

import uvicorn
from alembic import command
from alembic.config import Config
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse
from sqlalchemy.engine import make_url

from app.config.settings import get_settings
from app.db.session import SessionLocal, get_database_selection
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
from app.trading.decision_brain import default_brain_status, sync_decision_brain, write_blocked_decision_pack
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

logger = logging.getLogger(__name__)

_ALLOWED_ORIGINS = (
    "tauri://localhost",
    "http://tauri.localhost",
    "http://127.0.0.1:1420",  # Vite dev origin for tauri dev
)


def _build_market_service() -> KalshiMarketService:
    settings = get_settings()
    creds = KalshiWsCredentials(
        api_key_id=settings.kalshi_api_key_id.get_secret_value() if settings.kalshi_api_key_id else "",
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


def _upgrade_database() -> None:
    """Bring the active database to the latest Alembic revision.

    Runs against the URL the engine actually binds to (post-discovery), so the
    migrations touch the same SQLite file the app reads. The baseline migration
    is a no-op when a populated schema already exists.
    """
    repo_root = Path(__file__).resolve().parents[2]
    cfg = Config(str(repo_root / "alembic.ini"))
    cfg.set_main_option("script_location", str(repo_root / "migrations"))
    cfg.set_main_option("sqlalchemy.url", get_database_selection().url)
    command.upgrade(cfg, "head")


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
        "balldontlie_api_key": _redact_secret(settings.balldontlie_api_key),
        "kalshi_api_key_id": _redact_secret(settings.kalshi_api_key_id),
    }


def _redact_secret(value: Any) -> str | None:
    """Return ``"***"`` when a secret is set, otherwise ``None``. Never leaks the value."""
    return "***" if value is not None else None


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


def _resolve_app_token(app_token: str | None, *, rotate: bool = False) -> str:
    """Return a stable app token, persisting it at <app_data_dir>/.app_token.

    An explicit ``app_token`` always wins. Otherwise reuse the persisted token
    when present (and not rotating); else generate, persist, and return a new one.
    """
    if app_token:
        return app_token
    token_path = Path(get_settings().app_data_dir) / ".app_token"
    if not rotate:
        try:
            existing = token_path.read_text(encoding="utf-8").strip()
            if existing:
                return existing
        except OSError:
            pass
    token = secrets.token_urlsafe(32)
    try:
        token_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(token_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            os.write(fd, token.encode("utf-8"))
        finally:
            os.close(fd)
        try:
            os.chmod(str(token_path), 0o600)  # best-effort; no-op on Windows
        except OSError:
            pass
    except OSError as exc:
        logger.warning("app_token persist failed: %s", exc)
    return token


def create_app(
    *,
    startup_coordinator: StartupCoordinator | None = None,
    board_cache: BoardCache | None = None,
    app_token: str | None = None,
    rotate_token: bool = False,
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
                    api_key_id=settings.kalshi_api_key_id.get_secret_value() if settings.kalshi_api_key_id else "",
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
                logger.warning("wallet-init failed: %s", exc)
        await brain_resync.start()
        try:
            yield
        finally:
            await brain_resync.stop()
            await market_service.stop()

    app = FastAPI(
        title="NBA Prop Probability Engine API",
        version=_app_version(),
        lifespan=lifespan,
        default_response_class=ORJSONResponse,
    )
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
                logger.warning("Could not clear decision pack at startup: %s", exc)

    def _on_startup_success(result) -> None:  # noqa: ANN001
        resolved_board_cache.clear()
        entry = resolved_board_cache.populate(result.board_date)
        settings = get_settings()
        if settings.kalshi_decision_brain_enabled and settings.kalshi_decision_brain_auto_sync_on_startup:
            try:
                startup_mode = (
                    "supervised-live"
                    if settings.trading_live_enabled and settings.kalshi_live_trading
                    else "observe"
                )
                brain_result = sync_decision_brain(
                    settings=settings,
                    board_entry=entry,
                    board_date=result.board_date,
                    mode=startup_mode,
                    resolve_markets=True,
                    build_pack=True,
                )
                logger.info(
                    "Decision brain startup sync: state=%s selected=%s ticker=%s",
                    brain_result.state,
                    brain_result.selected_candidate_id,
                    brain_result.selected_ticker,
                )
            except Exception as exc:  # noqa: BLE001 - startup must still finish if brain sync blocks
                logger.warning("Decision brain startup sync failed: %s", exc)

    resolved_startup_coordinator = startup_coordinator or StartupCoordinator(
        on_start=_on_startup_start,
        on_success=_on_startup_success,
    )

    app.state.startup_coordinator = resolved_startup_coordinator
    app.state.board_cache = resolved_board_cache
    _upgrade_database()
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
        settings = get_settings()
        today = date.today()
        entry = resolved_board_cache.populate(today)
        try:
            return sync_decision_brain(
                settings=settings,
                board_entry=entry,
                board_date=today,
                mode="supervised-live",
                resolve_markets=True,
                build_pack=True,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("auto-resync failed: %s", exc)
            return default_brain_status(settings)

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
    app.state.app_token = _resolve_app_token(app_token, rotate=rotate_token)
    app.add_middleware(
        AppTokenMiddleware,
        protected_prefixes=("/api/local-agent", "/api/startup", "/api/trading"),
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
    parser.add_argument("--rotate-token", action="store_true", default=False)
    parser.add_argument("--reload", action="store_true", default=False)
    args = parser.parse_args()
    _configure_sidecar_logging()
    _install_exception_logging()
    logger.info("Starting sidecar on %s:%s", args.host, args.port)

    if sys.platform == "win32":
        try:
            import winloop
            winloop.install()
        except ImportError:
            pass

    uvicorn.run(
        create_app(app_token=args.app_token, rotate_token=args.rotate_token),
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_config=None,
    )


if __name__ == "__main__":
    cli()


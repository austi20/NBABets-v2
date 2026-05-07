from __future__ import annotations

import argparse
import logging
import secrets
import sys
import threading
import traceback
from importlib import metadata
from logging.handlers import RotatingFileHandler
from typing import Any

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.engine import make_url

from app.config.settings import get_settings
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
from app.trading.factory import build_exchange_adapter
from app.trading.ledger import InMemoryPortfolioLedger
from app.trading.risk import ExposureRiskEngine

_ALLOWED_ORIGINS = ("*",)


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
    app = FastAPI(title="NBA Prop Probability Engine API", version=_app_version())
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

    def _on_startup_success(result) -> None:  # noqa: ANN001
        resolved_board_cache.clear()
        resolved_board_cache.populate(result.board_date)

    resolved_startup_coordinator = startup_coordinator or StartupCoordinator(
        on_start=_on_startup_start,
        on_success=_on_startup_success,
    )

    app.state.startup_coordinator = resolved_startup_coordinator
    app.state.board_cache = resolved_board_cache
    app.state.trading_ledger = InMemoryPortfolioLedger()
    app.state.trading_risk = ExposureRiskEngine()
    exchange_config = build_exchange_adapter()
    app.state.trading_exchange = exchange_config.exchange
    app.state.trading_adapter = exchange_config.adapter
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


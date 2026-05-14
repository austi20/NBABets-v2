# tests/integration/server/test_trading_stream.py
from __future__ import annotations

import asyncio
import json

from httpx import ASGITransport, AsyncClient

from app.config.settings import get_settings
from app.server.main import create_app


def test_snapshot_live_returns_valid_payload(tmp_path, monkeypatch) -> None:
    data_dir = tmp_path / "app"
    symbols_path = tmp_path / "config" / "kalshi_symbols.json"
    decisions_path = tmp_path / "data" / "decisions.json"
    limits_path = tmp_path / "config" / "trading_limits.json"
    targets_path = tmp_path / "config" / "kalshi_targets.json"
    database_path = tmp_path / "trading.sqlite"
    for path, payload in (
        (symbols_path, {"version": 1, "symbols": [], "unresolved": []}),
        (decisions_path, {"version": 1, "decisions": []}),
        (
            limits_path,
            {
                "per_order_cap": 1.0,
                "per_market_cap": 1.0,
                "max_open_notional": 2.0,
                "daily_loss_cap": 2.0,
                "reject_cooldown_seconds": 300,
            },
        ),
        (targets_path, {"version": 1, "targets": []}),
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")
    data_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("APP_DATA_DIR", str(data_dir))
    monkeypatch.setenv("NBA_PROP_APP_DATA_DIR", str(data_dir))
    monkeypatch.setenv("LOGS_DIR", str(tmp_path / "logs"))
    monkeypatch.setenv("SNAPSHOT_DIR", str(tmp_path / "snapshots"))
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{database_path.as_posix()}")
    monkeypatch.setenv("TRADING_EXCHANGE", "paper")
    monkeypatch.setenv("TRADING_LIVE_ENABLED", "false")
    monkeypatch.setenv("KALSHI_LIVE_TRADING", "false")
    monkeypatch.setenv("KALSHI_WS_ENABLED", "false")
    monkeypatch.setenv("KALSHI_API_KEY_ID", "")
    monkeypatch.setenv("KALSHI_PRIVATE_KEY_PATH", "")
    monkeypatch.setenv("KALSHI_DECISION_BRAIN_ENABLED", "false")
    monkeypatch.setenv("KALSHI_DECISION_BRAIN_AUTO_SYNC_ON_STARTUP", "false")
    monkeypatch.setenv("AUTO_INIT_BUDGET_FROM_WALLET", "false")
    monkeypatch.setenv("KALSHI_SYMBOLS_PATH", str(symbols_path))
    monkeypatch.setenv("KALSHI_DECISIONS_PATH", str(decisions_path))
    monkeypatch.setenv("KALSHI_RESOLUTION_TARGETS_PATH", str(targets_path))
    monkeypatch.setenv("TRADING_LIMITS_PATH", str(limits_path))
    get_settings.cache_clear()

    async def _run() -> None:
        app = create_app(app_token="test-token")
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as client:
            response = await client.get("/api/trading/snapshot-live")
            assert response.status_code == 200
            payload = response.json()
            assert "kpis" in payload
            assert "picks" in payload
            assert "bet_slip" in payload
            assert "event_log" in payload

    try:
        asyncio.run(_run())
    finally:
        get_settings.cache_clear()

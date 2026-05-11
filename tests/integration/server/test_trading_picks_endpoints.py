# tests/integration/server/test_trading_picks_endpoints.py
from __future__ import annotations

import asyncio

from httpx import ASGITransport, AsyncClient

from app.server.main import create_app


def test_toggle_then_snapshot_reflects_change(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("NBA_PROP_APP_DATA_DIR", str(tmp_path))

    async def _run() -> None:
        app = create_app(app_token="test-token")
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as client:
            headers = {"X-App-Token": "test-token"}
            response = await client.post(
                "/api/trading/picks/test-candidate/toggle",
                json={"included": False},
                headers=headers,
            )
            assert response.status_code == 200
            payload = response.json()
            assert "picks" in payload

    asyncio.run(_run())


def test_limits_update_persists(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("NBA_PROP_APP_DATA_DIR", str(tmp_path))
    limits_path = tmp_path / "trading_limits.json"
    monkeypatch.setenv("TRADING_LIMITS_PATH", str(limits_path))

    async def _run() -> None:
        app = create_app(app_token="test-token")
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as client:
            response = await client.post(
                "/api/trading/limits",
                json={"max_open_notional": 25.50},
                headers={"X-App-Token": "test-token"},
            )
            assert response.status_code == 200
            assert response.json()["max_open_notional"] == 25.50

    asyncio.run(_run())

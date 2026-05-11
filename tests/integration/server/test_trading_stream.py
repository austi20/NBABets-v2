# tests/integration/server/test_trading_stream.py
from __future__ import annotations

import asyncio

from httpx import ASGITransport, AsyncClient

from app.server.main import create_app


def test_snapshot_live_returns_valid_payload(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("NBA_PROP_APP_DATA_DIR", str(tmp_path))

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

    asyncio.run(_run())

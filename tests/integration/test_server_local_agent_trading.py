from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from httpx import ASGITransport, AsyncClient

from app.server.main import create_app
from app.services.insights import LocalAgentStatus
from app.trading.types import Fill, MarketRef


def test_local_agent_and_trading_endpoints_with_app_token(monkeypatch) -> None:
    async def _run() -> None:
        fake_status = LocalAgentStatus(
            enabled=True,
            auto_execute_safe=False,
            updated_at=datetime.now(UTC),
            updated_by="test",
            note="note",
            last_run_status="ok",
            last_run_at=datetime.now(UTC),
            last_summary="summary",
            last_confidence=0.75,
        )

        def _fake_load_local_agent_status(_session):  # noqa: ANN001
            return fake_status

        def _fake_terminal_text(_session, *, endpoint: str, model: str, limit: int = 100):  # noqa: ANN001, ARG001
            return f"# terminal {endpoint} {model}"

        monkeypatch.setattr("app.server.routers.local_agent.load_local_agent_status", _fake_load_local_agent_status)
        monkeypatch.setattr("app.server.routers.local_agent.build_local_ai_terminal_text", _fake_terminal_text)
        monkeypatch.setattr(
            "app.server.routers.local_agent.update_local_agent_policy_state",
            lambda **kwargs: None,  # noqa: ARG005
        )

        app = create_app(app_token="secret-token")
        ledger = app.state.trading_ledger
        ledger.record_fill(
            Fill(
                fill_id="f1",
                intent_id="i1",
                market=MarketRef(
                    exchange="paper",
                    symbol="player-42-points-over-24.5",
                    market_key="points",
                    side="OVER",
                    line_value=24.5,
                ),
                side="buy",
                stake=10.0,
                price=1.9,
            )
        )

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://testserver", timeout=5.0) as client:
            status = await client.get("/api/local-agent/status")
            assert status.status_code == 200
            assert status.json()["enabled"] is True

            positions = await client.get("/api/trading/positions")
            assert positions.status_code == 200
            assert positions.json()[0]["market_key"] == "points"

            pnl = await client.get("/api/trading/pnl")
            assert pnl.status_code == 200
            assert pnl.json()["kill_switch_active"] is False

            fills = await client.get("/api/trading/fills/recent?limit=10")
            assert fills.status_code == 200
            assert fills.json()[0]["fill_id"] == "f1"

            unauthorized_policy = await client.post(
                "/api/local-agent/policy",
                json={"policy": "enable"},
            )
            assert unauthorized_policy.status_code == 401

            authorized_policy = await client.post(
                "/api/local-agent/policy",
                json={"policy": "enable"},
                headers={"X-App-Token": "secret-token"},
            )
            assert authorized_policy.status_code == 200

            unauthorized_kill = await client.post("/api/trading/kill-switch")
            assert unauthorized_kill.status_code == 401

            authorized_kill = await client.post(
                "/api/trading/kill-switch",
                headers={"X-App-Token": "secret-token"},
            )
            assert authorized_kill.status_code == 200
            assert authorized_kill.json()["kill_switch_active"] is True

            intent_payload = {
                "player_id": 42,
                "market": "points",
                "line": 24.5,
                "side": "over",
                "sportsbook_key": "draftkings",
                "stake": 1.0,
            }
            unauthorized_intent = await client.post("/api/trading/intent", json=intent_payload)
            assert unauthorized_intent.status_code == 401

            authorized_intent = await client.post(
                "/api/trading/intent",
                json=intent_payload,
                headers={"X-App-Token": "secret-token"},
            )
            assert authorized_intent.status_code == 200
            assert authorized_intent.json()["accepted"] is False
            assert "stub received" in authorized_intent.json()["message"]

    asyncio.run(_run())


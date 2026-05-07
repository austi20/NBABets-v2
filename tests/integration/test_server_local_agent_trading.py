from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path

from httpx import ASGITransport, AsyncClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.base import Base
from app.db.models.trading import (
    TradingDailyPnL,
    TradingFill,
    TradingKillSwitch,
    TradingOrder,
    TradingPosition,
)
from app.server.main import create_app
from app.services.insights import LocalAgentStatus
from app.trading.risk import ExposureRiskEngine, RiskLimits
from app.trading.types import ExecutionIntent, Fill, MarketRef, OrderEvent


class _AcceptedThenFailedAdapter:
    def place_order(self, intent: ExecutionIntent) -> tuple[list[OrderEvent], list[Fill]]:
        return (
            [
                OrderEvent(
                    intent_id=intent.intent_id,
                    event_type="accepted",
                    status="ok",
                    message="accepted upstream",
                ),
                OrderEvent(
                    intent_id=intent.intent_id,
                    event_type="error",
                    status="failed",
                    message="fill lookup failed",
                ),
            ],
            [],
        )


def test_local_agent_and_trading_endpoints_with_app_token(monkeypatch, tmp_path: Path) -> None:
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

        engine = create_engine(f"sqlite:///{tmp_path / 'trading.sqlite'}", future=True)
        Base.metadata.create_all(
            engine,
            tables=[
                TradingOrder.__table__,
                TradingFill.__table__,
                TradingPosition.__table__,
                TradingKillSwitch.__table__,
                TradingDailyPnL.__table__,
            ],
        )
        factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

        app = create_app(app_token="secret-token")
        app.state.trading_session_factory = factory
        app.state.trading_risk = ExposureRiskEngine(RiskLimits(per_order_cap=0.25))
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
            assert pnl.json()["active_limits"]["per_order_cap"] == 0.25

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

            intent_payload = {
                "game_id": 1001,
                "player_id": 42,
                "market": "points",
                "line": 24.5,
                "side": "over",
                "sportsbook_key": "draftkings",
                "stake": 0.25,
            }
            accepted_intent = await client.post(
                "/api/trading/intent",
                json=intent_payload,
                headers={"X-App-Token": "secret-token"},
            )
            assert accepted_intent.status_code == 200
            assert accepted_intent.json()["accepted"] is True
            assert "accepted" in accepted_intent.json()["message"].lower()

            app.state.trading_adapter = _AcceptedThenFailedAdapter()
            failed_after_accept = await client.post(
                "/api/trading/intent",
                json=intent_payload,
                headers={"X-App-Token": "secret-token"},
            )
            assert failed_after_accept.status_code == 200
            assert failed_after_accept.json()["accepted"] is False
            assert "fill lookup failed" in failed_after_accept.json()["message"]

            unauthorized_kill = await client.post("/api/trading/kill-switch")
            assert unauthorized_kill.status_code == 401

            authorized_kill = await client.post(
                "/api/trading/kill-switch",
                headers={"X-App-Token": "secret-token"},
            )
            assert authorized_kill.status_code == 200
            assert authorized_kill.json()["kill_switch_active"] is True
            with factory() as session:
                row = session.get(TradingKillSwitch, 1)
                assert row is not None
                assert row.killed is True

            unauthorized_intent = await client.post("/api/trading/intent", json=intent_payload)
            assert unauthorized_intent.status_code == 401

            authorized_intent = await client.post(
                "/api/trading/intent",
                json=intent_payload,
                headers={"X-App-Token": "secret-token"},
            )
            assert authorized_intent.status_code == 200
            assert authorized_intent.json()["accepted"] is False
            assert "kill switch" in authorized_intent.json()["message"]

    asyncio.run(_run())


"""Tests for ApiMonitorAgent — Task 6.1."""
from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.models.all import AgentRunEvent, RawPayload
from app.models.events import Base
from app.models.reference import Base as RefBase
from app.services.agents.api_monitor import ApiMonitorAgent
from app.services.agents.contracts import AgentTask


def _make_session() -> Session:
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    # Create all tables from both metadata objects
    Base.metadata.create_all(engine)
    RefBase.metadata.create_all(engine)
    return Session(engine)


def test_api_monitor_counts_failures_correctly() -> None:
    session = _make_session()
    now = datetime.now(UTC)

    # Seed 2 AgentRunEvent rows with status="error"
    for _ in range(2):
        ev = AgentRunEvent(
            task_id="t1",
            agent_role="api_monitor",
            event_type="run_complete",
            status="error",
            payload={"provider_name": "bdl"},
            created_at=now,
        )
        session.add(ev)

    # Seed 1 RawPayload row for "bdl"
    rp = RawPayload(
        provider_name="bdl",
        provider_type="stats",
        endpoint="/bdl/stats",
        content_hash="abc",
        payload={},
        fetched_at=now,
    )
    session.add(rp)
    session.commit()

    task = AgentTask(role="api_monitor", task_type="monitor")
    result = ApiMonitorAgent(session).handle(task)
    assert result.details["provider_failures_today"]["bdl"] == 2


def test_api_monitor_provider_contract_action_when_low_volume() -> None:
    session = _make_session()
    now = datetime.now(UTC)

    # Seed exactly 1 RawPayload (count < 2 triggers action)
    rp = RawPayload(
        provider_name="bdl",
        provider_type="stats",
        endpoint="/bdl/stats",
        content_hash="abc",
        payload={},
        fetched_at=now,
    )
    session.add(rp)
    session.commit()

    task = AgentTask(role="api_monitor", task_type="monitor")
    result = ApiMonitorAgent(session).handle(task)
    action_types = [a.action_type for a in result.actions]
    assert "provider_contract_review" in action_types

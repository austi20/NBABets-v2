"""Guardrail: recommend (non-auto) agent mode must not execute side effects."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.base import Base
from app.models import all as _models  # noqa: F401
from app.services.agents.contracts import AgentResult, AgentTask
from app.services.agents.control_plane import AgentControlPlane


@pytest.fixture
def clear_settings_cache() -> Iterator[None]:
    from app.config.settings import get_settings

    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def test_recommend_mode_forces_dry_run_on_tasks(
    clear_settings_cache,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[AgentTask] = []

    def fake_dispatch(self: object, task: AgentTask) -> AgentResult:
        captured.append(task)
        return AgentResult(task_id=task.task_id, role=task.role, status="ok", summary="stub")

    monkeypatch.setattr("app.services.agents.control_plane.AgentRouter.dispatch", fake_dispatch)

    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    plane = AgentControlPlane(session)
    plane.run(mode="recommend", dry_run=False, include_roles=("data_quality",))

    assert captured
    assert all(t.dry_run for t in captured)


def test_auto_mode_respects_dry_run_false_on_tasks(
    clear_settings_cache,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[AgentTask] = []

    def fake_dispatch(self: object, task: AgentTask) -> AgentResult:
        captured.append(task)
        return AgentResult(task_id=task.task_id, role=task.role, status="ok", summary="stub")

    monkeypatch.setattr("app.services.agents.control_plane.AgentRouter.dispatch", fake_dispatch)

    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    plane = AgentControlPlane(session)
    plane.run(mode="auto", dry_run=False, include_roles=("data_quality",))

    assert captured
    assert all(not t.dry_run for t in captured)

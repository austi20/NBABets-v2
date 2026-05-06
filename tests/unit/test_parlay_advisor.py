from __future__ import annotations

import uuid
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.base import Base
from app.models import all as _models  # noqa: F401
from app.services.agents.contracts import AgentTask
from app.services.agents.parlay_advisor import ParlayAdvisorAgent


def _make_session(monkeypatch):
    from app.config.settings import get_settings

    root = Path("temp") / f"pytest_parlay_advisor_{uuid.uuid4().hex}"
    root.mkdir(parents=True, exist_ok=True)
    db_url = f"sqlite:///{(root / 'pa.sqlite').resolve().as_posix()}"
    monkeypatch.setenv("DATABASE_URL", db_url)
    # Point AI at an unreachable port so it always falls back deterministically.
    monkeypatch.setenv("AI_LOCAL_ENDPOINT", "http://127.0.0.1:19999/v1/chat/completions")
    get_settings.cache_clear()
    engine = create_engine(db_url, connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()


def _make_parlay(correlation_penalty: float) -> dict:
    return {
        "correlation_penalty": correlation_penalty,
        "weakest_leg_hit_probability": 0.55,
        "joint_probability": 0.18,
        "legs": [
            {
                "player_name": "Player A",
                "market_key": "points",
                "hit_probability": 0.65,
                "recommended_side": "over",
            },
            {
                "player_name": "Player B",
                "market_key": "assists",
                "hit_probability": 0.60,
                "recommended_side": "over",
            },
        ],
    }


def test_flags_high_correlation(monkeypatch) -> None:
    """Parlay with correlation_penalty above threshold must be flagged even when AI is unreachable."""
    from app.config.settings import get_settings

    session = _make_session(monkeypatch)
    task = AgentTask(
        role="parlay_advisor",
        task_type="parlay_advisor_assessment",
        input_payload={"mode": "recommend", "parlays": [_make_parlay(0.75)]},
    )
    result = ParlayAdvisorAgent(session).handle(task)

    assert result.status == "recommendation"
    assert len(result.actions) == 1
    assert result.actions[0].action_type == "flag_high_correlation_parlay"
    assert result.actions[0].safe_to_auto_execute is False
    get_settings.cache_clear()


def test_ok_for_low_correlation(monkeypatch) -> None:
    from app.config.settings import get_settings

    session = _make_session(monkeypatch)
    task = AgentTask(
        role="parlay_advisor",
        task_type="parlay_advisor_assessment",
        input_payload={"mode": "recommend", "parlays": [_make_parlay(0.05)]},
    )
    result = ParlayAdvisorAgent(session).handle(task)

    assert result.status == "ok"
    assert result.actions == []
    get_settings.cache_clear()


def test_returns_ok_when_no_parlays(monkeypatch) -> None:
    from app.config.settings import get_settings

    session = _make_session(monkeypatch)
    task = AgentTask(
        role="parlay_advisor",
        task_type="parlay_advisor_assessment",
        input_payload={"mode": "recommend", "parlays": []},
    )
    result = ParlayAdvisorAgent(session).handle(task)

    assert result.status == "ok"
    assert result.details["parlay_count"] == 0
    assert result.actions == []
    get_settings.cache_clear()

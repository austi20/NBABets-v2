from __future__ import annotations

import uuid
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.base import Base
from app.models import all as _models  # noqa: F401
from app.services.agents.contracts import AgentTask
from app.services.agents.prediction_validator import PredictionValidatorAgent


def _make_session(monkeypatch):
    from app.config.settings import get_settings

    root = Path("temp") / f"pytest_pred_validator_{uuid.uuid4().hex}"
    root.mkdir(parents=True, exist_ok=True)
    db_url = f"sqlite:///{(root / 'pv.sqlite').resolve().as_posix()}"
    monkeypatch.setenv("DATABASE_URL", db_url)
    # Point AI at an unreachable port so it always falls back deterministically.
    monkeypatch.setenv("AI_LOCAL_ENDPOINT", "http://127.0.0.1:19999/v1/chat/completions")
    get_settings.cache_clear()
    engine = create_engine(db_url, connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()


def test_returns_ok_when_no_extreme_predictions(monkeypatch) -> None:
    from app.config.settings import get_settings

    session = _make_session(monkeypatch)
    task = AgentTask(
        role="prediction_validator",
        task_type="prediction_validator_assessment",
        input_payload={"mode": "recommend", "limit": 10},
    )
    result = PredictionValidatorAgent(session).handle(task)

    assert result.status == "ok"
    assert result.actions == []
    assert result.details["extreme_count"] == 0
    get_settings.cache_clear()


def test_threshold_setting_applied(monkeypatch) -> None:
    from app.config.settings import get_settings

    monkeypatch.setenv("PREDICTION_VALIDATOR_EXTREME_PROB_THRESHOLD", "0.85")
    session = _make_session(monkeypatch)

    agent = PredictionValidatorAgent(session)
    assert agent._settings.prediction_validator_extreme_prob_threshold == 0.85
    get_settings.cache_clear()

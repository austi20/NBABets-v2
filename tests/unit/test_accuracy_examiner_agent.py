"""Unit tests for ``AccuracyExaminerAgent``."""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import date
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.base import Base
from app.models import all as _models  # noqa: F401
from app.services.agents.accuracy_examiner import AccuracyExaminerAgent
from app.services.agents.contracts import AgentTask
from app.services.ai_orchestrator import AIOrchestrator, AIResult

_CSV_HEADER = (
    "game_date,game_id,player_name,player_team,opponent,home_team,away_team,"
    "market,sportsbook,line_value,over_odds,under_odds,actual,hit_over,hit_under,"
    "push,minutes,source"
)


@pytest.fixture
def clear_settings_cache() -> Iterator[None]:
    import app.services.agents.accuracy_examiner as examiner_mod
    import app.services.ai_orchestrator as ai_mod
    from app.config.settings import get_settings

    get_settings.cache_clear()
    examiner_mod.get_settings.cache_clear()
    ai_mod.get_settings.cache_clear()
    yield
    get_settings.cache_clear()
    examiner_mod.get_settings.cache_clear()
    ai_mod.get_settings.cache_clear()


def _write_csv(tmp_path: Path) -> Path:
    rows = [
        "2026-03-28,1,Real McCoy,NYK,PHI,NYK,PHI,Player Points,fanduel,22.5,-110,-110,24,True,False,False,33.1,real",
    ]
    p = tmp_path / "props.csv"
    p.write_text("\n".join([_CSV_HEADER, *rows]) + "\n", encoding="utf-8")
    return p


def test_retrain_action_is_never_auto_and_brain_called(
    clear_settings_cache,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    csv_path = _write_csv(tmp_path)
    brain_sqlite = tmp_path / "brain.sqlite"
    monkeypatch.setenv("EXAMINER_CSV_PATH", str(csv_path))
    monkeypatch.setenv("BRAIN_DB_PATH", str(brain_sqlite))
    monkeypatch.setenv("BRAIN_VAULT_ROOT", str(tmp_path / "vault"))
    from app.config.settings import get_settings

    get_settings.cache_clear()

    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    corrections: list[object] = []

    class _StubBrain:
        def record_correction(self, record: object) -> int:
            corrections.append(record)
            return len(corrections)

        def close(self) -> None:
            return None

    payload = {
        "errors": [
            {
                "signal": "calibration_drift",
                "headline": "ece high",
                "detail": "drift",
                "market": "points",
                "confidence": 0.6,
            }
        ],
        "data_filters": [],
        "calculation_checks": [],
        "retrain_recommendation": {"trigger": True, "confidence": 0.9, "reason": "ece"},
    }

    def _fake_summarize(self, *, task_name: str, prompt: str) -> AIResult:
        return AIResult(provider="local", model="stub", text=json.dumps(payload))

    monkeypatch.setattr(AIOrchestrator, "summarize", _fake_summarize)
    monkeypatch.setattr("app.services.agents.accuracy_examiner.Brain", _StubBrain)
    monkeypatch.setattr(
        "app.services.agents.accuracy_examiner.build_feature_attribution_review",
        lambda *a, **k: {},
    )

    agent = AccuracyExaminerAgent(session)
    res = agent.handle(
        AgentTask(
            role="accuracy_examiner",
            task_type="accuracy_examiner_assessment",
            input_payload={
                "report_date": date(2026, 3, 29).isoformat(),
                "markets": ("points",),
                "latest_model_metrics": {},
                "latest_backtest_metrics": {},
                "trend_alerts": (),
            },
            dry_run=True,
        )
    )
    assert res.status == "recommendation"
    retrain_actions = [a for a in res.actions if a.action_type == "retrain_and_predict"]
    assert len(retrain_actions) == 1
    assert retrain_actions[0].safe_to_auto_execute is False
    assert len(corrections) >= 2

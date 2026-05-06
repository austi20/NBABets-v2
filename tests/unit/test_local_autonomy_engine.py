from __future__ import annotations

import json
import uuid
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.base import Base
from app.models import all as _models  # noqa: F401
from app.services.local_autonomy.contracts import LocalAgentDecision
from app.services.local_autonomy.engine import (
    LocalAutonomyEngine,
    _deterministic_blockers,
    render_local_autonomy_markdown,
)
from app.services.local_autonomy.overfit_intel import build_overfit_intel_snapshot
from app.services.local_autonomy.policy_state import load_local_agent_policy_state, update_local_agent_policy_state


def test_local_autonomy_engine_records_decision(monkeypatch) -> None:
    from app.config.settings import get_settings

    root = Path("temp") / f"pytest_local_autonomy_{uuid.uuid4().hex}"
    root.mkdir(parents=True, exist_ok=True)
    db_url = f"sqlite:///{(root / 'auto.sqlite').resolve().as_posix()}"
    monkeypatch.setenv("DATABASE_URL", db_url)
    monkeypatch.setenv("REPORTS_DIR", str(root))
    monkeypatch.setenv("LOCAL_AGENT_POLICY_STATE_PATH", str(root / "local_policy.json"))
    monkeypatch.setenv("AI_LOCAL_ENDPOINT", "http://127.0.0.1:9/v1/chat/completions")
    monkeypatch.setenv("LOCAL_AUTONOMY_ENABLED", "true")
    get_settings.cache_clear()

    engine = create_engine(db_url, connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    update_local_agent_policy_state(
        enabled=True,
        auto_execute_safe=False,
        updated_by="test",
        note="unit_test",
    )
    result = LocalAutonomyEngine(session).run(
        report_date=date(2026, 4, 6),
        mode="recommend",
        dry_run=True,
        latest_model_metrics={"calibration_diagnostics": {"points": {"ece": 0.11}}},
        latest_backtest_metrics={"summary_rows": [{"sample_sufficient": 0.2}]},
        release_status="CAUTION",
        trend_alerts=["PRA ECE deteriorated by +0.03"],
    )
    rendered = render_local_autonomy_markdown(result)

    assert result.decision.snapshot_hash
    assert result.policy_state_enabled is True
    assert "Overfit Signals" in rendered
    assert result.decision.status in {"advisory", "hold", "execute", "disabled"}
    get_settings.cache_clear()


def test_policy_state_toggle_roundtrip(monkeypatch) -> None:
    from app.config.settings import get_settings

    root = Path("temp") / f"pytest_local_policy_{uuid.uuid4().hex}"
    root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("LOCAL_AGENT_POLICY_STATE_PATH", str(root / "policy.json"))
    get_settings.cache_clear()

    update_local_agent_policy_state(
        enabled=False,
        auto_execute_safe=True,
        updated_by="tester",
        note="toggle",
    )
    state = load_local_agent_policy_state()

    assert state.enabled is False
    assert state.auto_execute_safe is True
    assert state.updated_by == "tester"
    assert isinstance(state.updated_at, datetime)
    assert state.updated_at.tzinfo == UTC
    get_settings.cache_clear()


def _local_autonomy_engine_sqlite(monkeypatch) -> LocalAutonomyEngine:
    from app.config.settings import get_settings

    root = Path("temp") / f"pytest_local_autonomy_exec_{uuid.uuid4().hex}"
    root.mkdir(parents=True, exist_ok=True)
    db_url = f"sqlite:///{(root / 'auto.sqlite').resolve().as_posix()}"
    monkeypatch.setenv("DATABASE_URL", db_url)
    monkeypatch.setenv("REPORTS_DIR", str(root))
    monkeypatch.setenv("AI_LOCAL_ENDPOINT", "http://127.0.0.1:9/v1/chat/completions")
    get_settings.cache_clear()
    eng = create_engine(db_url, connect_args={"check_same_thread": False})
    Base.metadata.create_all(eng)
    Session = sessionmaker(bind=eng)
    return LocalAutonomyEngine(Session())


def test_execute_downgraded_to_hold_when_release_status_hold(monkeypatch) -> None:
    from app.config.settings import get_settings

    engine = _local_autonomy_engine_sqlite(monkeypatch)
    fallback = LocalAgentDecision(
        run_id="test_run",
        status="advisory",
        confidence=0.0,
        summary="baseline",
        overfit_risk_score=0.0,
        overfit_signals=tuple(),
        debug_hints=tuple(),
        actions=tuple(),
        deterministic_blockers=tuple(),
        snapshot_hash="abc",
    )
    ai_text = json.dumps(
        {"status": "execute", "confidence": 0.9, "summary": "proceed", "actions": []},
        sort_keys=True,
    )
    decision = engine._decision_from_response(
        ai_text=ai_text,
        fallback=fallback,
        overfit=SimpleNamespace(risk_score=0.1, signals=tuple()),
        debug_hints=tuple(),
        snapshot_hash="abc",
        release_status="HOLD",
    )
    assert decision.status == "hold"
    assert "release_status_hold" in decision.deterministic_blockers
    get_settings.cache_clear()


def test_execute_downgraded_to_hold_when_release_status_blocked(monkeypatch) -> None:
    from app.config.settings import get_settings

    engine = _local_autonomy_engine_sqlite(monkeypatch)
    fallback = LocalAgentDecision(
        run_id="test_run",
        status="advisory",
        confidence=0.0,
        summary="baseline",
        overfit_risk_score=0.0,
        overfit_signals=tuple(),
        debug_hints=tuple(),
        actions=tuple(),
        deterministic_blockers=tuple(),
        snapshot_hash="abc",
    )
    ai_text = json.dumps(
        {"status": "execute", "confidence": 0.85, "summary": "go", "actions": []},
        sort_keys=True,
    )
    decision = engine._decision_from_response(
        ai_text=ai_text,
        fallback=fallback,
        overfit=SimpleNamespace(risk_score=0.2, signals=tuple()),
        debug_hints=tuple(),
        snapshot_hash="abc",
        release_status="BLOCKED",
    )
    assert decision.status == "hold"
    assert "release_status_blocked" in decision.deterministic_blockers
    get_settings.cache_clear()


def test_execute_downgraded_to_hold_when_overfit_risk_extreme(monkeypatch) -> None:
    from app.config.settings import get_settings

    engine = _local_autonomy_engine_sqlite(monkeypatch)
    fallback = LocalAgentDecision(
        run_id="test_run",
        status="advisory",
        confidence=0.0,
        summary="baseline",
        overfit_risk_score=0.0,
        overfit_signals=tuple(),
        debug_hints=tuple(),
        actions=tuple(),
        deterministic_blockers=tuple(),
        snapshot_hash="abc",
    )
    ai_text = json.dumps(
        {"status": "execute", "confidence": 0.88, "summary": "ship it", "actions": []},
        sort_keys=True,
    )
    decision = engine._decision_from_response(
        ai_text=ai_text,
        fallback=fallback,
        overfit=SimpleNamespace(risk_score=0.95, signals=tuple()),
        debug_hints=tuple(),
        snapshot_hash="abc",
        release_status="CAUTION",
    )
    assert decision.status == "hold"
    assert "overfit_risk_extreme" in decision.deterministic_blockers
    get_settings.cache_clear()


def test_overfit_intel_signals_sorted_descending_by_score() -> None:
    snap = build_overfit_intel_snapshot(
        latest_model_metrics={
            "calibration_diagnostics": {
                "points": {"ece": 0.06},
                "pra": {"ece": 0.084},
                "turnovers": {"ece": 0.12},
            }
        },
        latest_backtest_metrics={"summary_rows": [{"sample_sufficient": 0.2}]},
        trend_alerts=["PRA ECE deteriorated"],
    )
    scores = [s.score for s in snap.signals]
    assert scores == sorted(scores, reverse=True)
    assert scores[0] >= scores[-1]


def test_overfit_intel_mitigation_triggers_watch_market_sample_sufficiency_trend() -> None:
    snap = build_overfit_intel_snapshot(
        latest_model_metrics={
            "calibration_diagnostics": {
                "turnovers": {"ece": 0.12},
            }
        },
        latest_backtest_metrics={"summary_rows": [{"sample_sufficient": 0.2}]},
        trend_alerts=["PRA ECE deteriorated"],
    )
    joined = " ".join(snap.mitigations)
    assert "Tighten watch-market calibration thresholds" in joined
    assert "Increase backtest sample density" in joined
    assert "Run bounded deep-eval and compare watch-market drift" in joined


def test_overfit_block_threshold_reads_from_settings(monkeypatch) -> None:
    from app.config.settings import get_settings

    # At threshold 0.75: risk_score=0.80 must trigger the blocker.
    blockers_low = _deterministic_blockers(
        release_status="CAUTION",
        risk_score=0.80,
        overfit_block_threshold=0.75,
    )
    assert "overfit_risk_extreme" in blockers_low

    # At default threshold 0.90: risk_score=0.80 must NOT trigger.
    blockers_default = _deterministic_blockers(
        release_status="CAUTION",
        risk_score=0.80,
        overfit_block_threshold=0.90,
    )
    assert "overfit_risk_extreme" not in blockers_default

    # Verify the setting itself reflects the configured default.
    monkeypatch.setenv("LOCAL_AUTONOMY_OVERFIT_BLOCK_THRESHOLD", "0.75")
    get_settings.cache_clear()
    assert get_settings().local_autonomy_overfit_block_threshold == 0.75
    get_settings.cache_clear()

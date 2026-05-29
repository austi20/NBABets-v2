"""Tests for self_correct env gate and SEMI_AUTO pre-validation — Tasks 6.2 & 6.3."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from app.services.brain.contracts import CorrectionPlan, DiagnosticSignal, PlannedCorrection, StrategyMemory
from app.services.brain.self_correct import run_self_correction_cycle


def _mock_signal() -> DiagnosticSignal:
    return DiagnosticSignal(signal_type="overfit", severity="high", market="points")


def _planned(*, strategy_source: StrategyMemory | None = None, confidence: float = 0.8) -> PlannedCorrection:
    return PlannedCorrection(
        signal=_mock_signal(),
        action_type="weight_adjust",
        market="points",
        confidence=confidence,
        strategy_source=strategy_source,
    )


def _make_plan(corrections: tuple[PlannedCorrection, ...], dry_run: bool = True) -> CorrectionPlan:
    return CorrectionPlan(corrections=corrections, dry_run=dry_run)


# ── Task 6.2: env gate ────────────────────────────────────────────────────────

def test_autonomy_auto_stays_dry_run_without_env_flag(monkeypatch: object) -> None:
    monkeypatch.delenv("WORKFLOW_AGENT_ALLOW_AUTO_ACTIONS", raising=False)  # type: ignore[attr-defined]
    correction = _planned()
    with (
        patch("app.services.brain.self_correct.Brain") as MockBrain,
        patch("app.services.brain.self_correct.interpret_report", return_value=[_mock_signal()]),
        patch("app.services.brain.self_correct.plan_corrections", return_value=_make_plan((correction,), dry_run=False)),
        patch("app.services.brain.self_correct.execute_correction_plan", return_value=[1]),
    ):
        MockBrain.return_value.__enter__ = lambda s: s
        MockBrain.return_value.__exit__ = MagicMock(return_value=False)
        MockBrain.return_value.close = MagicMock()
        result = run_self_correction_cycle(report_text="overfit in points", autonomy_level="auto")
    assert result["dry_run"] is True


def test_autonomy_auto_executes_with_env_flag(monkeypatch: object) -> None:
    monkeypatch.setenv("WORKFLOW_AGENT_ALLOW_AUTO_ACTIONS", "true")  # type: ignore[attr-defined]
    correction = _planned()
    with (
        patch("app.services.brain.self_correct.Brain") as MockBrain,
        patch("app.services.brain.self_correct.interpret_report", return_value=[_mock_signal()]),
        patch("app.services.brain.self_correct.plan_corrections", return_value=_make_plan((correction,), dry_run=False)),
        patch("app.services.brain.self_correct.execute_correction_plan", return_value=[1]),
    ):
        MockBrain.return_value.__enter__ = lambda s: s
        MockBrain.return_value.__exit__ = MagicMock(return_value=False)
        MockBrain.return_value.close = MagicMock()
        result = run_self_correction_cycle(report_text="overfit in points", autonomy_level="auto")
    assert result["dry_run"] is False


def test_autonomy_semi_auto_stays_dry_run_without_env_flag(monkeypatch: object) -> None:
    monkeypatch.delenv("WORKFLOW_AGENT_ALLOW_AUTO_ACTIONS", raising=False)  # type: ignore[attr-defined]
    strategy = StrategyMemory(problem_type="overfit", action_template="weight_adjust", success_rate=0.8, sample_count=10)
    correction = _planned(strategy_source=strategy, confidence=0.8)
    with (
        patch("app.services.brain.self_correct.Brain") as MockBrain,
        patch("app.services.brain.self_correct.interpret_report", return_value=[_mock_signal()]),
        patch("app.services.brain.self_correct.plan_corrections", return_value=_make_plan((correction,), dry_run=False)),
        patch("app.services.brain.self_correct.execute_correction_plan", return_value=[1]),
    ):
        MockBrain.return_value.__enter__ = lambda s: s
        MockBrain.return_value.__exit__ = MagicMock(return_value=False)
        MockBrain.return_value.close = MagicMock()
        result = run_self_correction_cycle(report_text="overfit in points", autonomy_level="semi_auto")
    assert result["dry_run"] is True


def test_autonomy_semi_auto_executes_with_env_flag(monkeypatch: object) -> None:
    monkeypatch.setenv("WORKFLOW_AGENT_ALLOW_AUTO_ACTIONS", "true")  # type: ignore[attr-defined]
    strategy = StrategyMemory(problem_type="overfit", action_template="weight_adjust", success_rate=0.8, sample_count=10)
    correction = _planned(strategy_source=strategy, confidence=0.8)
    with (
        patch("app.services.brain.self_correct.Brain") as MockBrain,
        patch("app.services.brain.self_correct.interpret_report", return_value=[_mock_signal()]),
        patch("app.services.brain.self_correct.plan_corrections", return_value=_make_plan((correction,), dry_run=False)),
        patch("app.services.brain.self_correct.execute_correction_plan", return_value=[1]),
    ):
        MockBrain.return_value.__enter__ = lambda s: s
        MockBrain.return_value.__exit__ = MagicMock(return_value=False)
        MockBrain.return_value.close = MagicMock()
        result = run_self_correction_cycle(report_text="overfit in points", autonomy_level="semi_auto")
    assert result["dry_run"] is False


# ── Task 6.3: SEMI_AUTO pre-validation gate ───────────────────────────────────

def test_semi_auto_blocks_when_no_proven_strategies(monkeypatch: object) -> None:
    """SEMI_AUTO with no proven strategies → dry_run stays True."""
    monkeypatch.setenv("WORKFLOW_AGENT_ALLOW_AUTO_ACTIONS", "true")  # type: ignore[attr-defined]
    # correction has no strategy_source
    correction = _planned(strategy_source=None)
    with (
        patch("app.services.brain.self_correct.Brain") as MockBrain,
        patch("app.services.brain.self_correct.interpret_report", return_value=[_mock_signal()]),
        patch("app.services.brain.self_correct.plan_corrections", return_value=_make_plan((correction,), dry_run=False)),
        patch("app.services.brain.self_correct.execute_correction_plan", return_value=[]) as mock_exec,
    ):
        MockBrain.return_value.__enter__ = lambda s: s
        MockBrain.return_value.__exit__ = MagicMock(return_value=False)
        MockBrain.return_value.close = MagicMock()
        run_self_correction_cycle(report_text="overfit in points", autonomy_level="semi_auto")
    # No proven corrections → execute_correction_plan receives a dry_run plan
    plan_arg = mock_exec.call_args[0][0]
    assert plan_arg.dry_run is True


def test_semi_auto_passes_when_proven_strategy_present(monkeypatch: object) -> None:
    """SEMI_AUTO with proven strategy + confidence >= 0.6 → plan executed."""
    monkeypatch.setenv("WORKFLOW_AGENT_ALLOW_AUTO_ACTIONS", "true")  # type: ignore[attr-defined]
    strategy = StrategyMemory(
        problem_type="overfit",
        action_template="weight_adjust",
        success_rate=0.8,
        sample_count=10,
    )
    correction = _planned(strategy_source=strategy, confidence=0.8)
    with (
        patch("app.services.brain.self_correct.Brain") as MockBrain,
        patch("app.services.brain.self_correct.interpret_report", return_value=[_mock_signal()]),
        patch("app.services.brain.self_correct.plan_corrections", return_value=_make_plan((correction,), dry_run=False)),
        patch("app.services.brain.self_correct.execute_correction_plan", return_value=[1]) as mock_exec,
    ):
        MockBrain.return_value.__enter__ = lambda s: s
        MockBrain.return_value.__exit__ = MagicMock(return_value=False)
        MockBrain.return_value.close = MagicMock()
        run_self_correction_cycle(report_text="overfit in points", autonomy_level="semi_auto")
    plan_arg = mock_exec.call_args[0][0]
    assert plan_arg.dry_run is False

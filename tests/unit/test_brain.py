"""Tests for the Brain learning system."""

from __future__ import annotations

import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pytest

from app.services.brain.contracts import (
    CorrectionRecord,
    DiagnosticSignal,
    PredictionOutcome,
    StrategyMemory,
)
from app.services.brain.correction_planner import plan_corrections
from app.services.brain.report_interpreter import interpret_report
from app.services.brain.store import BrainStore


@pytest.fixture()
def brain_store() -> BrainStore:
    with tempfile.TemporaryDirectory() as td:
        store = BrainStore(Path(td) / "test_brain.sqlite")
        yield store
        store.close()


# -- Store Tests --------------------------------------------------------------


class TestBrainStore:
    def test_store_and_recall_correction(self, brain_store: BrainStore) -> None:
        record = CorrectionRecord(
            signal_type="overfit",
            action_type="weight_adjust",
            market="rebounds",
            params_before={"ewm_span10": 1.0},
            params_after={"ewm_span10": 0.85},
            ece_before=0.357,
            outcome="pending",
            confidence=0.7,
            created_at=datetime.now(UTC),
        )
        cid = brain_store.store_correction(record)
        assert cid > 0

        recalled = brain_store.recall_corrections(market="rebounds")
        assert len(recalled) == 1
        assert recalled[0].correction_id == cid
        assert recalled[0].market == "rebounds"
        assert recalled[0].ece_before == pytest.approx(0.357)

    def test_resolve_correction(self, brain_store: BrainStore) -> None:
        record = CorrectionRecord(
            signal_type="overfit",
            action_type="weight_adjust",
            market="points",
            ece_before=0.145,
            outcome="pending",
            created_at=datetime.now(UTC),
        )
        cid = brain_store.store_correction(record)
        brain_store.resolve_correction(cid, "improved", ece_after=0.09)

        recalled = brain_store.recall_corrections(market="points")
        assert recalled[0].outcome == "improved"
        assert recalled[0].ece_after == pytest.approx(0.09)

    def test_pending_corrections(self, brain_store: BrainStore) -> None:
        for i in range(3):
            brain_store.store_correction(
                CorrectionRecord(
                    signal_type="overfit",
                    action_type="weight_adjust",
                    market=f"market_{i}",
                    outcome="pending",
                    created_at=datetime.now(UTC),
                )
            )
        brain_store.resolve_correction(1, "improved")
        pending = brain_store.pending_corrections()
        assert len(pending) == 2

    def test_store_and_recall_outcome(self, brain_store: BrainStore) -> None:
        outcome = PredictionOutcome(
            prediction_id=42,
            player_name="LeBron James",
            market="points",
            line_value=25.5,
            predicted_probability=0.65,
            calibrated_probability=0.62,
            actual_value=28.0,
            hit=True,
            game_date="2026-04-10",
        )
        oid = brain_store.store_outcome(outcome)
        assert oid > 0

        recalled = brain_store.recall_outcomes(market="points")
        assert len(recalled) == 1
        assert recalled[0].player_name == "LeBron James"
        assert recalled[0].hit is True

    def test_upsert_strategy(self, brain_store: BrainStore) -> None:
        strategy = StrategyMemory(
            problem_type="overfit",
            action_template="weight_adjust",
            market="rebounds",
            parameters={"reduce_pct": 0.15},
            success_rate=0.75,
            avg_ece_improvement=0.04,
            sample_count=4,
        )
        sid = brain_store.upsert_strategy(strategy)
        assert sid > 0

        # Update same strategy
        updated = StrategyMemory(
            problem_type="overfit",
            action_template="weight_adjust",
            market="rebounds",
            parameters={"reduce_pct": 0.20},
            success_rate=0.80,
            avg_ece_improvement=0.05,
            sample_count=5,
        )
        sid2 = brain_store.upsert_strategy(updated)
        assert sid2 == sid

        recalled = brain_store.recall_strategies("overfit", "rebounds")
        assert len(recalled) == 1
        assert recalled[0].success_rate == pytest.approx(0.80)
        assert recalled[0].sample_count == 5

    def test_weight_overrides(self, brain_store: BrainStore) -> None:
        brain_store.set_weight_override("rebounds", "ewm_span10", 0.85, "test")
        brain_store.set_weight_override("rebounds", "season_avg", 0.90, "test")
        brain_store.set_weight_override("points", "ewm_span10", 0.70, "test")

        all_overrides = brain_store.get_weight_overrides()
        assert "rebounds" in all_overrides
        assert "points" in all_overrides
        assert all_overrides["rebounds"]["ewm_span10"] == pytest.approx(0.85)

        reb_only = brain_store.get_weight_overrides("rebounds")
        assert "points" not in reb_only

        brain_store.deactivate_weight_override("rebounds", "ewm_span10")
        reb_after = brain_store.get_weight_overrides("rebounds")
        assert "ewm_span10" not in reb_after.get("rebounds", {})

    def test_prune_weak_strategies(self, brain_store: BrainStore) -> None:
        # Weak strategy
        brain_store.upsert_strategy(
            StrategyMemory(
                problem_type="overfit",
                action_template="weight_adjust",
                market="turnovers",
                success_rate=0.20,
                sample_count=6,
            )
        )
        # Strong strategy
        brain_store.upsert_strategy(
            StrategyMemory(
                problem_type="overfit",
                action_template="calibration_patch",
                market="turnovers",
                success_rate=0.80,
                sample_count=10,
            )
        )
        pruned = brain_store.prune_weak_strategies(min_samples=5, max_success_rate=0.30)
        assert pruned == 1
        remaining = brain_store.recall_strategies("overfit", "turnovers")
        assert len(remaining) == 1
        assert remaining[0].action_template == "calibration_patch"

    def test_correction_stats(self, brain_store: BrainStore) -> None:
        for outcome in ["improved", "improved", "worsened", "pending"]:
            brain_store.store_correction(
                CorrectionRecord(
                    signal_type="overfit",
                    action_type="weight_adjust",
                    outcome=outcome,
                    created_at=datetime.now(UTC),
                )
            )
        stats = brain_store.correction_stats()
        assert stats["total"] == 4
        assert stats["improved"] == 2
        assert stats["worsened"] == 1
        assert stats["pending"] == 1
        assert stats["success_rate"] == pytest.approx(2 / 3)


# -- Report Interpreter Tests ------------------------------------------------


_SAMPLE_REPORT = """
# Daily Automation Report (2026-04-10)

## Data Quality Sentinel
- extreme_predictions_today (>97% or <3%): 586
- projection_line_divergences (>40% off line): 967
- sentinel_status: ALERT

### Overfit Signals
- points: score=1.00 (ECE=0.145 (high))
- rebounds: score=1.00 (ECE=0.357 (high))
- turnovers: score=1.00 (ECE=0.130 (high))
- threes: score=0.47 (ECE=0.056)
- pra: score=0.19 (ECE=0.023)
- assists: score=0.10 (ECE=0.012)

### prediction_validator
- action: `flag_unrealistic_prediction` | reason: [critical] LeBron James pra: probability 0.0041 is beyond 99/1%
- action: `flag_unrealistic_prediction` | reason: [critical] Kawhi Leonard pra: probability 0.0077 is beyond 99/1%
- action: `flag_unrealistic_prediction` | reason: [critical] Deni Avdija pra: probability 0.0086 is beyond 99/1%

## Release Recommendation
- rationale: Quality guardrail escalated recommendation: Average ECE 0.120 exceeds release ceiling 0.090.

## latest_training_data_quality
{'status': 'degraded', 'numeric_null_fraction': 0.056419, 'numeric_finite_ratio': 0.943581}

## latest_backtest
metrics={'summary_rows': []}
"""


class TestReportInterpreter:
    def test_extracts_extreme_probability_signal(self) -> None:
        signals = interpret_report(_SAMPLE_REPORT)
        extreme = [s for s in signals if s.signal_type == "extreme_probability"]
        assert len(extreme) == 1
        assert extreme[0].severity == "critical"
        assert extreme[0].metrics["extreme_count"] == 586

    def test_extracts_overfit_signals(self) -> None:
        signals = interpret_report(_SAMPLE_REPORT)
        overfit = [s for s in signals if s.signal_type == "overfit"]
        assert len(overfit) == 3  # points, rebounds, turnovers (score >= 0.75)
        markets = {s.market for s in overfit}
        assert markets == {"points", "rebounds", "turnovers"}

    def test_extracts_dnp_contamination(self) -> None:
        signals = interpret_report(_SAMPLE_REPORT)
        dnp = [s for s in signals if s.signal_type == "dnp_contamination"]
        assert len(dnp) >= 1
        # Should find flagged players
        all_players = set()
        for s in dnp:
            all_players.update(s.affected_players)
        assert "LeBron James" in all_players or "Kawhi Leonard" in all_players

    def test_extracts_empty_backtest(self) -> None:
        signals = interpret_report(_SAMPLE_REPORT)
        empty = [s for s in signals if s.signal_type == "empty_backtest"]
        assert len(empty) == 1

    def test_extracts_data_quality_degraded(self) -> None:
        signals = interpret_report(_SAMPLE_REPORT)
        dq = [s for s in signals if s.signal_type == "data_quality_degraded"]
        assert len(dq) == 1
        assert dq[0].metrics["null_fraction"] == pytest.approx(0.056419)

    def test_extracts_calibration_drift_from_release(self) -> None:
        signals = interpret_report(_SAMPLE_REPORT)
        drift = [s for s in signals if s.signal_type == "calibration_drift"]
        # Should have market-level (threes at 0.47) + release-level
        assert any(s.metrics.get("avg_ece") == pytest.approx(0.120) for s in drift)

    def test_extracts_projection_divergence(self) -> None:
        signals = interpret_report(_SAMPLE_REPORT)
        div = [s for s in signals if s.signal_type == "projection_divergence"]
        assert len(div) == 1
        assert div[0].metrics["divergence_count"] == 967

    def test_empty_report_returns_no_signals(self) -> None:
        signals = interpret_report("# Empty Report\nNothing here.")
        assert signals == []


# -- Correction Planner Tests ------------------------------------------------


class TestCorrectionPlanner:
    def test_plans_corrections_from_signals(self) -> None:
        from app.services.brain.brain import Brain

        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            brain = Brain(db_path=td_path / "plan_test.sqlite", vault_root=td_path / "vault")
            try:
                signals = [
                    DiagnosticSignal(
                        signal_type="overfit",
                        severity="critical",
                        market="rebounds",
                        metrics={"ece": 0.357},
                    ),
                    DiagnosticSignal(
                        signal_type="dnp_contamination",
                        severity="critical",
                        metrics={"extreme_count": 586},
                        affected_players=("Kawhi Leonard",),
                    ),
                ]
                plan = plan_corrections(signals, brain, dry_run=True)
                assert len(plan.corrections) >= 1
                assert plan.dry_run is True
                action_types = {c.action_type for c in plan.corrections}
                assert "dnp_filter" in action_types or "weight_adjust" in action_types
            finally:
                brain.close()

    def test_respects_max_corrections_cap(self) -> None:
        from app.services.brain.brain import Brain

        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            brain = Brain(db_path=td_path / "cap_test.sqlite", vault_root=td_path / "vault")
            try:
                signals = [
                    DiagnosticSignal(signal_type="overfit", severity="critical", market=f"market_{i}", metrics={"ece": 0.3})
                    for i in range(10)
                ]
                plan = plan_corrections(signals, brain, dry_run=True)
                assert len(plan.corrections) <= 3  # MAX_CORRECTIONS_PER_RUN
            finally:
                brain.close()

    def test_uses_proven_strategy_when_available(self) -> None:
        from app.services.brain.brain import Brain

        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            brain = Brain(db_path=td_path / "strat_test.sqlite", vault_root=td_path / "vault")
            try:
                # Seed a proven strategy
                brain._store.upsert_strategy(
                    StrategyMemory(
                        problem_type="overfit",
                        action_template="calibration_patch",
                        market="rebounds",
                        parameters={"method": "isotonic", "window_size": 200},
                        success_rate=0.80,
                        avg_ece_improvement=0.05,
                        sample_count=10,
                    )
                )
                signals = [
                    DiagnosticSignal(
                        signal_type="overfit",
                        severity="critical",
                        market="rebounds",
                        metrics={"ece": 0.30},
                    ),
                ]
                plan = plan_corrections(signals, brain, dry_run=True)
                assert len(plan.corrections) == 1
                correction = plan.corrections[0]
                assert correction.action_type == "calibration_patch"
                assert correction.strategy_source is not None
                assert correction.confidence == pytest.approx(0.80)
            finally:
                brain.close()

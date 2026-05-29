from __future__ import annotations

from app.services.model_quality.release_compare import _compare_model_metric_payloads


def test_missing_ece_returns_hold() -> None:
    result = _compare_model_metric_payloads(
        candidate_run_id=10,
        champion_run_id=9,
        candidate_metrics={},
        champion_metrics={"calibration_diagnostics": {"points": {"ece": 0.05}}},
    )
    assert result.status == "HOLD"
    assert "missing" in result.rationale.lower()


def test_both_missing_ece_returns_hold() -> None:
    result = _compare_model_metric_payloads(
        candidate_run_id=10,
        champion_run_id=9,
        candidate_metrics={},
        champion_metrics={},
    )
    assert result.status == "HOLD"
    assert "missing" in result.rationale.lower()


def test_ece_ceiling_breach_returns_hold() -> None:
    candidate_metrics = {
        "calibration_diagnostics": {"points": {"ece": 0.11}},
        "training_data_quality": {"status": "healthy"},
    }
    champion_metrics = {
        "calibration_diagnostics": {"points": {"ece": 0.09}},
        "training_data_quality": {"status": "healthy"},
    }
    result = _compare_model_metric_payloads(
        candidate_run_id=10,
        champion_run_id=9,
        candidate_metrics=candidate_metrics,
        champion_metrics=champion_metrics,
        ece_ceiling=0.10,
    )
    assert result.status == "HOLD"
    assert "ceiling" in result.rationale.lower()


def test_worse_ece_returns_hold() -> None:
    candidate_metrics = {
        "calibration_diagnostics": {"points": {"ece": 0.15}},
        "training_data_quality": {"status": "healthy"},
    }
    champion_metrics = {
        "calibration_diagnostics": {"points": {"ece": 0.10}},
        "training_data_quality": {"status": "healthy"},
    }
    result = _compare_model_metric_payloads(
        candidate_run_id=10,
        champion_run_id=9,
        candidate_metrics=candidate_metrics,
        champion_metrics=champion_metrics,
        ece_ceiling=0.10,
    )
    assert result.status == "HOLD"


def test_compare_model_metric_payloads_promotes_better_candidate() -> None:
    candidate_metrics = {
        "calibration_diagnostics": {"points": {"ece": 0.03}, "rebounds": {"ece": 0.02}},
        "training_data_quality": {"status": "healthy"},
    }
    champion_metrics = {
        "calibration_diagnostics": {"points": {"ece": 0.05}, "rebounds": {"ece": 0.04}},
        "training_data_quality": {"status": "healthy"},
    }
    result = _compare_model_metric_payloads(
        candidate_run_id=10,
        champion_run_id=9,
        candidate_metrics=candidate_metrics,
        champion_metrics=champion_metrics,
    )
    assert result.status == "PROMOTE_CANDIDATE"


def test_compare_model_metric_payloads_holds_degraded_candidate() -> None:
    candidate_metrics = {
        "calibration_diagnostics": {"points": {"ece": 0.01}},
        "training_data_quality": {"status": "degraded"},
    }
    champion_metrics = {
        "calibration_diagnostics": {"points": {"ece": 0.03}},
        "training_data_quality": {"status": "healthy"},
    }
    result = _compare_model_metric_payloads(
        candidate_run_id=10,
        champion_run_id=9,
        candidate_metrics=candidate_metrics,
        champion_metrics=champion_metrics,
    )
    assert result.status == "HOLD"

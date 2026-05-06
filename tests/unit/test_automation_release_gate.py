from __future__ import annotations

from app.services.automation import _build_quality_guardrail_status, _build_release_recommendation


def test_release_recommendation_blocked_on_tier_c() -> None:
    result = _build_release_recommendation(api_tier="C", prediction_count=100, model_run_count=1)
    assert result["status"] == "BLOCKED"


def test_release_recommendation_caution_on_tier_b() -> None:
    result = _build_release_recommendation(api_tier="B", prediction_count=100, model_run_count=1)
    assert result["status"] == "CAUTION"


def test_release_recommendation_hold_when_missing_outputs() -> None:
    result = _build_release_recommendation(api_tier="A", prediction_count=0, model_run_count=1)
    assert result["status"] == "HOLD"


def test_release_recommendation_go_on_healthy_state() -> None:
    result = _build_release_recommendation(api_tier="A", prediction_count=10, model_run_count=1)
    assert result["status"] == "GO"


def test_release_recommendation_escalates_on_quality_guardrail() -> None:
    result = _build_release_recommendation(
        api_tier="A",
        prediction_count=10,
        model_run_count=1,
        quality_guardrail={"status": "HOLD", "summary": "guardrail test"},
    )
    assert result["status"] == "HOLD"
    assert "guardrail" in result["rationale"].lower()


def test_release_recommendation_escalates_even_when_base_is_caution() -> None:
    result = _build_release_recommendation(
        api_tier="B",
        prediction_count=10,
        model_run_count=1,
        quality_guardrail={"status": "BLOCKED", "summary": "critical calibration breach"},
    )
    assert result["status"] == "BLOCKED"


def test_quality_guardrail_blocks_on_high_watch_market_ece() -> None:
    status = _build_quality_guardrail_status(
        latest_model_metrics={
            "calibration_diagnostics": {
                "points": {"ece": 0.05},
                "turnovers": {"ece": 0.12},
            }
        },
        latest_backtest_metrics={},
    )
    assert status["status"] == "BLOCKED"

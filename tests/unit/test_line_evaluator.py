from __future__ import annotations

from app.evaluation.line_evaluator import evaluate_projection_line


def test_line_evaluation_changes_probability_not_projection() -> None:
    low_line = evaluate_projection_line(
        projected_mean=20.0,
        projected_variance=16.0,
        line_value=14.5,
        market_key="points",
        over_odds=-110,
        under_odds=-110,
        simulations=1500,
    )
    high_line = evaluate_projection_line(
        projected_mean=20.0,
        projected_variance=16.0,
        line_value=24.5,
        market_key="points",
        over_odds=-110,
        under_odds=-110,
        simulations=1500,
    )

    assert low_line.summary.mean == high_line.summary.mean
    assert low_line.summary.variance == high_line.summary.variance
    assert low_line.over_probability > high_line.over_probability
    assert low_line.hit_probability != high_line.hit_probability


def test_line_evaluation_prices_coarse_kalshi_threshold_from_projection() -> None:
    result = evaluate_projection_line(
        projected_mean=24.0,
        projected_variance=25.0,
        line_value=19.5,
        threshold_value=20.0,
        market_key="points",
        simulations=1500,
    )

    assert result.threshold_value == 20.0
    assert result.over_probability > 0.5
    assert result.fair_over_price == result.over_probability


def test_line_evaluation_does_not_reuse_summary_for_a_different_line() -> None:
    low_line = evaluate_projection_line(
        projected_mean=20.0,
        projected_variance=16.0,
        line_value=14.5,
        market_key="points",
        simulations=1500,
    )
    high_line = evaluate_projection_line(
        projected_mean=20.0,
        projected_variance=16.0,
        line_value=24.5,
        market_key="points",
        simulations=1500,
    )
    mismatched_summary = evaluate_projection_line(
        projected_mean=20.0,
        projected_variance=16.0,
        line_value=24.5,
        market_key="points",
        summary=low_line.summary,
        summary_line_value=14.5,
        simulations=1500,
    )

    assert mismatched_summary.over_probability == high_line.over_probability
    assert mismatched_summary.over_probability != low_line.over_probability

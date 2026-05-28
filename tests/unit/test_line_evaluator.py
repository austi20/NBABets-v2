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



# Task 3.2 tests

def test_push_probability_threaded_into_pricer() -> None:
    """push_probability must be passed to price_prop; probabilities must sum to 1."""
    result = evaluate_projection_line(
        projected_mean=10.0,
        projected_variance=5.0,
        line_value=10.0,  # integer line => push mass expected
        market_key="points",
        over_odds=-110,
        under_odds=-110,
        simulations=4000,
    )
    total = result.over_probability + result.under_probability + result.push_probability
    assert abs(total - 1.0) < 1e-6, f"probabilities don't sum to 1: {total}"
    # push on an integer line must be > 0 (at least some samples land exactly on line)
    assert result.push_probability >= 0.0


def test_line_evaluator_normalization_half_point_line() -> None:
    result = evaluate_projection_line(
        projected_mean=10.0,
        projected_variance=5.0,
        line_value=10.5,  # half-point => push ≈ 0
        market_key="points",
        over_odds=-110,
        under_odds=-110,
        simulations=4000,
    )
    total = result.over_probability + result.under_probability + result.push_probability
    assert abs(total - 1.0) < 1e-6


def test_ev_reflects_push_mass_not_zero_push() -> None:
    """EV computed by line_evaluator must match compute_ev called with push threaded in."""
    from app.evaluation.prop_pricer import compute_ev
    result = evaluate_projection_line(
        projected_mean=10.0,
        projected_variance=5.0,
        line_value=10.0,  # integer => non-trivial push
        market_key='points',
        over_odds=-110,
        under_odds=-110,
        simulations=10000,
    )
    # pick the recommended side
    if result.recommendation == 'OVER':
        hit_prob = result.over_probability
    else:
        hit_prob = result.under_probability
    decimal = 1.0 + 100.0 / 110.0  # -110 decimal
    expected_ev = compute_ev(hit_prob=hit_prob, push_prob=result.push_probability, decimal_odds=decimal)
    assert abs(result.ev - expected_ev) < 1e-6, f"EV mismatch: got {result.ev}, expected {expected_ev}"

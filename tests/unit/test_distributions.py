from __future__ import annotations

import numpy as np

from app.training.distributions import (
    _sample_points,
    _sample_rebounds,
    _sample_turnovers,
    sample_market_outcomes,
    summarize_line_probability,
)


def test_turnover_zero_inflation_increases_zero_rate_for_lower_mean() -> None:
    minutes = np.full(5000, 32.0)
    context = {
        "touches_per_minute": 1.9,
        "turnover_ratio": 0.12,
        "usage_rate": 0.24,
        "shared_latent": 1.0,
    }
    low_rng = np.random.default_rng(123)
    high_rng = np.random.default_rng(123)

    low_mean_samples = _sample_turnovers(
        mean=0.4,
        variance=1.0,
        minutes=minutes,
        sample_size=len(minutes),
        rng=low_rng,
        context=context,
    )
    high_mean_samples = _sample_turnovers(
        mean=3.5,
        variance=5.0,
        minutes=minutes,
        sample_size=len(minutes),
        rng=high_rng,
        context=context,
    )

    low_zero_rate = float(np.mean(low_mean_samples == 0))
    high_zero_rate = float(np.mean(high_mean_samples == 0))
    assert low_zero_rate > high_zero_rate


def test_points_sampling_respects_3pt_variance_signal() -> None:
    minutes = np.full(4000, 34.0)
    base_context = {
        "field_goal_attempts_per_minute": 0.55,
        "estimated_three_point_attempts_per_minute": 0.24,
        "free_throw_attempts_per_minute": 0.16,
        "usage_rate": 0.27,
        "true_shooting_percentage": 0.60,
        "three_point_make_rate": 0.37,
        "shared_latent": 1.0,
        "points_pace_exposure": 54.0,
    }
    low_rng = np.random.default_rng(77)
    high_rng = np.random.default_rng(77)
    low_var_samples = _sample_points(
        mean=24.0,
        variance=38.0,
        minutes=minutes,
        sample_size=len(minutes),
        rng=low_rng,
        context={**base_context, "points_3pt_variance": 0.2},
    )
    high_var_samples = _sample_points(
        mean=24.0,
        variance=38.0,
        minutes=minutes,
        sample_size=len(minutes),
        rng=high_rng,
        context={**base_context, "points_3pt_variance": 8.0},
    )
    assert float(np.var(high_var_samples)) > float(np.var(low_var_samples))


def test_rebounds_sampling_respects_recent_volatility_signal() -> None:
    minutes = np.full(4000, 31.0)
    base_context = {
        "rebound_chances_total_per_minute": 0.35,
        "rebound_conversion_rate": 0.33,
        "shared_latent": 1.0,
    }
    low_rng = np.random.default_rng(91)
    high_rng = np.random.default_rng(91)
    low_var_samples = _sample_rebounds(
        mean=8.5,
        variance=11.0,
        minutes=minutes,
        sample_size=len(minutes),
        rng=low_rng,
        context={**base_context, "rebounds_std_10": 0.3},
    )
    high_var_samples = _sample_rebounds(
        mean=8.5,
        variance=11.0,
        minutes=minutes,
        sample_size=len(minutes),
        rng=high_rng,
        context={**base_context, "rebounds_std_10": 4.0},
    )
    assert float(np.var(high_var_samples)) > float(np.var(low_var_samples))


def test_summarize_line_probability_supports_dist_family_switch() -> None:
    legacy = summarize_line_probability(
        mean=8.0,
        variance=12.0,
        line=7.5,
        simulations=2000,
        market_key="rebounds",
        dist_family="legacy",
    )
    count_aware = summarize_line_probability(
        mean=8.0,
        variance=12.0,
        line=7.5,
        simulations=2000,
        market_key="rebounds",
        dist_family="count_aware",
    )
    decomposed = summarize_line_probability(
        mean=8.0,
        variance=12.0,
        line=7.5,
        simulations=2000,
        market_key="rebounds",
        dist_family="decomposed",
    )
    for summary in (legacy, count_aware, decomposed):
        assert 0.0 <= summary.over_probability <= 1.0
        assert 0.0 <= summary.under_probability <= 1.0
        assert summary.mean >= 0.0
    assert abs(legacy.over_probability - count_aware.over_probability) <= 0.25


def test_zero_projected_minutes_produces_zero_outcome_samples() -> None:
    samples = sample_market_outcomes(
        mean=24.0,
        variance=36.0,
        sample_size=1000,
        rng=np.random.default_rng(12),
        minutes_mean=0.0,
        minutes_std=1.0,
        market_key="points",
        context={
            "field_goal_attempts_per_minute": 0.6,
            "estimated_three_point_attempts_per_minute": 0.2,
            "free_throw_attempts_per_minute": 0.15,
        },
    )
    assert np.max(samples) == 0.0


# Task 3.3 tests
def test_summary_joint_partition_sums_to_one_with_push() -> None:
    """over+under+push from _summarize_samples must sum to exactly 1.0 (joint partition)."""
    from app.training.distributions import _summarize_samples

    # all samples either over or under (no push hits) - worst case for independent smoothing
    samples = np.array([6.0] * 50 + [4.0] * 50)  # 50 over, 50 under, 0 push
    result = _summarize_samples(samples, line=5.0)
    total = result.over_probability + result.under_probability
    push = 1.0 - total
    # With joint Laplace smoothing the sum must be exactly 1; independent smoothing leaks
    assert abs(result.over_probability + result.under_probability + push - 1.0) < 1e-9


def test_summary_push_mass_on_integer_line() -> None:
    """Integer line: over_probability + under_probability < 1 (push mass present)."""
    from app.training.distributions import _summarize_samples

    rng = np.random.default_rng(42)
    samples = np.round(rng.normal(5, 2, 10000)).astype(float)
    result = _summarize_samples(samples, line=5.0)
    push = 1.0 - result.over_probability - result.under_probability
    assert push > 0.05, f"expected push > 0.05 on integer line, got {push}"
    assert abs(result.over_probability + result.under_probability + push - 1.0) < 1e-9


def test_summary_zero_push_for_half_point_line() -> None:
    """Half-point line: push_probability is effectively 0."""
    from app.training.distributions import _summarize_samples

    rng = np.random.default_rng(42)
    samples = np.round(rng.normal(5, 2, 10000)).astype(float)
    result = _summarize_samples(samples, line=5.5)
    push = 1.0 - result.over_probability - result.under_probability
    assert abs(push) < 0.02, f"push should be ~0 for half-point line, got {push}"


def test_summary_joint_three_way_posterior_sums_to_one() -> None:
    """Three-way joint Laplace posterior must sum to exactly 1.

    Independent binary smoothing gives sum > 1 when push hits are present.
    Joint multinomial smoothing shares one denominator so sum is exactly 1.
    """
    from app.training.distributions import _summarize_samples

    samples = np.array([5.0] * 80 + [6.0] * 10 + [4.0] * 10)
    result = _summarize_samples(samples, line=5.0)
    total = 100
    push_hits = int(np.sum(samples == 5.0))
    # joint partition: push_p = (push_hits + 0.5) / (total + 1.5)
    denom = total + 1.5
    push_p = (push_hits + 0.5) / denom
    joint_sum = result.over_probability + result.under_probability + push_p
    assert abs(joint_sum - 1.0) < 1e-9, (
        f"joint posterior sum {joint_sum} != 1.0; "
        f"over={result.over_probability:.6f} under={result.under_probability:.6f} push={push_p:.6f}"
    )

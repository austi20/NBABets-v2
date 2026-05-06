from __future__ import annotations

import numpy as np

from app.training.distributions import _sample_points, _sample_rebounds, _sample_turnovers, summarize_line_probability


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

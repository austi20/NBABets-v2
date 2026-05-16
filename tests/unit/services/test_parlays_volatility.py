"""Tests: parlay EV uses volatility-adjusted probabilities when available."""
from __future__ import annotations

import math

from app.services.volatility import (
    FeatureSnapshot,
    compute_volatility,
)


def _high_vol_snap() -> FeatureSnapshot:
    return FeatureSnapshot(
        stat_std_10=2.5, stat_mean_10=2.0,
        predicted_minutes_std=6.0,
        minutes_std_10=8.0, minutes_mean_10=12.0,
        usage_std_10=0.10, usage_mean_10=0.15,
        mean_5=0.8, mean_season=2.0, std_season=1.5,
        starter_flag_rate=0.0, minutes_mean_season=8.0,
    )


def _low_vol_snap() -> FeatureSnapshot:
    return FeatureSnapshot(
        stat_std_10=2.0, stat_mean_10=22.0,
        predicted_minutes_std=1.0,
        minutes_std_10=2.5, minutes_mean_10=34.0,
        usage_std_10=0.015, usage_mean_10=0.28,
        mean_5=22.0, mean_season=22.0, std_season=4.0,
        starter_flag_rate=1.0, minutes_mean_season=33.0,
    )


def test_volatility_lowers_joint_probability() -> None:
    """The adjusted-prob product is lower than the raw-prob product when volatility is non-zero."""
    high = compute_volatility(raw_probability=0.65, features=_high_vol_snap())
    low = compute_volatility(raw_probability=0.65, features=_low_vol_snap())

    raw_combined = 0.65 * 0.65
    adjusted_combined = high.adjusted_probability * low.adjusted_probability

    assert adjusted_combined < raw_combined
    assert adjusted_combined > 0.0


def test_effective_hit_probability_prefers_adjusted() -> None:
    """The parlays helper uses adjusted_over_probability when present, falls back to quote.hit_probability."""
    from types import SimpleNamespace

    from app.services.parlays import _effective_hit_probability

    candidate_with_adjusted = SimpleNamespace(
        opportunity=SimpleNamespace(adjusted_over_probability=0.55),
        quote=SimpleNamespace(hit_probability=0.65),
    )
    assert math.isclose(_effective_hit_probability(candidate_with_adjusted), 0.55)

    candidate_no_adjusted = SimpleNamespace(
        opportunity=SimpleNamespace(adjusted_over_probability=None),
        quote=SimpleNamespace(hit_probability=0.65),
    )
    assert math.isclose(_effective_hit_probability(candidate_no_adjusted), 0.65)

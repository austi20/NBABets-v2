from __future__ import annotations

from app.services.volatility import (
    FeatureSnapshot,
    compute_volatility,
)


def test_high_volatility_drops_confidence_significantly() -> None:
    """A bench-player threes prop should drop hard once volatility hits."""
    snap = FeatureSnapshot(
        stat_std_10=2.5, stat_mean_10=2.0,
        predicted_minutes_std=6.0,
        minutes_std_10=8.0, minutes_mean_10=12.0,
        usage_std_10=0.10, usage_mean_10=0.15,
        mean_5=0.8, mean_season=2.0, std_season=1.5,
        starter_flag_rate=0.0, minutes_mean_season=8.0,
    )
    score = compute_volatility(raw_probability=0.78, features=snap)
    assert score.tier == "high"
    assert score.confidence_multiplier < 0.65
    assert score.adjusted_probability < 0.74


def test_low_volatility_preserves_confidence() -> None:
    """A stable starter prop should barely move."""
    snap = FeatureSnapshot(
        stat_std_10=2.0, stat_mean_10=22.0,
        predicted_minutes_std=1.0,
        minutes_std_10=2.5, minutes_mean_10=34.0,
        usage_std_10=0.015, usage_mean_10=0.28,
        mean_5=22.5, mean_season=22.0, std_season=4.0,
        starter_flag_rate=1.0, minutes_mean_season=33.0,
    )
    score = compute_volatility(raw_probability=0.78, features=snap)
    assert score.tier == "low"
    assert score.confidence_multiplier > 0.90
    assert score.adjusted_probability > 0.76

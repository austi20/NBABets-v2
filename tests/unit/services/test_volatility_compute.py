from __future__ import annotations

import math

from app.services.volatility import (
    FeatureSnapshot,
    compute_volatility,
)


def _full_snapshot(**overrides: float) -> FeatureSnapshot:
    base = dict(
        stat_std_10=0.0,
        stat_mean_10=20.0,
        predicted_minutes_std=0.0,
        minutes_std_10=0.0,
        minutes_mean_10=32.0,
        usage_std_10=0.0,
        usage_mean_10=0.25,
        mean_5=20.0,
        mean_season=20.0,
        std_season=5.0,
        starter_flag_rate=1.0,
        minutes_mean_season=30.0,
    )
    base.update(overrides)
    return FeatureSnapshot(**base)


def test_all_zero_inputs_yields_zero_coefficient() -> None:
    snap = _full_snapshot()
    score = compute_volatility(raw_probability=0.80, features=snap)
    assert math.isclose(score.coefficient, 0.0, abs_tol=1e-6)
    assert score.tier == "low"
    assert math.isclose(score.adjusted_probability, 0.80, abs_tol=1e-6)
    assert math.isclose(score.confidence_multiplier, 1.0, abs_tol=1e-6)
    assert score.reason == ""


def test_all_max_inputs_yields_one_coefficient() -> None:
    snap = _full_snapshot(
        stat_std_10=40.0,        # cv = 2.0 -> clip to max
        stat_mean_10=20.0,
        predicted_minutes_std=20.0,
        minutes_std_10=40.0,
        minutes_mean_10=20.0,
        usage_std_10=0.5,
        usage_mean_10=0.25,
        mean_5=50.0,             # huge divergence
        mean_season=20.0,
        std_season=5.0,
        starter_flag_rate=0.0,
        minutes_mean_season=0.0, # fringe
    )
    score = compute_volatility(raw_probability=0.80, features=snap)
    assert math.isclose(score.coefficient, 1.0, abs_tol=1e-6)
    assert score.tier == "high"
    assert math.isclose(score.adjusted_probability, 0.71, abs_tol=1e-3)
    assert math.isclose(score.confidence_multiplier, 0.45, abs_tol=1e-3)


def test_contributors_sum_matches_coefficient() -> None:
    snap = _full_snapshot(
        stat_std_10=5.0, stat_mean_10=20.0,    # cv 0.25 -> 0.1667 normalized -> contribution 0.30*0.1667=0.05
        predicted_minutes_std=4.0,
        minutes_std_10=9.0, minutes_mean_10=30.0,
        usage_std_10=0.055, usage_mean_10=0.22,
        mean_5=25.0, mean_season=20.0, std_season=5.0,
        starter_flag_rate=1.0, minutes_mean_season=30.0,
    )
    score = compute_volatility(raw_probability=0.65, features=snap)
    total = sum(c.contribution for c in score.contributors)
    assert math.isclose(total, score.coefficient, abs_tol=1e-6)


def test_missing_input_renormalizes_weights() -> None:
    # Drop stat_cv (weight 0.30). Remaining weights = 0.70; should renormalize.
    snap = FeatureSnapshot(
        stat_std_10=None,
        stat_mean_10=None,
        predicted_minutes_std=8.0,        # full
        minutes_std_10=18.0,
        minutes_mean_10=30.0,             # full
        usage_std_10=0.11, usage_mean_10=0.22,  # full
        mean_5=30.0, mean_season=20.0, std_season=5.0,  # full
        starter_flag_rate=0.0, minutes_mean_season=0.0, # fringe -> full
    )
    score = compute_volatility(raw_probability=0.80, features=snap)
    # All four remaining inputs at max -> renormalized weights sum to 1.0 -> coefficient = 1.0
    assert math.isclose(score.coefficient, 1.0, abs_tol=1e-6)
    contributor_names = {c.name for c in score.contributors}
    assert "stat_cv" not in contributor_names
    assert len(contributor_names) == 4


def test_all_inputs_missing_returns_neutral_score() -> None:
    snap = FeatureSnapshot(
        stat_std_10=None, stat_mean_10=None,
        predicted_minutes_std=None, minutes_std_10=None, minutes_mean_10=None,
        usage_std_10=None, usage_mean_10=None,
        mean_5=None, mean_season=None, std_season=None,
        starter_flag_rate=None, minutes_mean_season=None,
    )
    score = compute_volatility(raw_probability=0.80, features=snap)
    assert math.isclose(score.coefficient, 0.5, abs_tol=1e-6)
    assert score.tier == "medium"
    assert score.contributors == ()
    assert score.reason == "insufficient_features"
    assert math.isclose(score.adjusted_probability, 0.5 + 0.30 * (1 - 0.5 * 0.30), abs_tol=1e-3)


def test_partial_missing_uses_remaining_inputs() -> None:
    # Only archetype data present (weight 0.15).
    snap = FeatureSnapshot(
        stat_std_10=None, stat_mean_10=None,
        predicted_minutes_std=None, minutes_std_10=None, minutes_mean_10=None,
        usage_std_10=None, usage_mean_10=None,
        mean_5=None, mean_season=None, std_season=None,
        starter_flag_rate=0.0, minutes_mean_season=0.0,
    )
    score = compute_volatility(raw_probability=0.80, features=snap)
    # Only fringe archetype contributes -> renormalized weight 1.0 -> coefficient 1.0
    assert math.isclose(score.coefficient, 1.0, abs_tol=1e-6)
    assert score.reason == ""


def test_minutes_instability_falls_back_to_cv_when_predicted_std_missing() -> None:
    """When predicted_minutes_std is None but observed minutes data is present,
    minutes_instability must still contribute (CV-only fallback per spec notes).
    """
    snap = _full_snapshot(
        predicted_minutes_std=None,  # not available outside the training pipeline
        minutes_std_10=18.0,         # cv = 18/30 = 0.6 -> normalized to 1.0
        minutes_mean_10=30.0,
    )
    score = compute_volatility(raw_probability=0.65, features=snap)
    names = {c.name for c in score.contributors}
    assert "minutes_instability" in names, "minutes must not be dropped when only predicted_std is None"
    assert len(score.contributors) == 5
    minutes_contrib = next(c for c in score.contributors if c.name == "minutes_instability")
    # pred_component = 0 (None coerced to 0), cv_component = clip(0.6/0.6) = 1.0,
    # so normalize_minutes_instability returns (0 + 1.0)/2 = 0.5.
    assert math.isclose(minutes_contrib.raw_value, 0.5, abs_tol=1e-6)

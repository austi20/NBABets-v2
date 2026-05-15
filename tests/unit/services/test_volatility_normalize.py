from __future__ import annotations

import math

import pytest

from app.services.volatility import (
    DEFAULT_CONFIG,
    normalize_minutes_instability,
    normalize_recent_form_divergence,
    normalize_stat_cv,
    normalize_usage_instability,
)


@pytest.mark.parametrize(
    ("std", "mean", "expected"),
    [
        (0.0, 5.0, 0.0),
        (3.0, 5.0, min(0.6 / 1.5, 1.0)),  # cv=0.6 -> 0.4
        (7.5, 5.0, 1.0),                  # cv=1.5 -> 1.0
        (15.0, 5.0, 1.0),                 # cv=3.0 -> clipped to 1
        (5.0, 0.0, 1.0),                  # mean below eps -> max
    ],
)
def test_normalize_stat_cv(std: float, mean: float, expected: float) -> None:
    assert math.isclose(normalize_stat_cv(std, mean), expected, abs_tol=1e-6)


@pytest.mark.parametrize(
    ("predicted_std", "mean_10", "std_10", "expected"),
    [
        (0.0, 30.0, 0.0, 0.0),
        (4.0, 30.0, 9.0, 0.5),    # 0.5 + 0.5  -> avg = 0.5
        (8.0, 30.0, 18.0, 1.0),   # both at max -> 1.0
        (16.0, 30.0, 36.0, 1.0),  # over max -> clipped 1.0
    ],
)
def test_normalize_minutes_instability(
    predicted_std: float, mean_10: float, std_10: float, expected: float
) -> None:
    result = normalize_minutes_instability(
        predicted_std=predicted_std,
        minutes_std_10=std_10,
        minutes_mean_10=mean_10,
    )
    assert math.isclose(result, expected, abs_tol=1e-6)


@pytest.mark.parametrize(
    ("std", "mean", "expected"),
    [
        (0.0, 0.22, 0.0),
        (0.055, 0.22, 0.5),  # cv = 0.25 -> 0.5
        (0.11, 0.22, 1.0),   # cv = 0.5 -> 1.0
        (0.20, 0.22, 1.0),   # over max -> 1.0
        (0.05, 0.0, 1.0),    # mean below eps -> max
    ],
)
def test_normalize_usage_instability(std: float, mean: float, expected: float) -> None:
    assert math.isclose(normalize_usage_instability(std, mean), expected, abs_tol=1e-6)


@pytest.mark.parametrize(
    ("m5", "m_season", "std_season", "expected"),
    [
        (20.0, 20.0, 5.0, 0.0),
        (25.0, 20.0, 5.0, 0.5),   # z = 1.0 -> 0.5
        (30.0, 20.0, 5.0, 1.0),   # z = 2.0 -> 1.0
        (40.0, 20.0, 5.0, 1.0),   # over max -> clipped
        (10.0, 20.0, 5.0, 1.0),   # z=2 (absolute) -> 1.0
        (20.0, 20.0, 0.0, 0.0),   # zero std + zero divergence -> 0
    ],
)
def test_normalize_recent_form_divergence(
    m5: float, m_season: float, std_season: float, expected: float
) -> None:
    result = normalize_recent_form_divergence(
        mean_5=m5, mean_season=m_season, std_season=std_season
    )
    assert math.isclose(result, expected, abs_tol=1e-6)


def test_normalizers_respect_config_constants() -> None:
    # Using a custom config with halved caps should double the normalized score
    from app.services.volatility import VolatilityConfig

    tight = VolatilityConfig(
        weights=dict(DEFAULT_CONFIG.weights),
        prob_alpha=DEFAULT_CONFIG.prob_alpha,
        conf_alpha=DEFAULT_CONFIG.conf_alpha,
        tier_low_cap=DEFAULT_CONFIG.tier_low_cap,
        tier_high_cap=DEFAULT_CONFIG.tier_high_cap,
        stat_cv_max=0.75,
    )
    assert math.isclose(normalize_stat_cv(3.0, 5.0, config=tight), 0.8, abs_tol=1e-6)

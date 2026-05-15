from __future__ import annotations

import math

import pytest
from app.services.volatility import (
    DEFAULT_CONFIG,
    VolatilityConfig,
    VolatilityContributor,
    VolatilityScore,
    adjust_probability,
    confidence_multiplier,
    tier_from_coefficient,
)


def test_default_config_weights_sum_to_one() -> None:
    total = sum(DEFAULT_CONFIG.weights.values())
    assert math.isclose(total, 1.0, abs_tol=1e-9)


def test_config_rejects_invalid_weight_sum() -> None:
    with pytest.raises(ValueError, match="must sum to 1"):
        VolatilityConfig(
            weights={"stat_cv": 0.5, "minutes_instability": 0.2},
            prob_alpha=0.30,
            conf_alpha=0.55,
            tier_low_cap=0.33,
            tier_high_cap=0.66,
        )


@pytest.mark.parametrize(
    ("coefficient", "expected"),
    [
        (0.0, "low"),
        (0.10, "low"),
        (0.32, "low"),
        (0.33, "medium"),
        (0.50, "medium"),
        (0.65, "medium"),
        (0.66, "high"),
        (0.90, "high"),
        (1.0, "high"),
    ],
)
def test_tier_from_coefficient(coefficient: float, expected: str) -> None:
    assert tier_from_coefficient(coefficient) == expected


@pytest.mark.parametrize(
    ("raw_p", "coefficient", "expected"),
    [
        (0.90, 1.0, 0.78),
        (0.80, 1.0, 0.71),
        (0.70, 1.0, 0.64),
        (0.60, 1.0, 0.57),
        (0.55, 1.0, 0.535),
        (0.50, 1.0, 0.50),
        (0.20, 1.0, 0.29),
        (0.80, 0.0, 0.80),
        (0.80, 0.5, 0.755),
    ],
)
def test_adjust_probability_table(raw_p: float, coefficient: float, expected: float) -> None:
    result = adjust_probability(raw_p, coefficient)
    assert math.isclose(result, expected, abs_tol=1e-3)


def test_adjust_probability_preserves_side() -> None:
    for raw_p in [0.51, 0.55, 0.60, 0.80, 0.95]:
        assert adjust_probability(raw_p, 1.0) >= 0.5
    for raw_p in [0.49, 0.45, 0.40, 0.20, 0.05]:
        assert adjust_probability(raw_p, 1.0) <= 0.5


@pytest.mark.parametrize(
    ("coefficient", "expected"),
    [
        (0.0, 1.0),
        (0.5, 0.725),
        (1.0, 0.45),
    ],
)
def test_confidence_multiplier(coefficient: float, expected: float) -> None:
    result = confidence_multiplier(coefficient)
    assert math.isclose(result, expected, abs_tol=1e-3)


def test_score_dataclass_is_frozen() -> None:
    score = VolatilityScore(
        coefficient=0.5,
        tier="medium",
        contributors=(VolatilityContributor(name="x", raw_value=1.0, weight=0.5, contribution=0.5),),
        adjusted_probability=0.6,
        confidence_multiplier=0.725,
    )
    with pytest.raises(AttributeError):
        score.coefficient = 0.7  # type: ignore[misc]

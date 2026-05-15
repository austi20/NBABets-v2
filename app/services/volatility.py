"""Per-prop volatility coefficient + tier.

See docs/superpowers/specs/2026-05-15-volatility-tier-design.md for the full
specification. This module is intended to be a pure-function dependency:
callers assemble a FeatureSnapshot and pass it in along with the prediction
row, and the module returns a VolatilityScore.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Literal

Tier = Literal["low", "medium", "high"]


@dataclass(frozen=True)
class VolatilityContributor:
    name: str
    raw_value: float
    weight: float
    contribution: float


@dataclass(frozen=True)
class VolatilityScore:
    coefficient: float
    tier: Tier
    contributors: tuple[VolatilityContributor, ...]
    adjusted_probability: float
    confidence_multiplier: float
    reason: str = ""


@dataclass(frozen=True)
class VolatilityConfig:
    weights: dict[str, float]
    prob_alpha: float
    conf_alpha: float
    tier_low_cap: float
    tier_high_cap: float
    stat_cv_max: float = 1.5
    minutes_std_max: float = 8.0
    minutes_cv_max: float = 0.6
    usage_cv_max: float = 0.5
    recent_form_z_max: float = 2.0

    def __post_init__(self) -> None:
        total = sum(self.weights.values())
        if not math.isclose(total, 1.0, abs_tol=1e-6):
            raise ValueError(
                f"VolatilityConfig.weights must sum to 1.0, got {total:.4f}"
            )


DEFAULT_CONFIG = VolatilityConfig(
    weights={
        "stat_cv": 0.30,
        "minutes_instability": 0.20,
        "usage_instability": 0.15,
        "recent_form_divergence": 0.20,
        "archetype_risk": 0.15,
    },
    prob_alpha=0.30,
    conf_alpha=0.55,
    tier_low_cap=0.33,
    tier_high_cap=0.66,
)


def tier_from_coefficient(coefficient: float, config: VolatilityConfig = DEFAULT_CONFIG) -> Tier:
    if coefficient < config.tier_low_cap:
        return "low"
    if coefficient < config.tier_high_cap:
        return "medium"
    return "high"


def adjust_probability(raw_p: float, coefficient: float, config: VolatilityConfig = DEFAULT_CONFIG) -> float:
    """Gentle, edge-preserving shrinkage. Never crosses 0.5."""
    edge = raw_p - 0.5
    return 0.5 + edge * (1.0 - coefficient * config.prob_alpha)


def confidence_multiplier(coefficient: float, config: VolatilityConfig = DEFAULT_CONFIG) -> float:
    """Sharper discount intended for the 1-99 confidence score."""
    return 1.0 - coefficient * config.conf_alpha

"""Per-prop volatility coefficient + tier.

See docs/superpowers/specs/2026-05-15-volatility-tier-design.md for the full
specification. This module is intended to be a pure-function dependency:
callers assemble a FeatureSnapshot and pass it in along with the prediction
row, and the module returns a VolatilityScore.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
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
    weights: Mapping[str, float]
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
        object.__setattr__(self, "weights", MappingProxyType(dict(self.weights)))
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


_STAT_CV_EPS = 0.5
_USAGE_CV_EPS = 0.05
_MIN_MINUTES_DENOM = 1.0


def _clip01(value: float) -> float:
    if value < 0.0:
        return 0.0
    if value > 1.0:
        return 1.0
    return value


def normalize_stat_cv(
    std: float, mean: float, config: VolatilityConfig = DEFAULT_CONFIG
) -> float:
    if mean < _STAT_CV_EPS:
        return 1.0
    cv = std / mean
    return _clip01(cv / config.stat_cv_max)


def normalize_minutes_instability(
    *,
    predicted_std: float,
    minutes_std_10: float,
    minutes_mean_10: float,
    config: VolatilityConfig = DEFAULT_CONFIG,
) -> float:
    pred_component = _clip01(predicted_std / config.minutes_std_max)
    denom = max(minutes_mean_10, _MIN_MINUTES_DENOM)
    cv_component = _clip01((minutes_std_10 / denom) / config.minutes_cv_max)
    return _clip01((pred_component + cv_component) / 2.0)


def normalize_usage_instability(
    std: float, mean: float, config: VolatilityConfig = DEFAULT_CONFIG
) -> float:
    if mean < _USAGE_CV_EPS:
        return 1.0
    cv = std / mean
    return _clip01(cv / config.usage_cv_max)


def normalize_recent_form_divergence(
    *,
    mean_5: float,
    mean_season: float,
    std_season: float,
    config: VolatilityConfig = DEFAULT_CONFIG,
) -> float:
    denom = max(std_season, 1.0)
    z = abs(mean_5 - mean_season) / denom
    return _clip01(z / config.recent_form_z_max)


Archetype = Literal["starter", "rotation", "bench", "fringe"]

_ARCHETYPE_RISK: dict[Archetype, float] = {
    "starter": 0.0,
    "rotation": 0.3,
    "bench": 0.7,
    "fringe": 1.0,
}


def classify_archetype(*, starter_flag_rate: float, minutes_mean_season: float) -> Archetype:
    if starter_flag_rate >= 0.7 and minutes_mean_season >= 24.0:
        return "starter"
    if minutes_mean_season >= 18.0:
        return "rotation"
    if minutes_mean_season >= 10.0:
        return "bench"
    return "fringe"


def archetype_risk(archetype: Archetype) -> float:
    return _ARCHETYPE_RISK[archetype]


@dataclass(frozen=True)
class FeatureSnapshot:
    """Inputs required to compute a volatility coefficient.

    Each field is Optional; `compute_volatility` drops missing inputs and
    renormalizes the remaining weights. If every input is None, the score
    is neutral (coefficient=0.5) with reason="insufficient_features".
    """

    stat_std_10: float | None
    stat_mean_10: float | None
    predicted_minutes_std: float | None
    minutes_std_10: float | None
    minutes_mean_10: float | None
    usage_std_10: float | None
    usage_mean_10: float | None
    mean_5: float | None
    mean_season: float | None
    std_season: float | None
    starter_flag_rate: float | None
    minutes_mean_season: float | None


def _maybe_stat_cv(snap: FeatureSnapshot, config: VolatilityConfig) -> float | None:
    if snap.stat_std_10 is None or snap.stat_mean_10 is None:
        return None
    return normalize_stat_cv(snap.stat_std_10, snap.stat_mean_10, config=config)


def _maybe_minutes(snap: FeatureSnapshot, config: VolatilityConfig) -> float | None:
    if (
        snap.predicted_minutes_std is None
        or snap.minutes_std_10 is None
        or snap.minutes_mean_10 is None
    ):
        return None
    return normalize_minutes_instability(
        predicted_std=snap.predicted_minutes_std,
        minutes_std_10=snap.minutes_std_10,
        minutes_mean_10=snap.minutes_mean_10,
        config=config,
    )


def _maybe_usage(snap: FeatureSnapshot, config: VolatilityConfig) -> float | None:
    if snap.usage_std_10 is None or snap.usage_mean_10 is None:
        return None
    return normalize_usage_instability(snap.usage_std_10, snap.usage_mean_10, config=config)


def _maybe_recent_form(snap: FeatureSnapshot, config: VolatilityConfig) -> float | None:
    if snap.mean_5 is None or snap.mean_season is None or snap.std_season is None:
        return None
    return normalize_recent_form_divergence(
        mean_5=snap.mean_5,
        mean_season=snap.mean_season,
        std_season=snap.std_season,
        config=config,
    )


def _maybe_archetype(snap: FeatureSnapshot) -> float | None:
    if snap.starter_flag_rate is None or snap.minutes_mean_season is None:
        return None
    archetype = classify_archetype(
        starter_flag_rate=snap.starter_flag_rate,
        minutes_mean_season=snap.minutes_mean_season,
    )
    return archetype_risk(archetype)


def compute_volatility(
    *,
    raw_probability: float,
    features: FeatureSnapshot,
    config: VolatilityConfig = DEFAULT_CONFIG,
) -> VolatilityScore:
    raw_inputs: dict[str, float | None] = {
        "stat_cv": _maybe_stat_cv(features, config),
        "minutes_instability": _maybe_minutes(features, config),
        "usage_instability": _maybe_usage(features, config),
        "recent_form_divergence": _maybe_recent_form(features, config),
        "archetype_risk": _maybe_archetype(features),
    }

    available = {name: value for name, value in raw_inputs.items() if value is not None}

    if not available:
        return VolatilityScore(
            coefficient=0.5,
            tier=tier_from_coefficient(0.5, config=config),
            contributors=(),
            adjusted_probability=adjust_probability(raw_probability, 0.5, config=config),
            confidence_multiplier=confidence_multiplier(0.5, config=config),
            reason="insufficient_features",
        )

    weight_sum = sum(config.weights[name] for name in available)
    contributors: list[VolatilityContributor] = []
    coefficient = 0.0
    for name, normalized in available.items():
        renormalized_weight = config.weights[name] / weight_sum
        contribution = renormalized_weight * normalized
        contributors.append(
            VolatilityContributor(
                name=name,
                raw_value=normalized,
                weight=renormalized_weight,
                contribution=contribution,
            )
        )
        coefficient += contribution

    coefficient = _clip01(coefficient)
    return VolatilityScore(
        coefficient=coefficient,
        tier=tier_from_coefficient(coefficient, config=config),
        contributors=tuple(contributors),
        adjusted_probability=adjust_probability(raw_probability, coefficient, config=config),
        confidence_multiplier=confidence_multiplier(coefficient, config=config),
        reason="",
    )

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
from datetime import date as _date_t
from types import MappingProxyType
from typing import Literal

from sqlalchemy import select
from sqlalchemy.orm import Session

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


# Allowed market keys map to the `player_game_logs` column we aggregate.
_MARKET_TO_COLUMN: dict[str, str] = {
    "points": "points",
    "rebounds": "rebounds",
    "assists": "assists",
    "threes": "threes",
    "turnovers": "turnovers",
    "steals": "steals",
    "blocks": "blocks",
    "pra": "_pra_synthetic",  # computed below
}


def _safe_mean(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _safe_std(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    m = sum(values) / len(values)
    variance = sum((v - m) ** 2 for v in values) / (len(values) - 1)
    return math.sqrt(variance)


def _usage_proxy(row: object) -> float:
    minutes = max(getattr(row, "minutes", 0.0) or 0.0, 1.0)
    return (
        (getattr(row, "field_goal_attempts", 0) or 0)
        + 0.44 * (getattr(row, "free_throw_attempts", 0) or 0)
        + (getattr(row, "turnovers", 0) or 0)
    ) / minutes


def build_feature_snapshot(
    *,
    session: Session,
    player_id: int,
    market_key: str,
    as_of_date: _date_t,
    predicted_minutes_std: float | None,
) -> FeatureSnapshot:
    """Aggregate recent player_game_logs into a FeatureSnapshot.

    Filters games to those before `as_of_date` and orders most-recent first.
    """
    from app.models.all import Game, PlayerGameLog

    stmt = (
        select(PlayerGameLog)
        .join(Game, Game.game_id == PlayerGameLog.game_id)
        .where(PlayerGameLog.player_id == player_id)
        .where(Game.game_date < as_of_date)
        .order_by(Game.game_date.desc())
        .limit(82)
    )
    rows = list(session.scalars(stmt))

    last_10 = rows[:10]
    last_5 = rows[:5]
    season = rows[:82]

    def _stat_values(slice_: list) -> list[float]:
        if market_key == "pra":
            return [
                float((r.points or 0) + (r.rebounds or 0) + (r.assists or 0))
                for r in slice_
            ]
        column = _MARKET_TO_COLUMN.get(market_key)
        if column is None or column == "_pra_synthetic":
            return []
        return [float(getattr(r, column, 0) or 0) for r in slice_]

    stat_10 = _stat_values(last_10)
    stat_5 = _stat_values(last_5)
    stat_season = _stat_values(season)

    minutes_10 = [float(r.minutes or 0.0) for r in last_10]
    minutes_season = [float(r.minutes or 0.0) for r in season]
    usage_10 = [_usage_proxy(r) for r in last_10]

    starter_flags = [float(bool(r.starter_flag)) for r in last_10]
    starter_flag_rate = _safe_mean(starter_flags) if starter_flags else None
    minutes_mean_season = _safe_mean(minutes_season) if minutes_season else None

    return FeatureSnapshot(
        stat_std_10=_safe_std(stat_10) if stat_10 else None,
        stat_mean_10=_safe_mean(stat_10) if stat_10 else None,
        predicted_minutes_std=predicted_minutes_std,
        minutes_std_10=_safe_std(minutes_10) if minutes_10 else None,
        minutes_mean_10=_safe_mean(minutes_10) if minutes_10 else None,
        usage_std_10=_safe_std(usage_10) if usage_10 else None,
        usage_mean_10=_safe_mean(usage_10) if usage_10 else None,
        mean_5=_safe_mean(stat_5) if stat_5 else None,
        mean_season=_safe_mean(stat_season) if stat_season else None,
        std_season=_safe_std(stat_season) if stat_season else None,
        starter_flag_rate=starter_flag_rate,
        minutes_mean_season=minutes_mean_season,
    )

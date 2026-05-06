from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CanonicalFieldSpec:
    name: str
    family: str
    description: str


NBA_API_ADVANCED_FIELD_MAP: dict[str, str] = {
    "assistPercentage": "assist_percentage",
    "assistRatio": "assist_ratio",
    "assistToTurnover": "assist_to_turnover",
    "defensiveRating": "defensive_rating",
    "defensiveReboundPercentage": "defensive_rebound_percentage",
    "effectiveFieldGoalPercentage": "effective_field_goal_percentage",
    "estimatedDefensiveRating": "estimated_defensive_rating",
    "estimatedNetRating": "estimated_net_rating",
    "estimatedOffensiveRating": "estimated_offensive_rating",
    "estimatedPace": "estimated_pace",
    "estimatedUsagePercentage": "estimated_usage_percentage",
    "netRating": "net_rating",
    "offensiveRating": "offensive_rating",
    "offensiveReboundPercentage": "offensive_rebound_percentage",
    "pace": "pace",
    "pacePer40": "pace_per_40",
    "PIE": "pie",
    "possessions": "possessions",
    "reboundPercentage": "rebound_percentage",
    "trueShootingPercentage": "true_shooting_percentage",
    "turnoverRatio": "turnover_ratio",
    "usagePercentage": "usage_percentage",
}

NBA_API_TRACKING_FIELD_MAP: dict[str, str] = {
    "contestedFieldGoalPercentage": "contested_field_goal_percentage",
    "contestedFieldGoalsAttempted": "contested_field_goals_attempted",
    "contestedFieldGoalsMade": "contested_field_goals_made",
    "defendedAtRimFieldGoalPercentage": "defended_at_rim_field_goal_percentage",
    "defendedAtRimFieldGoalsAttempted": "defended_at_rim_field_goals_attempted",
    "defendedAtRimFieldGoalsMade": "defended_at_rim_field_goals_made",
    "distance": "distance",
    "fieldGoalPercentage": "tracking_field_goal_percentage",
    "freeThrowAssists": "free_throw_assists",
    "passes": "passes",
    "reboundChancesDefensive": "rebound_chances_defensive",
    "reboundChancesOffensive": "rebound_chances_offensive",
    "reboundChancesTotal": "rebound_chances_total",
    "secondaryAssists": "secondary_assists",
    "speed": "speed",
    "touches": "touches",
    "uncontestedFieldGoalPercentage": "uncontested_field_goal_percentage",
    "uncontestedFieldGoalsAttempted": "uncontested_field_goals_attempted",
    "uncontestedFieldGoalsMade": "uncontested_field_goals_made",
}

NBA_API_SCORING_FIELD_MAP: dict[str, str] = {
    "percentageAssisted2pt": "percentage_assisted_2pt",
    "percentageAssisted3pt": "percentage_assisted_3pt",
    "percentageAssistedFGM": "percentage_assisted_fgm",
    "percentageFieldGoalsAttempted2pt": "percentage_field_goals_attempted_2pt",
    "percentageFieldGoalsAttempted3pt": "percentage_field_goals_attempted_3pt",
    "percentagePoints2pt": "percentage_points_2pt",
    "percentagePoints3pt": "percentage_points_3pt",
    "percentagePointsFastBreak": "percentage_points_fast_break",
    "percentagePointsFreeThrow": "percentage_points_free_throw",
    "percentagePointsMidrange2pt": "percentage_points_midrange_2pt",
    "percentagePointsOffTurnovers": "percentage_points_off_turnovers",
    "percentagePointsPaint": "percentage_points_paint",
    "percentageUnassisted2pt": "percentage_unassisted_2pt",
    "percentageUnassisted3pt": "percentage_unassisted_3pt",
    "percentageUnassistedFGM": "percentage_unassisted_fgm",
}

CORE_CANONICAL_FIELDS: dict[str, CanonicalFieldSpec] = {
    "minutes": CanonicalFieldSpec("minutes", "core", "Player minutes"),
    "points": CanonicalFieldSpec("points", "core", "Player points"),
    "rebounds": CanonicalFieldSpec("rebounds", "core", "Player rebounds"),
    "assists": CanonicalFieldSpec("assists", "core", "Player assists"),
    "threes": CanonicalFieldSpec("threes", "core", "Player made threes"),
    "turnovers": CanonicalFieldSpec("turnovers", "core", "Player turnovers"),
    "field_goal_attempts": CanonicalFieldSpec("field_goal_attempts", "core", "Field goal attempts"),
    "field_goals_made": CanonicalFieldSpec("field_goals_made", "core", "Field goals made"),
    "free_throw_attempts": CanonicalFieldSpec("free_throw_attempts", "core", "Free throw attempts"),
    "free_throws_made": CanonicalFieldSpec("free_throws_made", "core", "Free throws made"),
    "offensive_rebounds": CanonicalFieldSpec("offensive_rebounds", "core", "Offensive rebounds"),
    "defensive_rebounds": CanonicalFieldSpec("defensive_rebounds", "core", "Defensive rebounds"),
    "starter_flag": CanonicalFieldSpec("starter_flag", "core", "Starter flag"),
    "is_home": CanonicalFieldSpec("is_home", "core", "Home or away flag"),
    "days_rest": CanonicalFieldSpec("days_rest", "core", "Days since previous game"),
    "back_to_back": CanonicalFieldSpec("back_to_back", "core", "Back-to-back flag"),
    "team_injuries": CanonicalFieldSpec("team_injuries", "core", "Count of active injury reports on own team"),
}

ADVANCED_CANONICAL_FIELDS: dict[str, CanonicalFieldSpec] = {
    canonical_name: CanonicalFieldSpec(canonical_name, "advanced", raw_name)
    for raw_name, canonical_name in NBA_API_ADVANCED_FIELD_MAP.items()
}

TRACKING_CANONICAL_FIELDS: dict[str, CanonicalFieldSpec] = {
    canonical_name: CanonicalFieldSpec(canonical_name, "tracking", raw_name)
    for raw_name, canonical_name in NBA_API_TRACKING_FIELD_MAP.items()
}

SCORING_CANONICAL_FIELDS: dict[str, CanonicalFieldSpec] = {
    canonical_name: CanonicalFieldSpec(canonical_name, "scoring", raw_name)
    for raw_name, canonical_name in NBA_API_SCORING_FIELD_MAP.items()
}

ODDS_CANONICAL_FIELDS: dict[str, CanonicalFieldSpec] = {
    "line_value": CanonicalFieldSpec("line_value", "odds", "Consensus line value"),
    "raw_implied_over_probability": CanonicalFieldSpec("raw_implied_over_probability", "odds", "Raw implied over probability"),
    "raw_implied_under_probability": CanonicalFieldSpec("raw_implied_under_probability", "odds", "Raw implied under probability"),
    "no_vig_over_probability": CanonicalFieldSpec("no_vig_over_probability", "odds", "No-vig over probability"),
    "no_vig_under_probability": CanonicalFieldSpec("no_vig_under_probability", "odds", "No-vig under probability"),
    "consensus_line_mean": CanonicalFieldSpec("consensus_line_mean", "odds", "Mean line across books"),
    "consensus_line_median": CanonicalFieldSpec("consensus_line_median", "odds", "Median line across books"),
    "consensus_line_std": CanonicalFieldSpec("consensus_line_std", "odds", "Line standard deviation across books"),
    "consensus_prob_mean": CanonicalFieldSpec("consensus_prob_mean", "odds", "Mean no-vig probability across books"),
    "consensus_prob_std": CanonicalFieldSpec("consensus_prob_std", "odds", "Std of no-vig probability across books"),
    "best_over_price": CanonicalFieldSpec("best_over_price", "odds", "Best over price across books"),
    "best_under_price": CanonicalFieldSpec("best_under_price", "odds", "Best under price across books"),
    "book_count": CanonicalFieldSpec("book_count", "odds", "Books contributing to consensus"),
    "market_count": CanonicalFieldSpec("market_count", "odds", "Quote count for the market"),
    "line_movement_1h": CanonicalFieldSpec("line_movement_1h", "odds", "Line change over 1 hour"),
    "line_movement_6h": CanonicalFieldSpec("line_movement_6h", "odds", "Line change over 6 hours"),
    "line_movement_24h": CanonicalFieldSpec("line_movement_24h", "odds", "Line change over 24 hours"),
}

CANONICAL_FEATURE_REGISTRY: dict[str, CanonicalFieldSpec] = {
    **CORE_CANONICAL_FIELDS,
    **ADVANCED_CANONICAL_FIELDS,
    **TRACKING_CANONICAL_FIELDS,
    **SCORING_CANONICAL_FIELDS,
    **ODDS_CANONICAL_FIELDS,
}

PROVIDER_CAPABILITIES: dict[str, dict[str, bool]] = {
    "nba_api": {
        **dict.fromkeys(CORE_CANONICAL_FIELDS, True),
        **dict.fromkeys(ADVANCED_CANONICAL_FIELDS, True),
        **dict.fromkeys(TRACKING_CANONICAL_FIELDS, True),
        **dict.fromkeys(SCORING_CANONICAL_FIELDS, True),
    },
}

_MISSING_FIELD_LOGS: set[tuple[str, str]] = set()


def normalize_provider_boxscore_row(row: dict[str, Any], field_map: dict[str, str]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for raw_name, canonical_name in field_map.items():
        value = row.get(raw_name)
        if value in (None, ""):
            continue
        normalized[canonical_name] = _coerce_scalar(value)
    return normalized


def capability_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for provider_name, capabilities in sorted(PROVIDER_CAPABILITIES.items()):
        for field_name, supported in sorted(capabilities.items()):
            spec = CANONICAL_FEATURE_REGISTRY.get(field_name)
            rows.append(
                {
                    "provider_name": provider_name,
                    "field_name": field_name,
                    "family": spec.family if spec is not None else "unknown",
                    "supported": bool(supported),
                }
            )
    return rows


def log_missing_canonical_fields(provider_name: str, field_names: list[str], available_columns: list[str]) -> None:
    missing = sorted(field for field in field_names if field not in available_columns)
    if not missing:
        return
    cache_key = (provider_name, "|".join(missing))
    if cache_key in _MISSING_FIELD_LOGS:
        return
    _MISSING_FIELD_LOGS.add(cache_key)
    logger.info(
        "canonical provider fields missing",
        extra={
            "provider_name": provider_name,
            "missing_fields": missing,
        },
    )


def _coerce_scalar(value: Any) -> Any:
    if isinstance(value, str):
        text = value.strip()
        if text == "":
            return None
        try:
            if "." in text:
                return float(text)
            return int(text)
        except ValueError:
            return text
    return value

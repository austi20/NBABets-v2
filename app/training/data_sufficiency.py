"""Data sufficiency tier classification for full prediction coverage."""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from app.config.settings import get_settings

_log = logging.getLogger(__name__)

RECENT_N = 10


def _downgrade_tier(tier: str) -> str:
    if tier == "A":
        return "B"
    if tier == "B":
        return "C"
    return "D"


def classify_data_sufficiency_tier(
    historical_games: int,
    historical_minutes_total: float,
    recent_avg_minutes: float,
    team_changed: bool = False,
) -> str:
    """Classify a player into the A/B/C/D sufficiency tiers from the plan."""

    settings = get_settings()
    if historical_games <= 0:
        return "D"

    if historical_games < settings.data_sufficiency_tier_b_min_games:
        tier = "C"
    elif (
        historical_games < settings.data_sufficiency_tier_a_min_games
        or historical_minutes_total < settings.data_sufficiency_tier_a_min_minutes
    ):
        tier = "B" if historical_minutes_total >= settings.data_sufficiency_tier_b_min_minutes else "C"
    elif recent_avg_minutes < settings.data_sufficiency_recent_avg_minutes_floor:
        tier = "B"
    else:
        tier = "A"

    if team_changed:
        tier = _downgrade_tier(tier)
    return tier


def classify_player_tier(
    historical_games: int,
    historical_minutes_total: float,
    recent_avg_minutes: float,
    team_changed: bool = False,
) -> str:
    """Backward-compatible alias for older imports."""

    return classify_data_sufficiency_tier(
        historical_games=historical_games,
        historical_minutes_total=historical_minutes_total,
        recent_avg_minutes=recent_avg_minutes,
        team_changed=team_changed,
    )


def annotate_tiers(upcoming: pd.DataFrame, historical: pd.DataFrame) -> pd.DataFrame:
    """Annotate every upcoming row with a data sufficiency tier."""

    if upcoming.empty:
        upcoming["_data_sufficiency_tier"] = pd.Series(dtype=str)
        return upcoming

    if historical.empty:
        upcoming["_data_sufficiency_tier"] = "D"
        return upcoming

    player_history = (
        historical.groupby("player_id", as_index=False)
        .agg(
            historical_games=("game_id", "nunique"),
            historical_minutes_total=("minutes", "sum"),
        )
    )
    recent_history = (
        historical.sort_values("game_date")
        .groupby("player_id")
        .tail(RECENT_N)
        .groupby("player_id", as_index=False)
        .agg(
            recent_games=("game_id", "nunique"),
            recent_minutes_total=("minutes", "sum"),
        )
    )

    merged = upcoming.merge(player_history, on="player_id", how="left")
    merged = merged.merge(recent_history, on="player_id", how="left")

    for col in ("historical_games", "recent_games"):
        merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(0).astype(int)
    for col in ("historical_minutes_total", "recent_minutes_total"):
        merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(0.0)
    merged["recent_avg_minutes"] = (
        merged["recent_minutes_total"] / merged["recent_games"].replace(0, np.nan)
    ).fillna(0.0)

    merged["_data_sufficiency_tier"] = [
        classify_data_sufficiency_tier(
            historical_games=int(row["historical_games"]),
            historical_minutes_total=float(row["historical_minutes_total"]),
            recent_avg_minutes=float(row["recent_avg_minutes"]),
            team_changed=bool(row.get("_team_changed", False)),
        )
        for _, row in merged.iterrows()
    ]

    tier_counts = merged["_data_sufficiency_tier"].value_counts().to_dict()
    _log.info(
        "Data sufficiency tiers: %s (total=%d)",
        ", ".join(f"{tier}={count}" for tier, count in sorted(tier_counts.items())),
        len(merged),
    )
    return merged

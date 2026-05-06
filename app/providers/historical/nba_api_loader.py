from __future__ import annotations

import logging
import time
from typing import Any

import pandas as pd
from nba_api.stats.endpoints import playergamelogs
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

logger = logging.getLogger(__name__)

SEASON_TYPES = ("Regular Season", "Playoffs")
_REQUEST_TIMEOUT = 60
_BETWEEN_CALLS_SECONDS = 0.6

# Columns emitted by this loader - match PlayerGameLogPayload field names
COLUMNS = [
    "provider_game_id",
    "provider_player_id",
    "team_abbreviation",
    "opponent_abbreviation",
    "minutes",
    "points",
    "rebounds",
    "assists",
    "threes",
    "steals",
    "blocks",
    "turnovers",
    "fouls",
    "field_goal_attempts",
    "field_goals_made",
    "free_throw_attempts",
    "free_throws_made",
    "offensive_rebounds",
    "defensive_rebounds",
    "plus_minus",
    "starter_flag",
    "overtime_flag",
    "season",
    "game_date",
    "season_type",
    "player_name",
]


def _season_code(year: int) -> str:
    """Convert start year (e.g. 2024) to NBA season code (e.g. '2024-25')."""
    return f"{year}-{str(year + 1)[-2:]}"


def _parse_minutes(raw: Any) -> float:
    if raw is None:
        return 0.0
    if isinstance(raw, (int, float)):
        return float(raw)
    text = str(raw).strip()
    if ":" not in text:
        return float(text or 0.0)
    minutes, seconds = text.split(":", 1)
    return float(minutes) + float(seconds) / 60.0


@retry(
    stop=stop_after_attempt(5),
    wait=wait_random_exponential(multiplier=1, max=60),
    retry=retry_if_exception_type(Exception),
    reraise=True,
)
def _fetch_season_type_rows(season_code: str, season_type: str) -> list[dict[str, Any]]:
    response = playergamelogs.PlayerGameLogs(
        season_nullable=season_code,
        season_type_nullable=season_type,
        timeout=_REQUEST_TIMEOUT,
    )
    return list(response.get_data_frames()[0].to_dict("records"))


def _row_to_record(row: dict[str, Any], season_type: str) -> dict[str, Any]:
    matchup = str(row.get("MATCHUP") or "")
    team_abbr = str(row.get("TEAM_ABBREVIATION") or "").upper()
    if " vs. " in matchup:
        opponent_abbr = matchup.split(" vs. ", 1)[1].strip().upper()
    elif " @ " in matchup:
        opponent_abbr = matchup.split(" @ ", 1)[1].strip().upper()
    else:
        opponent_abbr = None

    raw_date = row.get("GAME_DATE")
    game_date = str(raw_date).strip() if raw_date else None

    return {
        "provider_game_id": str(row["GAME_ID"]),
        "provider_player_id": str(row["PLAYER_ID"]),
        "team_abbreviation": team_abbr,
        "opponent_abbreviation": opponent_abbr,
        "minutes": _parse_minutes(row.get("MIN_SEC") or row.get("MIN")),
        "points": int(row.get("PTS") or 0),
        "rebounds": int(row.get("REB") or 0),
        "assists": int(row.get("AST") or 0),
        "threes": int(row.get("FG3M") or 0),
        "steals": int(row.get("STL") or 0),
        "blocks": int(row.get("BLK") or 0),
        "turnovers": int(row.get("TOV") or 0),
        "fouls": int(row.get("PF") or 0),
        "field_goal_attempts": int(row.get("FGA") or 0),
        "field_goals_made": int(row.get("FGM") or 0),
        "free_throw_attempts": int(row.get("FTA") or 0),
        "free_throws_made": int(row.get("FTM") or 0),
        "offensive_rebounds": int(row.get("OREB") or 0),
        "defensive_rebounds": int(row.get("DREB") or 0),
        "plus_minus": float(row["PLUS_MINUS"]) if row.get("PLUS_MINUS") is not None else None,
        "starter_flag": False,
        "overtime_flag": False,
        "season": str(row.get("SEASON_YEAR") or ""),
        "game_date": game_date,
        "season_type": season_type,
        "player_name": str(row.get("PLAYER_NAME") or "").strip(),
    }


def fetch_player_game_logs(season: str) -> pd.DataFrame:
    """Fetch all player game logs for a full NBA season.

    Args:
        season: Start year of the season as a string, e.g. "2024" for 2024-25.

    Returns:
        DataFrame with columns matching PlayerGameLogPayload field names.
    """
    season_code = _season_code(int(season))
    records: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    for season_type in SEASON_TYPES:
        logger.info("Fetching %s %s ...", season_code, season_type)
        try:
            rows = _fetch_season_type_rows(season_code, season_type)
        except Exception:
            logger.warning("Failed to fetch %s %s - skipping", season_code, season_type)
            rows = []

        for row in rows:
            key = (str(row["PLAYER_ID"]), str(row["GAME_ID"]))
            if key in seen:
                continue
            seen.add(key)
            records.append(_row_to_record(row, season_type))

        if rows:
            time.sleep(_BETWEEN_CALLS_SECONDS)

    if not records:
        return pd.DataFrame(columns=COLUMNS)

    return pd.DataFrame(records, columns=COLUMNS)

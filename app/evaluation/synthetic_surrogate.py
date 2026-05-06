from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from app.training.data import MINIMUM_QUALIFYING_MINUTES

MARKET_STAT_COLUMNS: dict[str, str] = {
    "points": "points",
    "rebounds": "rebounds",
    "assists": "assists",
    "threes": "threes",
    "turnovers": "turnovers",
    "pra": "pra",
}


@dataclass(frozen=True)
class SurrogateConfig:
    trailing_games: int = 4
    market_source: str = "synthetic_surrogate_v1"


def _coerce_season_start_year(row: pd.Series) -> int | None:
    season_raw = row.get("season")
    if season_raw is not None:
        text = str(season_raw).strip()
        if text:
            head = text.split("-", 1)[0]
            if head.isdigit():
                return int(head)
    game_date = pd.to_datetime(row.get("game_date"), errors="coerce")
    if pd.isna(game_date):
        return None
    # NBA season start year: Oct-Dec belong to current year, Jan-Jun belong to previous year.
    return int(game_date.year if game_date.month >= 10 else game_date.year - 1)


def generate_surrogate_lines(
    box_scores: pd.DataFrame,
    trailing_games: int = 4,
) -> pd.DataFrame:
    """Generate trailing-average synthetic lines without target leakage.

    For each player-game-market row:
    - `line_value` uses only prior games for that player (shifted rolling mean).
    - `label_over` compares the current game's actual stat to that line.
    """
    if box_scores.empty:
        return pd.DataFrame()
    if trailing_games < 1:
        raise ValueError("trailing_games must be >= 1")

    frame = box_scores.copy()
    required_columns = {"provider_player_id", "provider_game_id", "game_date"}
    missing = required_columns.difference(frame.columns)
    if missing:
        raise ValueError(f"Missing required box-score columns: {sorted(missing)}")

    frame["player_id"] = pd.to_numeric(frame["provider_player_id"], errors="coerce")
    frame["game_id"] = pd.to_numeric(frame["provider_game_id"], errors="coerce")
    frame["game_date"] = pd.to_datetime(frame["game_date"], errors="coerce")
    frame["minutes"] = pd.to_numeric(frame.get("minutes"), errors="coerce")
    frame = frame[frame["minutes"] >= MINIMUM_QUALIFYING_MINUTES].copy()
    if "pra" not in frame.columns:
        frame["pra"] = (
            pd.to_numeric(frame.get("points"), errors="coerce").fillna(0.0)
            + pd.to_numeric(frame.get("rebounds"), errors="coerce").fillna(0.0)
            + pd.to_numeric(frame.get("assists"), errors="coerce").fillna(0.0)
        )
    frame["season"] = frame.apply(_coerce_season_start_year, axis=1)
    frame = frame.dropna(subset=["player_id", "game_id", "game_date", "season"]).copy()
    frame = frame.sort_values(["player_id", "game_date", "game_id"]).reset_index(drop=True)

    rows: list[pd.DataFrame] = []
    for market_key, stat_column in MARKET_STAT_COLUMNS.items():
        if stat_column not in frame.columns:
            continue
        market_frame = frame[
            [
                "season",
                "game_date",
                "game_id",
                "player_id",
                "player_name",
                stat_column,
            ]
        ].copy()
        market_frame["market_key"] = market_key
        market_frame["actual_value"] = pd.to_numeric(market_frame[stat_column], errors="coerce")
        market_frame["line_value"] = (
            market_frame.groupby("player_id")["actual_value"]
            .transform(lambda values: values.shift(1).rolling(trailing_games, min_periods=trailing_games).mean())
        )
        market_frame = market_frame.dropna(subset=["line_value", "actual_value"]).copy()
        market_frame["label_over"] = (market_frame["actual_value"] > market_frame["line_value"]).astype(int)
        market_frame["label_push"] = (market_frame["actual_value"] == market_frame["line_value"]).astype(int)
        market_frame["market_source"] = "synthetic_surrogate_v1"
        market_frame["eligible_for_training"] = True
        market_frame["trailing_games"] = trailing_games
        rows.append(market_frame)

    if not rows:
        return pd.DataFrame()
    result = pd.concat(rows, ignore_index=True)
    return result[
        [
            "season",
            "game_date",
            "game_id",
            "player_id",
            "player_name",
            "market_key",
            "line_value",
            "actual_value",
            "label_over",
            "label_push",
            "market_source",
            "eligible_for_training",
            "trailing_games",
        ]
    ].sort_values(["season", "game_date", "game_id", "player_id", "market_key"]).reset_index(drop=True)

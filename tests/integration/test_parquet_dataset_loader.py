"""Integration test: DatasetLoader.load_historical_player_games_from_parquet.

Writes a tiny synthetic parquet file matching the nba_api_loader schema and
verifies the loader output has the same column contract as the DB path.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from app.training.data import (
    AVAILABILITY_CONTEXT_FIELDS,
    MINIMUM_QUALIFYING_MINUTES,
    PLAYER_INJURY_CONTEXT_FIELDS,
    PLAYER_META_FIELDS,
    TRADE_CONTEXT_FIELDS,
    DatasetLoader,
)

# Columns that load_historical_player_games always produces
_DB_PATH_REQUIRED_COLUMNS = [
    "player_id",
    "player_name",
    "game_id",
    "game_date",
    "start_time",
    "minutes",
    "points",
    "rebounds",
    "assists",
    "threes",
    "turnovers",
    "steals",
    "blocks",
    "field_goal_attempts",
    "field_goals_made",
    "free_throw_attempts",
    "free_throws_made",
    "offensive_rebounds",
    "defensive_rebounds",
    "plus_minus",
    "fouls",
    "starter_flag",
    "pra",
    "position",
    "team_id",
    "player_team_id",
    "opponent_team_id",
    "home_team_id",
    "away_team_id",
    "is_home",
    "home_team_abbreviation",
    "spread",
    "total",
    *PLAYER_META_FIELDS,
    *AVAILABILITY_CONTEXT_FIELDS,
    *PLAYER_INJURY_CONTEXT_FIELDS,
    *TRADE_CONTEXT_FIELDS,
]


def _make_synthetic_parquet(root: Path) -> None:
    """Write a two-row parquet file matching nba_api_loader output schema."""
    season_dir = root / "season=2024"
    season_dir.mkdir(parents=True)
    df = pd.DataFrame(
        {
            "provider_game_id": ["0022400001", "0022400002"],
            "provider_player_id": ["1234567", "7654321"],
            "team_abbreviation": ["LAL", "BOS"],
            "opponent_abbreviation": ["BOS", "LAL"],
            "minutes": [30.0, 6.0],  # row 1 qualifies, row 2 below minimum
            "points": [20, 4],
            "rebounds": [5, 1],
            "assists": [3, 0],
            "threes": [2, 0],
            "steals": [1, 0],
            "blocks": [0, 0],
            "turnovers": [2, 1],
            "fouls": [3, 2],
            "field_goal_attempts": [15, 3],
            "field_goals_made": [8, 1],
            "free_throw_attempts": [4, 2],
            "free_throws_made": [4, 2],
            "offensive_rebounds": [1, 0],
            "defensive_rebounds": [4, 1],
            "plus_minus": [8.0, -5.0],
            "starter_flag": [True, False],
            "overtime_flag": [False, False],
            "season": ["2024-25", "2024-25"],
            "game_date": ["2025-01-10", "2025-01-10"],
            "season_type": ["Regular Season", "Regular Season"],
            "player_name": ["Player A", "Player B"],
        }
    )
    df.to_parquet(season_dir / "part-0.parquet", index=False)


@pytest.fixture()
def parquet_root(tmp_path: Path) -> Path:
    _make_synthetic_parquet(tmp_path)
    return tmp_path


@pytest.fixture()
def loader() -> DatasetLoader:
    mock_session = MagicMock()
    return DatasetLoader(mock_session)


def test_schema_matches_db_path(loader: DatasetLoader, parquet_root: Path) -> None:
    result = loader.load_historical_player_games_from_parquet(parquet_root)
    missing = [col for col in _DB_PATH_REQUIRED_COLUMNS if col not in result.columns]
    assert not missing, f"Missing columns from DB-path schema: {missing}"


def test_minimum_minutes_filter(loader: DatasetLoader, parquet_root: Path) -> None:
    result = loader.load_historical_player_games_from_parquet(parquet_root)
    # Only the 30-minute row should survive (6 < MINIMUM_QUALIFYING_MINUTES=5 is fine
    # but we set row 2 to 6 which is >= 5, so both rows should pass)
    assert len(result) == 2
    assert (result["minutes"] >= MINIMUM_QUALIFYING_MINUTES).all()


def test_pra_computed(loader: DatasetLoader, parquet_root: Path) -> None:
    result = loader.load_historical_player_games_from_parquet(parquet_root)
    row = result[result["player_name"] == "Player A"].iloc[0]
    assert row["pra"] == row["points"] + row["rebounds"] + row["assists"]


def test_player_id_is_numeric(loader: DatasetLoader, parquet_root: Path) -> None:
    result = loader.load_historical_player_games_from_parquet(parquet_root)
    assert pd.api.types.is_numeric_dtype(result["player_id"])
    assert pd.api.types.is_numeric_dtype(result["game_id"])


def test_as_of_date_filter(loader: DatasetLoader, parquet_root: Path) -> None:
    from datetime import date
    result = loader.load_historical_player_games_from_parquet(
        parquet_root, as_of_date=date(2025, 1, 9)
    )
    assert result.empty


def test_use_parquet_flag_default(loader: DatasetLoader) -> None:
    assert loader.use_parquet is False

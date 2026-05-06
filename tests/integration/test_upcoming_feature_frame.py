"""Integration test: build_upcoming_feature_frame.

Mocks the BDL schedule call and the DatasetLoader DB methods so no live
network or database connections are needed.  Verifies that the function
wires together DatasetLoader, annotate_tiers, and FeatureEngineer and
produces a non-empty DataFrame with the expected feature columns.
"""
from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from app.schemas.domain import GamePayload
from app.training.data import AVAILABILITY_CONTEXT_FIELDS, PLAYER_INJURY_CONTEXT_FIELDS, TRADE_CONTEXT_FIELDS
from app.training.upcoming import build_upcoming_feature_frame, load_upcoming_scoped

# ── synthetic data helpers ────────────────────────────────────────────────────

def _make_historical() -> pd.DataFrame:
    """Minimal historical frame with two games for one player."""
    game_date = pd.Timestamp("2025-01-05")
    return pd.DataFrame(
        {
            "player_id": [1, 1],
            "player_name": ["Test Player", "Test Player"],
            "game_id": [100, 101],
            "game_date": [game_date, game_date + pd.Timedelta(days=2)],
            "start_time": [game_date, game_date + pd.Timedelta(days=2)],
            "position": ["G", "G"],
            "team_id": [10, 10],
            "player_team_id": [10, 10],
            "opponent_team_id": [20, 20],
            "home_team_id": [10, 20],
            "away_team_id": [20, 10],
            "is_home": [1, 0],
            "home_team_abbreviation": ["LAL", "BOS"],
            "spread": [0.0, 0.0],
            "total": [220.0, 220.0],
            "minutes": [30.0, 28.0],
            "points": [18, 22],
            "rebounds": [4, 5],
            "assists": [6, 3],
            "threes": [2, 3],
            "turnovers": [2, 1],
            "steals": [1, 2],
            "blocks": [0, 0],
            "field_goal_attempts": [14, 16],
            "field_goals_made": [7, 8],
            "free_throw_attempts": [4, 6],
            "free_throws_made": [4, 6],
            "offensive_rebounds": [0, 1],
            "defensive_rebounds": [4, 4],
            "plus_minus": [5.0, -3.0],
            "fouls": [2, 3],
            "starter_flag": [1, 1],
            "pra": [28, 30],
            # Advanced stats required by _prepare_base_frame (direct column access)
            "possessions": [45.0, 43.0],
            "touches": [60.0, 55.0],
            "passes": [30.0, 28.0],
            "secondary_assists": [1.0, 0.0],
            "free_throw_assists": [0.0, 1.0],
            "percentage_field_goals_attempted_3pt": [0.35, 0.30],
            "percentage_field_goals_attempted_2pt": [0.65, 0.70],
            # Context fields added by _attach_*_context in the real DB path
            **{col: [0.0, 0.0] for col in AVAILABILITY_CONTEXT_FIELDS},
            **{col: [0.0, 0.0] for col in PLAYER_INJURY_CONTEXT_FIELDS},
            **{col: [0.0, 0.0] for col in TRADE_CONTEXT_FIELDS},
        }
    )


def _make_upcoming() -> pd.DataFrame:
    """Minimal upcoming frame: one player, one game, two markets."""
    game_date = pd.Timestamp("2025-01-10")
    return pd.DataFrame(
        {
            "player_id": [1, 1],
            "player_name": ["Test Player", "Test Player"],
            "game_id": [200, 200],
            "game_date": [game_date, game_date],
            "start_time": [game_date, game_date],
            "position": ["G", "G"],
            "team_id": [10, 10],
            "is_home": [1, 1],
            "home_team_id": [10, 10],
            "away_team_id": [20, 20],
            "opponent_team_id": [20, 20],
            "home_team_abbreviation": ["LAL", "LAL"],
            "spread": [0.0, 0.0],
            "total": [220.0, 220.0],
            "market_key": ["points", "rebounds"],
            "line_value": [20.5, 4.5],
            "over_odds": [-110, -110],
            "under_odds": [-110, -110],
            "snapshot_id": [9001, 9002],
            "_data_sufficiency_tier": ["A", "A"],
        }
    )


def _make_bdl_games() -> list[GamePayload]:
    return [
        GamePayload(
            provider_game_id="999",
            game_date=date(2025, 1, 10),
            start_time=datetime(2025, 1, 10, 20, 0, tzinfo=UTC),
            home_team_abbreviation="LAL",
            away_team_abbreviation="BOS",
        )
    ]


# ── fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture()
def mock_session() -> MagicMock:
    session = MagicMock()
    session.bind = MagicMock()
    return session


# ── tests ─────────────────────────────────────────────────────────────────────

@patch("app.training.upcoming._async_fetch_schedule", new_callable=AsyncMock)
@patch("app.training.data.DatasetLoader.load_upcoming_player_lines")
def test_build_upcoming_feature_frame_returns_nonempty(
    mock_load_upcoming: MagicMock,
    mock_fetch_schedule: AsyncMock,
    mock_session: MagicMock,
) -> None:
    mock_fetch_schedule.return_value = _make_bdl_games()
    mock_load_upcoming.return_value = _make_upcoming()
    historical = _make_historical()

    result = build_upcoming_feature_frame(
        date(2025, 1, 10),
        mock_session,
        historical=historical,
    )

    assert not result.empty, "Expected a non-empty feature frame"
    assert "player_id" in result.columns
    assert "game_id" in result.columns
    assert "market_key" in result.columns


@patch("app.training.upcoming._async_fetch_schedule", new_callable=AsyncMock)
@patch("app.training.data.DatasetLoader.load_upcoming_player_lines")
def test_build_upcoming_feature_frame_has_rolling_features(
    mock_load_upcoming: MagicMock,
    mock_fetch_schedule: AsyncMock,
    mock_session: MagicMock,
) -> None:
    mock_fetch_schedule.return_value = _make_bdl_games()
    mock_load_upcoming.return_value = _make_upcoming()
    historical = _make_historical()

    result = build_upcoming_feature_frame(
        date(2025, 1, 10),
        mock_session,
        historical=historical,
    )

    rolling_cols = [c for c in result.columns if "_avg_" in c or "_std_" in c]
    assert rolling_cols, "Expected rolling-window feature columns in output"


@patch("app.training.upcoming._async_fetch_schedule", new_callable=AsyncMock)
@patch("app.training.data.DatasetLoader.load_upcoming_player_lines")
def test_build_upcoming_feature_frame_empty_upcoming(
    mock_load_upcoming: MagicMock,
    mock_fetch_schedule: AsyncMock,
    mock_session: MagicMock,
) -> None:
    mock_fetch_schedule.return_value = []
    mock_load_upcoming.return_value = pd.DataFrame()
    historical = _make_historical()

    result = build_upcoming_feature_frame(
        date(2025, 1, 10),
        mock_session,
        historical=historical,
    )

    assert result.empty


@patch("app.training.data.DatasetLoader.load_upcoming_player_lines")
def test_load_upcoming_scoped_caps_game_count(
    mock_load_upcoming: MagicMock,
    mock_session: MagicMock,
) -> None:
    upcoming = _make_upcoming()
    # Add a second game to make sure the cap fires
    extra = upcoming.copy()
    extra["game_id"] = 201
    extra["snapshot_id"] = [9003, 9004]
    mock_load_upcoming.return_value = pd.concat([upcoming, extra], ignore_index=True)

    _, game_ids = load_upcoming_scoped(date(2025, 1, 10), mock_session, max_game_count=1)

    assert game_ids is not None
    assert len(game_ids) == 1


@patch("app.training.data.DatasetLoader.load_upcoming_player_lines")
def test_load_upcoming_scoped_no_cap(
    mock_load_upcoming: MagicMock,
    mock_session: MagicMock,
) -> None:
    mock_load_upcoming.return_value = _make_upcoming()

    frame, game_ids = load_upcoming_scoped(date(2025, 1, 10), mock_session, max_game_count=0)

    assert game_ids is None
    assert not frame.empty

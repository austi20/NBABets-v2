from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from app.providers.historical.nba_api_loader import (
    COLUMNS,
    _parse_minutes,
    _season_code,
    fetch_player_game_logs,
)

_FAKE_ROWS = [
    {
        "GAME_ID": "0022400001",
        "PLAYER_ID": "123456",
        "TEAM_ABBREVIATION": "LAL",
        "PLAYER_NAME": "Test Player",
        "MATCHUP": "LAL vs. BOS",
        "MIN_SEC": "32:15",
        "PTS": 20,
        "REB": 5,
        "AST": 7,
        "FG3M": 3,
        "STL": 1,
        "BLK": 0,
        "TOV": 2,
        "PF": 3,
        "FGA": 15,
        "FGM": 8,
        "FTA": 4,
        "FTM": 4,
        "OREB": 1,
        "DREB": 4,
        "PLUS_MINUS": 5.0,
        "SEASON_YEAR": "2024-25",
        "GAME_DATE": "2024-10-22",
    }
]


def _mock_response(rows: list[dict]) -> MagicMock:
    df = pd.DataFrame(rows)
    mock = MagicMock()
    mock.get_data_frames.return_value = [df]
    return mock


class TestSeasonCode:
    def test_converts_start_year(self) -> None:
        assert _season_code(2024) == "2024-25"
        assert _season_code(1996) == "1996-97"
        assert _season_code(2000) == "2000-01"


class TestParseMinutes:
    def test_mm_ss_format(self) -> None:
        assert _parse_minutes("32:15") == pytest.approx(32.25)

    def test_float_passthrough(self) -> None:
        assert _parse_minutes(30.5) == 30.5

    def test_none_returns_zero(self) -> None:
        assert _parse_minutes(None) == 0.0


class TestFetchPlayerGameLogs:
    @patch("app.providers.historical.nba_api_loader.playergamelogs.PlayerGameLogs")
    def test_returns_dataframe_with_expected_columns(self, mock_cls: MagicMock) -> None:
        mock_cls.return_value = _mock_response(_FAKE_ROWS)

        df = fetch_player_game_logs("2024")

        assert isinstance(df, pd.DataFrame)
        for col in COLUMNS:
            assert col in df.columns, f"Missing column: {col}"

    @patch("app.providers.historical.nba_api_loader.playergamelogs.PlayerGameLogs")
    def test_maps_fields_correctly(self, mock_cls: MagicMock) -> None:
        mock_cls.return_value = _mock_response(_FAKE_ROWS)

        df = fetch_player_game_logs("2024")
        row = df.iloc[0]

        assert row["provider_game_id"] == "0022400001"
        assert row["provider_player_id"] == "123456"
        assert row["team_abbreviation"] == "LAL"
        assert row["opponent_abbreviation"] == "BOS"
        assert row["points"] == 20
        assert row["rebounds"] == 5
        assert row["assists"] == 7
        assert row["minutes"] == pytest.approx(32.25)

    @patch("app.providers.historical.nba_api_loader.playergamelogs.PlayerGameLogs")
    def test_deduplicates_player_game_rows(self, mock_cls: MagicMock) -> None:
        duplicate_rows = _FAKE_ROWS + _FAKE_ROWS
        mock_cls.return_value = _mock_response(duplicate_rows)

        df = fetch_player_game_logs("2024")

        assert len(df) == 1

    @patch("app.providers.historical.nba_api_loader.playergamelogs.PlayerGameLogs")
    def test_returns_empty_df_on_all_failures(self, mock_cls: MagicMock) -> None:
        mock_cls.side_effect = RuntimeError("network error")

        df = fetch_player_game_logs("2024")

        assert df.empty
        assert list(df.columns) == COLUMNS

from __future__ import annotations

import pandas as pd

from app.evaluation.synthetic_surrogate import generate_surrogate_lines


def _sample_box_scores(points_fourth_game: float) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "provider_game_id": ["1", "2", "3", "4", "5"],
            "provider_player_id": ["7", "7", "7", "7", "7"],
            "player_name": ["Player A"] * 5,
            "game_date": pd.to_datetime(
                ["2024-01-01", "2024-01-03", "2024-01-05", "2024-01-07", "2024-01-09"]
            ),
            "minutes": [30.0, 30.0, 30.0, 30.0, 30.0],
            "points": [10.0, 20.0, 30.0, points_fourth_game, 50.0],
            "rebounds": [5.0, 5.0, 5.0, 5.0, 5.0],
            "assists": [2.0, 2.0, 2.0, 2.0, 2.0],
            "threes": [1.0, 1.0, 1.0, 1.0, 1.0],
            "turnovers": [1.0, 1.0, 1.0, 1.0, 1.0],
        }
    )


def test_surrogate_line_uses_only_prior_games_no_leakage() -> None:
    high_current = generate_surrogate_lines(_sample_box_scores(points_fourth_game=1000.0), trailing_games=3)
    low_current = generate_surrogate_lines(_sample_box_scores(points_fourth_game=0.0), trailing_games=3)

    high_points = high_current[high_current["market_key"] == "points"].reset_index(drop=True)
    low_points = low_current[low_current["market_key"] == "points"].reset_index(drop=True)

    # First eligible row is game 4 -> line is avg of games 1-3 = 20.0 in both cases.
    assert high_points.iloc[0]["game_id"] == 4
    assert high_points.iloc[0]["line_value"] == 20.0
    assert low_points.iloc[0]["line_value"] == 20.0

    # Current-game stat only affects the label and future rolling windows.
    assert high_points.iloc[0]["label_over"] == 1
    assert low_points.iloc[0]["label_over"] == 0


def test_surrogate_marks_market_source_and_training_eligibility() -> None:
    rows = generate_surrogate_lines(_sample_box_scores(points_fourth_game=40.0), trailing_games=3)
    assert not rows.empty
    assert set(rows["market_source"].unique()) == {"synthetic_surrogate_v1"}
    assert bool(rows["eligible_for_training"].all())


def test_surrogate_uses_nba_season_start_year_not_calendar_year() -> None:
    source = _sample_box_scores(points_fourth_game=40.0)
    source["season"] = ["2024"] * len(source)
    source.loc[source.index[-1], "game_date"] = pd.Timestamp("2025-01-09")
    rows = generate_surrogate_lines(source, trailing_games=3)
    assert not rows.empty
    assert set(rows["season"].unique()) == {2024}


def test_surrogate_excludes_low_minute_rows() -> None:
    source = _sample_box_scores(points_fourth_game=40.0)
    source.loc[source.index[2], "minutes"] = 3.0
    source.loc[source.index[[0, 1, 3, 4]], "minutes"] = 30.0
    rows = generate_surrogate_lines(source, trailing_games=2)
    assert not rows.empty
    assert (rows["actual_value"] >= 0.0).all()
    # The low-minute game is removed before rolling windows are built.
    assert int(rows["game_id"].min()) > 2

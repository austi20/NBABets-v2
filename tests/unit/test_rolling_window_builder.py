"""Characterization tests for RollingWindowBuilder (AG-TECH-001)."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from app.training.feature_builders.rolling_windows import RollingWindowBuilder


def _minimal_history_frame() -> pd.DataFrame:
    """Single-player sequence with stable inputs for deterministic rolling stats."""
    n = 6
    return pd.DataFrame(
        {
            "player_id": [1] * n,
            "minutes": [32.0] * n,
            "points": [10.0, 14.0, 18.0, 22.0, 26.0, 30.0],
            "rebounds": [5.0] * n,
            "assists": [3.0] * n,
            "threes": [1.0] * n,
            "turnovers": [2.0] * n,
            "pra": [18.0, 22.0, 26.0, 30.0, 34.0, 38.0],
            "field_goal_attempts": [12.0] * n,
            "field_goals_made": [6.0] * n,
            "free_throw_attempts": [4.0] * n,
            "free_throws_made": [3.0] * n,
            "offensive_rebounds": [1.0] * n,
            "defensive_rebounds": [4.0] * n,
            "plus_minus": [0.0] * n,
            "fouls": [3.0] * n,
            "team_injuries": [0.5, 1.0, 1.5, 2.0, 2.5, 3.0],
            "lineup_instability_score": [0.1 * i for i in range(n)],
        }
    )


def test_rolling_window_builder_points_avg_10_last_row() -> None:
    frame = _minimal_history_frame()
    out = RollingWindowBuilder().build_player_history_features(frame)
    # Last row: shifted points = [10,14,18,22,26]; 10-game mean = 18.0
    assert np.isclose(float(out["points_avg_10"].iloc[-1]), 18.0)


def test_rolling_window_builder_points_prev_and_std() -> None:
    frame = _minimal_history_frame()
    out = RollingWindowBuilder().build_player_history_features(frame)
    assert np.isclose(float(out["points_prev"].iloc[2]), 14.0)
    # Shifted series at index 3: [10, 14, 18]; rolling std ddof=1 → 4.0
    assert np.isclose(float(out["points_std_10"].iloc[3]), float(np.std([10.0, 14.0, 18.0], ddof=1)))


def test_rolling_window_builder_team_injuries_and_lineup_avg() -> None:
    frame = _minimal_history_frame()
    out = RollingWindowBuilder().build_player_history_features(frame)
    # Shifted team_injuries at last row: 0.5..2.5 -> mean 1.5
    assert np.isclose(float(out["team_injuries_avg_10"].iloc[-1]), 1.5)
    # lineup_instability: shifted [0,0.1,0.2,0.3,0.4] mean 0.2
    assert np.isclose(float(out["lineup_instability_score_avg_10"].iloc[-1]), 0.2)


def test_rolling_window_builder_key_set_stable() -> None:
    frame = _minimal_history_frame()
    out = RollingWindowBuilder().build_player_history_features(frame)
    expected_subset = {
        "points_avg_10",
        "points_std_5",
        "points_ewm_10",
        "points_season_avg",
        "minutes_prev",
        "team_injuries_avg_10",
        "lineup_instability_score_avg_10",
    }
    assert expected_subset.issubset(out.keys())


def test_rolling_window_builder_home_away_split_points() -> None:
    """B1: conditional home/away 10-game means use only prior games at that site."""
    n = 12
    base = _minimal_history_frame()
    frame = pd.concat([base, base], ignore_index=True)
    assert len(frame) == n
    is_home = [1, 0] * (n // 2)
    points = [30.0 if h else 10.0 for h in is_home]
    frame["points"] = points
    frame["pra"] = frame["points"] + frame["rebounds"] + frame["assists"]
    frame["is_home"] = is_home
    out = RollingWindowBuilder().build_player_history_features(frame)
    assert "points_avg_10_home" in out and "points_avg_10_away" in out
    assert "home_away_points_delta" in out
    last_home = float(out["points_avg_10_home"].iloc[-1])
    last_away = float(out["points_avg_10_away"].iloc[-1])
    assert last_home > last_away + 5.0
    assert np.isclose(float(out["home_away_points_delta"].iloc[-1]), last_home - last_away)


@pytest.mark.parametrize("pid", [1, 2])
def test_rolling_window_builder_multiplayer_isolation(pid: int) -> None:
    f1 = _minimal_history_frame()
    f2 = f1.copy()
    f2["player_id"] = 2
    f2["points"] = [100.0] * len(f2)
    frame = pd.concat([f1, f2], ignore_index=True)
    out = RollingWindowBuilder().build_player_history_features(frame)
    mask = frame["player_id"] == pid
    sub = frame.loc[mask].reset_index(drop=True)
    direct = RollingWindowBuilder().build_player_history_features(sub)
    pd.testing.assert_series_equal(
        out["points_avg_10"][mask].reset_index(drop=True),
        direct["points_avg_10"],
        check_names=False,
    )


def test_rolling_window_builder_k_seasons_2_does_not_raise_window_min_periods_error() -> None:
    frame = _minimal_history_frame()
    out = RollingWindowBuilder(k_seasons=2).build_player_history_features(frame)
    assert "points_avg_3" in out
    assert out["points_avg_3"].notna().sum() >= 1

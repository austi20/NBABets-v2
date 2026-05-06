"""AG-MOD-001..004: model feature columns (B1/B2/B4/B5) smoke + parity checks."""

from __future__ import annotations

import pandas as pd

from app.training.data import AVAILABILITY_CONTEXT_FIELDS
from app.training.features import TEAM_HOME_ARENA_COORDS, FeatureEngineer


def _history_row(
    *,
    game_id: int,
    game_date: str,
    is_home: int,
    home_abbr: str,
    player_team_id: int,
    opponent_team_id: int,
    minutes: float,
    fouls: int,
) -> dict[str, object]:
    base = {
        "player_id": 1,
        "player_name": "Test Player",
        "position": "G",
        "team_id": player_team_id,
        "game_id": game_id,
        "game_date": pd.Timestamp(game_date),
        "start_time": pd.Timestamp(f"{game_date} 19:00:00"),
        "home_team_id": player_team_id if is_home else opponent_team_id,
        "away_team_id": opponent_team_id if is_home else player_team_id,
        "is_home": is_home,
        "home_team_abbreviation": home_abbr,
        "player_team_id": player_team_id,
        "opponent_team_id": opponent_team_id,
        "spread": 0.0,
        "total": 220.0,
        "minutes": minutes,
        "points": 20.0,
        "rebounds": 5.0,
        "assists": 4.0,
        "threes": 2.0,
        "turnovers": 2.0,
        "steals": 1.0,
        "blocks": 0.0,
        "field_goal_attempts": 14,
        "field_goals_made": 7,
        "free_throw_attempts": 4,
        "free_throws_made": 3,
        "offensive_rebounds": 1,
        "defensive_rebounds": 4,
        "plus_minus": 0.0,
        "fouls": fouls,
        "starter_flag": True,
        "possessions": 80.0,
        "pace": 96.0,
        "estimated_pace": 96.0,
        "usage_percentage": 0.22,
        "estimated_usage_percentage": 0.22,
        "touches": 50.0,
        "passes": 30.0,
        "secondary_assists": 0.0,
        "free_throw_assists": 0.0,
        "percentage_field_goals_attempted_3pt": 0.4,
        "percentage_field_goals_attempted_2pt": 0.6,
        "player_meta": {},
    }
    for field in AVAILABILITY_CONTEXT_FIELDS:
        base[field] = 0.0
    base["lineup_instability_score"] = 0.1
    base["teammate_absence_pressure"] = 0.0
    return base


def _minimal_historical(n_games: int = 12) -> pd.DataFrame:
    """Single-player schedule: team 10 alternates home (BOS) / road (at LAL)."""
    rows: list[dict[str, object]] = []
    for i in range(n_games):
        is_home = 1 if i % 2 == 0 else 0
        abbr = "BOS" if is_home else "LAL"
        rows.append(
            _history_row(
                game_id=1000 + i,
                game_date=f"2025-11-{i + 1:02d}",
                is_home=is_home,
                home_abbr=abbr,
                player_team_id=10,
                opponent_team_id=11,
                minutes=32.0,
                fouls=4 if i >= 6 else 2,
            )
        )
    return pd.DataFrame(rows)


def test_feature_engineer_ag_mod_columns_present() -> None:
    fe = FeatureEngineer()
    hist = _minimal_historical()
    fs = fe.build_training_frame(hist)
    cols = set(fs.feature_columns)
    for name in (
        "points_avg_10_home",
        "points_avg_10_away",
        "home_away_points_delta",
        "travel_distance_km",
        "travel_fatigue_score",
        "long_haul_travel_leg",
        "min_minutes_floor_10",
        "minutes_floor_reliability",
        "foul_rate_10",
        "high_foul_risk",
    ):
        assert name in cols, f"missing feature column {name}"
    assert float(fs.frame["travel_distance_km"].iloc[0]) == 0.0
    assert float(fs.frame["travel_distance_km"].iloc[3]) > 0.0
    assert float(fs.frame["foul_rate_10"].iloc[-1]) >= 0.0


def test_inference_recomputes_travel_from_venues() -> None:
    fe = FeatureEngineer()
    hist = _minimal_historical()
    upcoming = pd.DataFrame(
        {
            "game_id": [2000],
            "game_date": pd.Timestamp("2025-12-01"),
            "start_time": pd.Timestamp("2025-12-01 20:00:00"),
            "player_id": [1],
            "player_name": ["Test Player"],
            "team_id": [10],
            "position": ["G"],
            "is_home": [0],
            "home_team_id": [11],
            "away_team_id": [10],
            "opponent_team_id": [11],
            # Last historical game in _minimal_historical ends at LAL (road); fly to DEN next.
            "home_team_abbreviation": ["DEN"],
            "spread": [0.0],
            "total": [220.0],
            "market_key": ["points"],
            "line_value": [20.5],
            "snapshot_id": [1],
            "timestamp": pd.Timestamp("2025-12-01 18:00:00"),
            "sportsbook_id": [1],
            **{k: [0.0] for k in AVAILABILITY_CONTEXT_FIELDS if k not in {"lineup_instability_score"}},
        }
    )
    upcoming["lineup_instability_score"] = [0.1]
    inf = fe.build_inference_frame(hist, upcoming)
    assert "travel_distance_km" in inf.frame.columns
    assert float(inf.frame["travel_distance_km"].iloc[0]) > 0.0


def test_team_arena_coords_cover_aliases() -> None:
    assert "BKN" in TEAM_HOME_ARENA_COORDS
    assert "BRK" in TEAM_HOME_ARENA_COORDS
    assert TEAM_HOME_ARENA_COORDS["BRK"] == TEAM_HOME_ARENA_COORDS["BKN"]

from __future__ import annotations

import math

import pandas as pd

from app.training.data import AVAILABILITY_CONTEXT_FIELDS
from app.training.features import FeatureEngineer


def _history_row(*, game_id: int, game_date: str, starter_flag: bool, points: float) -> dict[str, object]:
    row = {
        "player_id": 1,
        "player_name": "Test Guard",
        "position": "G",
        "team_id": 10,
        "game_id": game_id,
        "game_date": pd.Timestamp(game_date),
        "start_time": pd.Timestamp(f"{game_date} 19:00:00"),
        "home_team_id": 10,
        "away_team_id": 11,
        "is_home": 1,
        "home_team_abbreviation": "BOS",
        "player_team_id": 10,
        "opponent_team_id": 11,
        "spread": 0.0,
        "total": 220.0,
        "minutes": 32.0 if starter_flag else 20.0,
        "points": points,
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
        "fouls": 2.0,
        "starter_flag": starter_flag,
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
        row[field] = 0.0
    row["lineup_instability_score"] = 0.1
    row["teammate_absence_pressure"] = 0.0
    return row


def test_build_population_priors_uses_role_bucket_names() -> None:
    engineer = FeatureEngineer()
    historical = pd.DataFrame(
        [
            _history_row(game_id=1, game_date="2026-01-01", starter_flag=True, points=24.0),
            _history_row(game_id=2, game_date="2026-01-03", starter_flag=True, points=22.0),
            _history_row(game_id=3, game_date="2026-01-05", starter_flag=False, points=11.0),
            _history_row(game_id=4, game_date="2026-01-07", starter_flag=False, points=10.0),
        ]
    )

    feature_set = engineer.build_training_frame(historical)
    priors = engineer.build_population_priors(feature_set.frame, feature_set.feature_columns)

    assert "G" in priors["position_feature_priors"]
    assert "G_starter" in priors["role_feature_priors"]
    assert "G_bench" in priors["role_feature_priors"]


def test_fill_with_population_priors_uses_position_for_tier_c_and_role_for_tier_d() -> None:
    engineer = FeatureEngineer()
    priors = {
        "global_feature_priors": {"starter_flag": 0.0, "points_avg_10": 9.0},
        "position_feature_priors": {"G": {"starter_flag": 0.25, "points_avg_10": 12.5}},
        "role_feature_priors": {
            "G_starter": {"starter_flag": 1.0, "points_avg_10": 23.0},
            "G_bench": {"starter_flag": 0.0, "points_avg_10": 7.0},
        },
        "role_bucket_thresholds": {"points": 18.0},
    }
    frame = pd.DataFrame(
        [
            {
                "_data_sufficiency_tier": "D",
                "position_group": "G",
                "market_key": "points",
                "line_value": 22.5,
                "starter_flag": 0.0,
                "points_avg_10": math.nan,
            },
            {
                "_data_sufficiency_tier": "C",
                "position_group": "G",
                "market_key": "points",
                "line_value": 12.5,
                "starter_flag": 0.0,
                "points_avg_10": math.nan,
            },
        ]
    )

    filled = engineer._fill_with_population_priors(
        frame,
        priors,
        "_data_sufficiency_tier",
        ["starter_flag", "points_avg_10"],
    )

    assert float(filled.loc[0, "starter_flag"]) == 1.0
    assert float(filled.loc[0, "points_avg_10"]) == 23.0
    assert float(filled.loc[1, "starter_flag"]) == 0.0
    assert float(filled.loc[1, "points_avg_10"]) == 12.5

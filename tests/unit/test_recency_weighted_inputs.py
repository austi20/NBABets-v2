from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from app.evaluation.backtest import _simulation_context as backtest_simulation_context
from app.training.baseline import RecencyBaseline
from app.training.constants import MARKET_TARGETS
from app.training.data import AVAILABILITY_CONTEXT_FIELDS
from app.training.features import FeatureEngineer
from app.training.models import FittedMarketModel
from app.training.pipeline import TrainingPipeline


def _baseline_frame() -> pd.DataFrame:
    row: dict[str, float] = {}
    for market_key, target in MARKET_TARGETS.items():
        row[f"{target}_avg_3"] = 11.0
        row[f"{target}_avg_5"] = 13.0
        row[f"{target}_avg_10"] = 17.0
        row[f"{target}_avg_20"] = 19.0
        row[f"{target}_recency_weighted"] = 24.0
        row[f"{target}_season_avg"] = 18.0
        row[f"{target}_std_10"] = 2.0
        row[f"{target}_std_recency_weighted"] = 3.0
        row[f"opponent_allowed_{market_key}_10"] = 21.0
    return pd.DataFrame([row])


def _historical_recency_frame() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    start = pd.Timestamp("2025-01-01")
    for i in range(25):
        game_date = start + pd.Timedelta(days=i)
        row: dict[str, object] = {
            "player_id": 1,
            "player_name": "Recency Player",
            "position": "G",
            "team_id": 10,
            "player_team_id": 10,
            "game_id": 1000 + i,
            "game_date": game_date,
            "start_time": game_date + pd.Timedelta(hours=19),
            "home_team_id": 10 if i % 2 == 0 else 20,
            "away_team_id": 20 if i % 2 == 0 else 10,
            "opponent_team_id": 20,
            "is_home": int(i % 2 == 0),
            "home_team_abbreviation": "BOS" if i % 2 == 0 else "LAL",
            "spread": 0.0,
            "total": 220.0,
            "minutes": 30.0,
            "points": 10.0 + i,
            "rebounds": 5.0,
            "assists": 4.0,
            "threes": 2.0,
            "turnovers": 2.0,
            "field_goal_attempts": 11.0 + i,
            "field_goals_made": 6.0,
            "free_throw_attempts": 4.0,
            "free_throws_made": 3.0,
            "offensive_rebounds": 1.0,
            "defensive_rebounds": 4.0,
            "plus_minus": 0.0,
            "fouls": 2.0,
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
            row.setdefault(field, 0.0)
        rows.append(row)
    return pd.DataFrame(rows)


def _upcoming_recency_frame() -> pd.DataFrame:
    game_date = pd.Timestamp("2025-02-01")
    return pd.DataFrame(
        [
            {
                "player_id": 1,
                "player_name": "Recency Player",
                "game_id": 2000,
                "game_date": game_date,
                "start_time": game_date + pd.Timedelta(hours=19),
                "position": "G",
                "team_id": 10,
                "is_home": 1,
                "home_team_id": 10,
                "away_team_id": 20,
                "opponent_team_id": 20,
                "home_team_abbreviation": "BOS",
                "spread": 0.0,
                "total": 220.0,
                "market_key": "points",
                "line_value": 20.5,
                "over_odds": -110,
                "under_odds": -110,
                "snapshot_id": 1,
                "_data_sufficiency_tier": "A",
            }
        ]
    )


def test_recency_baseline_uses_selected_baseline_weights() -> None:
    output = RecencyBaseline().predict(_baseline_frame())

    assert float(output["points_baseline_mean"].iloc[0]) == pytest.approx((0.80 * 24.0) + (0.10 * 18.0) + (0.10 * 21.0))
    assert float(output["points_baseline_variance"].iloc[0]) == pytest.approx(9.0)


def test_market_history_anchor_prefers_recency_weighted_signal() -> None:
    frame = pd.DataFrame(
        {
            "points_avg_3": [10.0],
            "points_avg_5": [12.0],
            "points_avg_10": [20.0],
            "points_avg_20": [18.0],
            "points_recency_weighted": [30.0],
            "points_season_avg": [16.0],
            "points_std_10": [2.0],
            "points_std_recency_weighted": [4.0],
        }
    )
    model = FittedMarketModel(market_key="points", mean_model=object(), residual_std=1.0, feature_columns=[])

    weighted_anchor, history_anchor, recent_std, *_ = model._market_history_anchor(frame, "points", np.array([14.0]))

    assert weighted_anchor[0] == pytest.approx(30.0)
    assert history_anchor[0] == pytest.approx((0.70 * 30.0) + (0.20 * 20.0) + (0.10 * 16.0))
    assert recent_std[0] == pytest.approx(4.0)


def test_blended_features_and_feature_selection_include_recency_weighted_columns() -> None:
    engineer = FeatureEngineer.__new__(FeatureEngineer)
    frame = pd.DataFrame(
        {
            "points_avg_3": [2.0],
            "points_avg_5": [4.0],
            "points_avg_10": [6.0],
            "points_avg_20": [8.0],
            "points_recency_weighted": [10.0],
            "points_ewm_10": [5.0],
            "points_season_avg": [7.0],
            "role_stability": [0.0],
        }
    )

    blended = engineer._add_blended_features(frame)
    feature_columns = engineer._feature_columns(blended)

    expected_short = (0.65 * 10.0) + (0.35 * 5.0)
    expected_blended = (0.65 * 10.0) + (0.35 * expected_short)
    assert blended["points_blended"].iloc[0] == pytest.approx(expected_blended)
    assert "points_recency_weighted" in feature_columns


def test_inference_frame_carries_latest_recency_weighted_columns() -> None:
    engineer = FeatureEngineer()
    historical = _historical_recency_frame()
    training = engineer.build_training_frame(historical, fill_terminal_nan=None).frame
    expected = training.sort_values(["game_date", "start_time", "game_id", "player_id"]).groupby("player_id").tail(1).iloc[0]

    inference = engineer.build_inference_frame(historical, _upcoming_recency_frame()).frame

    assert inference["points_recency_weighted"].iloc[0] == pytest.approx(expected["points_recency_weighted"])
    assert inference["points_recency_weighted"].iloc[0] > 0.0
    assert inference["field_goal_attempts_per_minute_recency_weighted"].iloc[0] == pytest.approx(
        expected["field_goal_attempts_per_minute_recency_weighted"]
    )
    assert inference["field_goal_attempts_per_minute_recency_weighted"].iloc[0] > 0.0


def test_live_simulation_context_prefers_adjusted_then_recency_weighted_values() -> None:
    pipeline = TrainingPipeline.__new__(TrainingPipeline)
    row = {
        "usage_rate_recency_weighted": 0.31,
        "usage_rate_blended": 0.22,
        "usage_rate_avg_10": 0.18,
        "field_goal_attempts_per_minute_recency_weighted": 0.52,
        "field_goal_attempts_per_minute_blended": 0.41,
        "field_goal_attempts_per_minute_avg_10": 0.33,
        "estimated_three_point_attempts_per_minute_recency_weighted": 0.24,
        "estimated_three_point_attempts_per_minute_blended": 0.19,
        "threes_per_minute_recency_weighted": 0.09,
        "rebounds_std_recency_weighted": 3.4,
        "rebounds_std_10": 1.2,
    }

    context = pipeline._simulation_context(row, "points")
    assert context["usage_rate"] == pytest.approx(0.31)
    assert context["field_goal_attempts_per_minute"] == pytest.approx(0.52)
    assert context["rebounds_std_10"] == pytest.approx(3.4)

    adjusted_context = pipeline._simulation_context({**row, "adjusted_usage_rate": 0.43}, "points")
    assert adjusted_context["usage_rate"] == pytest.approx(0.43)


def test_live_simulation_context_keeps_zero_recency_and_raw_fallback() -> None:
    pipeline = TrainingPipeline.__new__(TrainingPipeline)

    zero_context = pipeline._simulation_context(
        {
            "field_goal_attempts_per_minute_recency_weighted": 0.0,
            "field_goal_attempts_per_minute_blended": 0.41,
            "estimated_three_point_attempts_per_minute": 0.20,
            "threes_per_minute": 0.05,
        },
        "points",
    )
    assert zero_context["field_goal_attempts_per_minute"] == pytest.approx(0.0)
    assert zero_context["three_point_make_rate"] == pytest.approx(0.25)


def test_backtest_simulation_context_prefers_adjusted_then_recency_weighted_values() -> None:
    row = {
        "usage_rate_recency_weighted": 0.31,
        "usage_rate_blended": 0.22,
        "usage_rate_avg_10": 0.18,
        "field_goal_attempts_per_minute_recency_weighted": 0.52,
        "field_goal_attempts_per_minute_blended": 0.41,
        "field_goal_attempts_per_minute_avg_10": 0.33,
        "estimated_three_point_attempts_per_minute_recency_weighted": 0.24,
        "estimated_three_point_attempts_per_minute_blended": 0.19,
        "threes_per_minute_recency_weighted": 0.09,
        "rebounds_std_recency_weighted": 3.4,
        "rebounds_std_10": 1.2,
    }

    context = backtest_simulation_context(row)
    assert context["usage_rate"] == pytest.approx(0.31)
    assert context["field_goal_attempts_per_minute"] == pytest.approx(0.52)
    assert context["rebounds_std_10"] == pytest.approx(3.4)

    adjusted_context = backtest_simulation_context({**row, "adjusted_usage_rate": 0.43})
    assert adjusted_context["usage_rate"] == pytest.approx(0.43)


def test_backtest_simulation_context_keeps_zero_recency_and_raw_fallback() -> None:
    context = backtest_simulation_context(
        {
            "field_goal_attempts_per_minute_recency_weighted": 0.0,
            "field_goal_attempts_per_minute_blended": 0.41,
            "estimated_three_point_attempts_per_minute": 0.20,
            "threes_per_minute": 0.05,
            "assist_creation_proxy_per_minute": 0.40,
            "assists_per_minute": 0.10,
        }
    )

    assert context["field_goal_attempts_per_minute"] == pytest.approx(0.0)
    assert context["three_point_make_rate"] == pytest.approx(0.25)
    assert context["assist_conversion_rate"] == pytest.approx(0.25)

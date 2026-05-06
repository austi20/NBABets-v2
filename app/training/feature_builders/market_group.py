"""Market-group prior feature helpers extracted from FeatureEngineer."""

from __future__ import annotations

import numpy as np
import pandas as pd

MARKET_GROUP_PRIORS: dict[str, dict[str, tuple[float, tuple[str, ...]]]] = {
    "points": {
        "minutes_exposure": (0.30, ("minutes_blended", "expected_possessions", "blended_game_pace")),
        "usage_touches": (0.25, ("usage_rate_blended", "touches_per_minute_blended", "passes_per_minute_blended")),
        "shot_volume_mix": (
            0.20,
            (
                "field_goal_attempts_per_minute_blended",
                "free_throw_attempts_per_minute_blended",
                "estimated_three_point_attempts_per_minute_blended",
                "true_shooting_percentage_blended",
            ),
        ),
        "pace_environment": (0.10, ("team_pace_avg_10", "opponent_pace_avg_10", "total")),
        "opponent_context": (
            0.10,
            ("opponent_allowed_points_10", "opponent_allowed_points_per_possession_10", "opponent_position_allowed_points_10"),
        ),
        "market_consensus": (
            0.05,
            ("points_consensus_prob_mean", "points_book_count", "points_line_movement_6h", "points_line_delta_5"),
        ),
    },
    "rebounds": {
        "minutes_exposure": (0.25, ("minutes_blended", "expected_possessions", "blended_game_pace")),
        "rebound_chances": (
            0.35,
            (
                "rebound_chances_total_per_minute_blended",
                "rebound_conversion_rate_blended",
                "rebound_percentage_blended",
            ),
        ),
        "pace_environment": (0.15, ("team_pace_avg_10", "opponent_pace_avg_10", "total")),
        "lineup_context": (0.10, ("team_injuries", "is_center", "is_forward", "role_stability")),
        "opponent_context": (
            0.10,
            ("opponent_allowed_rebounds_10", "opponent_allowed_rebounds_per_possession_10", "opponent_position_allowed_rebounds_10"),
        ),
        "market_consensus": (
            0.05,
            ("rebounds_consensus_prob_mean", "rebounds_book_count", "rebounds_line_movement_6h", "rebounds_line_delta_5"),
        ),
    },
    "assists": {
        "minutes_exposure": (0.28, ("minutes_blended", "expected_possessions", "blended_game_pace")),
        "creation": (
            0.30,
            (
                "assist_ratio_blended",
                "touches_per_minute_blended",
                "passes_per_minute_blended",
                "assist_creation_proxy_per_minute_blended",
            ),
        ),
        "team_environment": (0.12, ("usage_rate_blended", "team_injuries", "percentage_assisted_fgm_blended")),
        "pace_environment": (0.10, ("team_pace_avg_10", "opponent_pace_avg_10", "total")),
        "opponent_context": (
            0.10,
            ("opponent_allowed_assists_10", "opponent_allowed_assists_per_possession_10", "opponent_position_allowed_assists_10"),
        ),
        "market_consensus": (
            0.10,
            ("assists_consensus_prob_mean", "assists_book_count", "assists_line_movement_6h", "assists_line_delta_5"),
        ),
    },
    "threes": {
        "minutes_exposure": (0.25, ("minutes_blended", "expected_possessions", "blended_game_pace")),
        "attempt_rate_mix": (
            0.30,
            (
                "estimated_three_point_attempts_per_minute_blended",
                "percentage_field_goals_attempted_3pt_blended",
                "percentage_assisted_3pt_blended",
            ),
        ),
        "usage_touches": (0.10, ("usage_rate_blended", "touches_per_minute_blended")),
        "opponent_environment": (
            0.10,
            ("opponent_allowed_threes_10", "opponent_allowed_threes_per_possession_10", "opponent_position_allowed_threes_10"),
        ),
        "team_environment": (0.10, ("team_pace_avg_10", "opponent_pace_avg_10", "total")),
        "market_consensus": (
            0.15,
            ("threes_consensus_prob_mean", "threes_book_count", "threes_line_movement_6h", "threes_line_delta_5"),
        ),
    },
    "turnovers": {
        "minutes_exposure": (0.20, ("minutes_blended", "expected_possessions", "blended_game_pace")),
        "usage": (0.35, ("usage_rate_blended", "touches_per_minute_blended", "turnover_ratio_blended")),
        "ball_load": (0.10, ("passes_per_minute_blended", "touches_per_minute_blended")),
        "opponent_pressure": (
            0.15,
            ("opponent_allowed_turnovers_10", "opponent_allowed_turnovers_per_possession_10", "opponent_position_allowed_turnovers_10"),
        ),
        "pace_environment": (0.10, ("team_pace_avg_10", "opponent_pace_avg_10", "total")),
        "market_consensus": (
            0.10,
            ("turnovers_consensus_prob_mean", "turnovers_book_count", "turnovers_line_movement_6h", "turnovers_line_delta_5"),
        ),
    },
    "pra": {
        "minutes_exposure": (0.30, ("minutes_blended", "expected_possessions", "blended_game_pace")),
        "usage": (0.20, ("usage_rate_blended", "touches_per_minute_blended", "scoring_opportunities_blended")),
        "creation": (0.20, ("assist_creation_proxy_per_minute_blended", "rebound_chances_total_per_minute_blended")),
        "pace_environment": (0.10, ("team_pace_avg_10", "opponent_pace_avg_10", "total")),
        "opponent_context": (0.10, ("opponent_allowed_pra_10", "opponent_position_allowed_pra_10")),
        "market_consensus": (0.10, ("pra_consensus_prob_mean", "pra_book_count", "pra_line_movement_6h", "pra_line_delta_5")),
    },
}


def add_market_group_features(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    for market_key, groups in MARKET_GROUP_PRIORS.items():
        for group_name, (prior_weight, columns) in groups.items():
            available = [column for column in columns if column in result.columns]
            if not available:
                result[f"{market_key}_group_{group_name}"] = 0.0
                continue
            scaled = []
            for column in available:
                values = pd.to_numeric(result[column], errors="coerce").fillna(0.0)
                if values.abs().max() > 5:
                    values = np.log1p(np.clip(values, 0.0, None))
                scaled.append(values)
            group_score = pd.concat(scaled, axis=1).mean(axis=1)
            result[f"{market_key}_group_{group_name}"] = prior_weight * group_score
    return result


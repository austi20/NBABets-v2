from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd

from app.training.pipeline import TrainingPipeline


def _fixture_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "game_id": 7001,
                "player_id": 41,
                "player_name": "Player A",
                "game_date": pd.Timestamp("2026-02-01"),
                "start_time": pd.Timestamp("2026-02-01 19:00:00"),
                "player_team_id": 90,
                "team_id": 90,
                "position": "G",
                "predicted_minutes": 30.0,
                "minutes_avg_10": 30.0,
                "blended_game_pace": 96.0,
                "usage_rate_blended": 0.22,
                "field_goal_attempts_per_minute_blended": 0.62,
                "free_throw_attempts_per_minute_blended": 0.18,
                "assist_creation_proxy_per_minute_blended": 0.26,
                "rebound_chances_total_per_minute_blended": 0.34,
                "estimated_three_point_attempts_per_minute_blended": 0.31,
                "touches_per_minute_blended": 2.9,
                "passes_per_minute_blended": 2.4,
            }
        ]
    )


def _pipeline() -> TrainingPipeline:
    session = MagicMock()
    session.bind = MagicMock()
    return TrainingPipeline(session)


def test_rotation_treatment_features_only_keeps_effective_minutes(monkeypatch) -> None:
    monkeypatch.setenv("ROTATION_SHOCK_ENABLED", "true")
    monkeypatch.setenv("ROTATION_SHOCK_ABLATION_MODE", "features-only")
    pipeline = _pipeline()
    frame = _fixture_frame()

    treated = pipeline._apply_rotation_treatment_mode(frame, write_audit=False)

    assert treated["predicted_minutes"].iloc[0] == 30.0
    assert treated["baseline_projected_minutes"].iloc[0] == 30.0
    assert treated["adjusted_projected_minutes"].iloc[0] == 30.0
    assert "adjusted_field_goal_attempts_per_minute" in treated.columns
    assert "adjusted_passes_per_minute" in treated.columns


def test_rotation_treatment_full_uses_adjusted_minutes_for_effective_input(monkeypatch) -> None:
    monkeypatch.setenv("ROTATION_SHOCK_ENABLED", "true")
    monkeypatch.setenv("ROTATION_SHOCK_ABLATION_MODE", "full")
    pipeline = _pipeline()
    frame = _fixture_frame()
    monkeypatch.setattr(
        pipeline,
        "_apply_rotation_adjustments",
        lambda base_frame, write_audit, historical_frame=None: base_frame.assign(
            adjusted_projected_minutes=36.0,
            adjusted_usage_share=0.27,
            adjusted_usage_rate=0.27,
        ),
    )

    treated = pipeline._apply_rotation_treatment_mode(frame, write_audit=False)

    assert treated["baseline_projected_minutes"].iloc[0] == 30.0
    assert treated["adjusted_projected_minutes"].iloc[0] == 36.0
    assert treated["predicted_minutes"].iloc[0] == 36.0
    assert treated["expected_possessions"].iloc[0] == 72.0

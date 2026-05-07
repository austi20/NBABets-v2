from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd

from app.training.features import FeatureEngineer
from app.training.models import _market_feature_columns
from app.training.pipeline import (
    TrainingPipeline,
    _availability_branch_context,
    _legacy_pipeline_enabled,
    _lookup_availability_context,
    _normalize_shadow_official_absences,
    _rotation_shock_mode,
    _stat_feature_columns,
)


def test_feature_allowlist_includes_adjusted_columns() -> None:
    frame = pd.DataFrame(
        [
            {
                "adjusted_projected_minutes": 31.0,
                "adjusted_usage_rate": 0.24,
                "adjusted_field_goal_attempts_per_minute": 0.58,
                "line_value": 21.5,
            }
        ]
    )

    columns = FeatureEngineer()._feature_columns(frame)

    assert "adjusted_projected_minutes" in columns
    assert "adjusted_usage_rate" in columns
    assert "adjusted_field_goal_attempts_per_minute" in columns


def test_adjusted_columns_reach_market_model_feature_selection() -> None:
    frame = pd.DataFrame(
        [
            {
                "predicted_minutes": 34.0,
                "adjusted_projected_minutes": 34.0,
                "adjusted_usage_rate": 0.27,
                "adjusted_field_goal_attempts_per_minute": 0.67,
                "adjusted_free_throw_attempts_per_minute": 0.22,
                "adjusted_rebound_chances_total_per_minute": 0.48,
                "adjusted_assist_creation_proxy_per_minute": 0.31,
                "rotation_shock_magnitude": 12.0,
            }
        ]
    )

    feature_columns = _stat_feature_columns(FeatureEngineer(), frame, [])
    points_columns = _market_feature_columns(frame, "points", feature_columns)
    rebounds_columns = _market_feature_columns(frame, "rebounds", feature_columns)
    assists_columns = _market_feature_columns(frame, "assists", feature_columns)

    assert "adjusted_projected_minutes" in points_columns
    assert "adjusted_usage_rate" in points_columns
    assert "adjusted_field_goal_attempts_per_minute" in points_columns
    assert "adjusted_rebound_chances_total_per_minute" in rebounds_columns
    assert "adjusted_assist_creation_proxy_per_minute" in assists_columns
    assert "rotation_shock_magnitude" in points_columns


def test_post_tip_official_inactive_is_downgraded_to_post_hoc() -> None:
    frame = pd.DataFrame(
        [
            {
                "game_id": 7001,
                "team_id": 90,
                "player_id": 41,
                "player_name": "Pregame Out",
                "position": "G",
                "status": "inactive",
                "report_timestamp": pd.Timestamp("2026-02-01 17:00:00"),
                "start_time": pd.Timestamp("2026-02-01 19:00:00"),
            },
            {
                "game_id": 7001,
                "team_id": 90,
                "player_id": 42,
                "player_name": "Post Tip Out",
                "position": "F",
                "status": "inactive",
                "report_timestamp": pd.Timestamp("2026-02-01 19:30:00"),
                "start_time": pd.Timestamp("2026-02-01 19:00:00"),
            },
        ]
    )

    normalized = _normalize_shadow_official_absences(frame)
    by_player = {int(row["player_id"]): row for row in normalized.to_dict("records")}

    assert by_player[41]["source"] == "official_inactive"
    assert by_player[41]["rotation_shock_confidence"] == 1.0
    assert by_player[42]["source"] == "post_hoc"
    assert by_player[42]["rotation_shock_confidence"] == 0.5


def test_rotation_metadata_includes_weights_hash(tmp_path, monkeypatch) -> None:
    artifact_dir = tmp_path / "data" / "artifacts"
    artifact_dir.mkdir(parents=True)
    artifact_path = artifact_dir / "rotation_weights.parquet"
    artifact_path.write_bytes(b"rotation-weights")

    session = MagicMock()
    session.bind = MagicMock()
    pipeline = TrainingPipeline(session)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ROTATION_SHOCK_ENABLED", "true")
    monkeypatch.setenv("ROTATION_SHOCK_ABLATION_MODE", "full")

    metadata = pipeline._rotation_metadata()

    assert metadata["enabled"] is True
    assert metadata["ablation_mode"] == "full"
    assert metadata["version"]
    assert isinstance(metadata["weights_artifact_hash"], str)
    assert len(metadata["weights_artifact_hash"]) == 64
    assert metadata["legacy_pipeline_enabled"] is True


def test_rotation_shock_promoted_default_is_full(monkeypatch) -> None:
    monkeypatch.delenv("ROTATION_SHOCK_ENABLED", raising=False)
    monkeypatch.delenv("ROTATION_SHOCK_ABLATION_MODE", raising=False)

    assert _rotation_shock_mode() == "full"


def test_rotation_shock_explicit_false_is_rollback(monkeypatch) -> None:
    monkeypatch.setenv("ROTATION_SHOCK_ENABLED", "false")
    monkeypatch.delenv("ROTATION_SHOCK_ABLATION_MODE", raising=False)

    assert _rotation_shock_mode() == "off"
    assert _legacy_pipeline_enabled() is True


def test_availability_branch_context_counts_uncertain_players() -> None:
    frame = pd.DataFrame(
        [
            {"game_id": 1, "player_team_id": 10, "player_id": 101},
            {"game_id": 1, "player_team_id": 10, "player_id": 102},
            {"game_id": 1, "player_team_id": 10, "player_id": 103},
        ]
    )
    absences = pd.DataFrame(
        [
            {"game_id": 1, "team_id": 10, "player_id": 101, "play_probability": 0.5},
            {"game_id": 1, "team_id": 10, "player_id": 102, "play_probability": 0.2},
        ]
    )

    context = _availability_branch_context(frame, absences, max_exact_players=8, sampled_branch_count=10000)
    row_context = _lookup_availability_context(context, {"game_id": 1, "player_team_id": 10, "player_id": 101})

    assert row_context["availability_branches"] == 4
    assert row_context["dnp_risk"] == 0.5

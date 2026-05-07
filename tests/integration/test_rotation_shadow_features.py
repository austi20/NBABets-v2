from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd

from app.services import rotation_audit
from app.training.pipeline import TrainingPipeline
from app.training.rotation import RotationWeightTable


def _shadow_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "game_id": 1001,
                "player_id": 2,
                "player_name": "B",
                "game_date": pd.Timestamp("2026-01-01"),
                "start_time": pd.Timestamp("2026-01-01 19:00:00"),
                "player_team_id": 10,
                "team_id": 10,
                "position": "F",
                "starter_flag": 1.0,
                "predicted_minutes": 30.0,
                "usage_rate_avg_10": 0.20,
                "usage_proxy": 14.0,
            },
            {
                "game_id": 1001,
                "player_id": 3,
                "player_name": "C",
                "game_date": pd.Timestamp("2026-01-01"),
                "start_time": pd.Timestamp("2026-01-01 19:00:00"),
                "player_team_id": 10,
                "team_id": 10,
                "position": "G",
                "starter_flag": 0.0,
                "predicted_minutes": 30.0,
                "usage_rate_avg_10": 0.16,
                "usage_proxy": 10.0,
            },
        ]
    )


def _absence_profiles() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "game_id": 1001,
                "team_id": 10,
                "player_id": 1,
                "player_name": "A",
                "position": "G",
                "status": "out",
                "source": "injury_report",
                "rotation_shock_confidence": 1.0,
                "play_probability": 0.0,
                "season": 2025,
                "baseline_minutes": 20.0,
                "baseline_usage_share": 0.30,
                "baseline_usage_rate": 0.30,
                "baseline_usage_proxy": 18.0,
                "baseline_assist_share": 0.20,
                "baseline_rebound_share": 0.05,
                "baseline_three_point_share": 0.20,
                "starter_flag": 1.0,
                "archetype_label": "primary_creator",
            },
        ]
    )


def test_rotation_shadow_mode_adds_adjusted_columns_and_writes_audit(tmp_path, monkeypatch) -> None:
    session = MagicMock()
    session.bind = MagicMock()
    pipeline = TrainingPipeline(session)
    monkeypatch.setenv("ROTATION_SHOCK_SHADOW_MODE", "true")
    monkeypatch.setattr(rotation_audit, "AUDIT_ROOT", tmp_path / "rotation_audit")
    monkeypatch.setattr(pipeline, "_load_rotation_weights_table", lambda: RotationWeightTable())
    monkeypatch.setattr(pipeline, "_load_rotation_shadow_absence_profiles", lambda _frame: _absence_profiles())
    adjusted = pipeline._apply_rotation_shadow_mode(_shadow_frame())

    assert "baseline_projected_minutes" in adjusted.columns
    assert "adjusted_projected_minutes" in adjusted.columns
    assert "adjusted_usage_share" in adjusted.columns
    assert len(adjusted) == 2
    assert adjusted["baseline_projected_minutes"].sum() == 60.0
    assert adjusted["adjusted_projected_minutes"].sum() == 80.0
    assert set(adjusted["adjusted_projected_minutes"]) == {40.0}

    payload = rotation_audit.get_redistribution(1001)
    assert len(payload["absences"]) == 1
    assert len(payload["adjustments"]) == 2
    assert len(payload["team_environment"]) == 1
    assert payload["absences"][0]["player_id"] == 1
    assert "minutes_delta" not in payload["absences"][0]


def test_rotation_shadow_mode_off_is_noop(monkeypatch) -> None:
    session = MagicMock()
    session.bind = MagicMock()
    pipeline = TrainingPipeline(session)
    monkeypatch.delenv("ROTATION_SHOCK_SHADOW_MODE", raising=False)
    frame = _shadow_frame()

    adjusted = pipeline._apply_rotation_shadow_mode(frame)

    assert adjusted is frame
    assert "adjusted_projected_minutes" not in adjusted.columns


def test_rotation_shadow_mode_overlays_uncertain_active_player(tmp_path, monkeypatch) -> None:
    session = MagicMock()
    session.bind = MagicMock()
    pipeline = TrainingPipeline(session)
    monkeypatch.setenv("ROTATION_SHOCK_SHADOW_MODE", "true")
    monkeypatch.setattr(rotation_audit, "AUDIT_ROOT", tmp_path / "rotation_audit")
    monkeypatch.setattr(pipeline, "_load_rotation_weights_table", lambda: RotationWeightTable())
    uncertain_profile = pd.DataFrame(
        [
            {
                **_absence_profiles().iloc[0].to_dict(),
                "player_id": 2,
                "player_name": "B",
                "status": "questionable",
                "play_probability": 0.5,
            }
        ]
    )
    monkeypatch.setattr(pipeline, "_load_rotation_shadow_absence_profiles", lambda _frame: uncertain_profile)

    adjusted = pipeline._apply_rotation_shadow_mode(_shadow_frame())

    adjusted_by_player = dict(zip(adjusted["player_id"], adjusted["adjusted_projected_minutes"], strict=False))
    assert adjusted_by_player[2] == 15.0
    assert adjusted_by_player[3] == 45.0
    payload = rotation_audit.get_redistribution(1001)
    assert payload["absences"][0]["player_id"] == 2
    assert payload["absences"][0]["play_probability"] == 0.5


def test_rotation_shadow_audit_keeps_both_teams(tmp_path, monkeypatch) -> None:
    session = MagicMock()
    session.bind = MagicMock()
    pipeline = TrainingPipeline(session)
    monkeypatch.setenv("ROTATION_SHOCK_SHADOW_MODE", "true")
    monkeypatch.setattr(rotation_audit, "AUDIT_ROOT", tmp_path / "rotation_audit")
    monkeypatch.setattr(pipeline, "_load_rotation_weights_table", lambda: RotationWeightTable())
    frame = pd.concat(
        [
            _shadow_frame().assign(game_id=1002),
            _shadow_frame().assign(game_id=1002, player_team_id=20, team_id=20, player_id=[12, 13]),
        ],
        ignore_index=True,
    )
    absences = pd.concat(
        [
            _absence_profiles().assign(game_id=1002),
            _absence_profiles().assign(game_id=1002, team_id=20, player_id=11),
        ],
        ignore_index=True,
    )
    monkeypatch.setattr(pipeline, "_load_rotation_shadow_absence_profiles", lambda _frame: absences)

    pipeline._apply_rotation_shadow_mode(frame)

    payload = rotation_audit.get_redistribution(1002)
    assert {record["team_id"] for record in payload["absences"]} == {10, 20}
    assert {record["team_id"] for record in payload["team_environment"]} == {10, 20}

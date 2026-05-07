from __future__ import annotations

from app.services import rotation_audit


def test_rotation_audit_roundtrip(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(rotation_audit, "AUDIT_ROOT", tmp_path / "rotation_audit")
    path = rotation_audit.write_game_audit(
        game_id=9001,
        absences=[{"game_id": 9001, "team_id": 10, "player_id": 1, "status": "out"}],
        adjustments=[{"game_id": 9001, "team_id": 10, "player_id": 2, "minutes_delta": 3.5}],
        team_environment=[
            {"game_id": 9001, "team_id": 10, "pace_delta": -0.01},
            {"game_id": 9001, "team_id": 20, "pace_delta": 0.01},
        ],
    )
    assert path.exists()
    payload = rotation_audit.get_redistribution(9001)
    assert len(payload["absences"]) == 1
    assert len(payload["adjustments"]) == 1
    assert len(payload["team_environment"]) == 2
    assert payload["adjustments"][0]["player_id"] == 2
    assert "pace_delta" not in payload["absences"][0]
    assert "status" not in payload["team_environment"][0]

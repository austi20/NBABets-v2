from __future__ import annotations

import pandas as pd

from app.training.rotation import RotationWeight, RotationWeightTable
from app.training.rotation_weights import LEAGUE_TEAM_SENTINEL, aggregate_rotation_weights
from scripts.learn_rotation_weights import _build_observations, _write_sanity_report


def test_aggregate_rotation_weights_has_required_guards() -> None:
    observations = pd.DataFrame(
        [
            {
                "team_id": 10,
                "season": 2025,
                "absent_archetype": "primary_creator",
                "candidate_archetype": "scoring_wing",
                "minute_delta": 4.0,
                "usage_delta": 2.0,
            },
            {
                "team_id": 10,
                "season": 2025,
                "absent_archetype": "primary_creator",
                "candidate_archetype": "scoring_wing",
                "minute_delta": 3.0,
                "usage_delta": 1.5,
            },
            {
                "team_id": 10,
                "season": 2025,
                "absent_archetype": "primary_creator",
                "candidate_archetype": "rim_big",
                "minute_delta": 1.0,
                "usage_delta": 0.4,
            },
            {
                "team_id": 12,
                "season": 2025,
                "absent_archetype": "primary_creator",
                "candidate_archetype": "scoring_wing",
                "minute_delta": 2.0,
                "usage_delta": 1.0,
            },
        ]
    )
    weights = aggregate_rotation_weights(observations)
    assert not weights.empty
    assert weights["minute_gain_weight"].isna().sum() == 0
    assert weights["usage_gain_weight"].isna().sum() == 0
    assert (weights["minute_gain_weight"] >= 0).all()
    assert (weights["usage_gain_weight"] >= 0).all()
    assert (weights["sample_size"] >= 1).all()
    assert (weights["team_id"] == LEAGUE_TEAM_SENTINEL).any()
    assert "last_updated" in weights.columns
    assert weights["last_updated"].notna().all()

    # Weights should normalize within each absent archetype cell.
    team_cell = weights[
        (weights["team_id"] == "10")
        & (weights["season"] == 2025)
        & (weights["absent_archetype"] == "primary_creator")
    ]
    assert abs(float(team_cell["minute_gain_weight"].sum()) - 1.0) < 1e-6
    assert abs(float(team_cell["usage_gain_weight"].sum()) - 1.0) < 1e-6


def test_weight_lookup_prefers_league_over_team_fallback() -> None:
    observations = pd.DataFrame(
        [
            {
                "team_id": 10,
                "season": 2025,
                "absent_archetype": "primary_creator",
                "candidate_archetype": "scoring_wing",
                "minute_delta": 4.0,
                "usage_delta": 2.0,
            },
            {
                "team_id": 12,
                "season": 2025,
                "absent_archetype": "primary_creator",
                "candidate_archetype": "scoring_wing",
                "minute_delta": 5.0,
                "usage_delta": 2.5,
            },
            {
                "team_id": 12,
                "season": 2025,
                "absent_archetype": "primary_creator",
                "candidate_archetype": "scoring_wing",
                "minute_delta": 5.5,
                "usage_delta": 2.8,
            },
        ]
    )
    weights = aggregate_rotation_weights(observations)
    team_10_row = weights[(weights["team_id"] == "10") & (weights["candidate_archetype"] == "scoring_wing")].iloc[0]
    assert team_10_row["weight_source"] == "fallback"

    table = RotationWeightTable(
        [
            RotationWeight(
                team_id=row["team_id"],
                season=int(row["season"]),
                absent_archetype=row["absent_archetype"],
                candidate_archetype=row["candidate_archetype"],
                minute_gain_weight=float(row["minute_gain_weight"]),
                usage_gain_weight=float(row["usage_gain_weight"]),
                sample_size=int(row["sample_size"]),
                weight_source=row["weight_source"],
            )
            for row in weights.to_dict("records")
        ]
    )
    _, _, source = table.lookup("primary_creator", "scoring_wing", team_id=10, season=2025)
    assert source == "league"


def test_build_observations_uses_explicit_absences_and_proportional_attribution() -> None:
    historical = pd.DataFrame(
        [
            *_prior_rows(player_id=1, player_name="Absent Low Usage", fga=10.0, minutes=30.0),
            *_prior_rows(player_id=2, player_name="Absent High Usage", fga=20.0, minutes=30.0),
            *_prior_rows(player_id=3, player_name="Candidate A", fga=8.0, minutes=20.0),
            *_prior_rows(player_id=4, player_name="Candidate B", fga=8.0, minutes=20.0),
            _history_row(
                game_id=99,
                game_date="2026-01-08",
                player_id=3,
                player_name="Candidate A",
                minutes=30.0,
                fga=14.0,
            ),
            _history_row(
                game_id=99,
                game_date="2026-01-08",
                player_id=4,
                player_name="Candidate B",
                minutes=25.0,
                fga=10.0,
            ),
        ]
    )
    absences = pd.DataFrame(
        [
            {
                "game_id": 99,
                "team_id": 10,
                "player_id": 1,
                "player_name": "Absent Low Usage",
                "position": "G",
                "status": "out",
                "source": "injury_report",
                "rotation_shock_confidence": 1.0,
            },
            {
                "game_id": 99,
                "team_id": 10,
                "player_id": 2,
                "player_name": "Absent High Usage",
                "position": "G",
                "status": "out",
                "source": "injury_report",
                "rotation_shock_confidence": 1.0,
            },
        ]
    )

    observations = _build_observations(historical, absences, include_post_hoc=False)

    assert len(observations) == 4
    by_absent = observations.groupby("absent_player_id")["minute_delta"].sum().to_dict()
    assert abs(by_absent[1] - 5.0) < 1e-6
    assert abs(by_absent[2] - 10.0) < 1e-6
    assert set(observations["candidate_player_id"]) == {3, 4}


def test_sanity_report_contains_required_diagnostics(tmp_path) -> None:
    observations = pd.DataFrame(
        [
            {
                "team_id": 10,
                "season": 2025,
                "absent_archetype": "bench_depth",
                "candidate_archetype": "scoring_wing",
                "minute_delta": 8.0,
                "usage_delta": 2.0,
            },
            {
                "team_id": 10,
                "season": 2025,
                "absent_archetype": "bench_depth",
                "candidate_archetype": "rim_big",
                "minute_delta": 1.0,
                "usage_delta": 0.5,
            },
        ]
    )
    weights = aggregate_rotation_weights(observations)
    report_path = _write_sanity_report(tmp_path, weights)

    report = report_path.read_text(encoding="utf-8")
    assert "Extreme Minute Transfer Cells" in report
    assert "Suspicious Bench Depth Absences" in report
    assert "Usage Normalization Checks" in report
    assert "Fallback diagnostic rows" in report


def _prior_rows(*, player_id: int, player_name: str, fga: float, minutes: float) -> list[dict[str, object]]:
    return [
        _history_row(
            game_id=game_id,
            game_date=f"2026-01-0{game_id}",
            player_id=player_id,
            player_name=player_name,
            minutes=minutes,
            fga=fga,
        )
        for game_id in [1, 2, 3]
    ]


def _history_row(
    *,
    game_id: int,
    game_date: str,
    player_id: int,
    player_name: str,
    minutes: float,
    fga: float,
) -> dict[str, object]:
    return {
        "player_id": player_id,
        "player_name": player_name,
        "position": "G",
        "team_id": 10,
        "player_team_id": 10,
        "game_id": game_id,
        "game_date": pd.Timestamp(game_date),
        "start_time": pd.Timestamp(f"{game_date} 19:00:00"),
        "minutes": minutes,
        "field_goal_attempts": fga,
        "free_throw_attempts": 0.0,
        "assists": 4.0,
        "rebounds": 4.0,
        "threes": 2.0,
        "starter_flag": 1.0,
    }

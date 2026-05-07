from __future__ import annotations

import pytest

from app.training.rotation import (
    PlayerRotationProfile,
    RoleVector,
    RotationWeight,
    RotationWeightTable,
    redistribute,
)


def _role(player_id: int, archetype: str) -> RoleVector:
    return RoleVector(
        player_id=player_id,
        season=2026,
        position_group="G",
        usage_proxy=1.0,
        usage_share=0.2,
        assist_share=0.2,
        rebound_share=0.2,
        three_point_rate=0.35,
        rim_attempt_rate=0.2,
        touches_per_minute=2.0,
        passes_per_minute=1.0,
        rebound_chances_per_minute=0.2,
        blocks_per_minute=0.02,
        starter_score=0.8,
        role_stability=0.9,
        archetype_label=archetype,
    )


def _player(
    player_id: int,
    *,
    player_name: str | None = None,
    team_id: int = 10,
    game_id: int = 1,
    status: str = "available",
    position_group: str = "G",
    baseline_minutes: float = 30.0,
    baseline_usage_share: float = 0.20,
    archetype: str = "scoring_wing",
) -> PlayerRotationProfile:
    return PlayerRotationProfile(
        game_id=game_id,
        team_id=team_id,
        player_id=player_id,
        player_name=player_name or f"Player {player_id}",
        status=status,
        position_group=position_group,
        baseline_minutes=baseline_minutes,
        baseline_usage_share=baseline_usage_share,
        role_vector=_role(player_id, archetype),
    )


def test_redistribute_zero_absence_is_noop() -> None:
    players = [
        PlayerRotationProfile(
            game_id=1,
            team_id=10,
            player_id=1,
            player_name="A",
            status="available",
            position_group="G",
            baseline_minutes=32.0,
            baseline_usage_share=0.28,
            role_vector=_role(1, "primary_creator"),
        ),
        PlayerRotationProfile(
            game_id=1,
            team_id=10,
            player_id=2,
            player_name="B",
            status="available",
            position_group="F",
            baseline_minutes=30.0,
            baseline_usage_share=0.22,
            role_vector=_role(2, "scoring_wing"),
        ),
    ]
    result = redistribute(
        game_id=1,
        team_id=10,
        players=players,
        weights=RotationWeightTable(),
        play_probabilities={1: 1.0, 2: 1.0},
        mode="expected_value",
    )
    assert len(result.absences) == 0
    assert len(result.teammate_adjustments) == 0
    assert len(result.mass_conservation_warnings) == 0
    assert [p.adjusted_minutes for p in result.adjusted_players] == [32.0, 30.0]


def test_redistribute_conserves_minutes_and_usage_mass() -> None:
    players = [
        PlayerRotationProfile(
            game_id=1,
            team_id=10,
            player_id=1,
            player_name="Out Player",
            status="out",
            position_group="G",
            baseline_minutes=34.0,
            baseline_usage_share=0.30,
            role_vector=_role(1, "primary_creator"),
        ),
        PlayerRotationProfile(
            game_id=1,
            team_id=10,
            player_id=2,
            player_name="Wing",
            status="available",
            position_group="F",
            baseline_minutes=30.0,
            baseline_usage_share=0.20,
            role_vector=_role(2, "scoring_wing"),
        ),
        PlayerRotationProfile(
            game_id=1,
            team_id=10,
            player_id=3,
            player_name="Big",
            status="available",
            position_group="C",
            baseline_minutes=28.0,
            baseline_usage_share=0.18,
            role_vector=_role(3, "rim_big"),
        ),
    ]
    weights = RotationWeightTable(
        [
            RotationWeight(
                team_id=10,
                season=2026,
                absent_archetype="primary_creator",
                candidate_archetype="scoring_wing",
                minute_gain_weight=0.7,
                usage_gain_weight=0.8,
            ),
            RotationWeight(
                team_id=10,
                season=2026,
                absent_archetype="primary_creator",
                candidate_archetype="rim_big",
                minute_gain_weight=0.3,
                usage_gain_weight=0.2,
            ),
        ]
    )
    result = redistribute(
        game_id=1,
        team_id=10,
        players=players,
        weights=weights,
        play_probabilities={1: 0.0, 2: 1.0, 3: 1.0},
        mode="expected_value",
    )
    assert len(result.mass_conservation_warnings) == 0
    adjusted = {player.player_id: player for player in result.adjusted_players}
    assert adjusted[1].adjusted_minutes == 0.0
    removed_minutes = 34.0
    gained_minutes = (adjusted[2].adjusted_minutes - 30.0) + (adjusted[3].adjusted_minutes - 28.0)
    assert abs(removed_minutes - gained_minutes) < 1e-6
    removed_usage = 0.30
    gained_usage = (adjusted[2].adjusted_usage_share - 0.20) + (adjusted[3].adjusted_usage_share - 0.18)
    assert abs(removed_usage - gained_usage) < 1e-6


def test_redistribute_scopes_candidates_to_requested_team_and_game() -> None:
    players = [
        _player(
            1,
            player_name="Team10 Out",
            status="out",
            baseline_minutes=30.0,
            baseline_usage_share=0.25,
            archetype="primary_creator",
        ),
        _player(2, player_name="Team10 Teammate", baseline_minutes=10.0),
        _player(3, player_name="Opponent", team_id=20, baseline_minutes=25.0),
        _player(4, player_name="Other Game", game_id=2, baseline_minutes=25.0),
    ]
    result = redistribute(
        game_id=1,
        team_id=10,
        players=players,
        weights=RotationWeightTable(),
        play_probabilities={1: 0.0, 2: 1.0, 3: 1.0, 4: 1.0},
        mode="expected_value",
    )

    adjusted = {player.player_id: player for player in result.adjusted_players}
    assert set(adjusted) == {1, 2}
    assert adjusted[2].adjusted_minutes == pytest.approx(40.0)


def test_redistribute_prefers_team_season_weights_over_other_teams() -> None:
    players = [
        _player(1, status="out", baseline_minutes=20.0, baseline_usage_share=0.20, archetype="primary_creator"),
        _player(2, baseline_minutes=20.0, archetype="spacing_guard"),
        _player(3, position_group="C", baseline_minutes=20.0, archetype="rim_big"),
    ]
    weights = RotationWeightTable(
        [
            RotationWeight(999, 2026, "primary_creator", "spacing_guard", 1.0, 1.0),
            RotationWeight(999, 2026, "primary_creator", "rim_big", 0.0, 0.0),
            RotationWeight(10, 2026, "primary_creator", "spacing_guard", 0.0, 0.0),
            RotationWeight(10, 2026, "primary_creator", "rim_big", 1.0, 1.0),
        ]
    )

    result = redistribute(
        game_id=1,
        team_id=10,
        players=players,
        weights=weights,
        play_probabilities={1: 0.0, 2: 1.0, 3: 1.0},
        mode="expected_value",
    )

    adjusted = {player.player_id: player for player in result.adjusted_players}
    assert adjusted[2].adjusted_minutes == pytest.approx(20.0)
    assert adjusted[3].adjusted_minutes == pytest.approx(40.0)


def test_redistribute_multiple_absences_are_weighted_by_removed_mass() -> None:
    players = [
        _player(1, status="out", baseline_minutes=35.0, baseline_usage_share=0.20, archetype="primary_creator"),
        _player(2, status="out", baseline_minutes=5.0, baseline_usage_share=0.05, archetype="bench_depth"),
        _player(3, baseline_minutes=10.0, baseline_usage_share=0.05, archetype="spacing_guard"),
        _player(4, position_group="C", baseline_minutes=10.0, baseline_usage_share=0.05, archetype="rim_big"),
    ]
    weights = RotationWeightTable(
        [
            RotationWeight(10, 2026, "primary_creator", "spacing_guard", 1.0, 1.0),
            RotationWeight(10, 2026, "primary_creator", "rim_big", 0.0, 0.0),
            RotationWeight(10, 2026, "bench_depth", "spacing_guard", 0.0, 0.0),
            RotationWeight(10, 2026, "bench_depth", "rim_big", 1.0, 1.0),
        ]
    )

    result = redistribute(
        game_id=1,
        team_id=10,
        players=players,
        weights=weights,
        play_probabilities={1: 0.0, 2: 0.0, 3: 1.0, 4: 1.0},
        mode="expected_value",
    )

    adjusted = {player.player_id: player for player in result.adjusted_players}
    assert adjusted[3].adjusted_minutes == pytest.approx(45.0)
    assert adjusted[4].adjusted_minutes == pytest.approx(15.0)


def test_redistribute_uncertain_player_does_not_absorb_own_absence() -> None:
    players = [
        _player(1, status="questionable", baseline_minutes=30.0, baseline_usage_share=0.30, archetype="primary_creator"),
        _player(2, baseline_minutes=30.0, baseline_usage_share=0.20, archetype="scoring_wing"),
    ]

    result = redistribute(
        game_id=1,
        team_id=10,
        players=players,
        weights=RotationWeightTable(),
        play_probabilities={1: 0.5, 2: 1.0},
        mode="expected_value",
    )

    adjusted = {player.player_id: player for player in result.adjusted_players}
    assert adjusted[1].adjusted_minutes == pytest.approx(15.0)
    assert adjusted[1].adjusted_usage_share == pytest.approx(0.15)
    assert adjusted[2].adjusted_minutes == pytest.approx(45.0)
    assert adjusted[2].adjusted_usage_share == pytest.approx(0.35)
    assert [adjustment.player_id for adjustment in result.teammate_adjustments] == [2]


def test_redistribute_caps_impossible_minutes_and_warns() -> None:
    players = [
        _player(1, status="out", baseline_minutes=40.0, baseline_usage_share=0.30, archetype="primary_creator"),
        _player(2, baseline_minutes=44.0, baseline_usage_share=0.28, archetype="scoring_wing"),
    ]

    result = redistribute(
        game_id=1,
        team_id=10,
        players=players,
        weights=RotationWeightTable(),
        play_probabilities={1: 0.0, 2: 1.0},
        mode="expected_value",
    )

    adjusted = {player.player_id: player for player in result.adjusted_players}
    assert adjusted[2].adjusted_minutes == pytest.approx(48.0)
    assert "minutes_capacity_exhausted" in result.mass_conservation_warnings
    assert "minutes_mass_not_conserved" in result.mass_conservation_warnings

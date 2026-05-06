from __future__ import annotations

import pandas as pd

from app.training.data_sufficiency import annotate_tiers, classify_data_sufficiency_tier


def test_classify_data_sufficiency_tier_a_defaults() -> None:
    assert classify_data_sufficiency_tier(
        historical_games=14,
        historical_minutes_total=420.0,
        recent_avg_minutes=31.5,
        team_changed=False,
    ) == "A"


def test_classify_data_sufficiency_tier_b_for_mid_history() -> None:
    assert classify_data_sufficiency_tier(
        historical_games=7,
        historical_minutes_total=160.0,
        recent_avg_minutes=24.0,
        team_changed=False,
    ) == "B"


def test_classify_data_sufficiency_tier_c_for_rookie_sample() -> None:
    assert classify_data_sufficiency_tier(
        historical_games=3,
        historical_minutes_total=54.0,
        recent_avg_minutes=18.0,
        team_changed=False,
    ) == "C"


def test_classify_data_sufficiency_tier_d_for_zero_history() -> None:
    assert classify_data_sufficiency_tier(
        historical_games=0,
        historical_minutes_total=0.0,
        recent_avg_minutes=0.0,
        team_changed=False,
    ) == "D"


def test_classify_data_sufficiency_tier_downgrades_on_team_change() -> None:
    assert classify_data_sufficiency_tier(
        historical_games=14,
        historical_minutes_total=420.0,
        recent_avg_minutes=31.5,
        team_changed=True,
    ) == "B"


def test_annotate_tiers_preserves_all_rows() -> None:
    upcoming = pd.DataFrame(
        [
            {"player_id": 1, "game_id": 100, "_team_changed": False},
            {"player_id": 2, "game_id": 100, "_team_changed": False},
            {"player_id": 3, "game_id": 100, "_team_changed": True},
            {"player_id": 4, "game_id": 100, "_team_changed": False},
        ]
    )
    historical = pd.DataFrame(
        [
            {"player_id": 1, "game_id": 1, "minutes": 32.0, "game_date": "2026-03-01"},
            {"player_id": 1, "game_id": 2, "minutes": 34.0, "game_date": "2026-03-03"},
            {"player_id": 1, "game_id": 3, "minutes": 31.0, "game_date": "2026-03-05"},
            {"player_id": 1, "game_id": 4, "minutes": 33.0, "game_date": "2026-03-07"},
            {"player_id": 1, "game_id": 5, "minutes": 35.0, "game_date": "2026-03-09"},
            {"player_id": 1, "game_id": 6, "minutes": 34.0, "game_date": "2026-03-11"},
            {"player_id": 1, "game_id": 7, "minutes": 32.0, "game_date": "2026-03-13"},
            {"player_id": 1, "game_id": 8, "minutes": 31.0, "game_date": "2026-03-15"},
            {"player_id": 1, "game_id": 9, "minutes": 30.0, "game_date": "2026-03-17"},
            {"player_id": 1, "game_id": 10, "minutes": 29.0, "game_date": "2026-03-19"},
            {"player_id": 2, "game_id": 11, "minutes": 24.0, "game_date": "2026-03-01"},
            {"player_id": 2, "game_id": 12, "minutes": 22.0, "game_date": "2026-03-03"},
            {"player_id": 2, "game_id": 13, "minutes": 25.0, "game_date": "2026-03-05"},
            {"player_id": 2, "game_id": 14, "minutes": 26.0, "game_date": "2026-03-07"},
            {"player_id": 2, "game_id": 15, "minutes": 28.0, "game_date": "2026-03-09"},
            {"player_id": 3, "game_id": 16, "minutes": 21.0, "game_date": "2026-03-01"},
            {"player_id": 3, "game_id": 17, "minutes": 20.0, "game_date": "2026-03-03"},
            {"player_id": 3, "game_id": 18, "minutes": 19.0, "game_date": "2026-03-05"},
        ]
    )
    historical["game_date"] = pd.to_datetime(historical["game_date"])

    annotated = annotate_tiers(upcoming=upcoming, historical=historical)

    assert len(annotated) == 4
    assert list(annotated["_data_sufficiency_tier"]) == ["A", "B", "D", "D"]

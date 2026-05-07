from __future__ import annotations

from app.training.rotation import status_to_play_probability


def test_official_inactive_overrides_status() -> None:
    assert status_to_play_probability("probable", official_inactive=True) == 0.0


def test_expected_availability_flag_floor_and_cap() -> None:
    assert status_to_play_probability("questionable", expected_availability_flag=True) == 0.85
    assert status_to_play_probability("available", expected_availability_flag=False) == 0.0


def test_default_status_mapping() -> None:
    assert status_to_play_probability("out") == 0.0
    assert status_to_play_probability("doubtful") == 0.15
    assert status_to_play_probability("questionable") == 0.50
    assert status_to_play_probability("probable") == 0.85
    assert status_to_play_probability("available") == 1.0


def test_out_for_season_is_zero() -> None:
    assert status_to_play_probability("Out For Season") == 0.0
    assert status_to_play_probability("out for season") == 0.0

from __future__ import annotations

import pytest

from app.services.volatility import (
    archetype_risk,
    classify_archetype,
)


@pytest.mark.parametrize(
    ("starter_rate", "minutes_mean", "expected"),
    [
        (0.95, 32.0, "starter"),
        (0.70, 24.0, "starter"),
        (0.69, 28.0, "rotation"),
        (0.40, 22.0, "rotation"),
        (0.10, 18.0, "rotation"),
        (0.05, 17.5, "bench"),
        (0.0, 10.0, "bench"),
        (0.0, 9.0, "fringe"),
        (0.0, 0.0, "fringe"),
    ],
)
def test_classify_archetype(starter_rate: float, minutes_mean: float, expected: str) -> None:
    assert classify_archetype(starter_flag_rate=starter_rate, minutes_mean_season=minutes_mean) == expected


@pytest.mark.parametrize(
    ("archetype", "expected"),
    [
        ("starter", 0.0),
        ("rotation", 0.3),
        ("bench", 0.7),
        ("fringe", 1.0),
    ],
)
def test_archetype_risk(archetype: str, expected: float) -> None:
    assert archetype_risk(archetype) == expected

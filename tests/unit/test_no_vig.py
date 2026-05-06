from __future__ import annotations

import math

from app.evaluation.no_vig import additive_no_vig, multiplicative_no_vig


def test_multiplicative_no_vig_even_odds_round_trip_to_half() -> None:
    over, under = multiplicative_no_vig(-110, -110)
    assert math.isclose(over, 0.5, rel_tol=1e-9)
    assert math.isclose(under, 0.5, rel_tol=1e-9)


def test_multiplicative_no_vig_preserves_probability_mass() -> None:
    over, under = multiplicative_no_vig(+120, -140)
    assert 0.0 <= over <= 1.0
    assert 0.0 <= under <= 1.0
    assert math.isclose(over + under, 1.0, rel_tol=1e-9)


def test_additive_no_vig_falls_back_to_valid_probabilities() -> None:
    over, under = additive_no_vig(+150, -180)
    assert 0.0 <= over <= 1.0
    assert 0.0 <= under <= 1.0
    assert math.isclose(over + under, 1.0, rel_tol=1e-9)

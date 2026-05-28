from __future__ import annotations

import math

import pytest

from app.evaluation.no_vig import multiplicative_no_vig


def test_multiplicative_no_vig_even_odds_round_trip_to_half() -> None:
    over, under = multiplicative_no_vig(-110, -110)
    assert math.isclose(over, 0.5, rel_tol=1e-9)
    assert math.isclose(under, 0.5, rel_tol=1e-9)


def test_multiplicative_no_vig_preserves_probability_mass() -> None:
    over, under = multiplicative_no_vig(+120, -140)
    assert 0.0 <= over <= 1.0
    assert 0.0 <= under <= 1.0
    assert math.isclose(over + under, 1.0, rel_tol=1e-9)


def test_multiplicative_no_vig_raises_on_non_positive_total() -> None:
    """multiplicative_no_vig must raise ValueError when implied probability total <= 0."""
    # Patch _american_to_probability to return 0 for both sides to trigger the guard
    from app.evaluation import no_vig
    original = no_vig._american_to_probability
    try:
        no_vig._american_to_probability = lambda odds: 0.0
        with pytest.raises(ValueError, match="implied probability total must be positive"):
            multiplicative_no_vig(-110, -110)
    finally:
        no_vig._american_to_probability = original

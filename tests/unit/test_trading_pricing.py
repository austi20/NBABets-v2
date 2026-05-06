from __future__ import annotations

from app.trading.pricing import american_to_prob, no_vig_over_probability, prob_to_clob_price, prob_to_decimal


def test_pricing_probability_converters() -> None:
    assert 0.49 < american_to_prob(-110) < 0.53
    assert prob_to_decimal(0.5) == 2.0
    assert prob_to_clob_price(0.503) == 50


def test_no_vig_probability_uses_phase3_helper() -> None:
    over_prob = no_vig_over_probability(-110, -110)
    assert abs(over_prob - 0.5) < 1e-6

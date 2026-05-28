from __future__ import annotations

from app.evaluation.prop_pricer import price_prop


def test_price_prop_returns_over_decision_with_positive_edge() -> None:
    decision = price_prop(
        prediction={
            "calibration_adjusted_probability": 0.58,
            "under_probability": 0.38,
        },
        line_snapshot={
            "market_key": "player_points",
            "line_value": 24.5,
            "over_odds": -110,
            "under_odds": -110,
        },
    )

    assert decision.recommendation == "OVER"
    assert decision.model_prob == 0.58
    assert round(decision.no_vig_market_prob, 3) == 0.5
    assert decision.ev > 0.0


def test_price_prop_handles_missing_under_price() -> None:
    decision = price_prop(
        prediction={"calibration_adjusted_probability": 0.61},
        line_snapshot={
            "market_key": "player_rebounds",
            "line_value": 8.5,
            "over_odds": -125,
            "under_odds": None,
        },
    )

    assert decision.recommendation == "OVER"
    assert decision.market_prob > 0.5


# Task 3.1 tests
import pytest


def test_ev_subtracts_only_loss_mass_not_push() -> None:
    from app.evaluation.prop_pricer import compute_ev

    # over=0.55, push=0.05, loss=0.40, decimal=2.0
    # ev = 0.55 * (2.0 - 1) - 0.40 * 1 = 0.55 - 0.40 = 0.15
    assert compute_ev(hit_prob=0.55, push_prob=0.05, decimal_odds=2.0) == pytest.approx(0.15)


def test_ev_zero_push_matches_legacy() -> None:
    from app.evaluation.prop_pricer import compute_ev

    # over=0.55, push=0.0, loss=0.45, decimal=2.0
    # ev = 0.55 * 1 - 0.45 * 1 = 0.10
    assert compute_ev(hit_prob=0.55, push_prob=0.0, decimal_odds=2.0) == pytest.approx(0.10)

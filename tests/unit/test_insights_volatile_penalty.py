"""Tests for volatile market penalty in _prop_confidence_score."""
from __future__ import annotations

import pytest

from app.services.insights import _VOLATILE_MARKET_PENALTY, _prop_confidence_score
from app.services.prop_analysis import PropOpportunity, SportsbookQuote


def _make_quote(
    market_key: str = "points",
    hit_probability: float = 0.5,
    is_alternate_line: bool = False,
) -> SportsbookQuote:
    return SportsbookQuote(
        game_id=1,
        sportsbook_key="draftkings",
        sportsbook_name="DraftKings",
        icon="",
        market_key=market_key,
        line_value=20.5,
        over_odds=-110,
        under_odds=-110,
        timestamp="2026-01-01T00:00:00",
        is_live_quote=False,
        verification_status="verified",
        odds_source_provider="draftkings",
        over_probability=0.5,
        under_probability=0.5,
        push_probability=0.0,
        calibrated_over_probability=hit_probability,
        calibrated_under_probability=1.0 - hit_probability,
        recommended_side="over",
        hit_probability=hit_probability,
        is_alternate_line=is_alternate_line,
    )


def _make_opportunity(market_key: str = "points") -> PropOpportunity:
    quote = _make_quote(market_key=market_key)
    return PropOpportunity(
        rank=1,
        game_id=1,
        player_id=1,
        player_name="Test Player",
        player_icon="",
        market_key=market_key,
        consensus_line=20.5,
        projected_mean=21.0,
        recommended_side="over",
        hit_probability=0.5,
        likelihood_score=50,
        calibrated_over_probability=0.5,
        sportsbooks_summary="DraftKings",
        top_features=[],
        quotes=[quote],
        data_confidence_score=1.0,
    )


def _score(market_key: str) -> int:
    opp = _make_opportunity(market_key=market_key)
    quote = _make_quote(market_key=market_key)
    return _prop_confidence_score(
        opportunity=opp,
        best_quote=quote,
        edge=0.0,
        latest_quote_at=None,
        uncertainty_ratio=None,
        injury=None,
        now=None,
    )


def _baseline_score() -> int:
    return _score("points")


def test_threes_docks_10() -> None:
    assert _baseline_score() - _score("threes") == 10


def test_blocks_docks_8() -> None:
    assert _baseline_score() - _score("blocks") == 8


def test_steals_docks_8() -> None:
    assert _baseline_score() - _score("steals") == 8


def test_turnovers_docks_12() -> None:
    assert _baseline_score() - _score("turnovers") == 12


def test_unknown_market_key_docks_0() -> None:
    assert _baseline_score() - _score("assists") == 0


def test_score_clamped_to_minimum_1_with_large_penalty() -> None:
    # Build an opportunity where base score would be very low,
    # then apply a volatile market to confirm floor of 1.
    opp = _make_opportunity("turnovers")
    # Use hit_probability=0.0 -> probability contribution = 0
    quote = _make_quote(market_key="turnovers", hit_probability=0.0)
    opp2 = PropOpportunity(
        rank=1,
        game_id=1,
        player_id=1,
        player_name="Test Player",
        player_icon="",
        market_key="turnovers",
        consensus_line=0.5,
        projected_mean=0.1,
        recommended_side="under",
        hit_probability=0.0,
        likelihood_score=1,
        calibrated_over_probability=0.0,
        sportsbooks_summary="DraftKings",
        top_features=[],
        quotes=[quote],
        data_confidence_score=0.0,
    )
    result = _prop_confidence_score(
        opportunity=opp2,
        best_quote=quote,
        edge=0.0,
        latest_quote_at=None,
        uncertainty_ratio=None,
        injury=None,
        now=None,
    )
    assert result >= 1

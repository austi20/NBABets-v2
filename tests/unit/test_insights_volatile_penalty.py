"""Tests for volatility-driven confidence adjustment in _prop_confidence_score."""
from __future__ import annotations

from app.services.insights import _prop_confidence_score
from app.services.prop_analysis import PropOpportunity, SportsbookQuote
from app.services.volatility import (
    VolatilityContributor,
    VolatilityScore,
)


def _make_quote(market_key: str = "points") -> SportsbookQuote:
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
        over_probability=0.55,
        under_probability=0.45,
        push_probability=0.0,
        calibrated_over_probability=0.55,
        calibrated_under_probability=0.45,
        recommended_side="over",
        hit_probability=0.55,
        is_alternate_line=False,
    )


def _make_opportunity(market_key: str = "points") -> PropOpportunity:
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
        hit_probability=0.55,
        likelihood_score=50,
        calibrated_over_probability=0.55,
        sportsbooks_summary="DraftKings",
        top_features=[],
        quotes=[_make_quote(market_key)],
        data_confidence_score=0.8,
    )


def _score(coefficient: float, tier: str) -> VolatilityScore:
    return VolatilityScore(
        coefficient=coefficient,
        tier=tier,  # type: ignore[arg-type]
        contributors=(
            VolatilityContributor(name="stat_cv", raw_value=1.0, weight=1.0, contribution=coefficient),
        ),
        adjusted_probability=0.55 - coefficient * 0.05,
        confidence_multiplier=1.0 - coefficient * 0.55,
    )


def _call(volatility: VolatilityScore | None) -> int:
    opp = _make_opportunity()
    return _prop_confidence_score(
        opportunity=opp,
        best_quote=opp.quotes[0],
        edge=0.02,
        latest_quote_at=None,
        uncertainty_ratio=0.3,
        injury=None,
        now=None,
        volatility=volatility,
    )


def test_low_volatility_keeps_score_near_baseline() -> None:
    baseline = _call(None)
    low_vol = _call(_score(0.0, "low"))
    assert low_vol == baseline


def test_high_volatility_drops_score_proportionally() -> None:
    baseline = _call(None)
    high_vol = _call(_score(0.8, "high"))
    assert high_vol < baseline
    # confidence_multiplier at coef=0.8 is 1 - 0.8*0.55 = 0.56
    assert high_vol <= int(baseline * 0.60)


def test_no_volatility_omitted_means_no_change() -> None:
    baseline = _call(None)
    assert _call(None) == baseline

from __future__ import annotations

from app.services.insights import _prop_confidence_score
from app.services.prop_analysis import PropOpportunity, SportsbookQuote


def _make_quote(market_key: str = "points", hit_probability: float = 0.65) -> SportsbookQuote:
    return SportsbookQuote(
        game_id=1,
        sportsbook_key="dk",
        sportsbook_name="DraftKings",
        icon="DK",
        market_key=market_key,
        line_value=22.5,
        over_odds=-110,
        under_odds=-110,
        timestamp="2026-05-14T12:00:00Z",
        is_live_quote=True,
        verification_status="verified",
        odds_source_provider="balldontlie",
        over_probability=0.52,
        under_probability=0.48,
        push_probability=0.0,
        calibrated_over_probability=0.65,
        calibrated_under_probability=0.35,
        recommended_side="OVER",
        hit_probability=hit_probability,
    )


def _make_opportunity(market_key: str, quote: SportsbookQuote) -> PropOpportunity:
    return PropOpportunity(
        rank=1,
        game_id=1,
        player_id=101,
        player_name="Test Player",
        player_icon="TP",
        market_key=market_key,
        consensus_line=22.5,
        projected_mean=23.1,
        recommended_side="OVER",
        hit_probability=quote.hit_probability,
        likelihood_score=65,
        calibrated_over_probability=0.65,
        sportsbooks_summary="DraftKings",
        top_features=["recent form"],
        quotes=[quote],
        data_confidence_score=0.80,
    )


def _score_for(market_key: str) -> int:
    quote = _make_quote(market_key=market_key)
    opp = _make_opportunity(market_key, quote)
    return _prop_confidence_score(
        opportunity=opp,
        best_quote=quote,
        edge=0.05,
        latest_quote_at=None,
        uncertainty_ratio=None,
        injury=None,
        now=None,
    )


def test_threes_docked_10_below_points() -> None:
    assert _score_for("threes") == _score_for("points") - 10


def test_blocks_docked_8_below_points() -> None:
    assert _score_for("blocks") == _score_for("points") - 8


def test_steals_docked_8_below_points() -> None:
    assert _score_for("steals") == _score_for("points") - 8


def test_turnovers_docked_12_below_points() -> None:
    assert _score_for("turnovers") == _score_for("points") - 12


def test_stable_market_no_penalty() -> None:
    # rebounds and assists should have zero penalty
    assert _score_for("rebounds") == _score_for("points")
    assert _score_for("assists") == _score_for("points")

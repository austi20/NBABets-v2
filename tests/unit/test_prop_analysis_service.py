from __future__ import annotations

from unittest.mock import patch

import pytest

from app.config.settings import get_settings
from app.evaluation.prop_decision import PropDecision
from app.services.prop_analysis import (
    PropAnalysisService,
    _quote_recommendation,
    _QuotePredictionRow,
)


def test_quote_recommendation_delegates_to_prop_pricer() -> None:
    decision = PropDecision(
        model_prob=0.61,
        market_prob=0.52,
        no_vig_market_prob=0.50,
        ev=0.17,
        recommendation="OVER",
        confidence="medium",
        driver="edge_vs_no_vig=0.110",
        market_key="player_points",
        line_value=24.5,
        over_odds=-110,
        under_odds=-110,
    )
    with patch("app.services.prop_analysis.price_prop", return_value=decision) as mock_price:
        result = _quote_recommendation(
            over_odds=-110,
            under_odds=-110,
            calibrated_over_probability=0.61,
            calibrated_under_probability=0.39,
        )

    assert result == ("OVER", 0.61, 0.50)
    assert mock_price.called


def test_quote_recommendation_applies_side_bias_offset() -> None:
    """6-day backtest tuning: input to price_prop must be the bias-corrected
    over/under probabilities (overs tilted down, unders tilted up by offset)."""
    # Force a clean global offset; clear any per-market override.
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setenv("OVER_PROBABILITY_BIAS_OFFSET", "0.05")
    monkeypatch.setenv("PER_MARKET_BIAS", "")
    try:
        get_settings.cache_clear()  # type: ignore[attr-defined]

        decision = PropDecision(
            model_prob=0.56, market_prob=0.50, no_vig_market_prob=0.50,
            ev=0.10, recommendation="UNDER", confidence="medium",
            driver="x", market_key="player_points", line_value=24.5,
            over_odds=-110, under_odds=-110,
        )
        with patch("app.services.prop_analysis.price_prop", return_value=decision) as mock_price:
            _quote_recommendation(
                over_odds=-110, under_odds=-110,
                calibrated_over_probability=0.61,
                calibrated_under_probability=0.39,
            )
        # price_prop should see 0.61 - 0.05 = 0.56 for over and 0.39 + 0.05 = 0.44 for under.
        args, kwargs = mock_price.call_args
        passed = kwargs["prediction"]
        assert abs(passed["calibration_adjusted_probability"] - 0.56) < 1e-9
        assert abs(passed["under_probability"] - 0.44) < 1e-9
    finally:
        monkeypatch.undo()
        get_settings.cache_clear()  # type: ignore[attr-defined]


def test_quote_recommendation_per_market_offset_overrides_global() -> None:
    """When a market has a per-market offset, it takes precedence over the
    global offset (steals empirically has a NEGATIVE offset — overs hit more
    than 50%, so we tilt toward over instead of away from it)."""
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setenv("OVER_PROBABILITY_BIAS_OFFSET", "0.07")
    monkeypatch.setenv("PER_MARKET_BIAS", "steals:-0.10,points:0.10")
    try:
        get_settings.cache_clear()  # type: ignore[attr-defined]

        decision = PropDecision(
            model_prob=0.60, market_prob=0.50, no_vig_market_prob=0.50,
            ev=0.10, recommendation="OVER", confidence="medium",
            driver="x", market_key="steals", line_value=1.5,
            over_odds=-110, under_odds=-110,
        )
        with patch("app.services.prop_analysis.price_prop", return_value=decision) as mock_price:
            # steals market: -0.10 offset means OVER gets +0.10 (tilt toward over)
            _quote_recommendation(
                over_odds=-110, under_odds=-110,
                calibrated_over_probability=0.50,
                calibrated_under_probability=0.50,
                market_key="steals",
            )
        passed = mock_price.call_args.kwargs["prediction"]
        # 0.50 - (-0.10) = 0.60 over; 0.50 + (-0.10) = 0.40 under
        assert abs(passed["calibration_adjusted_probability"] - 0.60) < 1e-9
        assert abs(passed["under_probability"] - 0.40) < 1e-9

        # points market uses its own +0.10 offset
        with patch("app.services.prop_analysis.price_prop", return_value=decision) as mock_price:
            _quote_recommendation(
                over_odds=-110, under_odds=-110,
                calibrated_over_probability=0.55,
                calibrated_under_probability=0.45,
                market_key="points",
            )
        passed = mock_price.call_args.kwargs["prediction"]
        # 0.55 - 0.10 = 0.45 over; 0.45 + 0.10 = 0.55 under
        assert abs(passed["calibration_adjusted_probability"] - 0.45) < 1e-9
        assert abs(passed["under_probability"] - 0.55) < 1e-9

        # Unlisted market falls back to the global 0.07 offset
        with patch("app.services.prop_analysis.price_prop", return_value=decision) as mock_price:
            _quote_recommendation(
                over_odds=-110, under_odds=-110,
                calibrated_over_probability=0.60,
                calibrated_under_probability=0.40,
                market_key="unknown_market",
            )
        passed = mock_price.call_args.kwargs["prediction"]
        assert abs(passed["calibration_adjusted_probability"] - 0.53) < 1e-9
        assert abs(passed["under_probability"] - 0.47) < 1e-9
    finally:
        monkeypatch.undo()
        get_settings.cache_clear()  # type: ignore[attr-defined]


def test_prediction_row_to_quote_exposes_no_vig_market_probability() -> None:
    service = object.__new__(PropAnalysisService)
    row = _QuotePredictionRow(
        game_id=10,
        player_id=20,
        player_name="Player",
        player_team_abbreviation="AAA",
        player_position="G",
        market_key="player_points",
        game_label="AAA @ BBB",
        game_start_time="2026-05-01T00:00:00",
        projected_mean=25.4,
        line_value=24.5,
        over_odds=-110,
        under_odds=-110,
        timestamp="2026-05-01T00:00:00",
        sportsbook_key="book",
        sportsbook_name="Book",
        is_live_quote=True,
        is_alternate_line=False,
        source_market_key="player_points",
        verification_status="provider_live",
        odds_source_provider="balldontlie",
        predicted_at="2026-05-01T00:00:00",
        projected_variance=9.0,
        confidence_interval_low=18.0,
        confidence_interval_high=31.0,
        over_probability=0.56,
        under_probability=0.36,
        push_probability=0.08,
        calibration_adjusted_probability=0.58,
        calibrated_under_probability=0.34,
        recommended_side="OVER",
        hit_probability=0.58,
        no_vig_market_probability=0.50,
        top_features=["usage_up"],
        data_sufficiency_tier="A",
        data_confidence_score=0.9,
    )

    quote = service._prediction_row_to_quote(row)
    assert quote.no_vig_market_probability == 0.50

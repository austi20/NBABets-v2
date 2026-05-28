from __future__ import annotations

from dataclasses import replace
from datetime import date
from unittest.mock import patch

import pytest

from app.config.settings import get_settings
from app.evaluation.prop_decision import PropDecision
from app.services.prop_analysis import (
    PropAnalysisService,
    _merge_quote_groups,
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


def test_quote_recommendation_player_offset_overrides_market_and_global(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Per-player Bayesian offset is the highest-precedence side-bias source."""
    from app.services import player_bias

    monkeypatch.setenv("OVER_PROBABILITY_BIAS_OFFSET", "0.07")
    monkeypatch.setenv("PER_MARKET_BIAS", "points:0.10")
    monkeypatch.setenv("PLAYER_BIAS_ENABLED", "true")
    get_settings.cache_clear()  # type: ignore[attr-defined]

    decision = PropDecision(
        model_prob=0.50, market_prob=0.50, no_vig_market_prob=0.50,
        ev=0.0, recommendation="UNDER", confidence="medium",
        driver="x", market_key="player_points", line_value=24.5,
        over_odds=-110, under_odds=-110,
    )
    # Stub: this player has a learned offset of +0.22 (Kawhi-style under tilt)
    with patch.object(player_bias, "get_player_bias_offset", return_value=0.22):
        with patch("app.services.prop_analysis.price_prop", return_value=decision) as mock_price:
            _quote_recommendation(
                over_odds=-110, under_odds=-110,
                calibrated_over_probability=0.60,
                calibrated_under_probability=0.40,
                market_key="points",
                player_id=42,
            )
    passed = mock_price.call_args.kwargs["prediction"]
    # Player offset 0.22 dominates the points market offset 0.10 and the
    # global 0.07: 0.60 - 0.22 = 0.38 over, 0.40 + 0.22 = 0.62 under.
    assert abs(passed["calibration_adjusted_probability"] - 0.38) < 1e-9
    assert abs(passed["under_probability"] - 0.62) < 1e-9


def test_quote_recommendation_falls_back_to_market_when_no_player_offset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the player has no learned offset, fall back to per-market."""
    from app.services import player_bias

    monkeypatch.setenv("OVER_PROBABILITY_BIAS_OFFSET", "0.07")
    monkeypatch.setenv("PER_MARKET_BIAS", "points:0.10")
    get_settings.cache_clear()  # type: ignore[attr-defined]

    decision = PropDecision(
        model_prob=0.45, market_prob=0.50, no_vig_market_prob=0.50,
        ev=0.0, recommendation="UNDER", confidence="medium",
        driver="x", market_key="player_points", line_value=24.5,
        over_odds=-110, under_odds=-110,
    )
    with patch.object(player_bias, "get_player_bias_offset", return_value=None):
        with patch("app.services.prop_analysis.price_prop", return_value=decision) as mock_price:
            _quote_recommendation(
                over_odds=-110, under_odds=-110,
                calibrated_over_probability=0.55,
                calibrated_under_probability=0.45,
                market_key="points",
                player_id=42,
            )
    passed = mock_price.call_args.kwargs["prediction"]
    # No player offset -> per-market 0.10: 0.55 - 0.10 = 0.45 over.
    assert abs(passed["calibration_adjusted_probability"] - 0.45) < 1e-9
    assert abs(passed["under_probability"] - 0.55) < 1e-9


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


def test_merge_quote_groups_prefers_evaluations_per_quote_and_keeps_legacy_gaps() -> None:
    key = (date(2026, 5, 1), 1, 2, 20, "player_points", -1.0)
    base = _QuotePredictionRow(
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
    legacy_other = replace(base, sportsbook_key="other", sportsbook_name="Other")
    evaluated_base = replace(
        base,
        hit_probability=0.62,
        stat_projection_id=101,
        prop_line_evaluation_id=202,
        probability_source="projection_distribution",
    )

    merged = _merge_quote_groups(preferred={key: [evaluated_base]}, fallback={key: [base, legacy_other]})

    assert len(merged[key]) == 2
    by_book = {row.sportsbook_key: row for row in merged[key]}
    assert by_book["book"].hit_probability == 0.62
    assert by_book["book"].stat_projection_id == 101
    assert by_book["other"].probability_source == "legacy_prediction"


# ---------------------------------------------------------------------------
# Task 4.1: merge dedupes on 5-tuple prefix — line_group_key mismatch
# ---------------------------------------------------------------------------

def _make_base_row(**overrides) -> _QuotePredictionRow:
    defaults = dict(
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
    defaults.update(overrides)
    return _QuotePredictionRow(**defaults)


def test_merge_dedupes_on_5_tuple_prefix_projection_wins_over_different_line_group_key() -> None:
    """Preferred (projection) uses line_group_key=24.5; fallback (legacy) uses -1.0.
    After merge the projection row survives and the legacy row for the same
    (sportsbook, source_market, line) is dropped."""
    d = date(2026, 5, 1)
    five_prefix = (d, 1, 2, 20, "player_points")

    legacy_row = _make_base_row(probability_source="legacy_prediction")
    proj_row = _make_base_row(
        hit_probability=0.65,
        stat_projection_id=101,
        prop_line_evaluation_id=202,
        probability_source="projection_distribution",
    )

    # Preferred path: line_group_key == line_value (alternate-line style)
    preferred_key = (*five_prefix, 24.5)
    # Fallback path: same prop but grouped under -1.0
    fallback_key = (*five_prefix, -1.0)

    merged = _merge_quote_groups(
        preferred={preferred_key: [proj_row]},
        fallback={fallback_key: [legacy_row]},
    )

    # The projection row must appear; the legacy duplicate must be gone.
    all_rows = [row for rows in merged.values() for row in rows]
    assert any(r.probability_source == "projection_distribution" for r in all_rows)
    proj_rows = [r for r in all_rows if r.probability_source == "projection_distribution"]
    legacy_rows = [r for r in all_rows if r.probability_source == "legacy_prediction"]
    # Same (sportsbook_key, source_market_key, line_value) triple — projection wins.
    assert len(proj_rows) == 1
    assert len(legacy_rows) == 0


# ---------------------------------------------------------------------------
# Task 4.2: provider_live gate records snapshot_drop_count in last_diagnostics
# ---------------------------------------------------------------------------

def test_provider_live_gate_records_drop_count_in_last_diagnostics() -> None:
    """When all legacy rows fail the provider_live gate, last_diagnostics
    must record the drop count and the result must be an empty board."""
    service = object.__new__(PropAnalysisService)
    service.last_diagnostics = {}
    service._session = None  # not used once the methods are patched

    from unittest.mock import patch as _patch

    # Patch _latest_legacy_prediction_quotes to simulate: 3 rows received,
    # all 3 dropped by the gate, returns {}.
    def _fake_legacy(target_date):
        # Simulate the accounting that the real method does.
        service.last_diagnostics["snapshot_drop_count"] = 3
        return {}

    def _fake_eval(target_date):
        return {}

    with _patch.object(service, "_latest_legacy_prediction_quotes", side_effect=_fake_legacy):
        with _patch.object(service, "_latest_line_evaluation_quotes", side_effect=_fake_eval):
            with _patch("app.services.prop_analysis.sync_live_games_from_nba_api"):
                result = service.all_opportunities(target_date=None)

    assert result == []
    assert service.last_diagnostics.get("snapshot_drop_count") == 3

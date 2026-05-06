from __future__ import annotations

import pandas as pd

from app.evaluation.backtest import RollingOriginBacktester, _build_segmented_summary


def test_build_segmented_summary_preserves_sufficiency_columns() -> None:
    results = pd.DataFrame(
        [
            {
                "market_key": "points",
                "sample_sufficient": 1,
                "train_quote_count_raw": 100,
                "validation_quote_count_raw": 40,
                "quote_density_train": 0.5,
                "quote_density_validation": 0.2,
                "mae": 1.0,
            }
        ]
    )
    bet_detail = pd.DataFrame()

    summary = _build_segmented_summary(results, bet_detail)

    market_row = summary[summary["market_key"] == "points"].iloc[0]
    assert float(market_row["sample_sufficient"]) == 1.0
    assert float(market_row["train_quote_count_raw"]) == 100.0
    assert float(market_row["validation_quote_count_raw"]) == 40.0


def test_build_segmented_summary_includes_odds_provider_segment() -> None:
    results = pd.DataFrame()
    bet_detail = pd.DataFrame(
        [
            {
                "odds_source_provider": "theoddsapi",
                "market_key": "points",
                "sportsbook_key": "book_a",
                "confidence_tier": "high",
                "evaluation_date": "2026-04-03",
                "bet_placed": True,
                "bet_result": "win",
                "realized_profit": 1.1,
                "expected_value": 0.2,
                "edge_vs_implied": 0.03,
                "edge_vs_no_vig": 0.02,
                "clv_line_delta": 0.1,
                "clv_probability_delta": 0.01,
            }
        ]
    )

    summary = _build_segmented_summary(results, bet_detail)
    assert "odds_provider" in set(summary["segment"])


def test_score_quote_rows_handles_empty_prediction_frame_without_merge_keys() -> None:
    backtester = object.__new__(RollingOriginBacktester)
    quote_rows = pd.DataFrame(
        [
            {
                "game_id": 1,
                "player_id": 2,
                "game_date": "2026-04-01",
                "line_value": 20.5,
                "actual_total": 22.0,
            }
        ]
    )
    predictions = pd.DataFrame()

    scored = backtester._score_quote_rows(predictions, quote_rows, "points")

    assert scored.empty
    assert set(scored.columns) == set(quote_rows.columns)

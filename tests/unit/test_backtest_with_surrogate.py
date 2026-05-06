from __future__ import annotations

import pandas as pd

from scripts.backtest_with_surrogate import _metric_means_from_folds, _season_start_year_from_frame


def test_season_start_year_filter_uses_nba_season_rules() -> None:
    frame = pd.DataFrame(
        {
            "game_date": pd.to_datetime(["2024-12-20", "2025-01-12", "2025-03-01"]),
            "season": ["2024", "2024", "2024"],
        }
    )
    derived = _season_start_year_from_frame(frame)
    assert derived.tolist() == [2024, 2024, 2024]


def test_metric_means_are_computed_from_fold_rows() -> None:
    folds = pd.DataFrame(
        {
            "log_loss": [0.72, 0.68, 0.70],
            "brier_score": [0.25, 0.24, 0.26],
            "expected_calibration_error": [0.03, 0.04, 0.05],
        }
    )
    mean_log_loss, mean_brier, mean_ece = _metric_means_from_folds(folds)
    assert round(mean_log_loss, 4) == 0.7000
    assert round(mean_brier, 4) == 0.2500
    assert round(mean_ece, 4) == 0.0400

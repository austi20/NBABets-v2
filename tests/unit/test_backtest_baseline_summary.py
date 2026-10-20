import pandas as pd

from app.evaluation.backtest import _baseline_summary_lines


def _folds() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "market_key": ["points", "points", "threes", "threes"],
            "model_mae_hist": [4.0, 5.0, 1.0, 1.0],
            "baseline_mae_avg5": [4.4, 5.4, 0.9, 0.9],
            "baseline_mae_avg10": [4.2, 5.2, 0.95, 0.95],
            "baseline_mae_recency": [4.3, 5.3, 0.92, 0.92],
            "history_rows": [100, 300, 50, 50],
        }
    )


def test_summary_weights_folds_by_rows_and_signs_the_gap():
    text = "\n".join(_baseline_summary_lines(_folds()))

    # points: model (4*100+5*300)/400 = 4.75, best baseline avg10 (4.2*100+5.2*300)/400 = 4.95
    assert "| points | 4.750 | 4.950 |" in text
    assert "+4.0%" in text
    # threes: model 1.0 vs best baseline 0.9, model loses
    assert "-11.1%" in text


def test_summary_is_empty_without_baseline_columns():
    assert _baseline_summary_lines(pd.DataFrame({"market_key": ["points"], "mae": [1.0]})) == []

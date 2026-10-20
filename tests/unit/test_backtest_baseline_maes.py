import numpy as np
import pandas as pd

from app.evaluation.backtest import _baseline_maes


def _frame() -> tuple[pd.DataFrame, pd.DataFrame]:
    # row 3 has no history, so it must be dropped from every number
    reg = pd.DataFrame(
        {
            "actual_total": [10.0, 20.0, 30.0, 99.0],
            "projected_mean": [12.0, 18.0, 30.0, 0.0],
        }
    )
    frame = pd.DataFrame(
        {
            "minutes_avg_10": [30.0, 30.0, 30.0, 0.0],
            "points_avg_5": [10.0, 25.0, 30.0, 0.0],
            "points_avg_10": [14.0, 16.0, 30.0, 0.0],
            "points_baseline_mean": [11.0, 21.0, 27.0, 0.0],
        }
    )
    return reg, frame


def test_baseline_maes_use_same_rows_with_history():
    reg, frame = _frame()
    out = _baseline_maes(reg, frame, "points")
    assert out["history_rows"] == 3
    assert out["model_mae_hist"] == np.mean([2.0, 2.0, 0.0])
    assert out["baseline_mae_avg5"] == np.mean([0.0, 5.0, 0.0])
    assert out["baseline_mae_avg10"] == np.mean([4.0, 4.0, 0.0])
    assert out["baseline_mae_recency"] == np.mean([1.0, 1.0, 3.0])


def test_baseline_maes_missing_column_is_nan_not_error():
    reg, frame = _frame()
    out = _baseline_maes(reg, frame.drop(columns=["points_avg_5"]), "points")
    assert np.isnan(out["baseline_mae_avg5"])


def test_baseline_maes_no_history_rows_gives_nan():
    reg, frame = _frame()
    frame["minutes_avg_10"] = 0.0
    out = _baseline_maes(reg, frame, "points")
    assert out["history_rows"] == 0
    assert np.isnan(out["model_mae_hist"])

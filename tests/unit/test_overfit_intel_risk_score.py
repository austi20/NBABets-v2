from __future__ import annotations

from app.services.local_autonomy.overfit_intel import build_overfit_intel_snapshot


def test_risk_score_reflects_max_not_mean() -> None:
    # 1 signal with score=1.0 from ECE=0.12 (score = min(1.0, 0.12/0.12) = 1.0)
    # 4 signals with score~0 from ECE=0.001
    metrics: dict = {
        "calibration_diagnostics": {
            "points": {"ece": 0.12},
            "rebounds": {"ece": 0.001},
            "assists": {"ece": 0.001},
            "threes": {"ece": 0.001},
            "turnovers": {"ece": 0.001},
        }
    }
    snapshot = build_overfit_intel_snapshot(
        latest_model_metrics=metrics,
        latest_backtest_metrics={},
        trend_alerts=[],
    )
    # max = 1.0; mean of top-5 would be ~0.2
    assert snapshot.risk_score == 1.0


def test_risk_score_single_signal() -> None:
    metrics: dict = {
        "calibration_diagnostics": {
            "points": {"ece": 0.108},  # score = min(1.0, 0.108/0.12) = 0.9
        }
    }
    snapshot = build_overfit_intel_snapshot(
        latest_model_metrics=metrics,
        latest_backtest_metrics={},
        trend_alerts=[],
    )
    assert abs(snapshot.risk_score - 0.9) < 1e-9

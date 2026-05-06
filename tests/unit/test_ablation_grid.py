from __future__ import annotations

import pandas as pd

from app.training.ablation_grid import AblationConfig, run_ablation


class _FakeBacktester:
    def __init__(self, _session: object, *, k_seasons: int, l1_alpha: float, dist_family: str) -> None:
        self._k_seasons = k_seasons
        self._l1_alpha = l1_alpha
        self._dist_family = dist_family

    def run(
        self,
        *,
        train_days: int = 120,
        validation_days: int = 14,
        step_days: int = 14,
        historical: pd.DataFrame | None = None,
    ) -> dict[str, object]:
        _ = (train_days, validation_days, step_days, historical)
        base = 0.60 + (self._k_seasons * 0.001) + (self._l1_alpha * 0.01)
        if self._dist_family == "count_aware":
            base -= 0.01
        return {
            "summary": [
                {
                    "segment": "market",
                    "market_key": "points",
                    "log_loss": base,
                    "brier_score": 0.22,
                    "expected_calibration_error": 0.04,
                    "mae": 4.0,
                    "rmse": 5.0,
                },
                {
                    "segment": "market",
                    "market_key": "rebounds",
                    "log_loss": base + 0.01,
                    "brier_score": 0.23,
                    "expected_calibration_error": 0.05,
                    "mae": 2.0,
                    "rmse": 2.8,
                },
            ]
        }


class _FakeBacktesterWithoutSegment:
    def __init__(self, _session: object, *, k_seasons: int, l1_alpha: float, dist_family: str) -> None:
        self._k_seasons = k_seasons
        self._l1_alpha = l1_alpha
        self._dist_family = dist_family

    def run(
        self,
        *,
        train_days: int = 120,
        validation_days: int = 14,
        step_days: int = 14,
        historical: pd.DataFrame | None = None,
    ) -> dict[str, object]:
        _ = (train_days, validation_days, step_days, historical)
        return {
            "summary": [
                {
                    "market_key": "points",
                    "log_loss": 0.61,
                    "brier_score": 0.22,
                    "expected_calibration_error": 0.04,
                    "mae": 4.0,
                    "rmse": 5.0,
                }
            ]
        }


def test_run_ablation_executes_multiple_configs_end_to_end() -> None:
    historical = pd.DataFrame(
        {
            "game_date": pd.to_datetime(
                [
                    "2023-01-01",
                    "2023-02-01",
                    "2024-01-01",
                    "2024-02-01",
                ]
            )
        }
    )
    configs = [
        AblationConfig(k_seasons=2, l1_alpha=0.0, dist_family="legacy"),
        AblationConfig(k_seasons=3, l1_alpha=0.0, dist_family="count_aware"),
        AblationConfig(k_seasons=4, l1_alpha=0.01, dist_family="decomposed"),
        AblationConfig(k_seasons=5, l1_alpha=0.01, dist_family="legacy"),
    ]

    result = run_ablation(
        configs=configs,
        holdout_seasons=["2023", "2024"],
        session=object(),  # type: ignore[arg-type]
        historical=historical,
        backtester_cls=_FakeBacktester,  # type: ignore[arg-type]
        train_days=30,
        validation_days=7,
        step_days=7,
    )

    assert not result.empty
    assert set(result["market_key"]) == {"points", "rebounds"}
    assert len(set(result["config_id"])) == 4


def test_run_ablation_handles_real_backtester_summary_shape_without_segment() -> None:
    historical = pd.DataFrame({"game_date": pd.to_datetime(["2023-01-01", "2023-02-01"])})
    configs = [AblationConfig(k_seasons=4, l1_alpha=0.0, dist_family="legacy")]

    result = run_ablation(
        configs=configs,
        holdout_seasons=["2023"],
        session=object(),  # type: ignore[arg-type]
        historical=historical,
        backtester_cls=_FakeBacktesterWithoutSegment,  # type: ignore[arg-type]
        train_days=30,
        validation_days=7,
        step_days=7,
    )

    assert not result.empty
    assert result.iloc[0]["market_key"] == "points"

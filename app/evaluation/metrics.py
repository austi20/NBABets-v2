from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from sklearn.metrics import brier_score_loss, log_loss, mean_absolute_error, mean_squared_error


@dataclass(frozen=True)
class RegressionMetrics:
    mae: float
    rmse: float


@dataclass(frozen=True)
class ProbabilityMetrics:
    log_loss: float
    brier_score: float
    expected_calibration_error: float


@dataclass(frozen=True)
class BettingMetrics:
    quote_count: int
    bet_count: int
    win_count: int
    loss_count: int
    push_count: int
    realized_profit: float
    realized_roi: float
    average_expected_value: float
    total_expected_value: float
    average_edge_implied: float
    average_edge_no_vig: float
    average_clv_line: float
    average_clv_probability: float
    win_rate: float
    push_rate: float


def compute_regression_metrics(actual: np.ndarray, predicted: np.ndarray) -> RegressionMetrics:
    return RegressionMetrics(
        mae=float(mean_absolute_error(actual, predicted)),
        rmse=float(mean_squared_error(actual, predicted) ** 0.5),
    )


def compute_probability_metrics(labels: np.ndarray, probabilities: np.ndarray, bins: int = 10) -> ProbabilityMetrics:
    return ProbabilityMetrics(
        log_loss=float(log_loss(labels, probabilities, labels=[0, 1])),
        brier_score=float(brier_score_loss(labels, probabilities)),
        expected_calibration_error=float(expected_calibration_error(labels, probabilities, bins=bins)),
    )


def expected_calibration_error(labels: np.ndarray, probabilities: np.ndarray, bins: int = 10) -> float:
    frame = pd.DataFrame({"label": labels, "probability": probabilities})
    bucket_edges = np.linspace(0.0, 1.0, bins + 1)
    frame["bucket"] = pd.cut(
        frame["probability"],
        bins=bucket_edges,
        labels=False,
        include_lowest=True,
    )
    ece = 0.0
    total = len(frame)
    for _, bucket in frame.groupby("bucket"):
        if bucket.empty:
            continue
        ece += abs(bucket["label"].mean() - bucket["probability"].mean()) * len(bucket) / total
    return float(ece)


def compute_betting_metrics(frame: pd.DataFrame) -> BettingMetrics:
    if frame.empty:
        return BettingMetrics(
            quote_count=0,
            bet_count=0,
            win_count=0,
            loss_count=0,
            push_count=0,
            realized_profit=0.0,
            realized_roi=0.0,
            average_expected_value=0.0,
            total_expected_value=0.0,
            average_edge_implied=0.0,
            average_edge_no_vig=0.0,
            average_clv_line=0.0,
            average_clv_probability=0.0,
            win_rate=0.0,
            push_rate=0.0,
        )

    bet_frame = frame.loc[frame["bet_placed"].fillna(False)].copy()
    quote_count = int(len(frame))
    bet_count = int(len(bet_frame))
    if bet_frame.empty:
        return BettingMetrics(
            quote_count=quote_count,
            bet_count=0,
            win_count=0,
            loss_count=0,
            push_count=0,
            realized_profit=0.0,
            realized_roi=0.0,
            average_expected_value=0.0,
            total_expected_value=0.0,
            average_edge_implied=float(pd.to_numeric(frame.get("edge_vs_implied", 0.0), errors="coerce").fillna(0.0).mean()),
            average_edge_no_vig=float(pd.to_numeric(frame.get("edge_vs_no_vig", 0.0), errors="coerce").fillna(0.0).mean()),
            average_clv_line=float(pd.to_numeric(frame.get("clv_line_delta", 0.0), errors="coerce").fillna(0.0).mean()),
            average_clv_probability=float(pd.to_numeric(frame.get("clv_probability_delta", 0.0), errors="coerce").fillna(0.0).mean()),
            win_rate=0.0,
            push_rate=0.0,
        )

    realized_profit = float(pd.to_numeric(bet_frame["realized_profit"], errors="coerce").fillna(0.0).sum())
    total_expected_value = float(pd.to_numeric(bet_frame["expected_value"], errors="coerce").fillna(0.0).sum())
    win_count = int((bet_frame["bet_result"] == "win").sum())
    loss_count = int((bet_frame["bet_result"] == "loss").sum())
    push_count = int((bet_frame["bet_result"] == "push").sum())
    settled_count = max(win_count + loss_count + push_count, 1)
    return BettingMetrics(
        quote_count=quote_count,
        bet_count=bet_count,
        win_count=win_count,
        loss_count=loss_count,
        push_count=push_count,
        realized_profit=realized_profit,
        realized_roi=float(realized_profit / max(bet_count, 1)),
        average_expected_value=float(total_expected_value / max(bet_count, 1)),
        total_expected_value=total_expected_value,
        average_edge_implied=float(pd.to_numeric(bet_frame["edge_vs_implied"], errors="coerce").fillna(0.0).mean()),
        average_edge_no_vig=float(pd.to_numeric(bet_frame["edge_vs_no_vig"], errors="coerce").fillna(0.0).mean()),
        average_clv_line=float(pd.to_numeric(bet_frame.get("clv_line_delta", 0.0), errors="coerce").fillna(0.0).mean()),
        average_clv_probability=float(
            pd.to_numeric(bet_frame.get("clv_probability_delta", 0.0), errors="coerce").fillna(0.0).mean()
        ),
        win_rate=float(win_count / settled_count),
        push_rate=float(push_count / settled_count),
    )

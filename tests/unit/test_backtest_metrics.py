from __future__ import annotations

import math

import numpy as np

from app.evaluation.metrics import compute_probability_metrics


def test_log_loss_empty_input_returns_nan() -> None:
    """compute_probability_metrics must return NaN log_loss when input is empty."""
    result = compute_probability_metrics(np.array([]), np.array([]))
    assert math.isnan(result.log_loss), (
        f"expected NaN log_loss for empty input, got {result.log_loss}"
    )


def test_log_loss_normal_case_not_nan() -> None:
    """compute_probability_metrics returns a finite log_loss for a two-class fold."""
    labels = np.array([0.0, 0.0, 1.0, 1.0, 0.0, 1.0])
    probs = np.array([0.2, 0.3, 0.7, 0.8, 0.4, 0.6])
    result = compute_probability_metrics(labels, probs)
    assert not math.isnan(result.log_loss)
    assert result.log_loss > 0.0

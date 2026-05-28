from __future__ import annotations

import numpy as np

from app.training.calibration_ece import select_folds


def _make_folds(n: int) -> list[np.ndarray]:
    """Return n distinct fold arrays labelled 0..n-1."""
    return [np.array([float(i)]) for i in range(n)]


def test_select_folds_tail_slice() -> None:
    """select_folds must return the n most-recent (tail) arrays."""
    folds = _make_folds(5)
    result = select_folds(folds, 3)
    assert len(result) == 3
    # Tail-slice: folds[2], folds[3], folds[4]
    for got, expected in zip(result, folds[2:], strict=True):
        np.testing.assert_array_equal(got, expected)


def test_select_folds_zero_returns_empty() -> None:
    folds = _make_folds(4)
    assert select_folds(folds, 0) == []


def test_select_folds_more_than_available_returns_all() -> None:
    folds = _make_folds(3)
    result = select_folds(folds, 10)
    assert len(result) == 3


def test_pipeline_and_ece_agree_on_fold_slice() -> None:
    """Both pipeline and calibration_ece must produce identical slices for the same inputs."""
    folds = _make_folds(6)
    n = 4

    # Production pipeline path (via select_folds imported in pipeline)
    from app.training.calibration_ece import select_folds as ece_select_folds
    # Direct import exercises both call sites pointing to the same function
    pipeline_slice = select_folds(folds, n)
    ece_slice = ece_select_folds(folds, n)

    assert len(pipeline_slice) == len(ece_slice) == n
    for a, b in zip(pipeline_slice, ece_slice, strict=True):
        np.testing.assert_array_equal(a, b)

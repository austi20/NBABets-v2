from __future__ import annotations

import numpy as np

from app.training.calibration import ProbabilityCalibrator, _restore_out_of_support_raw_probabilities


def test_restore_out_of_support_uses_calibrated_boundary_not_raw() -> None:
    """When all inputs are out of support the fallback must use calibrated boundary, not raw."""
    raw = np.array([0.01, 0.02])           # both below support_min=0.3
    calibrated = np.array([0.5, 0.6])      # calibrated values from fit set
    result = _restore_out_of_support_raw_probabilities(
        raw_probabilities=raw,
        calibrated=calibrated,
        support_min=0.3,
        support_max=0.9,
    )
    # Must NOT return raw values
    assert not np.allclose(result, raw), (
        f"expected clamped values, got raw passthrough: {result}"
    )
    # Must return something >= calibrated.min() (boundary of fit set)
    assert np.all(result >= calibrated.min() - 1e-9), (
        f"result {result} is below calibrated minimum {calibrated.min()}"
    )


def test_restore_out_of_support_above_uses_calibrated_max_not_raw() -> None:
    raw = np.array([0.95, 0.99])           # both above support_max=0.8
    calibrated = np.array([0.3, 0.4])      # calibrated values from fit set
    result = _restore_out_of_support_raw_probabilities(
        raw_probabilities=raw,
        calibrated=calibrated,
        support_min=0.1,
        support_max=0.8,
    )
    assert not np.allclose(result, raw), (
        f"expected clamped values, got raw passthrough: {result}"
    )
    assert np.all(result <= calibrated.max() + 1e-9), (
        f"result {result} exceeds calibrated maximum {calibrated.max()}"
    )


def test_restore_uses_stored_calibrated_min_max_from_fit() -> None:
    """calibrated_min/max kwargs must override the batch boundary computation."""
    raw = np.array([0.01])                 # below support
    calibrated = np.array([0.01])          # same as raw — would look like no clamping
    # But we pass explicit calibrated_min=0.45 from training set
    result = _restore_out_of_support_raw_probabilities(
        raw_probabilities=raw,
        calibrated=calibrated,
        support_min=0.3,
        support_max=0.9,
        calibrated_min=0.45,
        calibrated_max=0.90,
    )
    assert np.allclose(result, 0.45), (
        f"expected calibrated_min=0.45, got {result}"
    )


def test_out_of_support_clamped_not_raw_passthrough() -> None:
    """End-to-end: calibrator.transform on out-of-support inputs must not return raw values."""
    cal = ProbabilityCalibrator()
    # Fit with a clear signal so isotonic or sigmoid strategy is selected
    n = 200
    raw_train = np.linspace(0.35, 0.80, n)
    labels_train = (raw_train > 0.55).astype(float)
    cal.fit(raw_train, labels_train)

    # These inputs are well outside [0.35, 0.80] training support
    raw_oos = np.array([0.01, 0.99])
    result = cal.transform(raw_oos)

    # Result must differ from the raw input (it must be clamped)
    assert not np.allclose(result, raw_oos, atol=1e-3), (
        f"out-of-support values were passed through as raw: {result}"
    )
    # Result must be within [0, 1]
    assert np.all(result >= 0.0) and np.all(result <= 1.0)

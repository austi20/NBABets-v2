from __future__ import annotations

import numpy as np
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss


class ProbabilityCalibrator:
    def __init__(self) -> None:
        self.strategy = "sigmoid"
        self.isotonic: IsotonicRegression | None = None
        self.sigmoid: LogisticRegression | None = None
        self.constant_probability: float | None = None
        self.sample_size: int = 0
        self.market_prior_weight: float = 0.0
        self.design_width: int = 1
        self.support_min_probability: float | None = None
        self.support_max_probability: float | None = None

    def fit(
        self,
        raw_probabilities: np.ndarray,
        labels: np.ndarray,
        market_priors: np.ndarray | None = None,
    ) -> None:
        probabilities = np.asarray(raw_probabilities, dtype=float).reshape(-1)
        labels = np.asarray(labels, dtype=float).reshape(-1)
        self.sample_size = len(probabilities)
        probabilities = _bound_probabilities(probabilities, self.sample_size)
        self.support_min_probability = float(np.min(probabilities)) if probabilities.size else None
        self.support_max_probability = float(np.max(probabilities)) if probabilities.size else None
        priors = None
        if market_priors is not None:
            bounded_priors = _bound_probabilities(np.asarray(market_priors, dtype=float).reshape(-1), self.sample_size)
            if not _is_nearly_constant(bounded_priors):
                priors = bounded_priors
        self.design_width = 3 if priors is not None else 1
        if len(np.unique(labels)) < 2:
            self.strategy = "constant"
            self.constant_probability = float(labels.mean()) if len(labels) else 0.5
            return

        isotonic = IsotonicRegression(out_of_bounds="clip")
        isotonic.fit(probabilities, labels)
        sigmoid = LogisticRegression(random_state=42)
        sigmoid.fit(_calibration_design(probabilities, priors, self.design_width), labels)

        iso_pred = _bound_probabilities(isotonic.predict(probabilities), self.sample_size)
        sig_pred = _bound_probabilities(
            sigmoid.predict_proba(_calibration_design(probabilities, priors, self.design_width))[:, 1],
            self.sample_size,
        )
        identity_pred = probabilities
        boundary_rate = float(np.mean((iso_pred <= 0.01) | (iso_pred >= 0.99)))
        id_brier = brier_score_loss(labels, identity_pred)
        iso_brier = brier_score_loss(labels, iso_pred)
        sig_brier = brier_score_loss(labels, sig_pred)
        iso_valid = _is_calibration_order_valid(probabilities, iso_pred)
        sig_valid = _is_calibration_order_valid(probabilities, sig_pred)

        self.strategy = "identity"
        best_brier = id_brier
        # Direct-fit isotonic often collapses to 0/1 steps on small or narrow samples.
        if iso_valid and boundary_rate <= 0.01 and iso_brier < best_brier - 1e-4:
            self.strategy = "isotonic"
            best_brier = iso_brier
        if sig_valid and sig_brier < best_brier - 1e-4:
            self.strategy = "sigmoid"
        self.isotonic = isotonic
        self.sigmoid = sigmoid
        if priors is not None:
            correlation = _safe_correlation(probabilities, priors)
            self.market_prior_weight = min(0.35, max(0.05, correlation if len(priors) > 1 else 0.10))

    def transform(
        self,
        raw_probabilities: np.ndarray,
        market_priors: np.ndarray | None = None,
    ) -> np.ndarray:
        bounded_raw = _bound_probabilities(np.asarray(raw_probabilities, dtype=float), self.sample_size)
        priors = None
        if market_priors is not None:
            bounded_priors = _bound_probabilities(np.asarray(market_priors, dtype=float), self.sample_size)
            if not _is_nearly_constant(bounded_priors):
                priors = bounded_priors
        if self.strategy == "constant" and self.constant_probability is not None:
            calibrated = _bound_probabilities(
                np.full(bounded_raw.shape, self.constant_probability, dtype=float),
                self.sample_size,
            )
            return _blend_with_prior(calibrated, priors, self.market_prior_weight, self.sample_size)
        if self.strategy == "identity":
            return _blend_with_prior(bounded_raw, priors, self.market_prior_weight, self.sample_size)
        if self.strategy == "isotonic" and self.isotonic is not None:
            calibrated = np.asarray(self.isotonic.predict(bounded_raw.reshape(-1)), dtype=float)
            calibrated = _bound_probabilities(calibrated, self.sample_size)
            support_min = getattr(self, "support_min_probability", None)
            support_max = getattr(self, "support_max_probability", None)
            if support_min is None:
                support_min = getattr(self.isotonic, "X_min_", None)
            if support_max is None:
                support_max = getattr(self.isotonic, "X_max_", None)
            calibrated = _restore_out_of_support_raw_probabilities(
                raw_probabilities=bounded_raw,
                calibrated=calibrated,
                support_min=support_min,
                support_max=support_max,
            )
            if not _is_calibration_order_valid(bounded_raw, calibrated):
                return _blend_with_prior(bounded_raw, priors, 0.0, self.sample_size)
            return _blend_with_prior(calibrated, priors, self.market_prior_weight, self.sample_size)
        if self.sigmoid is not None:
            design_width = int(getattr(self.sigmoid, "n_features_in_", getattr(self, "design_width", 1)))
            calibrated = np.asarray(
                self.sigmoid.predict_proba(_calibration_design(bounded_raw, priors, design_width))[:, 1],
                dtype=float,
            )
            calibrated = _bound_probabilities(calibrated, self.sample_size)
            if not _is_calibration_order_valid(bounded_raw, calibrated):
                return _blend_with_prior(bounded_raw, priors, 0.0, self.sample_size)
            return _blend_with_prior(calibrated, priors, self.market_prior_weight, self.sample_size)
        return _blend_with_prior(bounded_raw, priors, self.market_prior_weight, self.sample_size)


def _bound_probabilities(probabilities: np.ndarray, sample_size: int) -> np.ndarray:
    epsilon = max(1e-4, min(0.02, 1.0 / max(sample_size + 2, 50)))
    return np.clip(probabilities, epsilon, 1.0 - epsilon)


def _calibration_design(
    probabilities: np.ndarray,
    priors: np.ndarray | None,
    expected_features: int = 1,
) -> np.ndarray:
    if expected_features <= 1:
        return probabilities.reshape(-1, 1)
    if priors is None:
        priors = probabilities
    components = [
        probabilities.reshape(-1, 1),
        priors.reshape(-1, 1),
        (probabilities - priors).reshape(-1, 1),
    ]
    return np.column_stack(components[:expected_features])


def _blend_with_prior(
    calibrated: np.ndarray,
    priors: np.ndarray | None,
    weight: float,
    sample_size: int,
) -> np.ndarray:
    if priors is None:
        return _bound_probabilities(calibrated, sample_size)
    weight = min(max(weight, 0.0), 0.35)
    blended = (1.0 - weight) * calibrated + weight * priors
    return _bound_probabilities(blended, sample_size)


def _safe_correlation(left: np.ndarray, right: np.ndarray) -> float:
    if len(left) <= 1 or len(right) <= 1:
        return 0.10
    if np.std(left) <= 1e-8 or np.std(right) <= 1e-8:
        return 0.10
    correlation = float(np.corrcoef(left, right)[0, 1])
    if not np.isfinite(correlation):
        return 0.10
    return correlation


def _is_nearly_constant(values: np.ndarray, tolerance: float = 1e-6) -> bool:
    if values.size <= 1:
        return True
    return float(np.nanstd(values)) <= tolerance


def _is_calibration_order_valid(raw_probabilities: np.ndarray, calibrated: np.ndarray) -> bool:
    if raw_probabilities.size != calibrated.size or raw_probabilities.size <= 1:
        return True
    return _safe_correlation(raw_probabilities, calibrated) > 0.0


def _restore_out_of_support_raw_probabilities(
    *,
    raw_probabilities: np.ndarray,
    calibrated: np.ndarray,
    support_min: float | None,
    support_max: float | None,
) -> np.ndarray:
    if support_min is None or support_max is None or raw_probabilities.shape != calibrated.shape:
        return calibrated
    restored = calibrated.copy()
    below_mask = raw_probabilities < float(support_min)
    above_mask = raw_probabilities > float(support_max)
    # Clamp out-of-support values to the calibrated boundary instead of
    # passing through raw extremes.  This prevents uncalibrated 0.9999
    # values from surviving when the training data never saw them.
    if np.any(below_mask):
        restored[below_mask] = calibrated[~(below_mask | above_mask)].min() if np.any(~(below_mask | above_mask)) else raw_probabilities[below_mask]
    if np.any(above_mask):
        restored[above_mask] = calibrated[~(below_mask | above_mask)].max() if np.any(~(below_mask | above_mask)) else raw_probabilities[above_mask]
    return restored

"""Post-hoc calibration patching using recent prediction outcomes.

Fits a thin correction layer on top of the existing calibrator without
requiring a full retrain. Useful when calibration drifts but features
are still sound.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import numpy as np
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression

from app.services.brain.contracts import CorrectionRecord, PlannedCorrection, PredictionOutcome

logger = logging.getLogger(__name__)

MIN_OUTCOMES_FOR_PATCH = 30


def execute_calibration_patch(
    correction: PlannedCorrection,
    outcomes: list[PredictionOutcome],
) -> tuple[CorrectionRecord, object | None]:
    """Fit a calibration patch from recent outcomes.

    Returns the correction record and a fitted patch object (isotonic or
    logistic) that can be applied as a post-hoc transform on probabilities.
    Returns None for the patch if insufficient data.
    """
    market = correction.market
    method = correction.parameters.get("method", "isotonic")

    # Filter outcomes to this market, only resolved ones
    relevant = [
        o for o in outcomes
        if o.market == market and o.hit is not None
    ]

    if len(relevant) < MIN_OUTCOMES_FOR_PATCH:
        logger.info(
            "Insufficient outcomes for %s calibration patch (%d < %d)",
            market,
            len(relevant),
            MIN_OUTCOMES_FOR_PATCH,
        )
        record = CorrectionRecord(
            signal_type=correction.signal.signal_type,
            action_type="calibration_patch",
            market=market,
            params_before={"method": method, "outcome_count": len(relevant)},
            params_after={"skipped": True, "reason": "insufficient_outcomes"},
            outcome="neutral",
            confidence=0.0,
            created_at=datetime.now(UTC),
            notes=f"Skipped: only {len(relevant)} outcomes (need {MIN_OUTCOMES_FOR_PATCH})",
        )
        return record, None

    probs = np.array([o.calibrated_probability for o in relevant], dtype=float)
    labels = np.array([1.0 if o.hit else 0.0 for o in relevant], dtype=float)

    patch = _fit_patch(probs, labels, method)
    patched_probs = _apply_patch(patch, probs)
    improvement = _brier_improvement(probs, patched_probs, labels)

    record = CorrectionRecord(
        signal_type=correction.signal.signal_type,
        action_type="calibration_patch",
        market=market,
        params_before={"method": method, "outcome_count": len(relevant)},
        params_after={
            "method": method,
            "brier_improvement": float(improvement),
            "patch_fitted": True,
        },
        ece_before=correction.signal.metrics.get("ece"),
        outcome="pending",
        confidence=correction.confidence,
        created_at=datetime.now(UTC),
        notes=f"Calibration patch ({method}) on {len(relevant)} outcomes, brier_delta={improvement:.4f}",
    )
    return record, patch


def _fit_patch(
    probs: np.ndarray,
    labels: np.ndarray,
    method: str,
) -> object:
    if method == "isotonic":
        model = IsotonicRegression(out_of_bounds="clip")
        model.fit(probs, labels)
        return model
    model = LogisticRegression(random_state=42)
    model.fit(probs.reshape(-1, 1), labels)
    return model


def _apply_patch(patch: object, probs: np.ndarray) -> np.ndarray:
    if isinstance(patch, IsotonicRegression):
        return np.asarray(patch.predict(probs), dtype=float)
    if isinstance(patch, LogisticRegression):
        return np.asarray(patch.predict_proba(probs.reshape(-1, 1))[:, 1], dtype=float)
    return probs


def _brier_improvement(
    original: np.ndarray,
    patched: np.ndarray,
    labels: np.ndarray,
) -> float:
    brier_before = float(np.mean((original - labels) ** 2))
    brier_after = float(np.mean((patched - labels) ** 2))
    return brier_before - brier_after

"""Adjusts feature weights via brain overrides without dropping features.

The training pipeline reads weight overrides from the brain store and
applies them as multiplicative scaling on feature columns before model fit.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from app.services.brain.brain import Brain
from app.services.brain.contracts import CorrectionRecord, PlannedCorrection

logger = logging.getLogger(__name__)

# Safety: never scale below 0.20 or above 2.0
SCALE_FLOOR = 0.20
SCALE_CEILING = 2.0


def execute_weight_adjustment(
    correction: PlannedCorrection,
    brain: Brain,
) -> CorrectionRecord:
    """Apply a feature weight adjustment and record it in the brain."""
    market = correction.market or "all"
    params = correction.parameters
    reduce_pct = params.get("reduce_pct", 0.15)
    target_features = params.get("target_features", [])

    current_overrides = brain.get_weight_overrides(market if market != "all" else None)
    market_overrides = current_overrides.get(market, {})

    params_before: dict[str, float] = {}
    params_after: dict[str, float] = {}

    for feature in target_features:
        current_scale = market_overrides.get(feature, 1.0)
        new_scale = max(SCALE_FLOOR, current_scale * (1.0 - reduce_pct))
        params_before[feature] = current_scale
        params_after[feature] = new_scale
        brain.set_weight_override(
            market=market,
            feature_name=feature,
            scale_factor=new_scale,
            reason=f"Brain auto-adjust: {correction.signal.signal_type}",
        )
        logger.info(
            "Weight override: %s/%s %.3f -> %.3f",
            market,
            feature,
            current_scale,
            new_scale,
        )

    ece_before = correction.signal.metrics.get("ece")
    record = CorrectionRecord(
        signal_type=correction.signal.signal_type,
        action_type="weight_adjust",
        market=correction.market,
        params_before=params_before,
        params_after=params_after,
        ece_before=ece_before,
        outcome="pending",
        confidence=correction.confidence,
        created_at=datetime.now(UTC),
        notes=f"Reduced {len(target_features)} features by {reduce_pct:.0%}",
    )
    return record


def revert_weight_adjustment(
    correction_record: CorrectionRecord,
    brain: Brain,
) -> None:
    """Revert a weight adjustment by restoring params_before."""
    market = correction_record.market or "all"
    for feature, original_scale in correction_record.params_before.items():
        if abs(original_scale - 1.0) < 1e-6:
            brain.deactivate_weight_override(market, feature)
        else:
            brain.set_weight_override(
                market=market,
                feature_name=feature,
                scale_factor=original_scale,
                reason="Brain auto-revert: correction worsened ECE",
            )
        logger.info("Reverted weight override: %s/%s -> %.3f", market, feature, original_scale)

"""Plans corrections based on diagnostic signals and brain memory.

Deterministic decision logic with safety caps. Uses the brain's strategy
memory to prefer proven fixes, and falls back to conservative defaults
for novel problems.
"""

from __future__ import annotations

import logging
from typing import Any

from app.services.brain.brain import Brain
from app.services.brain.contracts import (
    CorrectionPlan,
    DiagnosticSignal,
    PlannedCorrection,
)

logger = logging.getLogger(__name__)

# Safety caps
MAX_CORRECTIONS_PER_RUN = 3
MAX_WEIGHT_CHANGE_PCT = 0.20
REVERT_AFTER_RUNS = 2

# Default correction templates when brain has no memory
_DEFAULT_STRATEGIES: dict[str, dict[str, Any]] = {
    "dnp_contamination": {
        "action_type": "dnp_filter",
        "parameters": {"anomaly_drop_threshold": 0.50, "min_minutes_guard": 5},
        "confidence": 0.80,
        "expected_improvement": 0.03,
    },
    "overfit": {
        "action_type": "weight_adjust",
        "parameters": {"reduce_pct": 0.15, "target_features": ["simulation_samples"]},
        "confidence": 0.50,
        "expected_improvement": 0.02,
    },
    "calibration_drift": {
        "action_type": "calibration_patch",
        "parameters": {"method": "isotonic", "window_size": 200},
        "confidence": 0.60,
        "expected_improvement": 0.015,
    },
    "extreme_probability": {
        "action_type": "dnp_filter",
        "parameters": {"anomaly_drop_threshold": 0.50, "min_minutes_guard": 5},
        "confidence": 0.75,
        "expected_improvement": 0.025,
    },
    "projection_divergence": {
        "action_type": "selective_retrain",
        "parameters": {"full_retrain": False},
        "confidence": 0.40,
        "expected_improvement": 0.02,
    },
}


def plan_corrections(
    signals: list[DiagnosticSignal],
    brain: Brain,
    dry_run: bool = True,
) -> CorrectionPlan:
    """Build a correction plan from diagnostic signals using brain memory.

    1. For each signal, ask brain for a proven strategy.
    2. Fall back to defaults for novel problems.
    3. Rank by severity x expected impact.
    4. Cap at MAX_CORRECTIONS_PER_RUN.
    """
    candidates: list[PlannedCorrection] = []

    for signal in signals:
        correction = _plan_single_correction(signal, brain)
        if correction is not None:
            candidates.append(correction)

    # Deduplicate: keep the highest-confidence correction per (action_type, market)
    seen: dict[tuple[str, str | None], PlannedCorrection] = {}
    for c in candidates:
        key = (c.action_type, c.market)
        if key not in seen or c.confidence > seen[key].confidence:
            seen[key] = c

    # Sort by severity priority then confidence
    severity_order = {"critical": 0, "high": 1, "medium": 2}
    ranked = sorted(
        seen.values(),
        key=lambda c: (severity_order.get(c.signal.severity, 3), -c.confidence),
    )

    # Cap
    final = tuple(ranked[:MAX_CORRECTIONS_PER_RUN])

    logger.info(
        "Correction plan: %d signals -> %d candidates -> %d planned (dry_run=%s)",
        len(signals),
        len(candidates),
        len(final),
        dry_run,
    )

    return CorrectionPlan(
        corrections=final,
        max_corrections_per_run=MAX_CORRECTIONS_PER_RUN,
        max_weight_change_pct=MAX_WEIGHT_CHANGE_PCT,
        revert_after_runs=REVERT_AFTER_RUNS,
        dry_run=dry_run,
    )


def _plan_single_correction(
    signal: DiagnosticSignal,
    brain: Brain,
) -> PlannedCorrection | None:
    """Plan one correction for a single signal."""

    # 1. Ask brain for a proven strategy
    strategy = brain.best_strategy(signal.signal_type, signal.market)
    if strategy is not None and strategy.sample_count >= 3:
        logger.info(
            "Using proven strategy for %s/%s (success_rate=%.0f%%, n=%d)",
            signal.signal_type,
            signal.market or "global",
            strategy.success_rate * 100,
            strategy.sample_count,
        )
        return PlannedCorrection(
            signal=signal,
            action_type=strategy.action_template,
            market=signal.market,
            parameters=strategy.parameters,
            strategy_source=strategy,
            expected_improvement=strategy.avg_ece_improvement,
            confidence=strategy.success_rate,
            safety_notes=f"Proven strategy (n={strategy.sample_count})",
        )

    # 2. Fall back to default
    default = _DEFAULT_STRATEGIES.get(signal.signal_type)
    if default is None:
        logger.info("No default strategy for signal type: %s", signal.signal_type)
        return None

    return PlannedCorrection(
        signal=signal,
        action_type=default["action_type"],
        market=signal.market,
        parameters=dict(default["parameters"]),
        strategy_source=None,
        expected_improvement=default["expected_improvement"],
        confidence=default["confidence"],
        safety_notes="Default strategy (no brain memory yet)",
    )

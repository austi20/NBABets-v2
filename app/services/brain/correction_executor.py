"""Executes a correction plan with safety gates and brain recording.

Orchestrates the correction actions and records results in the brain
so the learning engine can evaluate them later.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from app.services.brain.brain import Brain
from app.services.brain.contracts import CorrectionPlan, CorrectionRecord, PlannedCorrection

logger = logging.getLogger(__name__)


def execute_correction_plan(
    plan: CorrectionPlan,
    brain: Brain,
    session: Any = None,
) -> list[int]:
    """Execute corrections from a plan and return correction IDs.

    If ``plan.dry_run`` is True, corrections are recorded but not applied.
    """
    correction_ids: list[int] = []

    if not plan.corrections:
        logger.info("No corrections to execute")
        return correction_ids

    for correction in plan.corrections:
        if plan.dry_run:
            cid = _record_dry_run(correction, brain)
        else:
            cid = _execute_single(correction, brain, session)
        correction_ids.append(cid)

    # Write daily summary to vault
    brain.write_daily_summary(
        report_date=date.today().isoformat(),
        signals_found=len({c.signal.signal_type for c in plan.corrections}),
        corrections_planned=len(plan.corrections),
        corrections_executed=0 if plan.dry_run else len(plan.corrections),
        dry_run=plan.dry_run,
        notes=_build_summary_notes(plan),
    )

    return correction_ids


def _record_dry_run(correction: PlannedCorrection, brain: Brain) -> int:
    """Record what WOULD have been done without actually doing it."""
    record = CorrectionRecord(
        signal_type=correction.signal.signal_type,
        action_type=correction.action_type,
        market=correction.market,
        params_before={},
        params_after=correction.parameters,
        ece_before=correction.signal.metrics.get("ece"),
        outcome="pending",
        confidence=correction.confidence,
        notes=f"[DRY RUN] Would execute: {correction.action_type} for {correction.market or 'global'}",
    )
    cid = brain.record_correction(record)
    logger.info(
        "[DRY RUN] Would execute %s for %s (confidence=%.2f)",
        correction.action_type,
        correction.market or "global",
        correction.confidence,
    )
    return cid


def _execute_single(
    correction: PlannedCorrection,
    brain: Brain,
    session: Any = None,
) -> int:
    """Execute a single correction and record it."""
    action_type = correction.action_type

    if action_type == "weight_adjust" or action_type == "feature_dampen":
        from app.services.brain.actions.weight_adjuster import execute_weight_adjustment

        record = execute_weight_adjustment(correction, brain)

    elif action_type == "selective_retrain":
        from app.services.brain.actions.selective_retrain import execute_selective_retrain

        record = execute_selective_retrain(correction, session)

    elif action_type == "calibration_patch":
        from app.services.brain.actions.calibration_patcher import execute_calibration_patch

        outcomes = brain.recall_outcomes(market=correction.market, limit=500)
        record, _patch = execute_calibration_patch(correction, outcomes)

    elif action_type == "dnp_filter":
        from app.services.brain.actions.dnp_filter import execute_dnp_filter

        record = execute_dnp_filter(correction)

    else:
        logger.warning("Unknown action type: %s", action_type)
        record = CorrectionRecord(
            signal_type=correction.signal.signal_type,
            action_type=action_type,
            market=correction.market,
            params_before={},
            params_after=correction.parameters,
            outcome="neutral",
            notes=f"Unknown action type: {action_type}",
        )

    cid = brain.record_correction(record)
    logger.info(
        "Executed %s for %s (correction_id=%d)",
        action_type,
        correction.market or "global",
        cid,
    )
    return cid


def _build_summary_notes(plan: CorrectionPlan) -> str:
    lines = []
    for c in plan.corrections:
        strategy_note = ""
        if c.strategy_source:
            strategy_note = f" (proven: {c.strategy_source.success_rate:.0%} success, n={c.strategy_source.sample_count})"
        else:
            strategy_note = " (default strategy)"
        lines.append(
            f"- {c.action_type} on {c.market or 'global'}: "
            f"confidence={c.confidence:.2f}, expected_improvement={c.expected_improvement:.4f}"
            f"{strategy_note}"
        )
    return "\n".join(lines)

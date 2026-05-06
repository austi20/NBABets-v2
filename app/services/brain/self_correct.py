"""Post-startup self-correction cycle.

Called after the automation report is generated. Reads the report,
diagnoses issues, plans corrections, executes them (or logs dry-run),
and triggers selective retrains if needed.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Any

from app.services.brain.brain import Brain
from app.services.brain.correction_executor import execute_correction_plan
from app.services.brain.correction_planner import plan_corrections
from app.services.brain.report_interpreter import interpret_report, interpret_report_file

logger = logging.getLogger(__name__)

# Graduated autonomy levels
AUTONOMY_OBSERVE = "observe"      # Log only, don't execute
AUTONOMY_SUGGEST = "suggest"      # Show in desktop, require approval
AUTONOMY_SEMI_AUTO = "semi_auto"  # Auto-execute proven strategies, manual for rest
AUTONOMY_AUTO = "auto"            # Full autonomous with revert safety net

# Thresholds for graduating autonomy
_MIN_OUTCOMES_FOR_SUGGEST = 50
_MIN_OUTCOMES_FOR_SEMI_AUTO = 100
_MIN_OUTCOMES_FOR_AUTO = 500


def run_self_correction_cycle(
    report_path: str | Path | None = None,
    report_text: str | None = None,
    session: Any = None,
    autonomy_level: str | None = None,
) -> dict[str, Any]:
    """Main entry point for the brain's self-correction cycle.

    Args:
        report_path: Path to the automation report markdown file.
        report_text: Raw report text (alternative to report_path).
        session: SQLAlchemy session for DB operations.
        autonomy_level: Override for graduated autonomy. If None, auto-detected.

    Returns:
        Summary dict with signals found, corrections planned/executed, etc.
    """
    brain = Brain()
    result: dict[str, Any] = {
        "signals": [],
        "plan": None,
        "correction_ids": [],
        "autonomy_level": "observe",
        "dry_run": True,
    }

    try:
        # 1. Interpret the report
        if report_text:
            signals = interpret_report(report_text)
        elif report_path:
            signals = interpret_report_file(report_path)
        else:
            logger.info("No report provided, skipping self-correction")
            return result

        result["signals"] = [
            {
                "type": s.signal_type,
                "severity": s.severity,
                "market": s.market,
                "metrics": s.metrics,
            }
            for s in signals
        ]

        if not signals:
            logger.info("No actionable signals found in report")
            return result

        logger.info("Found %d diagnostic signals", len(signals))

        # 2. Determine autonomy level
        level = autonomy_level or _detect_autonomy_level(brain)
        result["autonomy_level"] = level
        dry_run = level in (AUTONOMY_OBSERVE, AUTONOMY_SUGGEST)
        result["dry_run"] = dry_run

        # 3. Plan corrections
        plan = plan_corrections(signals, brain, dry_run=dry_run)
        result["plan"] = {
            "correction_count": len(plan.corrections),
            "dry_run": plan.dry_run,
            "corrections": [
                {
                    "action": c.action_type,
                    "market": c.market,
                    "confidence": c.confidence,
                    "strategy": "proven" if c.strategy_source else "default",
                }
                for c in plan.corrections
            ],
        }

        if not plan.corrections:
            logger.info("No corrections planned")
            return result

        # 4. For semi-auto, filter to only proven strategies
        if level == AUTONOMY_SEMI_AUTO:
            proven = tuple(c for c in plan.corrections if c.strategy_source is not None)
            from app.services.brain.contracts import CorrectionPlan

            plan = CorrectionPlan(
                corrections=proven,
                max_corrections_per_run=plan.max_corrections_per_run,
                max_weight_change_pct=plan.max_weight_change_pct,
                revert_after_runs=plan.revert_after_runs,
                dry_run=False,
            )

        # 5. Execute
        correction_ids = execute_correction_plan(plan, brain, session)
        result["correction_ids"] = correction_ids

        logger.info(
            "Self-correction cycle complete: %d signals, %d corrections %s",
            len(signals),
            len(correction_ids),
            "(dry run)" if dry_run else "(executed)",
        )

    except Exception:
        logger.exception("Self-correction cycle failed")
    finally:
        brain.close()

    return result


def run_outcome_learning(session: Any = None) -> dict[str, int]:
    """Collect outcomes and run the learning engine.

    Called the day after predictions were made, when game results
    are available.
    """
    brain = Brain()
    try:
        # Collect yesterday's outcomes
        from datetime import timedelta

        from app.services.brain.learner import learn_from_outcomes
        from app.services.brain.outcome_collector import collect_outcomes

        yesterday = date.today() - timedelta(days=1)
        if session is not None:
            collected = collect_outcomes(session, brain, target_date=yesterday)
            logger.info("Collected %d outcomes for %s", collected, yesterday)
        else:
            logger.warning("No session provided, skipping outcome collection")

        # Run learning
        stats = learn_from_outcomes(brain)
        return stats
    except Exception:
        logger.exception("Outcome learning failed")
        return {"error": True}
    finally:
        brain.close()


def _detect_autonomy_level(brain: Brain) -> str:
    """Auto-detect autonomy level based on brain maturity."""
    stats = brain.correction_stats()
    total = stats.get("total", 0)
    success_rate = stats.get("success_rate", 0.0)

    if total >= _MIN_OUTCOMES_FOR_AUTO and success_rate >= 0.70:
        return AUTONOMY_AUTO
    if total >= _MIN_OUTCOMES_FOR_SEMI_AUTO and success_rate >= 0.60:
        return AUTONOMY_SEMI_AUTO
    if total >= _MIN_OUTCOMES_FOR_SUGGEST:
        return AUTONOMY_SUGGEST
    return AUTONOMY_OBSERVE

"""Post-run grading: persist lesson cards from prior examiner AgentRunEvent rows."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config.settings import Settings
from app.models.all import AgentRunEvent
from app.services.examiner.contracts import LabeledPropDataset
from app.services.examiner.store import ExaminerStore

logger = logging.getLogger(__name__)


def capture_daily_feedback(
    session: Session,
    *,
    settings: Settings,
    report_date: date,
    csv_dataset: LabeledPropDataset | None,
) -> None:
    """Grade yesterday's examiner runs and append lesson cards (best-effort).

    Swallow all errors — callers wrap in try/except for defense in depth.
    """

    try:
        _capture_daily_feedback_impl(session, settings=settings, report_date=report_date, csv_dataset=csv_dataset)
    except Exception:
        logger.warning("capture_daily_feedback failed", exc_info=True)


def _capture_daily_feedback_impl(
    session: Session,
    *,
    settings: Settings,
    report_date: date,
    csv_dataset: LabeledPropDataset | None,
) -> None:
    day = report_date - timedelta(days=1)
    start = datetime.combine(day, datetime.min.time(), tzinfo=UTC)
    end = datetime.combine(report_date, datetime.min.time(), tzinfo=UTC)

    rows = list(
        session.scalars(
            select(AgentRunEvent)
            .where(
                AgentRunEvent.agent_role == "accuracy_examiner",
                AgentRunEvent.created_at >= start,
                AgentRunEvent.created_at < end,
                AgentRunEvent.status != "started",
            )
            .order_by(AgentRunEvent.created_at.desc())
        ).all()
    )

    if not rows:
        return

    store = ExaminerStore(settings.brain_db_path)
    try:
        for ev in rows:
            payload = ev.payload or {}
            if not isinstance(payload, dict):
                continue
            findings = payload.get("examiner_findings") or []
            if not isinstance(findings, list):
                continue
            for item in findings[:12]:
                if not isinstance(item, dict):
                    continue
                signal = str(item.get("signal") or "data_quality_degraded")
                headline = str(item.get("headline") or "examiner_finding")
                market = item.get("market")
                lb = item.get("line_bucket")
                cb = item.get("confidence_bucket")
                outcome = _grade_finding(item, csv_dataset, day)
                store.insert_lesson_card(
                    market=str(market) if market else None,
                    line_bucket=str(lb) if lb else None,
                    confidence_bucket=cb if cb in ("low", "mid", "high", "extreme") else None,
                    signal=signal,
                    headline=headline[:500],
                    body=str(ev.detail or "")[:2000],
                    ece_before=None,
                    ece_after=None,
                    outcome=outcome,
                )
    finally:
        store.close()


def _grade_finding(
    item: dict[str, Any],
    csv_dataset: LabeledPropDataset | None,
    graded_day: date,
) -> str:
    """Coarse outcome label; expand when richer ground truth joins exist."""

    if csv_dataset is None:
        return "neutral"
    market = item.get("market")
    if not market:
        return "neutral"
    for ex in csv_dataset.examples:
        if ex.game_date != graded_day:
            continue
        if ex.market != str(market):
            continue
        return "neutral"
    return "neutral"

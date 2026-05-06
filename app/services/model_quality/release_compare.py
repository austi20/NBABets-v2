from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.all import ModelRun


@dataclass(frozen=True)
class ReleaseComparison:
    status: str
    summary: str
    candidate_run_id: int | None
    champion_run_id: int | None
    candidate_avg_ece: float | None
    champion_avg_ece: float | None
    candidate_data_quality_status: str
    champion_data_quality_status: str
    rationale: str


def compare_latest_model_runs(session: Session) -> ReleaseComparison:
    rows = session.execute(
        select(ModelRun.model_run_id, ModelRun.completed_at, ModelRun.metrics)
        .where(ModelRun.model_version.not_like("%_backtest"))
        .order_by(ModelRun.completed_at.desc())
        .limit(2)
    ).all()
    if len(rows) < 2:
        return ReleaseComparison(
            status="INSUFFICIENT_HISTORY",
            summary="Need at least two model runs to perform champion-challenger comparison.",
            candidate_run_id=rows[0].model_run_id if rows else None,
            champion_run_id=None,
            candidate_avg_ece=_avg_ece(rows[0].metrics) if rows else None,
            champion_avg_ece=None,
            candidate_data_quality_status=_data_quality_status(rows[0].metrics) if rows else "unknown",
            champion_data_quality_status="unknown",
            rationale="Comparison skipped due to insufficient run history.",
        )

    candidate = rows[0]
    champion = rows[1]
    return _compare_model_metric_payloads(
        candidate_run_id=int(candidate.model_run_id),
        champion_run_id=int(champion.model_run_id),
        candidate_metrics=candidate.metrics if isinstance(candidate.metrics, dict) else {},
        champion_metrics=champion.metrics if isinstance(champion.metrics, dict) else {},
        candidate_completed_at=candidate.completed_at,
        champion_completed_at=champion.completed_at,
    )


def _compare_model_metric_payloads(
    *,
    candidate_run_id: int,
    champion_run_id: int,
    candidate_metrics: dict[str, Any],
    champion_metrics: dict[str, Any],
    candidate_completed_at: datetime | None = None,
    champion_completed_at: datetime | None = None,
) -> ReleaseComparison:
    candidate_ece = _avg_ece(candidate_metrics)
    champion_ece = _avg_ece(champion_metrics)
    candidate_dq = _data_quality_status(candidate_metrics)
    champion_dq = _data_quality_status(champion_metrics)

    status = "HOLD"
    rationale = "Candidate does not exceed champion quality thresholds."
    if candidate_ece is not None and champion_ece is not None:
        if candidate_ece <= champion_ece and candidate_dq != "degraded":
            status = "PROMOTE_CANDIDATE"
            rationale = "Candidate has equal/better average calibration error and acceptable data quality."
        elif candidate_dq == "degraded":
            rationale = "Candidate training data quality is degraded."
        else:
            rationale = "Candidate calibration diagnostics are weaker than champion."
    elif candidate_dq != "degraded":
        status = "CAUTION"
        rationale = "ECE diagnostics missing; candidate data quality appears acceptable."

    summary = (
        f"candidate={candidate_run_id} (ece={_fmt(candidate_ece)}, dq={candidate_dq}) vs "
        f"champion={champion_run_id} (ece={_fmt(champion_ece)}, dq={champion_dq})"
    )
    if candidate_completed_at is not None or champion_completed_at is not None:
        summary += (
            f"; completed_at candidate={candidate_completed_at}, champion={champion_completed_at}"
        )
    return ReleaseComparison(
        status=status,
        summary=summary,
        candidate_run_id=candidate_run_id,
        champion_run_id=champion_run_id,
        candidate_avg_ece=candidate_ece,
        champion_avg_ece=champion_ece,
        candidate_data_quality_status=candidate_dq,
        champion_data_quality_status=champion_dq,
        rationale=rationale,
    )


def _avg_ece(metrics: dict[str, Any]) -> float | None:
    diagnostics = metrics.get("calibration_diagnostics")
    if not isinstance(diagnostics, dict) or not diagnostics:
        return None
    values: list[float] = []
    for market_payload in diagnostics.values():
        if not isinstance(market_payload, dict):
            continue
        ece = market_payload.get("ece")
        if isinstance(ece, (int, float)):
            values.append(float(ece))
    if not values:
        return None
    return float(sum(values) / len(values))


def _data_quality_status(metrics: dict[str, Any]) -> str:
    dq = metrics.get("training_data_quality")
    if isinstance(dq, dict):
        status = dq.get("status")
        if isinstance(status, str):
            return status
    return "unknown"


def _fmt(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.4f}"

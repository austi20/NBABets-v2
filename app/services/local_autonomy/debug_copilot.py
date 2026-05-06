from __future__ import annotations

from collections import Counter
from datetime import UTC, date, datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.all import AgentRunEvent, AIProviderEvent
from app.services.local_autonomy.contracts import FAILURE_TAXONOMY, DebugHint


def build_debug_hints(
    session: Session,
    *,
    report_date: date,
    lookback_hours: int = 24,
    startup_error_log: str | None = None,
    orchestrator: Any | None = None,
) -> tuple[DebugHint, ...]:
    anchor = datetime.combine(report_date, datetime.min.time(), tzinfo=UTC)
    since = max(anchor, datetime.now(UTC) - timedelta(hours=lookback_hours))
    provider_errors = session.execute(
        select(AIProviderEvent.provider_name, AIProviderEvent.detail)
        .where(AIProviderEvent.status == "error", AIProviderEvent.created_at >= since)
        .order_by(AIProviderEvent.created_at.desc())
        .limit(30)
    ).all()
    agent_errors = session.execute(
        select(AgentRunEvent.error_category, AgentRunEvent.detail)
        .where(AgentRunEvent.status.in_(("error", "blocked", "degraded")), AgentRunEvent.created_at >= since)
        .order_by(AgentRunEvent.created_at.desc())
        .limit(40)
    ).all()

    hints: list[DebugHint] = []
    if provider_errors:
        counts = Counter(str(name) for name, _ in provider_errors)
        top_provider, error_count = counts.most_common(1)[0]
        hints.append(
            DebugHint(
                category="network_provider",
                summary=f"{error_count} AI provider errors from {top_provider} in lookback window.",
                next_steps=(
                    "Validate local endpoint reachability and timeout budget.",
                    "Check malformed-response rate and fallback to deterministic summary.",
                ),
            )
        )

    categorized: Counter[str] = Counter()
    for category, _ in agent_errors:
        normalized = str(category or "orchestration")
        categorized[normalized] += 1
    for category, count in categorized.most_common(3):
        normalized = category if category in FAILURE_TAXONOMY else "orchestration"
        hints.append(
            DebugHint(
                category=normalized,
                summary=f"{count} recent agent failures categorized as {normalized}.",
                next_steps=_next_steps_for_category(normalized),
            )
        )

    # --- Extreme prediction sentinel ---
    from app.models.all import Prediction  # noqa: E402 — deferred to avoid circular import

    extreme_prediction_count = session.scalar(
        select(Prediction.prediction_id)
        .where(
            Prediction.predicted_at >= since,
            (Prediction.over_probability > 0.97) | (Prediction.over_probability < 0.03),
        )
        .with_only_columns(Prediction.prediction_id)
        .limit(1)
    )
    if extreme_prediction_count is not None:
        hints.append(
            DebugHint(
                category="data",
                summary=(
                    "Extreme-probability predictions detected (>97% or <3%). "
                    "Likely DNP contamination or stale feature data."
                ),
                next_steps=(
                    "Check training data for zero-minute games leaking into rolling averages.",
                    "Verify pgl.minutes > 0 filter in DatasetLoader.load_historical_player_games().",
                    "Inspect calibration support boundaries for affected markets.",
                    "Consider full cache reset and retrain if contamination confirmed.",
                ),
            )
        )

    if not hints:
        hints.append(
            DebugHint(
                category="orchestration",
                summary="No critical failures detected in recent window.",
                next_steps=("Keep packetized validation cadence and monitor trend alerts.",),
            )
        )
    if startup_error_log is not None and orchestrator is not None:
        ai_hint = _build_startup_error_hint(startup_error_log, orchestrator)
        if ai_hint is not None:
            hints.append(ai_hint)
    return tuple(hints)


def _build_startup_error_hint(error_log: str, orchestrator: Any) -> DebugHint | None:
    from app.services.local_autonomy.contracts import extract_json_object

    prompt = (
        "Analyze this startup error and suggest a fix. "
        "Return STRICT JSON only: "
        '{"root_cause": str, "suggested_fix": str, "affected_component": str, '
        '"severity": "low"|"medium"|"high"}\n'
        f"Error log:\n{error_log[:3000]}"
    )
    ai_result = orchestrator.summarize(task_name="startup_error_analysis", prompt=prompt)
    parsed = extract_json_object(
        ai_result.text,
        required_keys=frozenset({"root_cause", "suggested_fix", "affected_component"}),
    )
    if parsed is None:
        return None
    return DebugHint(
        category="startup_error",
        summary=(
            f"[{parsed.get('severity', 'low')}] {parsed.get('root_cause', 'unknown')} "
            f"(component: {parsed.get('affected_component', 'unknown')})"
        ),
        next_steps=(str(parsed.get("suggested_fix", "Review startup error log manually.")),),
    )


def _next_steps_for_category(category: str) -> tuple[str, ...]:
    if category == "quality_gate":
        return (
            "Run narrow lint/type/test packet on the touched file cluster.",
            "Escalate to full gate only after packet checks pass.",
        )
    if category == "model_artifact":
        return (
            "Verify latest artifact namespace and model version alignment.",
            "Re-run bounded retrain smoke before full retrain.",
        )
    if category == "network_provider":
        return (
            "Check provider rate-limit and retry/backoff telemetry.",
            "Validate fallback provider chain and schema drift handling.",
        )
    if category == "data":
        return (
            "Validate source payload freshness and record counts.",
            "Confirm entity matching and market-join completeness.",
        )
    return (
        "Capture a bounded repro and classify with failure taxonomy.",
        "Apply minimum-change fix packet with explicit evidence logging.",
    )

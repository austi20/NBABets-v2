from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.config.settings import get_settings
from app.models.all import AgentRunEvent, RawPayload


@dataclass(frozen=True)
class ApiCoverageReport:
    tier: str
    summary: str
    reasons: list[str]
    payload_counts: dict[str, int]
    provider_counts: dict[str, int]
    network_error_count: int
    network_ok_count: int


def assess_api_coverage(session: Session, *, report_date: date) -> ApiCoverageReport:
    settings = get_settings()
    day_start = datetime.combine(report_date, datetime.min.time(), tzinfo=UTC)
    day_end = day_start + timedelta(days=1)

    payload_rows = session.execute(
        select(RawPayload.provider_type, func.count(RawPayload.payload_id))
        .where(RawPayload.fetched_at >= day_start, RawPayload.fetched_at < day_end)
        .group_by(RawPayload.provider_type)
    ).all()
    payload_counts = {str(provider_type): int(count or 0) for provider_type, count in payload_rows}

    provider_rows = session.execute(
        select(RawPayload.provider_name, func.count(RawPayload.payload_id))
        .where(RawPayload.fetched_at >= day_start, RawPayload.fetched_at < day_end)
        .group_by(RawPayload.provider_name)
    ).all()
    provider_counts = {str(provider_name): int(count or 0) for provider_name, count in provider_rows}

    network_ok_count = int(
        session.scalar(
            select(func.count(AgentRunEvent.run_id)).where(
                AgentRunEvent.agent_role == "network_observer",
                AgentRunEvent.status == "ok",
                AgentRunEvent.created_at >= day_start,
                AgentRunEvent.created_at < day_end,
            )
        )
        or 0
    )
    network_error_count = int(
        session.scalar(
            select(func.count(AgentRunEvent.run_id)).where(
                AgentRunEvent.agent_role == "network_observer",
                AgentRunEvent.status == "error",
                AgentRunEvent.created_at >= day_start,
                AgentRunEvent.created_at < day_end,
            )
        )
        or 0
    )

    stats_payloads = payload_counts.get("stats", 0)
    odds_payloads = payload_counts.get("odds", 0)
    injuries_payloads = payload_counts.get("injuries", 0)

    reasons: list[str] = []
    if stats_payloads <= 0:
        reasons.append("No stats payloads available for target day.")
    if odds_payloads <= 0:
        reasons.append("No odds payloads available for target day.")
    injuries_required = settings.injury_provider.lower() == "balldontlie"
    if injuries_required and injuries_payloads <= 0:
        reasons.append("Configured injuries provider has no same-day payloads.")

    if "nba_api" not in provider_counts and stats_payloads > 0:
        reasons.append("Primary stats provider (nba_api) not observed; fallback source likely active.")

    total_network_events = network_ok_count + network_error_count
    if total_network_events > 0:
        error_rate = network_error_count / total_network_events
        if error_rate >= 0.35:
            reasons.append(f"Network observer error rate high ({error_rate:.0%}).")

    tier = "A"
    if stats_payloads <= 0 or odds_payloads <= 0:
        tier = "C"
    elif reasons:
        tier = "B"

    summary = {
        "A": "Tier A (full fidelity): core provider coverage healthy.",
        "B": "Tier B (partial fidelity): usable coverage with caution.",
        "C": "Tier C (degraded): insufficient coverage for high-confidence model decisions.",
    }[tier]

    return ApiCoverageReport(
        tier=tier,
        summary=summary,
        reasons=reasons,
        payload_counts=payload_counts,
        provider_counts=provider_counts,
        network_error_count=network_error_count,
        network_ok_count=network_ok_count,
    )

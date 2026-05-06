from __future__ import annotations

from datetime import UTC, date, datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models.all import AgentRunEvent, RawPayload
from app.services.agents.contracts import AgentAction, AgentResult, AgentTask


class ApiMonitorAgent:
    role = "api_monitor"

    def __init__(self, session: Session) -> None:
        self._session = session

    def handle(self, task: AgentTask) -> AgentResult:
        today = date.today()
        day_start = datetime.combine(today, datetime.min.time(), tzinfo=UTC)
        rows = self._session.execute(
            select(RawPayload.provider_name, func.count(RawPayload.payload_id))
            .where(RawPayload.fetched_at >= day_start)
            .group_by(RawPayload.provider_name)
        ).all()
        counts = {str(provider): int(count or 0) for provider, count in rows}
        failure_rows = self._session.execute(
            select(AgentRunEvent.payload).where(
                AgentRunEvent.agent_role == "api_monitor",
                AgentRunEvent.status == "error",
                AgentRunEvent.created_at >= day_start,
            )
        ).all()
        failures: dict[str, int] = {}
        for (payload,) in failure_rows:
            provider_name = str((payload or {}).get("provider_name", "unknown"))
            failures[provider_name] = failures.get(provider_name, 0) + 1

        actions: list[AgentAction] = []
        for provider_name, count in counts.items():
            if count <= 0:
                continue
            # Very low payload volume is an early indicator that endpoint contracts changed.
            if count < 2:
                actions.append(
                    AgentAction(
                        action_type="provider_contract_review",
                        reason=f"Provider {provider_name} has unusually low payload volume today.",
                        payload={"provider_name": provider_name, "payloads_today": count},
                        safe_to_auto_execute=False,
                    )
                )
        for provider_name, failure_count in failures.items():
            if failure_count >= 3:
                actions.append(
                    AgentAction(
                        action_type="promote_fallback_provider",
                        reason=f"Provider {provider_name} hit repeated ingestion failures.",
                        payload={"provider_name": provider_name, "failure_count": failure_count},
                        safe_to_auto_execute=False,
                    )
                )
                actions.append(
                    AgentAction(
                        action_type="increase_backoff_profile",
                        reason=f"Provider {provider_name} likely needs lower request pressure.",
                        payload={"provider_name": provider_name},
                        safe_to_auto_execute=False,
                    )
                )

        status = "ok" if not actions else "recommendation"
        summary = "No API drift indicators detected." if not actions else f"Detected {len(actions)} API drift indicator(s)."
        return AgentResult(
            task_id=task.task_id,
            role=self.role,
            status=status,
            summary=summary,
            actions=actions,
            confidence=0.7 if actions else 0.9,
            details={"payload_counts_today": counts, "provider_failures_today": failures, "dry_run": task.dry_run},
        )

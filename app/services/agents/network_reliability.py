from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.config.settings import get_settings
from app.models.all import AgentRunEvent
from app.services.agents.contracts import AgentAction, AgentResult, AgentTask


class NetworkReliabilityAgent:
    role = "network"

    def __init__(self, session: Session) -> None:
        self._session = session
        self._settings = get_settings()

    def handle(self, task: AgentTask) -> AgentResult:
        window_start = datetime.now(UTC) - timedelta(hours=6)
        failure_count = self._session.scalar(
            select(func.count(AgentRunEvent.run_id)).where(
                AgentRunEvent.agent_role == "network_observer",
                AgentRunEvent.status == "error",
                AgentRunEvent.created_at >= window_start,
            )
        ) or 0
        timeout_count = self._session.scalar(
            select(func.count(AgentRunEvent.run_id)).where(
                AgentRunEvent.agent_role == "network_observer",
                AgentRunEvent.error_category == "timeout",
                AgentRunEvent.created_at >= window_start,
            )
        ) or 0
        avg_latency = self._session.scalar(
            select(func.avg(AgentRunEvent.latency_ms)).where(
                AgentRunEvent.agent_role == "network_observer",
                AgentRunEvent.latency_ms.is_not(None),
                AgentRunEvent.created_at >= window_start,
            )
        )

        actions: list[AgentAction] = []
        if failure_count >= self._settings.network_circuit_breaker_failures:
            actions.append(
                AgentAction(
                    action_type="recommend_provider_rotation",
                    reason="Recent endpoint failures exceeded circuit threshold.",
                    payload={"failures_last_6h": int(failure_count)},
                    safe_to_auto_execute=False,
                )
            )
        if timeout_count >= 3:
            actions.append(
                AgentAction(
                    action_type="recommend_retry_tuning",
                    reason="Timeout frequency indicates backoff/retry tuning needed.",
                    payload={"timeouts_last_6h": int(timeout_count)},
                    safe_to_auto_execute=False,
                )
            )

        summary = "Network reliability signals are healthy."
        status = "ok"
        if actions:
            status = "recommendation"
            summary = f"Generated {len(actions)} network reliability recommendation(s)."
        return AgentResult(
            task_id=task.task_id,
            role=self.role,
            status=status,
            summary=summary,
            actions=actions,
            confidence=0.72 if actions else 0.92,
            details={
                "window_hours": 6,
                "failures_last_6h": int(failure_count),
                "timeouts_last_6h": int(timeout_count),
                "avg_latency_ms": float(avg_latency) if avg_latency is not None else None,
                "dry_run": task.dry_run,
            },
        )

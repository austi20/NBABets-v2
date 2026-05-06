from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy.orm import Session

from app.models.all import AgentRunEvent


class AgentRunService:
    def __init__(self, session: Session) -> None:
        self._session = session

    def record(
        self,
        *,
        task_id: str,
        agent_role: str,
        event_type: str,
        status: str,
        latency_ms: int | None = None,
        confidence: float | None = None,
        action_summary: str | None = None,
        error_category: str | None = None,
        detail: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        event = AgentRunEvent(
            task_id=task_id,
            agent_role=agent_role,
            event_type=event_type,
            status=status,
            latency_ms=latency_ms,
            confidence=confidence,
            action_summary=action_summary,
            error_category=error_category,
            detail=detail,
            payload=payload or {},
            created_at=datetime.now(UTC),
        )
        self._session.add(event)
        self._session.commit()

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol
from uuid import uuid4


@dataclass(frozen=True)
class AgentAction:
    action_type: str
    reason: str
    payload: dict[str, Any] = field(default_factory=dict)
    safe_to_auto_execute: bool = False


@dataclass(frozen=True)
class AgentTask:
    role: str
    task_type: str
    input_payload: dict[str, Any] = field(default_factory=dict)
    dry_run: bool = True
    task_id: str = field(default_factory=lambda: uuid4().hex)


@dataclass(frozen=True)
class AgentResult:
    task_id: str
    role: str
    status: str
    summary: str
    actions: list[AgentAction] = field(default_factory=list)
    confidence: float | None = None
    details: dict[str, Any] = field(default_factory=dict)
    error_category: str | None = None


class AgentHandler(Protocol):
    role: str

    def handle(self, task: AgentTask) -> AgentResult:
        raise NotImplementedError


def utc_now() -> datetime:
    return datetime.now(UTC)

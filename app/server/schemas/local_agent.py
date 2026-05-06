from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel

from app.services.insights import LocalAgentStatus


class LocalAgentStatusModel(BaseModel):
    enabled: bool
    auto_execute_safe: bool
    updated_at: datetime
    updated_by: str
    note: str
    last_run_status: str
    last_run_at: datetime | None
    last_summary: str
    last_confidence: float | None

    @classmethod
    def from_dataclass(cls, value: LocalAgentStatus) -> LocalAgentStatusModel:
        return cls(**value.__dict__)


class LocalAgentPolicyRequest(BaseModel):
    policy: Literal["enable", "disable", "safe_auto_enable", "safe_auto_disable"]


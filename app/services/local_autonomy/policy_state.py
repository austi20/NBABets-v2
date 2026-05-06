from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from app.config.settings import get_settings


@dataclass(frozen=True)
class LocalAgentPolicyState:
    enabled: bool
    auto_execute_safe: bool
    updated_at: datetime
    updated_by: str
    note: str


def load_local_agent_policy_state() -> LocalAgentPolicyState:
    path = get_settings().local_agent_policy_state_path
    if not path.exists():
        state = LocalAgentPolicyState(
            enabled=get_settings().local_autonomy_enabled,
            auto_execute_safe=False,
            updated_at=datetime.now(UTC),
            updated_by="default",
            note="bootstrap default",
        )
        save_local_agent_policy_state(state)
        return state
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return LocalAgentPolicyState(
            enabled=False,
            auto_execute_safe=False,
            updated_at=datetime.now(UTC),
            updated_by="recover",
            note="invalid state file",
        )
    return _state_from_payload(raw)


def save_local_agent_policy_state(state: LocalAgentPolicyState) -> None:
    path = get_settings().local_agent_policy_state_path
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "enabled": state.enabled,
        "auto_execute_safe": state.auto_execute_safe,
        "updated_at": state.updated_at.astimezone(UTC).isoformat(),
        "updated_by": state.updated_by,
        "note": state.note,
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def update_local_agent_policy_state(
    *,
    enabled: bool | None = None,
    auto_execute_safe: bool | None = None,
    updated_by: str,
    note: str,
) -> LocalAgentPolicyState:
    previous = load_local_agent_policy_state()
    updated = LocalAgentPolicyState(
        enabled=previous.enabled if enabled is None else enabled,
        auto_execute_safe=previous.auto_execute_safe if auto_execute_safe is None else auto_execute_safe,
        updated_at=datetime.now(UTC),
        updated_by=updated_by,
        note=note,
    )
    save_local_agent_policy_state(updated)
    return updated


def _state_from_payload(payload: dict[str, Any]) -> LocalAgentPolicyState:
    enabled = bool(payload.get("enabled", False))
    auto_execute_safe = bool(payload.get("auto_execute_safe", False))
    updated_by = str(payload.get("updated_by", "unknown"))
    note = str(payload.get("note", ""))
    updated_at_raw = payload.get("updated_at")
    if isinstance(updated_at_raw, str):
        try:
            parsed = datetime.fromisoformat(updated_at_raw.replace("Z", "+00:00"))
            updated_at = parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
        except ValueError:
            updated_at = datetime.now(UTC)
    else:
        updated_at = datetime.now(UTC)
    return LocalAgentPolicyState(
        enabled=enabled,
        auto_execute_safe=auto_execute_safe,
        updated_at=updated_at.astimezone(UTC),
        updated_by=updated_by,
        note=note,
    )

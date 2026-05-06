from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from hashlib import sha256
from typing import Any, Literal
from uuid import uuid4

ActionClass = Literal["safe_auto", "guarded_write", "manual_only"]

ALLOWED_ACTION_CLASS: dict[str, ActionClass] = {
    "run_refresh_all": "safe_auto",
    "run_backtest": "safe_auto",
    "retrain_and_predict": "guarded_write",
    "set_release_override": "guarded_write",
    "promote_model_candidate": "manual_only",
    "patch_feature_logic": "manual_only",
}

FAILURE_TAXONOMY = {
    "data",
    "config",
    "network_provider",
    "model_artifact",
    "orchestration",
    "quality_gate",
}


@dataclass(frozen=True)
class LocalAgentAction:
    action_id: str
    action_type: str
    action_class: ActionClass
    reason: str
    confidence: float
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class OverfitSignal:
    market: str
    score: float
    note: str


@dataclass(frozen=True)
class DebugHint:
    category: str
    summary: str
    next_steps: tuple[str, ...]


@dataclass(frozen=True)
class LocalAgentDecision:
    run_id: str
    status: Literal["advisory", "hold", "execute", "disabled", "error"]
    confidence: float
    summary: str
    overfit_risk_score: float
    overfit_signals: tuple[OverfitSignal, ...]
    debug_hints: tuple[DebugHint, ...]
    actions: tuple[LocalAgentAction, ...]
    deterministic_blockers: tuple[str, ...]
    raw_response: str = ""
    snapshot_hash: str = ""


def build_snapshot_hash(payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return sha256(canonical.encode("utf-8")).hexdigest()


def policy_class_for_action(action_type: str) -> ActionClass:
    return ALLOWED_ACTION_CLASS.get(action_type, "manual_only")


def clamp_confidence(value: object, default: float = 0.0) -> float:
    if not isinstance(value, (int, float, str)):
        return default
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return max(0.0, min(parsed, 1.0))


def extract_json_object(
    text: str,
    *,
    required_keys: frozenset[str] | None = None,
) -> dict[str, Any] | None:
    stripped = text.strip()
    if not stripped:
        return None
    # Strategy 1: direct parse
    try:
        parsed = json.loads(stripped)
        if isinstance(parsed, dict):
            if required_keys is None or required_keys.issubset(parsed):
                return parsed
    except json.JSONDecodeError:
        pass
    # Strategy 2: markdown fence
    fence = re.search(r"```(?:json)?\s*(\{[\s\S]*\})\s*```", stripped, flags=re.IGNORECASE)
    any_fence = "```" in stripped
    if fence is not None:
        try:
            parsed = json.loads(fence.group(1))
            if isinstance(parsed, dict):
                if required_keys is None or required_keys.issubset(parsed):
                    return parsed
        except json.JSONDecodeError:
            pass
        return None  # fence found but unparseable — do NOT fall to greedy
    if any_fence:
        return None  # backticks present but fence regex missed — skip greedy entirely
    # Strategy 3: greedy brace (only when zero backticks in text)
    brace = re.search(r"(\{[\s\S]*\})", stripped)
    if brace is None:
        return None
    try:
        parsed = json.loads(brace.group(1))
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, dict):
        return None
    if required_keys is not None and not required_keys.issubset(parsed):
        return None
    return parsed


def action_from_payload(payload: dict[str, Any]) -> LocalAgentAction | None:
    action_type = str(payload.get("action_type", "")).strip()
    reason = str(payload.get("reason", "")).strip()
    if not action_type or not reason:
        return None
    confidence = clamp_confidence(payload.get("confidence"), default=0.0)
    action_payload = payload.get("payload")
    normalized_payload = action_payload if isinstance(action_payload, dict) else {}
    return LocalAgentAction(
        action_id=str(payload.get("action_id") or uuid4().hex),
        action_type=action_type,
        action_class=policy_class_for_action(action_type),
        reason=reason,
        confidence=confidence,
        payload=normalized_payload,
    )

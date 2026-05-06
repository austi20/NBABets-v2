from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ALLOWED_ACTION_TYPES = frozenset(
    {
        "run_refresh_all",
        "run_backtest",
        "retrain_and_predict",
        "set_release_override",
        "promote_model_candidate",
        "patch_feature_logic",
    }
)


@dataclass(frozen=True)
class JsonlEvalSummary:
    total: int
    autonomy_parse_ok: int
    autonomy_action_violations: int
    csv_qa_parse_ok: int
    csv_qa_schema_ok: int
    csv_qa_rows: int
    automation_bullets_ok: int
    automation_rows: int


def _extract_json_object(text: str) -> dict[str, Any] | None:
    stripped = text.strip()
    if not stripped:
        return None
    try:
        parsed = json.loads(stripped)
        return parsed if isinstance(parsed, dict) else None
    except json.JSONDecodeError:
        pass
    fence = re.search(r"```(?:json)?\s*(\{[\s\S]*\})\s*```", stripped, flags=re.IGNORECASE)
    if fence is not None:
        try:
            parsed = json.loads(fence.group(1))
            return parsed if isinstance(parsed, dict) else None
        except json.JSONDecodeError:
            return None
    brace = re.search(r"(\{[\s\S]*\})", stripped)
    if brace is None:
        return None
    try:
        parsed = json.loads(brace.group(1))
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def eval_autonomy_assistant(content: str) -> tuple[bool, int]:
    """Returns (parsed_ok, unknown_action_count)."""
    parsed = _extract_json_object(content)
    if parsed is None:
        return False, 0
    if "status" not in parsed or "confidence" not in parsed:
        return False, 0
    violations = 0
    actions = parsed.get("actions", [])
    if not isinstance(actions, list):
        return True, 0
    for item in actions:
        if not isinstance(item, dict):
            continue
        at = str(item.get("action_type", "")).strip()
        if at and at not in ALLOWED_ACTION_TYPES:
            violations += 1
    return True, violations


def eval_csv_qa_assistant(content: str) -> tuple[bool, bool]:
    """Returns (json_ok, schema_ok): schema_ok checks required keys and issue codes."""
    parsed = _extract_json_object(content)
    if parsed is None:
        return False, False
    rec = parsed.get("recalculated")
    if not isinstance(rec, dict):
        return True, False
    for key in ("hit_over", "hit_under", "push"):
        if key not in rec or not isinstance(rec[key], bool):
            return True, False
    agrees = parsed.get("agrees_with_file")
    if not isinstance(agrees, bool):
        return True, False
    issues = parsed.get("issues")
    if not isinstance(issues, list) or not issues:
        return True, False
    for item in issues:
        if not isinstance(item, dict):
            return True, False
        if str(item.get("code", "")) not in {
            "none",
            "hit_mismatch",
            "missing_odds",
            "suspicious_minutes",
            "line_actual_inconsistent",
            "ambiguous_push",
        }:
            return True, False
    return True, True


def eval_automation_assistant(content: str, *, task: str) -> bool:
    text = content.strip()
    if task == "retrain_decision":
        return bool(re.search(r"Trigger\s*=\s*(YES|NO)", text, re.I))
    lines = [ln for ln in text.splitlines() if ln.strip().startswith("-")]
    return len(lines) >= 4


def summarize_jsonl(path: Path) -> JsonlEvalSummary:
    total = 0
    autonomy_parse_ok = 0
    autonomy_action_violations = 0
    csv_qa_parse_ok = 0
    csv_qa_schema_ok = 0
    csv_qa_rows = 0
    automation_ok = 0
    automation_rows = 0

    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            total += 1
            row = json.loads(line)
            meta = row.get("meta") or {}
            curriculum = str(meta.get("curriculum", ""))
            messages = row.get("messages") or []
            assistant = ""
            for m in messages:
                if isinstance(m, dict) and m.get("role") == "assistant":
                    assistant = str(m.get("content", ""))
                    break
            if curriculum == "csv_qa":
                csv_qa_rows += 1
                ok, schema_ok = eval_csv_qa_assistant(assistant)
                if ok:
                    csv_qa_parse_ok += 1
                if schema_ok:
                    csv_qa_schema_ok += 1
            elif curriculum == "local_autonomy":
                ok, viol = eval_autonomy_assistant(assistant)
                if ok:
                    autonomy_parse_ok += 1
                autonomy_action_violations += viol
            elif curriculum == "automation":
                automation_rows += 1
                task = str(meta.get("task", "model_health"))
                if eval_automation_assistant(assistant, task=task):
                    automation_ok += 1

    return JsonlEvalSummary(
        total=total,
        autonomy_parse_ok=autonomy_parse_ok,
        autonomy_action_violations=autonomy_action_violations,
        csv_qa_parse_ok=csv_qa_parse_ok,
        csv_qa_schema_ok=csv_qa_schema_ok,
        csv_qa_rows=csv_qa_rows,
        automation_bullets_ok=automation_ok,
        automation_rows=automation_rows,
    )

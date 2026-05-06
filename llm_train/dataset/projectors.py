from __future__ import annotations

import json
import math
from typing import Any

import pandas as pd

LOCAL_AUTONOMY_INSTRUCTION = (
    "You are the local autonomy copilot for NBA prop modeling. "
    "Return STRICT JSON only with keys: status, confidence, summary, actions. "
    "status must be advisory|hold|execute. confidence is 0..1.\n"
    "Each action object keys: action_type, reason, confidence, payload.\n"
    "Prefer safe actions first. Do not recommend destructive or schema-changing actions.\n"
)


def autonomy_instruction_prefix() -> str:
    return LOCAL_AUTONOMY_INSTRUCTION


def _floatish(value: object) -> float | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _boolish(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip().lower()
    if text in {"true", "1", "yes"}:
        return True
    if text in {"false", "0", "no"}:
        return False
    return None


def recalculate_hit_flags(*, actual: float, line: float) -> dict[str, bool]:
    """Half-point style: push iff actual == line (within tolerance)."""
    if math.isclose(actual, line, rel_tol=0.0, abs_tol=1e-6):
        return {"hit_over": False, "hit_under": False, "push": True}
    if actual > line:
        return {"hit_over": True, "hit_under": False, "push": False}
    return {"hit_over": False, "hit_under": True, "push": False}


def build_csv_qa_gold(row: pd.Series) -> dict[str, Any]:
    actual = _floatish(row.get("actual"))
    line = _floatish(row.get("line_value"))
    file_over = _boolish(row.get("hit_over"))
    file_under = _boolish(row.get("hit_under"))
    file_push = _boolish(row.get("push"))
    minutes = _floatish(row.get("minutes"))
    over_odds = row.get("over_odds")
    under_odds = row.get("under_odds")

    issues: list[dict[str, str]] = []

    if actual is None or line is None:
        issues.append(
            {
                "code": "line_actual_inconsistent",
                "severity": "error",
                "detail": "Missing actual or line_value; cannot verify hits.",
            }
        )
        gold = {"hit_over": False, "hit_under": False, "push": False}
    else:
        gold = recalculate_hit_flags(actual=actual, line=line)

    if actual is not None and line is not None:
        if file_over is not None and file_under is not None and file_push is not None:
            agrees = (
                bool(file_over) == gold["hit_over"]
                and bool(file_under) == gold["hit_under"]
                and bool(file_push) == gold["push"]
            )
            if not agrees:
                issues.append(
                    {
                        "code": "hit_mismatch",
                        "severity": "warn",
                        "detail": "File hit flags disagree with actual vs line recalculation.",
                    }
                )
        else:
            agrees = False
            issues.append(
                {
                    "code": "hit_mismatch",
                    "severity": "warn",
                    "detail": "Incomplete hit_over/hit_under/push in file; cannot confirm.",
                },
            )
    else:
        agrees = False

    def _missing_odds(val: object) -> bool:
        if val is None:
            return True
        if isinstance(val, float) and math.isnan(val):
            return True
        return str(val).strip() == ""

    if _missing_odds(over_odds) and _missing_odds(under_odds):
        issues.append(
            {
                "code": "missing_odds",
                "severity": "info",
                "detail": "Both over_odds and under_odds are empty.",
            }
        )

    if minutes is not None and (minutes < 0 or minutes > 48.5):
        issues.append(
            {
                "code": "suspicious_minutes",
                "severity": "warn",
                "detail": f"Minutes {minutes} is outside plausible NBA range.",
            }
        )

    if not issues:
        issues.append({"code": "none", "severity": "info", "detail": "No issues detected for this row."})

    return {
        "issues": issues,
        "recalculated": gold,
        "agrees_with_file": agrees,
    }


def csv_row_user_content(row: pd.Series) -> str:
    payload = {
        "game_date": str(row.get("game_date", "")),
        "game_id": row.get("game_id"),
        "player_name": str(row.get("player_name", "")),
        "player_team": str(row.get("player_team", "")),
        "opponent": str(row.get("opponent", "")),
        "market": str(row.get("market", "")),
        "sportsbook": str(row.get("sportsbook", "")),
        "line_value": row.get("line_value"),
        "over_odds": row.get("over_odds"),
        "under_odds": row.get("under_odds"),
        "actual": row.get("actual"),
        "hit_over": row.get("hit_over"),
        "hit_under": row.get("hit_under"),
        "push": row.get("push"),
        "minutes": row.get("minutes"),
        "source": str(row.get("source", "")),
    }
    return (
        "You are a data QA assistant for NBA player prop rows. "
        "Given the JSON row, respond with STRICT JSON only matching the gold schema: "
        "issues (array of {code, severity, detail}), recalculated {hit_over, hit_under, push}, "
        "agrees_with_file (boolean). "
        "Use codes: none, hit_mismatch, missing_odds, suspicious_minutes, line_actual_inconsistent, ambiguous_push.\n"
        f"Row:\n{json.dumps(payload, indent=2, sort_keys=True)}"
    )


def csv_row_to_messages(row: pd.Series, *, row_index: int) -> dict[str, Any]:
    gold = build_csv_qa_gold(row)
    user = csv_row_user_content(row)
    gd = row.get("game_date")
    game_date = str(gd) if gd is not None else ""
    return {
        "messages": [
            {"role": "user", "content": user},
            {"role": "assistant", "content": json.dumps(gold, sort_keys=True, separators=(",", ":"))},
        ],
        "meta": {
            "curriculum": "csv_qa",
            "game_date": game_date,
            "source_row": row_index,
        },
    }

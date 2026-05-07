from __future__ import annotations

from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

AUDIT_ROOT = Path("data/artifacts/rotation_audit")


def write_game_audit(
    *,
    game_id: int,
    absences: list[dict[str, Any]],
    adjustments: list[dict[str, Any]],
    team_environment: dict[str, Any] | list[dict[str, Any]] | None = None,
) -> Path:
    AUDIT_ROOT.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    for record in absences:
        rows.append({"record_type": "absence", **record})
    for record in adjustments:
        rows.append({"record_type": "adjustment", **record})
    for record in _environment_records(team_environment):
        rows.append({"record_type": "team_environment", **record})
    frame = pd.DataFrame(rows)
    path = AUDIT_ROOT / f"{int(game_id)}.parquet"
    frame.to_parquet(path, index=False)
    return path


def get_redistribution(game_id: int) -> dict[str, Any]:
    path = AUDIT_ROOT / f"{int(game_id)}.parquet"
    if not path.exists():
        return {"absences": [], "adjustments": [], "team_environment": []}
    frame = pd.read_parquet(path)
    absences = _records_by_type(frame, "absence")
    adjustments = _records_by_type(frame, "adjustment")
    env_rows = _records_by_type(frame, "team_environment")
    return {
        "absences": absences,
        "adjustments": adjustments,
        "team_environment": env_rows,
    }


def get_player_adjustments(player_id: int, since_date: date | None = None) -> list[dict[str, Any]]:
    if not AUDIT_ROOT.exists():
        return []
    result: list[dict[str, Any]] = []
    for path in sorted(AUDIT_ROOT.glob("*.parquet")):
        frame = pd.read_parquet(path)
        if frame.empty or "record_type" not in frame.columns:
            continue
        adjustments = frame[frame["record_type"] == "adjustment"].drop(columns=["record_type"], errors="ignore")
        if adjustments.empty:
            continue
        subset = adjustments[pd.to_numeric(adjustments.get("player_id"), errors="coerce") == int(player_id)]
        if subset.empty:
            continue
        if since_date is not None and "game_date" in subset.columns:
            subset_dates = pd.to_datetime(subset["game_date"], errors="coerce").dt.date
            subset = subset[subset_dates >= since_date]
        result.extend(_clean_record(record) for record in subset.to_dict("records"))
    return result


def dataclass_records(values: list[Any]) -> list[dict[str, Any]]:
    return [asdict(value) for value in values]


def _environment_records(team_environment: dict[str, Any] | list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    if team_environment is None:
        return []
    if isinstance(team_environment, dict):
        return [team_environment]
    return list(team_environment)


def _records_by_type(frame: pd.DataFrame, record_type: str) -> list[dict[str, Any]]:
    if frame.empty or "record_type" not in frame.columns:
        return []
    records = frame[frame["record_type"] == record_type].drop(columns=["record_type"], errors="ignore").to_dict("records")
    return [_clean_record(record) for record in records]


def _clean_record(record: dict[str, Any]) -> dict[str, Any]:
    return {key: _clean_value(value) for key, value in record.items() if not _is_missing(value)}


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float | np.floating):
        return not np.isfinite(float(value))
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _clean_value(value: Any) -> Any:
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    return value

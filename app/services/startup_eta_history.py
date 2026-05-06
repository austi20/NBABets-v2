"""Persist rolling per-step startup durations for ETA estimation (m4 / AG-TECH-002)."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_MAX_SAMPLES_PER_STEP = 15
_MIN_DURATION = 0.05
_MAX_DURATION = 7200.0


def _read_store(path: Path) -> dict[str, list[float]]:
    if not path.exists():
        return {}
    try:
        payload: Any = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.debug("startup ETA history unreadable (%s): %s", path, exc)
        return {}
    if not isinstance(payload, dict):
        return {}
    out: dict[str, list[float]] = {}
    for key, value in payload.items():
        if not isinstance(value, list):
            continue
        nums: list[float] = []
        for item in value:
            try:
                nums.append(float(item))
            except (TypeError, ValueError):
                continue
        if nums:
            out[str(key)] = nums
    return out


def _write_store(path: Path, store: dict[str, list[float]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(store, indent=2, sort_keys=True), encoding="utf-8")


def load_step_estimates(path: Path, defaults: dict[str, float]) -> dict[str, float]:
    """Return per-step ``estimated_seconds`` using rolling mean of recent runs, else defaults."""
    store = _read_store(path)
    result = dict(defaults)
    for key, samples in store.items():
        clean = [_clamp_duration(s) for s in samples if _MIN_DURATION <= s <= _MAX_DURATION]
        if clean:
            result[key] = float(sum(clean) / len(clean))
    return result


def record_step_duration(path: Path, step_key: str, seconds: float) -> None:
    """Append one observed duration; keeps a short rolling history per step."""
    try:
        sec = float(seconds)
    except (TypeError, ValueError):
        return
    if sec < _MIN_DURATION or sec > _MAX_DURATION:
        return
    store = _read_store(path)
    history = store.get(step_key, [])
    history.append(sec)
    store[step_key] = history[-_MAX_SAMPLES_PER_STEP:]
    try:
        _write_store(path, store)
    except OSError as exc:
        logger.debug("startup ETA history write failed (%s): %s", path, exc)


def _clamp_duration(seconds: float) -> float:
    return float(max(_MIN_DURATION, min(_MAX_DURATION, seconds)))

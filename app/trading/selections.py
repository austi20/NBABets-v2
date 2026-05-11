# app/trading/selections.py
from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

try:
    from filelock import FileLock
except ImportError:  # pragma: no cover - filelock declared in pyproject
    FileLock = None  # type: ignore[assignment,misc]


_PRUNE_DAYS = 7


@dataclass(frozen=True)
class Thresholds:
    """Global min-hit and min-edge thresholds applied to all boards."""

    min_hit_pct: float = 0.0
    min_edge_bps: int = 0


@dataclass
class SelectionStore:
    """Persistent per-board prop inclusion/exclusion state plus global thresholds.

    Selections older than 7 days are pruned on save. The default for any
    candidate not present in the store is True (included) — the opt-out model.
    """

    path: Path
    thresholds: Thresholds = field(default_factory=Thresholds)
    selections: dict[str, dict[str, bool]] = field(default_factory=dict)
    last_pruned_at: datetime | None = None

    @classmethod
    def load(cls, path: Path) -> SelectionStore:
        if not path.is_file():
            return cls(path=path)
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return cls(path=path)
        thresholds_raw = payload.get("thresholds") or {}
        thresholds = Thresholds(
            min_hit_pct=float(thresholds_raw.get("min_hit_pct", 0.0)),
            min_edge_bps=int(thresholds_raw.get("min_edge_bps", 0)),
        )
        selections_raw = payload.get("selections") or {}
        selections: dict[str, dict[str, bool]] = {}
        for board_date, by_candidate in selections_raw.items():
            if not isinstance(by_candidate, dict):
                continue
            selections[board_date] = {
                str(k): bool(v) for k, v in by_candidate.items()
            }
        last_pruned_raw = payload.get("last_pruned_at")
        last_pruned = None
        if isinstance(last_pruned_raw, str):
            try:
                last_pruned = datetime.fromisoformat(last_pruned_raw)
            except ValueError:
                last_pruned = None
        return cls(
            path=path,
            thresholds=thresholds,
            selections=selections,
            last_pruned_at=last_pruned,
        )

    def is_selected(self, board_date: date, candidate_id: str) -> bool:
        """Default to True -- opt-out model."""
        return self.selections.get(board_date.isoformat(), {}).get(candidate_id, True)

    def set_selection(self, board_date: date, candidate_id: str, included: bool) -> None:
        key = board_date.isoformat()
        self.selections.setdefault(key, {})[candidate_id] = included

    def bulk_set(self, board_date: date, mapping: dict[str, bool]) -> None:
        self.selections.setdefault(board_date.isoformat(), {}).update(mapping)

    def update_thresholds(self, *, min_hit_pct: float, min_edge_bps: int) -> None:
        self.thresholds = Thresholds(min_hit_pct=min_hit_pct, min_edge_bps=min_edge_bps)

    def save(self, *, today: date | None = None) -> None:
        cutoff = (today or date.today()) - timedelta(days=_PRUNE_DAYS)
        self.selections = {
            board: by_candidate
            for board, by_candidate in self.selections.items()
            if _keep_board(board, cutoff)
        }
        self.last_pruned_at = datetime.now(UTC)
        payload: dict[str, Any] = {
            "thresholds": {
                "min_hit_pct": self.thresholds.min_hit_pct,
                "min_edge_bps": self.thresholds.min_edge_bps,
            },
            "selections": self.selections,
            "last_pruned_at": self.last_pruned_at.isoformat(),
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write_json(self.path, payload)


def _keep_board(board: str, cutoff: date) -> bool:
    parsed = _parse_date(board)
    return parsed is None or parsed >= cutoff


def _parse_date(value: str) -> date | None:
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    lock_path = path.with_suffix(path.suffix + ".lock")
    text = json.dumps(payload, indent=2, sort_keys=True)
    if FileLock is not None:
        with FileLock(str(lock_path), timeout=5):
            _write_temp_replace(path, text)
    else:
        _write_temp_replace(path, text)


def _write_temp_replace(path: Path, text: str) -> None:
    fd, tmp_name = tempfile.mkstemp(prefix=path.name, dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
        os.replace(tmp_name, path)
    except BaseException:
        if os.path.exists(tmp_name):
            os.remove(tmp_name)
        raise

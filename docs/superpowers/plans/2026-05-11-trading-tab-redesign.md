# Trading Tab Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Each phase ends with a green-gate commit per spec §15.

**Goal:** Rebuild the Trading tab as a dark data-terminal UI with live SSE updates, interactive pick curation, soft-cap proportional allocation, and wallet-initialized budget.

**Architecture:** Single SSE stream (`/api/trading/stream`) feeds a Zustand store via `useTradingStream` hook. Per-cell reactivity, no skeletons after first paint. Backend builds `TradingLiveSnapshot` from `MarketBook` + decision pack + persisted selections + limits. Allocation algorithm shared between the live snapshot and `run_trading_loop.py`.

**Tech Stack:** FastAPI 0.111 (Python 3.11), `sse_starlette`, Pydantic v2. React 18 + Vite + TanStack Router + Zustand 5 + Recharts. Tauri 2.

**Spec reference:** [docs/superpowers/specs/2026-05-11-trading-tab-redesign-design.md](../specs/2026-05-11-trading-tab-redesign-design.md)

**Subagent model assignments (per spec §15):**
- Implementation: `claude-sonnet-4-6` (general-purpose agent)
- Python review: `python-reviewer` on `claude-sonnet-4-6`
- TypeScript review: `typescript-reviewer` on `claude-sonnet-4-6`
- Security review: `security-reviewer` on `claude-sonnet-4-6` (wallet/limits/SSE phases only)
- Build resolution first-pass: `build-error-resolver` on `claude-haiku-4-5`
- End-of-phase holistic review: `code-reviewer` on `claude-opus-4-7`

**Gates per phase:** `pytest` on touched modules · `ruff check .` · `mypy app` · `npx tsc --noEmit` (frontend phases) · `npm run build --prefix desktop_tauri` (frontend phases) · reviewer agents report no CRITICAL/HIGH.

---

## Phase 1 — Allocation helper + selections store

Backend-only foundation. Two pure modules + integration into the existing multi-prop loop runner.

### Task 1.1: Create allocation algorithm module

**Files:**
- Create: `app/trading/allocation.py`
- Test: `tests/unit/trading/test_allocation.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/trading/test_allocation.py
from __future__ import annotations

from app.trading.allocation import AllocationPick, allocate_proportional_with_soft_cap


def _pick(candidate_id: str, model_prob: float) -> AllocationPick:
    return AllocationPick(candidate_id=candidate_id, model_prob=model_prob)


def test_empty_input_returns_empty_dict() -> None:
    assert allocate_proportional_with_soft_cap([], budget=10.0) == {}


def test_single_pick_capped_at_soft_cap() -> None:
    result = allocate_proportional_with_soft_cap([_pick("a", 0.6)], budget=10.0)
    assert result == {"a": 3.5}  # 35% of 10


def test_two_equal_picks_below_cap_split_evenly() -> None:
    picks = [_pick("a", 0.5), _pick("b", 0.5)]
    result = allocate_proportional_with_soft_cap(picks, budget=4.0)
    # raw alloc 2.0 each, cap 1.4 — both capped
    assert result == {"a": 1.4, "b": 1.4}


def test_three_picks_one_overcap_redistributes_overflow() -> None:
    # weights 0.6/0.2/0.2, budget 10, cap 3.5
    # raw: 6.0 / 2.0 / 2.0 → cap "a" at 3.5, overflow 2.5 to b+c proportionally
    # b and c get extra 1.25 each → 3.25 each, both still under 3.5 cap
    picks = [_pick("a", 0.6), _pick("b", 0.2), _pick("c", 0.2)]
    result = allocate_proportional_with_soft_cap(picks, budget=10.0)
    assert result["a"] == 3.5
    assert abs(result["b"] - 3.25) < 1e-6
    assert abs(result["c"] - 3.25) < 1e-6
    assert sum(result.values()) <= 10.0 + 1e-6


def test_all_picks_above_cap_total_lt_budget() -> None:
    picks = [_pick("a", 0.5), _pick("b", 0.5)]
    result = allocate_proportional_with_soft_cap(picks, budget=2.0)
    # cap 0.7, raw 1.0/1.0 — both cap, total 1.4 < 2.0
    assert result == {"a": 0.7, "b": 0.7}


def test_zero_total_weight_returns_zero_stakes() -> None:
    picks = [_pick("a", 0.0), _pick("b", 0.0)]
    result = allocate_proportional_with_soft_cap(picks, budget=10.0)
    assert result == {"a": 0.0, "b": 0.0}


def test_custom_cap_fraction() -> None:
    result = allocate_proportional_with_soft_cap(
        [_pick("a", 1.0)], budget=10.0, cap_fraction=0.5
    )
    assert result == {"a": 5.0}


def test_converges_within_max_iterations() -> None:
    picks = [_pick(f"p{i}", 0.1 * (i + 1)) for i in range(5)]
    result = allocate_proportional_with_soft_cap(picks, budget=10.0, max_iterations=3)
    assert sum(result.values()) <= 10.0 + 1e-6
    assert all(stake <= 10.0 * 0.35 + 1e-6 for stake in result.values())
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/trading/test_allocation.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app.trading.allocation'`

- [ ] **Step 3: Write minimal implementation**

```python
# app/trading/allocation.py
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AllocationPick:
    """Minimal input shape for the allocation algorithm.

    Only carries what allocation needs — keeps the function pure and trivial to mock.
    """

    candidate_id: str
    model_prob: float


def allocate_proportional_with_soft_cap(
    selected_picks: list[AllocationPick],
    budget: float,
    cap_fraction: float = 0.35,
    max_iterations: int = 3,
) -> dict[str, float]:
    """Allocate ``budget`` across ``selected_picks`` proportional to ``model_prob``.

    Any single allocation greater than ``budget * cap_fraction`` is capped at that
    ceiling. Overflow is redistributed proportionally among the uncapped picks
    (also subject to the cap). Iterates up to ``max_iterations`` times to settle.

    Total stake may be less than ``budget`` if all picks hit the cap. This is the
    intended safety property: never overspend, never violate the per-pick ceiling.
    """
    if not selected_picks:
        return {}

    cap = budget * cap_fraction
    stakes: dict[str, float] = {pick.candidate_id: 0.0 for pick in selected_picks}
    remaining = list(selected_picks)
    remaining_budget = budget

    for _ in range(max_iterations):
        if not remaining:
            break
        total_weight = sum(pick.model_prob for pick in remaining)
        if total_weight <= 0:
            break

        newly_capped: list[AllocationPick] = []
        for pick in remaining:
            raw = remaining_budget * pick.model_prob / total_weight
            allowed = cap - stakes[pick.candidate_id]
            if raw >= allowed:
                stakes[pick.candidate_id] = cap
                remaining_budget -= allowed
                newly_capped.append(pick)
            else:
                stakes[pick.candidate_id] += raw
                remaining_budget -= raw

        if not newly_capped:
            break
        remaining = [pick for pick in remaining if pick not in newly_capped]

    return stakes
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/trading/test_allocation.py -v`
Expected: 8 passed.

- [ ] **Step 5: Type check**

Run: `mypy app/trading/allocation.py`
Expected: Success: no issues found.

- [ ] **Step 6: Lint**

Run: `ruff check app/trading/allocation.py tests/unit/trading/test_allocation.py`
Expected: All checks passed.

- [ ] **Step 7: Commit**

```bash
git add app/trading/allocation.py tests/unit/trading/test_allocation.py
git commit -m "feat(trading): proportional allocation with soft 35% per-prop cap"
```

---

### Task 1.2: Selections persistence module

**Files:**
- Create: `app/trading/selections.py`
- Test: `tests/unit/trading/test_selections.py`
- Modify: `pyproject.toml` — add `filelock` to dependencies

- [ ] **Step 1: Add filelock to dependencies**

In `pyproject.toml`, under `[project]` → `dependencies`, add `"filelock>=3.13"` next to the existing entries. Then run:

```bash
pip install -e .[dev]
```

- [ ] **Step 2: Write failing tests**

```python
# tests/unit/trading/test_selections.py
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.trading.selections import (
    SelectionStore,
    Thresholds,
)


@pytest.fixture()
def store_path(tmp_path: Path) -> Path:
    return tmp_path / "trading_selections.json"


def test_load_missing_file_returns_empty_store(store_path: Path) -> None:
    store = SelectionStore.load(store_path)
    assert store.thresholds == Thresholds(min_hit_pct=0.0, min_edge_bps=0)
    assert store.selections == {}


def test_save_then_load_roundtrip(store_path: Path) -> None:
    store = SelectionStore.load(store_path)
    store.set_selection(date(2026, 5, 11), "cunningham-pts", True)
    store.set_selection(date(2026, 5, 11), "hardaway-3pt", False)
    store.save()

    reloaded = SelectionStore.load(store_path)
    assert reloaded.is_selected(date(2026, 5, 11), "cunningham-pts") is True
    assert reloaded.is_selected(date(2026, 5, 11), "hardaway-3pt") is False


def test_default_for_unknown_candidate_is_true(store_path: Path) -> None:
    store = SelectionStore.load(store_path)
    assert store.is_selected(date(2026, 5, 11), "never-toggled") is True


def test_threshold_update(store_path: Path) -> None:
    store = SelectionStore.load(store_path)
    store.update_thresholds(min_hit_pct=0.55, min_edge_bps=50)
    store.save()
    reloaded = SelectionStore.load(store_path)
    assert reloaded.thresholds.min_hit_pct == 0.55
    assert reloaded.thresholds.min_edge_bps == 50


def test_selections_older_than_7_days_pruned_on_save(store_path: Path) -> None:
    today = date(2026, 5, 11)
    old = today - timedelta(days=8)
    store = SelectionStore.load(store_path)
    store.set_selection(old, "old-candidate", False)
    store.set_selection(today, "today-candidate", False)
    store.save(today=today)

    reloaded = SelectionStore.load(store_path)
    assert old.isoformat() not in reloaded.selections
    assert today.isoformat() in reloaded.selections


def test_bulk_update_replaces_selections_for_date(store_path: Path) -> None:
    store = SelectionStore.load(store_path)
    today = date(2026, 5, 11)
    store.set_selection(today, "a", True)
    store.set_selection(today, "b", True)
    store.bulk_set(today, {"a": False, "b": False})
    assert store.is_selected(today, "a") is False
    assert store.is_selected(today, "b") is False
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest tests/unit/trading/test_selections.py -v`
Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 4: Write implementation**

```python
# app/trading/selections.py
from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
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
        """Default to True — opt-out model."""
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
            if _parse_date(board) is None or _parse_date(board) >= cutoff
        }
        self.last_pruned_at = datetime.now(timezone.utc)
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
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/unit/trading/test_selections.py -v`
Expected: 6 passed.

- [ ] **Step 6: Type check and lint**

Run: `mypy app/trading/selections.py && ruff check app/trading/selections.py tests/unit/trading/test_selections.py`
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add app/trading/selections.py tests/unit/trading/test_selections.py pyproject.toml
git commit -m "feat(trading): persistent selection store with 7-day pruning"
```

---

### Task 1.3: Wire allocation helper into run_trading_loop.py

**Files:**
- Modify: `scripts/run_trading_loop.py:280-310` (the existing multi-prop loop)
- Test: `tests/integration/scripts/test_run_trading_loop_allocation.py`

- [ ] **Step 1: Write failing integration test**

```python
# tests/integration/scripts/test_run_trading_loop_allocation.py
from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.trading.allocation import AllocationPick, allocate_proportional_with_soft_cap


def test_run_trading_loop_uses_shared_allocation(tmp_path: Path) -> None:
    """Verifies the loop runner pulls from the shared allocation helper rather than
    doing its own math. Smoke test only — full live flow is covered elsewhere."""
    import scripts.run_trading_loop as runner

    source = Path(runner.__file__).read_text(encoding="utf-8")
    assert "from app.trading.allocation import" in source, (
        "run_trading_loop.py should import the shared allocation helper"
    )
    assert "allocate_proportional_with_soft_cap" in source
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/integration/scripts/test_run_trading_loop_allocation.py -v`
Expected: FAIL — import string not yet in source.

- [ ] **Step 3: Modify run_trading_loop.py to use the shared helper**

Find the block in `scripts/run_trading_loop.py` starting at the line `loop = TradingLoop(...)` and ending at the `return 0` of `main`. Replace the per-decision stake computation with the shared helper. Add at the top of the file (with other imports):

```python
from app.trading.allocation import AllocationPick, allocate_proportional_with_soft_cap
```

Replace the existing allocation loop (currently `total_prob = sum(d.model_prob for d in decisions) or 1.0` block) with:

```python
        loop = TradingLoop(
            risk_engine=risk,
            ledger=ledger,
            adapter=adapter,
            session_factory=SessionLocal,
        )
        alloc_picks = [
            AllocationPick(candidate_id=d.market_key, model_prob=float(d.model_prob))
            for d in decisions
        ]
        stakes_by_id = allocate_proportional_with_soft_cap(
            alloc_picks,
            budget=limits.max_open_notional,
            cap_fraction=0.35,
        )
        accepted = rejected = fills = events = 0
        for decision in decisions:
            stake_i = stakes_by_id.get(decision.market_key, 0.0)
            if stake_i <= 0:
                continue
            r = loop.run_decisions([decision], exchange="kalshi", stake=stake_i)
            accepted += r.accepted
            rejected += r.rejected
            fills += r.fills
            events += r.events
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/integration/scripts/test_run_trading_loop_allocation.py -v`
Expected: PASS.

- [ ] **Step 5: Verify the existing run_trading_loop tests still pass**

Run: `pytest tests/ -k run_trading_loop -v`
Expected: all green (including any previously-passing tests).

- [ ] **Step 6: Type check and lint**

Run: `mypy app scripts/run_trading_loop.py && ruff check scripts/run_trading_loop.py`
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add scripts/run_trading_loop.py tests/integration/scripts/test_run_trading_loop_allocation.py
git commit -m "refactor(trading): use shared allocation helper in run_trading_loop"
```

---

### Phase 1 — End-of-phase gates

- [ ] Run full Python gates:

```bash
pytest tests/unit/trading/ tests/integration/scripts/ -v
ruff check .
mypy app
```

- [ ] Dispatch `python-reviewer` subagent on the Phase 1 diff:

> Review the Phase 1 diff (app/trading/allocation.py, app/trading/selections.py, scripts/run_trading_loop.py and their tests). Focus: correctness of the cap-and-redistribute loop, file-locking edge cases, opt-out default semantics. Report CRITICAL/HIGH issues only.

Address any CRITICAL/HIGH issues. Re-run gates. Then proceed to Phase 2.

---

## Phase 2 — Live snapshot builder + SSE stream endpoint

Pure builder + SSE publisher. No frontend changes yet; payload returned via curl-able endpoints.

### Task 2.1: New Pydantic models in trading schemas

**Files:**
- Modify: `app/server/schemas/trading.py` — append new models

- [ ] **Step 1: Append models to trading.py**

Append to `app/server/schemas/trading.py` (after existing models):

```python
# ---- Live snapshot models (Phase 2) ----

class KpiPnlModel(BaseModel):
    daily_pnl: float
    realized: float
    unrealized: float
    loss_cap: float
    loss_progress: float  # 0..1


class KpiBudgetModel(BaseModel):
    max_open_notional: float
    allocated: float
    free: float
    usage_progress: float  # 0..1


class KpiPicksModel(BaseModel):
    available: int
    selected: int
    excluded: int
    blocked: int
    est_total_profit: float


class KpiSystemModel(BaseModel):
    status: Literal["ready", "blocked", "checking"]
    mode: Literal["observe", "supervised-live"]
    gates_passed: int
    gates_total: int
    ws_connected: bool
    summary: str


class KpiTilesModel(BaseModel):
    pnl: KpiPnlModel
    budget: KpiBudgetModel
    picks: KpiPicksModel
    system: KpiSystemModel


class ControlBarStateModel(BaseModel):
    mode: Literal["observe", "supervised-live"]
    loop_state: Literal["idle", "starting", "running", "killed", "exited", "failed", "blocked"]
    can_start: bool
    start_label: str  # e.g. "Start Auto-Bet (2 picks · $1.00)"
    kill_switch_active: bool


class PickKalshiModel(BaseModel):
    ticker: str | None
    yes_bid: float | None
    yes_ask: float | None
    spread: float | None
    last_quote_at: datetime | None


class PickRowModel(BaseModel):
    candidate_id: str
    rank: int
    prop_label: str  # e.g. "Cunningham PTS o22.5"
    game_label: str | None  # e.g. "DET vs CLE 7:00p"
    hit_pct: float  # 0..1 (display formats to %)
    edge_bps: int
    model_prob: float
    market_prob: float | None
    alloc: float  # 0 if not selected
    est_profit: float  # 0 if not selected
    state: Literal["queued", "excluded", "blocked", "filled", "partial"]
    selected: bool
    blocker_reason: str | None
    kalshi: PickKalshiModel


class BetSlipPickModel(BaseModel):
    candidate_id: str
    prop_label: str
    hit_pct: float
    edge_bps: int
    alloc: float
    est_profit: float


class BetSlipModel(BaseModel):
    selected: list[BetSlipPickModel]
    total_stake: float
    cap_total: float  # max_open_notional
    est_total_profit: float
    unused_budget: float


class SystemDiagnosticsModel(BaseModel):
    readiness: TradingReadinessModel
    brain: TradingBrainSyncModel | None


class EventLogLineModel(BaseModel):
    cursor: int  # monotonic for delta-encoded streaming
    timestamp: datetime
    level: Literal["info", "warn", "error"]
    message: str


class PnlPointModel(BaseModel):
    index: int
    pnl: float


class TradingLiveSnapshotModel(BaseModel):
    observed_at: datetime
    kpis: KpiTilesModel
    control: ControlBarStateModel
    picks: list[PickRowModel]
    bet_slip: BetSlipModel
    positions: list[LivePositionModel]
    fills: list[FillModel]
    quotes: list[TradingQuoteModel]
    resting_orders: list[RestingOrderModel]
    diagnostics: SystemDiagnosticsModel
    event_log: list[EventLogLineModel]
    pnl_trend: list[PnlPointModel]
    errors: list[str]
    stream_cursor: int  # for delta resync on reconnect
```

- [ ] **Step 2: Type check**

Run: `mypy app/server/schemas/trading.py`
Expected: Success.

- [ ] **Step 3: Commit**

```bash
git add app/server/schemas/trading.py
git commit -m "feat(trading): pydantic models for live trading snapshot"
```

---

### Task 2.2: Live snapshot builder

**Files:**
- Create: `app/trading/live_snapshot.py`
- Test: `tests/unit/trading/test_live_snapshot.py`

- [ ] **Step 1: Write failing test (skeleton — covers the public contract)**

```python
# tests/unit/trading/test_live_snapshot.py
from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from app.trading.allocation import AllocationPick
from app.trading.live_snapshot import (
    LiveSnapshotInputs,
    TradingLiveSnapshotBuilder,
)
from app.trading.selections import SelectionStore


@pytest.fixture()
def builder() -> TradingLiveSnapshotBuilder:
    return TradingLiveSnapshotBuilder()


@pytest.fixture()
def selections(tmp_path: Path) -> SelectionStore:
    return SelectionStore.load(tmp_path / "trading_selections.json")


def _decision_pack(*, candidates: list[dict]) -> dict:
    return {"board_date": "2026-05-11", "decisions": candidates}


def _row(candidate_id: str, **overrides) -> dict:
    base = {
        "candidate_id": candidate_id,
        "market_key": candidate_id,
        "recommendation": "buy_yes",
        "model_prob": 0.6,
        "edge_bps": 150,
        "ev": 0.08,
        "line_value": 22.5,
        "player_id": 12345,
        "game_id": 67890,
        "game_date": "2026-05-11",
        "player_name": "Cunningham",
        "market_label": "PTS",
        "kalshi": {"ticker": "KX-CUNN-PTS"},
        "execution": {"allow_live_submit": True},
        "gates": {
            "symbol_resolved": True,
            "fresh_market_snapshot": True,
            "market_open": True,
            "event_not_stale": True,
            "spread_within_limit": True,
            "one_order_cap_ok": True,
            "price_within_limit": True,
        },
        "mode": "live",
    }
    base.update(overrides)
    return base


def test_empty_decision_pack_yields_empty_picks(
    builder: TradingLiveSnapshotBuilder, selections: SelectionStore
) -> None:
    inputs = LiveSnapshotInputs(
        decision_pack={"decisions": []},
        market_book_snapshot={},
        selections=selections,
        board_date=date(2026, 5, 11),
        budget=10.0,
        cap_fraction=0.35,
        loop_state="idle",
        mode="observe",
        ws_connected=True,
        kill_switch_active=False,
        ledger_state=MagicMock(realized=0.0, unrealized=0.0, daily_loss_cap=2.0),
        positions=[],
        fills=[],
        resting_orders=[],
        event_log=[],
        pnl_trend=[],
        readiness=None,
        brain_status=None,
        stream_cursor=0,
        errors=[],
    )
    snapshot = builder.build(inputs)
    assert snapshot.picks == []
    assert snapshot.bet_slip.selected == []
    assert snapshot.kpis.picks.available == 0


def test_selected_picks_get_proportional_allocation(
    builder: TradingLiveSnapshotBuilder, selections: SelectionStore
) -> None:
    pack = _decision_pack(
        candidates=[
            _row("a", model_prob=0.6),
            _row("b", model_prob=0.4),
        ]
    )
    inputs = LiveSnapshotInputs(
        decision_pack=pack,
        market_book_snapshot={},
        selections=selections,
        board_date=date(2026, 5, 11),
        budget=10.0,
        cap_fraction=0.35,
        loop_state="idle",
        mode="supervised-live",
        ws_connected=True,
        kill_switch_active=False,
        ledger_state=MagicMock(realized=0.0, unrealized=0.0, daily_loss_cap=2.0),
        positions=[],
        fills=[],
        resting_orders=[],
        event_log=[],
        pnl_trend=[],
        readiness=None,
        brain_status=None,
        stream_cursor=0,
        errors=[],
    )
    snapshot = builder.build(inputs)
    allocs = {p.candidate_id: p.alloc for p in snapshot.picks}
    assert allocs["a"] > 0
    assert allocs["b"] > 0
    # both under cap (3.5), 60/40 split → 6.0/4.0 but capped at 3.5 each
    assert allocs["a"] == pytest.approx(3.5, abs=0.01)


def test_excluded_picks_have_zero_alloc(
    builder: TradingLiveSnapshotBuilder, selections: SelectionStore
) -> None:
    selections.set_selection(date(2026, 5, 11), "a", False)
    pack = _decision_pack(candidates=[_row("a")])
    inputs = LiveSnapshotInputs(
        decision_pack=pack,
        market_book_snapshot={},
        selections=selections,
        board_date=date(2026, 5, 11),
        budget=10.0,
        cap_fraction=0.35,
        loop_state="idle",
        mode="observe",
        ws_connected=True,
        kill_switch_active=False,
        ledger_state=MagicMock(realized=0.0, unrealized=0.0, daily_loss_cap=2.0),
        positions=[],
        fills=[],
        resting_orders=[],
        event_log=[],
        pnl_trend=[],
        readiness=None,
        brain_status=None,
        stream_cursor=0,
        errors=[],
    )
    snapshot = builder.build(inputs)
    assert snapshot.picks[0].selected is False
    assert snapshot.picks[0].alloc == 0.0
    assert snapshot.picks[0].state == "excluded"


def test_blocked_pick_cannot_be_selected(
    builder: TradingLiveSnapshotBuilder, selections: SelectionStore
) -> None:
    row = _row("a")
    row["gates"]["spread_within_limit"] = False
    pack = _decision_pack(candidates=[row])
    inputs = LiveSnapshotInputs(
        decision_pack=pack,
        market_book_snapshot={},
        selections=selections,
        board_date=date(2026, 5, 11),
        budget=10.0,
        cap_fraction=0.35,
        loop_state="idle",
        mode="supervised-live",
        ws_connected=True,
        kill_switch_active=False,
        ledger_state=MagicMock(realized=0.0, unrealized=0.0, daily_loss_cap=2.0),
        positions=[],
        fills=[],
        resting_orders=[],
        event_log=[],
        pnl_trend=[],
        readiness=None,
        brain_status=None,
        stream_cursor=0,
        errors=[],
    )
    snapshot = builder.build(inputs)
    pick = snapshot.picks[0]
    assert pick.state == "blocked"
    assert pick.selected is False
    assert pick.alloc == 0.0
    assert pick.blocker_reason is not None
    assert "spread" in pick.blocker_reason.lower()


def test_threshold_force_excludes_picks_below_min_hit(
    builder: TradingLiveSnapshotBuilder, selections: SelectionStore
) -> None:
    selections.update_thresholds(min_hit_pct=0.55, min_edge_bps=0)
    pack = _decision_pack(
        candidates=[
            _row("a", model_prob=0.50),  # below threshold
            _row("b", model_prob=0.70),  # above threshold
        ]
    )
    inputs = LiveSnapshotInputs(
        decision_pack=pack,
        market_book_snapshot={},
        selections=selections,
        board_date=date(2026, 5, 11),
        budget=10.0,
        cap_fraction=0.35,
        loop_state="idle",
        mode="supervised-live",
        ws_connected=True,
        kill_switch_active=False,
        ledger_state=MagicMock(realized=0.0, unrealized=0.0, daily_loss_cap=2.0),
        positions=[],
        fills=[],
        resting_orders=[],
        event_log=[],
        pnl_trend=[],
        readiness=None,
        brain_status=None,
        stream_cursor=0,
        errors=[],
    )
    snapshot = builder.build(inputs)
    picks_by_id = {p.candidate_id: p for p in snapshot.picks}
    assert picks_by_id["a"].selected is False
    assert picks_by_id["a"].state == "excluded"
    assert picks_by_id["b"].selected is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/trading/test_live_snapshot.py -v`
Expected: FAIL — `app.trading.live_snapshot` does not exist.

- [ ] **Step 3: Write the builder implementation**

```python
# app/trading/live_snapshot.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

from app.server.schemas.trading import (
    BetSlipModel,
    BetSlipPickModel,
    ControlBarStateModel,
    EventLogLineModel,
    KpiBudgetModel,
    KpiPicksModel,
    KpiPnlModel,
    KpiSystemModel,
    KpiTilesModel,
    PickKalshiModel,
    PickRowModel,
    PnlPointModel,
    SystemDiagnosticsModel,
    TradingLiveSnapshotModel,
)
from app.trading.allocation import AllocationPick, allocate_proportional_with_soft_cap
from app.trading.selections import SelectionStore


_GATE_FIELDS = (
    "symbol_resolved",
    "fresh_market_snapshot",
    "market_open",
    "event_not_stale",
    "spread_within_limit",
    "one_order_cap_ok",
    "price_within_limit",
)

_GATE_REASONS = {
    "symbol_resolved": "Kalshi symbol could not be resolved",
    "fresh_market_snapshot": "Market quote is stale",
    "market_open": "Kalshi market is closed",
    "event_not_stale": "Underlying event has started or ended",
    "spread_within_limit": "Spread too wide",
    "one_order_cap_ok": "One-order cap reached",
    "price_within_limit": "Quoted price exceeds policy max",
}


@dataclass
class LiveSnapshotInputs:
    decision_pack: dict[str, Any]
    market_book_snapshot: dict[str, dict[str, Any]]  # ticker → {bid, ask, ts}
    selections: SelectionStore
    board_date: date
    budget: float
    cap_fraction: float
    loop_state: str
    mode: str
    ws_connected: bool
    kill_switch_active: bool
    ledger_state: Any
    positions: list[Any]
    fills: list[Any]
    resting_orders: list[Any]
    event_log: list[EventLogLineModel]
    pnl_trend: list[PnlPointModel]
    readiness: Any | None
    brain_status: Any | None
    stream_cursor: int
    errors: list[str]


class TradingLiveSnapshotBuilder:
    """Pure builder. Given a snapshot of state, returns a TradingLiveSnapshotModel."""

    def build(self, inputs: LiveSnapshotInputs) -> TradingLiveSnapshotModel:
        rows = self._enrich_rows(inputs)
        bet_slip = self._build_bet_slip(rows, inputs.budget)
        kpis = self._build_kpis(rows, bet_slip, inputs)
        control = self._build_control(inputs, bet_slip)
        diagnostics = SystemDiagnosticsModel(
            readiness=inputs.readiness,
            brain=inputs.brain_status,
        )
        return TradingLiveSnapshotModel(
            observed_at=datetime.now(UTC),
            kpis=kpis,
            control=control,
            picks=rows,
            bet_slip=bet_slip,
            positions=inputs.positions,
            fills=inputs.fills,
            quotes=[],  # filled by stream publisher (Phase 2.4)
            resting_orders=inputs.resting_orders,
            diagnostics=diagnostics,
            event_log=inputs.event_log,
            pnl_trend=inputs.pnl_trend,
            errors=inputs.errors,
            stream_cursor=inputs.stream_cursor,
        )

    def _enrich_rows(self, inputs: LiveSnapshotInputs) -> list[PickRowModel]:
        raw_rows = inputs.decision_pack.get("decisions") or []
        thresholds = inputs.selections.thresholds
        rows: list[PickRowModel] = []
        for rank, raw in enumerate(raw_rows):
            candidate_id = str(raw.get("candidate_id") or raw.get("market_key") or "")
            if not candidate_id:
                continue
            gates = raw.get("gates") or {}
            blocker_reason = self._gate_blocker(gates, raw.get("mode"))
            below_thresholds = (
                float(raw.get("model_prob") or 0.0) < thresholds.min_hit_pct
                or int(raw.get("edge_bps") or 0) < thresholds.min_edge_bps
            )
            user_selected = inputs.selections.is_selected(inputs.board_date, candidate_id)
            if blocker_reason is not None:
                state = "blocked"
                selected = False
            elif not user_selected or below_thresholds:
                state = "excluded"
                selected = False
            else:
                state = "queued"
                selected = True
            ticker = (raw.get("kalshi") or {}).get("ticker")
            book_entry = inputs.market_book_snapshot.get(ticker) if ticker else None
            kalshi = PickKalshiModel(
                ticker=ticker,
                yes_bid=book_entry.get("yes_bid") if book_entry else None,
                yes_ask=book_entry.get("yes_ask") if book_entry else None,
                spread=book_entry.get("spread") if book_entry else None,
                last_quote_at=book_entry.get("ts") if book_entry else None,
            )
            rows.append(
                PickRowModel(
                    candidate_id=candidate_id,
                    rank=rank,
                    prop_label=self._prop_label(raw),
                    game_label=self._game_label(raw),
                    hit_pct=float(raw.get("model_prob") or 0.0),
                    edge_bps=int(raw.get("edge_bps") or 0),
                    model_prob=float(raw.get("model_prob") or 0.0),
                    market_prob=raw.get("market_prob"),
                    alloc=0.0,
                    est_profit=0.0,
                    state=state,
                    selected=selected,
                    blocker_reason=blocker_reason,
                    kalshi=kalshi,
                )
            )

        # Now allocate among selected rows
        selected_rows = [row for row in rows if row.selected]
        alloc_picks = [
            AllocationPick(candidate_id=row.candidate_id, model_prob=row.model_prob)
            for row in selected_rows
        ]
        stakes = allocate_proportional_with_soft_cap(
            alloc_picks, budget=inputs.budget, cap_fraction=inputs.cap_fraction
        )
        for row in rows:
            stake = stakes.get(row.candidate_id, 0.0)
            payout = self._payout_for(row)
            est = self._estimated_profit(row.model_prob, stake, payout)
            row.alloc = round(stake, 4)
            row.est_profit = round(est, 4)
        return rows

    def _gate_blocker(self, gates: dict[str, Any], mode: Any) -> str | None:
        if str(mode or "").lower() != "live":
            return None
        for field in _GATE_FIELDS:
            if gates.get(field) is not True:
                return _GATE_REASONS.get(field, f"gate {field} failed")
        return None

    def _prop_label(self, raw: dict[str, Any]) -> str:
        player = raw.get("player_name") or raw.get("player_id") or "?"
        market = raw.get("market_label") or raw.get("market_key") or "?"
        line = raw.get("line_value")
        side = "o" if str(raw.get("recommendation", "")).lower().startswith("buy_yes") else "u"
        if line is not None:
            return f"{player} {market} {side}{line}"
        return f"{player} {market}"

    def _game_label(self, raw: dict[str, Any]) -> str | None:
        return raw.get("game_label")

    def _payout_for(self, row: PickRowModel) -> float:
        # Kalshi binary contract: payout = 1.0 on win. Profit = (1 - entry_price) per contract.
        # Without a live entry price, estimate using market_prob or default 0.5.
        entry = row.kalshi.yes_ask or row.market_prob or 0.5
        return max(0.0, 1.0 - entry)

    def _estimated_profit(self, prob: float, stake: float, payout_per_unit: float) -> float:
        if stake <= 0:
            return 0.0
        win = prob * (stake * payout_per_unit / max(0.01, 1.0 - payout_per_unit))
        lose = (1 - prob) * stake
        return win - lose

    def _build_bet_slip(self, rows: list[PickRowModel], cap_total: float) -> BetSlipModel:
        selected = [
            BetSlipPickModel(
                candidate_id=row.candidate_id,
                prop_label=row.prop_label,
                hit_pct=row.hit_pct,
                edge_bps=row.edge_bps,
                alloc=row.alloc,
                est_profit=row.est_profit,
            )
            for row in rows
            if row.selected
        ]
        total_stake = round(sum(p.alloc for p in selected), 4)
        est_total = round(sum(p.est_profit for p in selected), 4)
        return BetSlipModel(
            selected=selected,
            total_stake=total_stake,
            cap_total=cap_total,
            est_total_profit=est_total,
            unused_budget=round(max(cap_total - total_stake, 0.0), 4),
        )

    def _build_kpis(
        self,
        rows: list[PickRowModel],
        bet_slip: BetSlipModel,
        inputs: LiveSnapshotInputs,
    ) -> KpiTilesModel:
        ledger = inputs.ledger_state
        realized = float(getattr(ledger, "realized", 0.0))
        unrealized = float(getattr(ledger, "unrealized", 0.0))
        loss_cap = float(getattr(ledger, "daily_loss_cap", 0.0))
        daily = realized + unrealized
        loss_progress = min(max(abs(min(daily, 0.0)) / loss_cap, 0.0), 1.0) if loss_cap > 0 else 0.0
        excluded = sum(1 for r in rows if r.state == "excluded")
        blocked = sum(1 for r in rows if r.state == "blocked")
        gates_total = 0
        gates_passed = 0
        if inputs.readiness is not None:
            checks = getattr(inputs.readiness, "checks", []) or []
            gates_total = len(checks)
            gates_passed = sum(1 for c in checks if getattr(c, "status", "") == "pass")
        return KpiTilesModel(
            pnl=KpiPnlModel(
                daily_pnl=round(daily, 4),
                realized=round(realized, 4),
                unrealized=round(unrealized, 4),
                loss_cap=loss_cap,
                loss_progress=loss_progress,
            ),
            budget=KpiBudgetModel(
                max_open_notional=inputs.budget,
                allocated=bet_slip.total_stake,
                free=bet_slip.unused_budget,
                usage_progress=(bet_slip.total_stake / inputs.budget) if inputs.budget > 0 else 0.0,
            ),
            picks=KpiPicksModel(
                available=len(rows),
                selected=len(bet_slip.selected),
                excluded=excluded,
                blocked=blocked,
                est_total_profit=bet_slip.est_total_profit,
            ),
            system=KpiSystemModel(
                status="ready" if gates_total == 0 or gates_passed == gates_total else "blocked",
                mode=inputs.mode,  # type: ignore[arg-type]
                gates_passed=gates_passed,
                gates_total=gates_total,
                ws_connected=inputs.ws_connected,
                summary=f"{inputs.mode} · {gates_passed}/{gates_total} gates · ws {'ok' if inputs.ws_connected else 'down'}",
            ),
        )

    def _build_control(
        self, inputs: LiveSnapshotInputs, bet_slip: BetSlipModel
    ) -> ControlBarStateModel:
        can_start = (
            inputs.mode == "supervised-live"
            and inputs.loop_state in {"idle", "exited", "killed", "failed"}
            and len(bet_slip.selected) > 0
            and not inputs.kill_switch_active
        )
        start_label = (
            f"Start Auto-Bet ({len(bet_slip.selected)} picks · ${bet_slip.total_stake:.2f})"
            if can_start
            else "Start Auto-Bet"
        )
        return ControlBarStateModel(
            mode=inputs.mode,  # type: ignore[arg-type]
            loop_state=inputs.loop_state,  # type: ignore[arg-type]
            can_start=can_start,
            start_label=start_label,
            kill_switch_active=inputs.kill_switch_active,
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/trading/test_live_snapshot.py -v`
Expected: 5 passed.

- [ ] **Step 5: Type check + lint**

Run: `mypy app/trading/live_snapshot.py && ruff check app/trading/live_snapshot.py tests/unit/trading/test_live_snapshot.py`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add app/trading/live_snapshot.py tests/unit/trading/test_live_snapshot.py
git commit -m "feat(trading): TradingLiveSnapshotBuilder — pure pricing + allocation + state"
```

---

### Task 2.3: Stream publisher

**Files:**
- Create: `app/trading/stream_publisher.py`
- Test: `tests/unit/trading/test_stream_publisher.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/trading/test_stream_publisher.py
from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from app.trading.stream_publisher import TradingStreamPublisher


def test_ring_buffer_caps_at_250_lines() -> None:
    pub = TradingStreamPublisher(event_log_capacity=250)
    for i in range(300):
        pub.log_event(level="info", message=f"line {i}")
    log = pub.event_log_snapshot()
    assert len(log) == 250
    assert log[0].message == "line 50"
    assert log[-1].message == "line 299"


def test_monotonic_cursor() -> None:
    pub = TradingStreamPublisher()
    pub.log_event(level="info", message="a")
    pub.log_event(level="warn", message="b")
    log = pub.event_log_snapshot()
    assert log[0].cursor < log[1].cursor


def test_event_log_since_cursor_returns_only_new() -> None:
    pub = TradingStreamPublisher()
    pub.log_event(level="info", message="a")
    pub.log_event(level="info", message="b")
    cursor_after_first = pub.event_log_snapshot()[0].cursor
    new_lines = pub.event_log_since(cursor_after_first)
    assert len(new_lines) == 1
    assert new_lines[0].message == "b"


@pytest.mark.asyncio
async def test_notify_wakes_waiting_subscribers() -> None:
    pub = TradingStreamPublisher()
    woken = asyncio.Event()

    async def waiter() -> None:
        await pub.wait_for_update(timeout=1.0)
        woken.set()

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0.05)
    pub.notify()
    await asyncio.wait_for(woken.wait(), timeout=1.0)
    task.cancel()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/trading/test_stream_publisher.py -v`
Expected: FAIL — module missing.

- [ ] **Step 3: Implement TradingStreamPublisher**

```python
# app/trading/stream_publisher.py
from __future__ import annotations

import asyncio
import itertools
from collections import deque
from datetime import UTC, datetime

from app.server.schemas.trading import EventLogLineModel


class TradingStreamPublisher:
    """In-memory event log + asyncio notification fan-out for the trading SSE stream."""

    def __init__(self, *, event_log_capacity: int = 250) -> None:
        self._cursor = itertools.count(start=1)
        self._buffer: deque[EventLogLineModel] = deque(maxlen=event_log_capacity)
        self._update_event = asyncio.Event()
        self._lock = asyncio.Lock()

    def log_event(self, *, level: str, message: str) -> EventLogLineModel:
        line = EventLogLineModel(
            cursor=next(self._cursor),
            timestamp=datetime.now(UTC),
            level=level,  # type: ignore[arg-type]
            message=message,
        )
        self._buffer.append(line)
        self.notify()
        return line

    def event_log_snapshot(self) -> list[EventLogLineModel]:
        return list(self._buffer)

    def event_log_since(self, cursor: int) -> list[EventLogLineModel]:
        return [line for line in self._buffer if line.cursor > cursor]

    def notify(self) -> None:
        """Wake all waiters. Idempotent within a single asyncio tick."""
        self._update_event.set()

    async def wait_for_update(self, *, timeout: float | None = None) -> bool:
        try:
            await asyncio.wait_for(self._update_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return False
        finally:
            self._update_event.clear()
        return True
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/trading/test_stream_publisher.py -v`
Expected: 4 passed.

- [ ] **Step 5: Type check + lint**

Run: `mypy app/trading/stream_publisher.py && ruff check app/trading/stream_publisher.py tests/unit/trading/test_stream_publisher.py`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add app/trading/stream_publisher.py tests/unit/trading/test_stream_publisher.py
git commit -m "feat(trading): ring-buffer event log + asyncio publisher fan-out"
```

---

### Task 2.4: Wire SSE + fallback endpoints in router

**Files:**
- Modify: `app/server/routers/trading.py` — add `/stream` and `/snapshot-live`
- Modify: `app/server/main.py` — instantiate publisher + snapshot service in lifespan

- [ ] **Step 1: Add a snapshot orchestrator service**

Create `app/trading/snapshot_service.py`:

```python
# app/trading/snapshot_service.py
from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from app.config.settings import Settings
from app.server.schemas.trading import TradingLiveSnapshotModel
from app.trading.live_snapshot import LiveSnapshotInputs, TradingLiveSnapshotBuilder
from app.trading.market_book import MarketBook
from app.trading.selections import SelectionStore
from app.trading.stream_publisher import TradingStreamPublisher


class TradingSnapshotService:
    """Owns the dependencies needed to assemble a TradingLiveSnapshotModel.

    Frontend reads either via the SSE stream or the single-shot fallback endpoint.
    Both routes call ``build()`` here.
    """

    def __init__(
        self,
        *,
        settings: Settings,
        market_book: MarketBook,
        selections_path: Path,
        publisher: TradingStreamPublisher,
    ) -> None:
        self._settings = settings
        self._market_book = market_book
        self._selections_path = selections_path
        self._publisher = publisher
        self._builder = TradingLiveSnapshotBuilder()

    @property
    def publisher(self) -> TradingStreamPublisher:
        return self._publisher

    def build(
        self,
        *,
        board_date: date,
        ledger_state: Any,
        positions: list[Any],
        fills: list[Any],
        resting_orders: list[Any],
        loop_state: str,
        mode: str,
        kill_switch_active: bool,
        readiness: Any | None,
        brain_status: Any | None,
        last_cursor: int = 0,
        errors: list[str] | None = None,
    ) -> TradingLiveSnapshotModel:
        selections = SelectionStore.load(self._selections_path)
        decision_pack = self._read_decision_pack()
        market_book_snapshot = self._market_book.snapshot()
        budget = self._read_budget()
        inputs = LiveSnapshotInputs(
            decision_pack=decision_pack,
            market_book_snapshot=market_book_snapshot,
            selections=selections,
            board_date=board_date,
            budget=budget,
            cap_fraction=0.35,
            loop_state=loop_state,
            mode=mode,
            ws_connected=self._market_book.is_connected(),
            kill_switch_active=kill_switch_active,
            ledger_state=ledger_state,
            positions=positions,
            fills=fills,
            resting_orders=resting_orders,
            event_log=self._publisher.event_log_since(last_cursor),
            pnl_trend=[],
            readiness=readiness,
            brain_status=brain_status,
            stream_cursor=self._publisher.event_log_snapshot()[-1].cursor if self._publisher.event_log_snapshot() else 0,
            errors=errors or [],
        )
        return self._builder.build(inputs)

    def _read_decision_pack(self) -> dict[str, Any]:
        path = Path(self._settings.kalshi_decisions_path)
        if not path.is_file():
            return {"decisions": []}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"decisions": []}

    def _read_budget(self) -> float:
        path = Path(self._settings.trading_limits_path)
        if not path.is_file():
            return 0.0
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return float(data.get("max_open_notional", 0.0))
        except (OSError, json.JSONDecodeError, ValueError):
            return 0.0
```

Note: `MarketBook` needs a `snapshot()` and `is_connected()` accessor. If those don't exist yet, add thin pass-throughs.

- [ ] **Step 2: Add MarketBook accessor stubs if missing**

Check `app/trading/market_book.py` for `snapshot()` and `is_connected()`. If missing, add:

```python
def snapshot(self) -> dict[str, dict[str, Any]]:
    """Return a {ticker: {yes_bid, yes_ask, spread, ts}} mapping."""
    with self._lock:
        return {
            ticker: {
                "yes_bid": entry.yes_bid,
                "yes_ask": entry.yes_ask,
                "spread": entry.spread,
                "ts": entry.ts,
            }
            for ticker, entry in self._entries.items()
        }

def is_connected(self) -> bool:
    return getattr(self, "_connected", False)
```

(Adapt field names to match the actual `MarketEntry` dataclass.)

- [ ] **Step 3: Add endpoints to trading router**

In `app/server/routers/trading.py`, add at the top:

```python
import asyncio
from sse_starlette.sse import EventSourceResponse
from app.trading.snapshot_service import TradingSnapshotService
```

Then add the endpoints (before the final `return router` if there is one):

```python
def _snapshot_service(request: Request) -> TradingSnapshotService:
    return cast(TradingSnapshotService, request.app.state.trading_snapshot_service)


def _board_date_today() -> date:
    return date.today()


def _build_live_snapshot(request: Request) -> TradingLiveSnapshotModel:
    service = _snapshot_service(request)
    controller: TradingLoopController = request.app.state.trading_loop_controller
    ledger = request.app.state.trading_ledger
    snapshot = ledger.daily_snapshot() if hasattr(ledger, "daily_snapshot") else None
    return service.build(
        board_date=_board_date_today(),
        ledger_state=snapshot,
        positions=getattr(snapshot, "positions", []) if snapshot else [],
        fills=getattr(snapshot, "fills", []) if snapshot else [],
        resting_orders=[],
        loop_state=controller.status().state,
        mode="supervised-live",  # populated from brain status in Phase 4
        kill_switch_active=getattr(snapshot, "kill_switch_active", False) if snapshot else False,
        readiness=None,
        brain_status=None,
    )


@router.get("/snapshot-live", response_model=TradingLiveSnapshotModel)
def trading_snapshot_live(request: Request) -> TradingLiveSnapshotModel:
    return _build_live_snapshot(request)


@router.get("/stream")
async def trading_stream(request: Request) -> EventSourceResponse:
    service = _snapshot_service(request)

    async def event_generator() -> Any:
        last_yielded = ""
        while True:
            if await request.is_disconnected():
                break
            snapshot = _build_live_snapshot(request)
            payload = snapshot.model_dump_json()
            if payload != last_yielded:
                last_yielded = payload
                yield {"event": "snapshot", "data": payload}
            # wait for explicit notify OR fall back to 2s tick
            await service.publisher.wait_for_update(timeout=2.0)

    return EventSourceResponse(event_generator())
```

- [ ] **Step 4: Instantiate the service in main.py lifespan**

In `app/server/main.py`, inside `create_app`, after `_build_market_service()`:

```python
from app.trading.snapshot_service import TradingSnapshotService
from app.trading.stream_publisher import TradingStreamPublisher

# Inside create_app, after market_service is built:
stream_publisher = TradingStreamPublisher()
snapshot_service = TradingSnapshotService(
    settings=get_settings(),
    market_book=market_service.book,
    selections_path=Path(get_settings().app_data_dir) / "trading_selections.json",
    publisher=stream_publisher,
)
```

And inside `lifespan`, after `market_service.start()`:

```python
_app.state.trading_stream_publisher = stream_publisher
_app.state.trading_snapshot_service = snapshot_service
stream_publisher.log_event(level="info", message="trading stream publisher started")
```

The middleware exempts SSE under app token in query string — extend `AppTokenMiddleware.exempt_paths` to allow `/api/trading/stream?token=...` (Phase 2 simplifies: token via query param). Add:

```python
exempt_paths=("/api/startup/run", "/api/trading/stream"),
```

(Move proper token validation to the SSE handler itself — verify `request.query_params.get("token") == request.app.state.app_token` before yielding.)

- [ ] **Step 5: Add a smoke integration test**

```python
# tests/integration/server/test_trading_stream.py
from __future__ import annotations

import asyncio
import json

import pytest
from httpx import AsyncClient

from app.server.main import create_app


@pytest.mark.asyncio
async def test_snapshot_live_returns_valid_payload(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("NBA_PROP_APP_DATA_DIR", str(tmp_path))
    app = create_app(app_token="test-token")
    async with AsyncClient(app=app, base_url="http://test") as client:
        async with app.router.lifespan_context(app):
            response = await client.get(
                "/api/trading/snapshot-live",
                headers={"X-App-Token": "test-token"},
            )
            assert response.status_code == 200
            payload = response.json()
            assert "kpis" in payload
            assert "picks" in payload
            assert "bet_slip" in payload
            assert "event_log" in payload
```

- [ ] **Step 6: Run integration test**

Run: `pytest tests/integration/server/test_trading_stream.py -v`
Expected: PASS.

- [ ] **Step 7: Type check, lint, commit**

```bash
mypy app
ruff check .
git add app/trading/snapshot_service.py app/trading/market_book.py app/server/routers/trading.py app/server/main.py tests/integration/server/test_trading_stream.py
git commit -m "feat(trading): SSE stream + snapshot-live endpoint backed by snapshot service"
```

---

### Phase 2 — End-of-phase gates

- [ ] Run gates: `pytest tests/unit/trading/ tests/integration/server/ -v && ruff check . && mypy app`
- [ ] Dispatch `python-reviewer` and `security-reviewer` on the Phase 2 diff.
  - `security-reviewer` focus: app token via query string risk, SSE replay attacks, error message leakage.
- [ ] Address findings. Then proceed to Phase 3.

---

## Phase 3 — Wallet-init + Limits modal endpoints

### Task 3.1: Add settings fields

**Files:**
- Modify: `app/config/settings.py` — add new fields

- [ ] **Step 1: Add three settings fields**

In `app/config/settings.py`, find the existing field declarations (look near `kalshi_live_trading`) and add:

```python
    auto_init_budget_from_wallet: bool = Field(
        default=True, alias="AUTO_INIT_BUDGET_FROM_WALLET"
    )
    brain_auto_resync_seconds: int = Field(
        default=300, alias="BRAIN_AUTO_RESYNC_SECONDS"
    )
    sportsbook_refresh_seconds: int = Field(
        default=600, alias="SPORTSBOOK_REFRESH_SECONDS"
    )
    trading_stream_max_hz: float = Field(
        default=1.0, alias="TRADING_STREAM_MAX_HZ"
    )
```

- [ ] **Step 2: Type check + lint**

Run: `mypy app/config/settings.py && ruff check app/config/settings.py`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add app/config/settings.py
git commit -m "feat(settings): wallet-init + brain-resync + sportsbook + stream-hz knobs"
```

---

### Task 3.2: Wallet-init service

**Files:**
- Create: `app/trading/wallet_init.py`
- Test: `tests/unit/trading/test_wallet_init.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/trading/test_wallet_init.py
from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from app.trading.wallet_init import init_budget_from_wallet


@pytest.fixture()
def limits_path(tmp_path: Path) -> Path:
    return tmp_path / "trading_limits.json"


def test_creates_file_when_missing(limits_path: Path) -> None:
    client = MagicMock()
    client.get_balance.return_value = 10.11
    init_budget_from_wallet(client=client, path=limits_path, today=date(2026, 5, 11))
    data = json.loads(limits_path.read_text())
    assert data["max_open_notional"] == 10.11
    assert data["per_market_cap"] == 10.11 / 2
    assert data["daily_loss_cap"] == 10.11 / 5  # 20% default
    assert data["wallet_init_done_at"].startswith("2026-05-11")


def test_skips_when_same_day_init_already_done(limits_path: Path) -> None:
    now = datetime.now(UTC).isoformat()
    limits_path.write_text(
        json.dumps(
            {
                "max_open_notional": 5.00,
                "per_market_cap": 2.50,
                "daily_loss_cap": 1.00,
                "reject_cooldown_seconds": 300,
                "wallet_init_done_at": now,
            }
        )
    )
    client = MagicMock()
    client.get_balance.return_value = 999.99
    init_budget_from_wallet(client=client, path=limits_path, today=date.fromisoformat(now[:10]))
    data = json.loads(limits_path.read_text())
    assert data["max_open_notional"] == 5.00  # unchanged
    client.get_balance.assert_not_called()


def test_reinitializes_when_yesterday(limits_path: Path) -> None:
    yesterday = (datetime.now(UTC) - timedelta(days=1)).isoformat()
    limits_path.write_text(
        json.dumps(
            {
                "max_open_notional": 5.00,
                "per_market_cap": 2.50,
                "daily_loss_cap": 1.00,
                "reject_cooldown_seconds": 300,
                "wallet_init_done_at": yesterday,
            }
        )
    )
    client = MagicMock()
    client.get_balance.return_value = 20.00
    init_budget_from_wallet(client=client, path=limits_path)
    data = json.loads(limits_path.read_text())
    assert data["max_open_notional"] == 20.00


def test_swallows_kalshi_errors(limits_path: Path) -> None:
    client = MagicMock()
    client.get_balance.side_effect = RuntimeError("kalshi down")
    # Should not raise; should leave file alone.
    init_budget_from_wallet(client=client, path=limits_path)
    assert not limits_path.is_file()
```

- [ ] **Step 2: Run test, expect failure**

Run: `pytest tests/unit/trading/test_wallet_init.py -v`
Expected: FAIL — module missing.

- [ ] **Step 3: Implement wallet-init**

```python
# app/trading/wallet_init.py
from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Protocol

_log = logging.getLogger("nba.trading.wallet_init")


class _BalanceClient(Protocol):
    def get_balance(self) -> float: ...


def init_budget_from_wallet(
    *,
    client: _BalanceClient,
    path: Path,
    today: date | None = None,
) -> None:
    """Seed ``config/trading_limits.json`` from the Kalshi wallet balance.

    Runs at most once per calendar day. Manual edits made via the UI between
    runs are preserved — we only overwrite when ``wallet_init_done_at`` is
    older than the current calendar day.
    """
    today_date = today or date.today()
    existing = _read_existing(path)
    if _already_initialized_today(existing, today_date):
        return
    try:
        balance = float(client.get_balance())
    except Exception as exc:  # noqa: BLE001 - wallet init must not crash startup
        _log.warning("wallet-init: balance fetch failed: %s", exc)
        return
    if balance <= 0:
        _log.info("wallet-init: balance is %.2f, skipping init", balance)
        return
    payload = {
        "max_open_notional": round(balance, 2),
        "per_market_cap": round(balance / 2, 2),
        "max_open_notional_per_order": None,
        "daily_loss_cap": round(balance / 5, 2),
        "reject_cooldown_seconds": int(existing.get("reject_cooldown_seconds", 300)),
        "wallet_init_done_at": datetime.now(UTC).isoformat(),
    }
    # Preserve any user override fields
    for preserved in ("per_order_cap_override",):
        if preserved in existing:
            payload[preserved] = existing[preserved]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    _log.info("wallet-init: max_open_notional=%.2f", payload["max_open_notional"])


def _read_existing(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _already_initialized_today(existing: dict[str, Any], today_date: date) -> bool:
    stamp = existing.get("wallet_init_done_at")
    if not isinstance(stamp, str):
        return False
    try:
        return datetime.fromisoformat(stamp).date() == today_date
    except ValueError:
        return False
```

Note: existing `load_live_limits` rejects unknown fields. Update `app/trading/live_limits.py` to skip `wallet_init_done_at` and `per_order_cap_override` rather than reject — or list them as allowed extras. Simplest fix: in `_REQUIRED_FIELDS`, leave as is, but in the validation loop, only enforce required fields. Existing code already does this (it iterates `_REQUIRED_FIELDS`), so extras are already tolerated. Verify with the test below.

- [ ] **Step 4: Run tests, expect pass**

Run: `pytest tests/unit/trading/test_wallet_init.py -v`
Expected: 4 passed.

- [ ] **Step 5: Confirm load_live_limits still works with extra fields**

```bash
python -c "import json, pathlib; from app.trading.live_limits import load_live_limits; p = pathlib.Path('/tmp/test_limits.json'); p.write_text(json.dumps({'max_open_notional': 10.0, 'per_market_cap': 5.0, 'daily_loss_cap': 2.0, 'reject_cooldown_seconds': 300, 'wallet_init_done_at': '2026-05-11T00:00:00Z'})); print(load_live_limits(p))"
```

Expected: prints RiskLimits with no error. If it errors on the extra field, patch `load_live_limits` to silently ignore unknown keys.

- [ ] **Step 6: Type check, lint, commit**

```bash
mypy app/trading/wallet_init.py
ruff check app/trading/wallet_init.py tests/unit/trading/test_wallet_init.py
git add app/trading/wallet_init.py tests/unit/trading/test_wallet_init.py
git commit -m "feat(trading): seed budget from Kalshi wallet balance once per day"
```

---

### Task 3.3: Limits, wallet, and pick action endpoints

**Files:**
- Modify: `app/server/routers/trading.py`
- Modify: `app/server/schemas/trading.py` — add request models

- [ ] **Step 1: Add request models to schemas**

Append to `app/server/schemas/trading.py`:

```python
# ---- Limits + wallet request models (Phase 3) ----

class LimitsUpdateRequestModel(BaseModel):
    max_open_notional: float | None = Field(default=None, gt=0.0, le=1_000_000.0)
    daily_loss_cap: float | None = Field(default=None, ge=0.0, le=1_000_000.0)
    reject_cooldown_seconds: int | None = Field(default=None, ge=0, le=86_400)
    per_order_cap_override: float | None = Field(default=None, ge=0.0)


class LimitsResponseModel(BaseModel):
    max_open_notional: float
    per_market_cap: float
    daily_loss_cap: float
    reject_cooldown_seconds: int
    per_order_cap_override: float | None
    wallet_init_done_at: datetime | None


class WalletBalanceResponseModel(BaseModel):
    balance: float
    fetched_at: datetime


class PickToggleRequestModel(BaseModel):
    included: bool


class PickBulkRequestModel(BaseModel):
    action: Literal["select_all_hittable", "deselect_all", "top_n"]
    n: int | None = Field(default=None, ge=1, le=50)


class ThresholdsUpdateRequestModel(BaseModel):
    min_hit_pct: float = Field(ge=0.0, le=1.0)
    min_edge_bps: int = Field(ge=0, le=5_000)
```

- [ ] **Step 2: Add endpoint handlers**

In `app/server/routers/trading.py`, add:

```python
from app.trading.selections import SelectionStore
from app.trading.wallet_init import init_budget_from_wallet


def _selections_path(request: Request) -> Path:
    return Path(get_settings().app_data_dir) / "trading_selections.json"


def _limits_path() -> Path:
    return Path(get_settings().trading_limits_path)


@router.post("/picks/{candidate_id}/toggle", response_model=TradingLiveSnapshotModel)
def trading_picks_toggle(
    request: Request, candidate_id: str, body: PickToggleRequestModel
) -> TradingLiveSnapshotModel:
    store = SelectionStore.load(_selections_path(request))
    store.set_selection(_board_date_today(), candidate_id, body.included)
    store.save(today=_board_date_today())
    publisher: TradingStreamPublisher = request.app.state.trading_stream_publisher
    publisher.log_event(
        level="info",
        message=f"pick {candidate_id} {'included' if body.included else 'excluded'}",
    )
    publisher.notify()
    return _build_live_snapshot(request)


@router.post("/picks/bulk", response_model=TradingLiveSnapshotModel)
def trading_picks_bulk(
    request: Request, body: PickBulkRequestModel
) -> TradingLiveSnapshotModel:
    snapshot = _build_live_snapshot(request)
    store = SelectionStore.load(_selections_path(request))
    today = _board_date_today()
    if body.action == "deselect_all":
        store.bulk_set(today, {row.candidate_id: False for row in snapshot.picks})
    elif body.action == "select_all_hittable":
        store.bulk_set(
            today,
            {row.candidate_id: True for row in snapshot.picks if row.state != "blocked"},
        )
    elif body.action == "top_n":
        n = body.n or 5
        ranked = sorted(
            (row for row in snapshot.picks if row.state != "blocked"),
            key=lambda r: r.edge_bps,
            reverse=True,
        )
        keep_ids = {row.candidate_id for row in ranked[:n]}
        store.bulk_set(
            today,
            {
                row.candidate_id: (row.candidate_id in keep_ids)
                for row in snapshot.picks
                if row.state != "blocked"
            },
        )
    store.save(today=today)
    publisher: TradingStreamPublisher = request.app.state.trading_stream_publisher
    publisher.log_event(level="info", message=f"bulk pick action: {body.action}")
    publisher.notify()
    return _build_live_snapshot(request)


@router.post("/thresholds", response_model=TradingLiveSnapshotModel)
def trading_thresholds_update(
    request: Request, body: ThresholdsUpdateRequestModel
) -> TradingLiveSnapshotModel:
    store = SelectionStore.load(_selections_path(request))
    store.update_thresholds(min_hit_pct=body.min_hit_pct, min_edge_bps=body.min_edge_bps)
    store.save(today=_board_date_today())
    publisher: TradingStreamPublisher = request.app.state.trading_stream_publisher
    publisher.log_event(
        level="info",
        message=f"thresholds set: min_hit={body.min_hit_pct:.2f} min_edge={body.min_edge_bps}bp",
    )
    publisher.notify()
    return _build_live_snapshot(request)


@router.post("/limits", response_model=LimitsResponseModel)
def trading_limits_update(
    request: Request, body: LimitsUpdateRequestModel
) -> LimitsResponseModel:
    path = _limits_path()
    existing = json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}
    if body.max_open_notional is not None:
        existing["max_open_notional"] = body.max_open_notional
        existing["per_market_cap"] = round(body.max_open_notional / 2, 2)
    if body.daily_loss_cap is not None:
        existing["daily_loss_cap"] = body.daily_loss_cap
    if body.reject_cooldown_seconds is not None:
        existing["reject_cooldown_seconds"] = body.reject_cooldown_seconds
    if body.per_order_cap_override is not None:
        existing["per_order_cap_override"] = body.per_order_cap_override
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(existing, indent=2, sort_keys=True), encoding="utf-8")
    publisher: TradingStreamPublisher = request.app.state.trading_stream_publisher
    publisher.log_event(level="info", message="limits updated via UI")
    publisher.notify()
    return LimitsResponseModel(
        max_open_notional=float(existing.get("max_open_notional", 0)),
        per_market_cap=float(existing.get("per_market_cap", 0)),
        daily_loss_cap=float(existing.get("daily_loss_cap", 0)),
        reject_cooldown_seconds=int(existing.get("reject_cooldown_seconds", 300)),
        per_order_cap_override=existing.get("per_order_cap_override"),
        wallet_init_done_at=existing.get("wallet_init_done_at"),
    )


@router.get("/limits", response_model=LimitsResponseModel)
def trading_limits_read(request: Request) -> LimitsResponseModel:
    path = _limits_path()
    existing = json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}
    return LimitsResponseModel(
        max_open_notional=float(existing.get("max_open_notional", 0)),
        per_market_cap=float(existing.get("per_market_cap", 0)),
        daily_loss_cap=float(existing.get("daily_loss_cap", 0)),
        reject_cooldown_seconds=int(existing.get("reject_cooldown_seconds", 300)),
        per_order_cap_override=existing.get("per_order_cap_override"),
        wallet_init_done_at=existing.get("wallet_init_done_at"),
    )


@router.get("/wallet", response_model=WalletBalanceResponseModel)
def trading_wallet_balance(request: Request) -> WalletBalanceResponseModel:
    settings = get_settings()
    if not (settings.kalshi_api_key_id and settings.kalshi_private_key_path):
        raise HTTPException(status_code=400, detail="Kalshi credentials not configured")
    from app.providers.exchanges.kalshi_client import KalshiClient

    client = KalshiClient(
        api_key_id=settings.kalshi_api_key_id,
        private_key_path=Path(settings.kalshi_private_key_path),
        base_url=settings.kalshi_base_url,
    )
    try:
        balance = float(client.get_balance())
    finally:
        client.close()
    return WalletBalanceResponseModel(balance=balance, fetched_at=datetime.now(UTC))
```

- [ ] **Step 3: Wire wallet-init into lifespan**

In `app/server/main.py` `lifespan`, after `market_service.start()`:

```python
if settings.auto_init_budget_from_wallet and settings.kalshi_api_key_id and settings.kalshi_private_key_path:
    try:
        from app.providers.exchanges.kalshi_client import KalshiClient

        client = KalshiClient(
            api_key_id=settings.kalshi_api_key_id,
            private_key_path=Path(settings.kalshi_private_key_path),
            base_url=settings.kalshi_base_url,
        )
        try:
            init_budget_from_wallet(client=client, path=Path(settings.trading_limits_path))
        finally:
            client.close()
    except Exception as exc:  # noqa: BLE001
        logging.getLogger("nba.sidecar").warning("wallet-init failed: %s", exc)
```

Import `init_budget_from_wallet` and `Path` at the top of `main.py`.

- [ ] **Step 4: Integration tests for the endpoints**

```python
# tests/integration/server/test_trading_picks_endpoints.py
from __future__ import annotations

import pytest
from httpx import AsyncClient

from app.server.main import create_app


@pytest.mark.asyncio
async def test_toggle_then_snapshot_reflects_change(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("NBA_PROP_APP_DATA_DIR", str(tmp_path))
    app = create_app(app_token="test-token")
    async with AsyncClient(app=app, base_url="http://test") as client:
        async with app.router.lifespan_context(app):
            headers = {"X-App-Token": "test-token"}
            response = await client.post(
                "/api/trading/picks/test-candidate/toggle",
                json={"included": False},
                headers=headers,
            )
            assert response.status_code == 200
            payload = response.json()
            assert "picks" in payload


@pytest.mark.asyncio
async def test_limits_update_persists(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("NBA_PROP_APP_DATA_DIR", str(tmp_path))
    limits_path = tmp_path / "trading_limits.json"
    monkeypatch.setenv("NBA_PROP_TRADING_LIMITS_PATH", str(limits_path))
    app = create_app(app_token="test-token")
    async with AsyncClient(app=app, base_url="http://test") as client:
        async with app.router.lifespan_context(app):
            response = await client.post(
                "/api/trading/limits",
                json={"max_open_notional": 25.50},
                headers={"X-App-Token": "test-token"},
            )
            assert response.status_code == 200
            assert response.json()["max_open_notional"] == 25.50
```

- [ ] **Step 5: Run tests**

Run: `pytest tests/integration/server/test_trading_picks_endpoints.py -v`
Expected: 2 passed.

- [ ] **Step 6: Type check, lint, commit**

```bash
mypy app
ruff check .
git add app/server/routers/trading.py app/server/schemas/trading.py app/server/main.py tests/integration/server/test_trading_picks_endpoints.py
git commit -m "feat(trading): pick toggle, bulk, thresholds, limits, wallet endpoints"
```

---

### Phase 3 — End-of-phase gates

- [ ] Run gates: `pytest tests/ -k trading -v && ruff check . && mypy app`
- [ ] Dispatch `python-reviewer` + `security-reviewer` on Phase 3 diff.
  - `security-reviewer` focus: limits validation bounds, JSON write race conditions, wallet endpoint exposure under app-token middleware.
- [ ] Address findings, proceed to Phase 4.

---

## Phase 4 — Brain auto-resync background task

### Task 4.1: Auto-resync service

**Files:**
- Create: `app/trading/brain_auto_resync.py`
- Test: `tests/unit/trading/test_brain_auto_resync.py`

- [ ] **Step 1: Write failing test**

```python
# tests/unit/trading/test_brain_auto_resync.py
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.trading.brain_auto_resync import BrainAutoResync


@pytest.mark.asyncio
async def test_runs_on_interval_in_live_mode() -> None:
    sync_fn = MagicMock()
    sync_fn.return_value = MagicMock(state="synced", selected_ticker="KX-A")
    publisher = MagicMock()
    resync = BrainAutoResync(
        interval_seconds=0.05,
        sync_fn=sync_fn,
        mode_fn=lambda: "supervised-live",
        publisher=publisher,
    )
    await resync.start()
    await asyncio.sleep(0.15)
    await resync.stop()
    assert sync_fn.call_count >= 2
    assert publisher.log_event.called


@pytest.mark.asyncio
async def test_pauses_in_observe_mode() -> None:
    sync_fn = MagicMock(return_value=MagicMock(state="observe_only"))
    resync = BrainAutoResync(
        interval_seconds=0.05,
        sync_fn=sync_fn,
        mode_fn=lambda: "observe",
        publisher=MagicMock(),
    )
    await resync.start()
    await asyncio.sleep(0.15)
    await resync.stop()
    sync_fn.assert_not_called()


@pytest.mark.asyncio
async def test_logs_errors_without_crashing() -> None:
    sync_fn = MagicMock(side_effect=RuntimeError("boom"))
    publisher = MagicMock()
    resync = BrainAutoResync(
        interval_seconds=0.05,
        sync_fn=sync_fn,
        mode_fn=lambda: "supervised-live",
        publisher=publisher,
    )
    await resync.start()
    await asyncio.sleep(0.10)
    await resync.stop()
    error_calls = [c for c in publisher.log_event.call_args_list if c.kwargs.get("level") == "error"]
    assert len(error_calls) >= 1
```

- [ ] **Step 2: Run test, expect failure**

Run: `pytest tests/unit/trading/test_brain_auto_resync.py -v`
Expected: FAIL.

- [ ] **Step 3: Implement BrainAutoResync**

```python
# app/trading/brain_auto_resync.py
from __future__ import annotations

import asyncio
import logging
from typing import Callable

from app.trading.stream_publisher import TradingStreamPublisher

_log = logging.getLogger("nba.trading.brain_resync")


class BrainAutoResync:
    """Periodically re-syncs the decision brain while in supervised-live mode.

    Pause-while-observing semantics: the timer keeps ticking but the sync is
    skipped when ``mode_fn`` returns ``"observe"``.
    """

    def __init__(
        self,
        *,
        interval_seconds: float,
        sync_fn: Callable[[], object],
        mode_fn: Callable[[], str],
        publisher: TradingStreamPublisher,
    ) -> None:
        self._interval = max(interval_seconds, 1.0)
        self._sync_fn = sync_fn
        self._mode_fn = mode_fn
        self._publisher = publisher
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stop = asyncio.Event()
        self._task = asyncio.create_task(self._loop(), name="brain-auto-resync")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._stop.set()
        self._task.cancel()
        try:
            await self._task
        except (asyncio.CancelledError, Exception):  # noqa: BLE001
            pass
        finally:
            self._task = None

    async def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._interval)
                return  # stop requested
            except asyncio.TimeoutError:
                pass
            if self._mode_fn() != "supervised-live":
                continue
            try:
                result = await asyncio.to_thread(self._sync_fn)
                state = getattr(result, "state", "unknown")
                ticker = getattr(result, "selected_ticker", None) or "—"
                self._publisher.log_event(
                    level="info",
                    message=f"brain auto-resync: state={state} top={ticker}",
                )
                self._publisher.notify()
            except Exception as exc:  # noqa: BLE001
                _log.warning("brain auto-resync error: %s", exc)
                self._publisher.log_event(
                    level="error",
                    message=f"brain auto-resync failed: {exc}",
                )
```

- [ ] **Step 4: Run tests**

Run: `pytest tests/unit/trading/test_brain_auto_resync.py -v`
Expected: 3 passed.

- [ ] **Step 5: Wire into main.py lifespan**

In `app/server/main.py`, inside `create_app` after `snapshot_service` is built, add:

```python
from app.trading.brain_auto_resync import BrainAutoResync

# Lazy import inside lifespan to avoid circular deps
```

Inside `lifespan`, after `stream_publisher.log_event(...)`:

```python
def _do_brain_sync() -> object:
    from datetime import date as _date
    from app.trading.decision_brain import sync_decision_brain

    settings = get_settings()
    if not settings.kalshi_decision_brain_enabled:
        return None
    board_entry = resolved_board_cache.populate(_date.today())
    return sync_decision_brain(
        settings=settings,
        board_entry=board_entry,
        board_date=_date.today(),
        mode="supervised-live",
        resolve_markets=True,
        build_pack=True,
    )

def _current_mode() -> str:
    # Read from brain last-known state file or default to observe
    decisions_path = Path(get_settings().kalshi_decisions_path)
    if not decisions_path.is_file():
        return "observe"
    try:
        rows = json.loads(decisions_path.read_text(encoding="utf-8"))
        first = rows[0] if isinstance(rows, list) else (rows.get("decisions") or [{}])[0]
        return "supervised-live" if first.get("mode") == "live" else "observe"
    except Exception:
        return "observe"

brain_resync = BrainAutoResync(
    interval_seconds=float(settings.brain_auto_resync_seconds),
    sync_fn=_do_brain_sync,
    mode_fn=_current_mode,
    publisher=stream_publisher,
)
await brain_resync.start()
_app.state.trading_brain_resync = brain_resync
```

And in the `finally:` block of `lifespan` (after `market_service.stop()`):

```python
await brain_resync.stop()
```

- [ ] **Step 6: Type check, lint, commit**

```bash
mypy app/trading/brain_auto_resync.py app/server/main.py
ruff check app/trading/brain_auto_resync.py app/server/main.py
git add app/trading/brain_auto_resync.py tests/unit/trading/test_brain_auto_resync.py app/server/main.py
git commit -m "feat(trading): brain auto-resync background task with mode pausing"
```

---

### Phase 4 — End-of-phase gates

- [ ] Gates: `pytest tests/unit/trading/ tests/integration/server/ -v && ruff check . && mypy app`
- [ ] `python-reviewer` on Phase 4 diff. Focus: asyncio task lifecycle, error swallowing, mode flapping.
- [ ] `code-reviewer` (Opus 4.7) end-of-backend-phases holistic review across Phases 1-4.
- [ ] Address findings. Backend is now complete. Proceed to Phase 5 (frontend foundation).

---

## Phase 5 — CSS tokens + new file scaffolding

Frontend-only, no behavior yet. Lays the foundation.

### Task 5.1: Add --trading-* tokens to theme.css

**Files:**
- Modify: `desktop_tauri/src/theme.css` — append a new `:root` declaration block

- [ ] **Step 1: Append tokens**

At the bottom of `desktop_tauri/src/theme.css`, append:

```css
/* ---- Trading terminal design tokens (Phase 5) ---- */
:root {
  --trading-bg: #0a0d14;
  --trading-surface: #111827;
  --trading-surface-alt: #0f1421;
  --trading-border: #1f2937;
  --trading-border-soft: #374151;

  --trading-fg: #e5e7eb;
  --trading-fg-muted: #9ca3af;
  --trading-fg-subtle: #6b7280;

  --trading-accent-pnl: #2ecc71;
  --trading-accent-budget: #3b82f6;
  --trading-accent-picks: #00d4aa;
  --trading-accent-system: #f59e0b;
  --trading-accent-danger: #ef4444;

  --trading-font-mono: ui-monospace, "Cascadia Code", "JetBrains Mono", Menlo, monospace;

  --trading-pad-sm: 8px;
  --trading-pad-md: 12px;
  --trading-pad-lg: 14px;

  --trading-pulse-positive: rgba(46, 204, 113, 0.25);
  --trading-pulse-negative: rgba(239, 68, 68, 0.25);
}
```

- [ ] **Step 2: Commit**

```bash
git add desktop_tauri/src/theme.css
git commit -m "feat(trading): --trading-* CSS tokens for terminal aesthetic"
```

---

### Task 5.2: Create trading.css and stub trading folder structure

**Files:**
- Create: `desktop_tauri/src/styles/trading.css`
- Create: `desktop_tauri/src/routes/trading/index.tsx` (stub)
- Create: `desktop_tauri/src/routes/trading/api/types.ts`
- Modify: `desktop_tauri/src/routes/trading.tsx` — change to re-export

- [ ] **Step 1: Create trading.css**

```css
/* desktop_tauri/src/styles/trading.css */
.trading-page-v2 {
  background: var(--trading-bg);
  color: var(--trading-fg);
  font-family: inherit;
  padding: var(--trading-pad-lg);
  min-height: 100vh;
}

.trading-page-v2 .mono {
  font-family: var(--trading-font-mono);
}

.trading-page-v2 .micro {
  color: var(--trading-fg-subtle);
  font-size: 9px;
  letter-spacing: 1.5px;
  text-transform: uppercase;
  font-weight: 600;
}

.trading-tile {
  background: var(--trading-surface);
  border-left: 2px solid var(--trading-border);
  border-radius: 6px;
  padding: var(--trading-pad-md) var(--trading-pad-lg);
}

.trading-tile.accent-pnl { border-left-color: var(--trading-accent-pnl); }
.trading-tile.accent-budget { border-left-color: var(--trading-accent-budget); }
.trading-tile.accent-picks { border-left-color: var(--trading-accent-picks); }
.trading-tile.accent-system { border-left-color: var(--trading-accent-system); }

.trading-tile-value {
  font-family: var(--trading-font-mono);
  font-size: 20px;
  font-weight: 700;
  margin-top: 2px;
}

.trading-tile-bar {
  background: var(--trading-border);
  height: 3px;
  border-radius: 2px;
  margin-top: 6px;
  overflow: hidden;
}

.trading-tile-bar > span {
  display: block;
  height: 100%;
  border-radius: 2px;
}

.trading-pulse-up {
  animation: trading-pulse-up 250ms ease-out;
}
.trading-pulse-down {
  animation: trading-pulse-down 250ms ease-out;
}

@keyframes trading-pulse-up {
  0% { background-color: var(--trading-pulse-positive); }
  100% { background-color: transparent; }
}
@keyframes trading-pulse-down {
  0% { background-color: var(--trading-pulse-negative); }
  100% { background-color: transparent; }
}

/* Picks table */
.picks-table {
  width: 100%;
  border-collapse: collapse;
  font-family: var(--trading-font-mono);
  font-size: 11px;
}

.picks-table th,
.picks-table td {
  padding: 9px var(--trading-pad-sm);
  border-bottom: 1px solid var(--trading-border);
  text-align: left;
}

.picks-table th {
  color: var(--trading-fg-subtle);
  font-size: 9px;
  letter-spacing: 1.5px;
  text-transform: uppercase;
  font-weight: 600;
  cursor: pointer;
}

.picks-table tr.excluded { opacity: 0.55; }
.picks-table tr.blocked { background: rgba(239, 68, 68, 0.04); }
.picks-table tr:hover { background: var(--trading-surface-alt); cursor: pointer; }

.pick-bullet {
  font-size: 14px;
  cursor: pointer;
  user-select: none;
}
.pick-bullet.included { color: var(--trading-accent-picks); }
.pick-bullet.excluded { color: var(--trading-border-soft); }
.pick-bullet.blocked { color: var(--trading-accent-danger); cursor: help; }

/* Bet slip sidebar */
.bet-slip {
  background: var(--trading-surface);
  border-left: 2px solid var(--trading-accent-picks);
  border-radius: 6px;
  padding: var(--trading-pad-lg);
  position: sticky;
  top: var(--trading-pad-md);
}

/* Event log */
.event-log {
  background: #000;
  border: 1px solid var(--trading-border);
  border-radius: 6px;
  padding: 10px 12px;
  font-family: var(--trading-font-mono);
  font-size: 10px;
  line-height: 1.6;
  height: 110px;
  overflow-y: auto;
}
.event-log .log-info { color: var(--trading-fg-muted); }
.event-log .log-warn { color: var(--trading-accent-system); }
.event-log .log-error { color: var(--trading-accent-danger); }

/* Filter pills */
.filter-pill {
  font-size: 10px;
  padding: 3px 10px;
  border-radius: 14px;
  background: var(--trading-surface);
  color: var(--trading-fg-muted);
  border: 1px solid var(--trading-border-soft);
  cursor: pointer;
}
.filter-pill.active {
  background: rgba(0, 212, 170, 0.12);
  color: var(--trading-accent-picks);
  border-color: rgba(0, 212, 170, 0.4);
}

/* Buttons */
.btn-trading {
  background: var(--trading-surface);
  border: 1px solid var(--trading-border-soft);
  color: var(--trading-fg);
  padding: 5px 12px;
  border-radius: 4px;
  font-size: 10px;
  font-family: inherit;
  cursor: pointer;
}
.btn-trading.primary {
  background: var(--trading-accent-picks);
  color: var(--trading-bg);
  border-color: var(--trading-accent-picks);
  font-weight: 700;
}
.btn-trading.danger {
  background: transparent;
  color: var(--trading-accent-danger);
  border-color: var(--trading-accent-danger);
}
.btn-trading.ghost { background: transparent; }
.btn-trading:disabled { opacity: 0.5; cursor: not-allowed; }
```

- [ ] **Step 2: Create types.ts**

```typescript
// desktop_tauri/src/routes/trading/api/types.ts

// Mirrors app/server/schemas/trading.py TradingLiveSnapshotModel
export type KpiPnl = {
  daily_pnl: number;
  realized: number;
  unrealized: number;
  loss_cap: number;
  loss_progress: number;
};

export type KpiBudget = {
  max_open_notional: number;
  allocated: number;
  free: number;
  usage_progress: number;
};

export type KpiPicks = {
  available: number;
  selected: number;
  excluded: number;
  blocked: number;
  est_total_profit: number;
};

export type KpiSystem = {
  status: "ready" | "blocked" | "checking";
  mode: "observe" | "supervised-live";
  gates_passed: number;
  gates_total: number;
  ws_connected: boolean;
  summary: string;
};

export type KpiTiles = {
  pnl: KpiPnl;
  budget: KpiBudget;
  picks: KpiPicks;
  system: KpiSystem;
};

export type ControlBarState = {
  mode: "observe" | "supervised-live";
  loop_state: "idle" | "starting" | "running" | "killed" | "exited" | "failed" | "blocked";
  can_start: boolean;
  start_label: string;
  kill_switch_active: boolean;
};

export type PickKalshi = {
  ticker: string | null;
  yes_bid: number | null;
  yes_ask: number | null;
  spread: number | null;
  last_quote_at: string | null;
};

export type PickState = "queued" | "excluded" | "blocked" | "filled" | "partial";

export type PickRow = {
  candidate_id: string;
  rank: number;
  prop_label: string;
  game_label: string | null;
  hit_pct: number;
  edge_bps: number;
  model_prob: number;
  market_prob: number | null;
  alloc: number;
  est_profit: number;
  state: PickState;
  selected: boolean;
  blocker_reason: string | null;
  kalshi: PickKalshi;
};

export type BetSlipPick = {
  candidate_id: string;
  prop_label: string;
  hit_pct: number;
  edge_bps: number;
  alloc: number;
  est_profit: number;
};

export type BetSlip = {
  selected: BetSlipPick[];
  total_stake: number;
  cap_total: number;
  est_total_profit: number;
  unused_budget: number;
};

export type EventLogLine = {
  cursor: number;
  timestamp: string;
  level: "info" | "warn" | "error";
  message: string;
};

export type PnlPoint = { index: number; pnl: number };

export type TradingLiveSnapshot = {
  observed_at: string;
  kpis: KpiTiles;
  control: ControlBarState;
  picks: PickRow[];
  bet_slip: BetSlip;
  positions: unknown[];
  fills: unknown[];
  quotes: unknown[];
  resting_orders: unknown[];
  diagnostics: { readiness: unknown; brain: unknown };
  event_log: EventLogLine[];
  pnl_trend: PnlPoint[];
  errors: string[];
  stream_cursor: number;
};
```

- [ ] **Step 3: Create stub index.tsx**

```typescript
// desktop_tauri/src/routes/trading/index.tsx
import { createRoute } from "@tanstack/react-router";
import { Route as rootRoute } from "../__root";
import "../../styles/trading.css";

export const Route = createRoute({
  getParentRoute: () => rootRoute,
  path: "/trading",
  component: TradingPageV2,
});

function TradingPageV2() {
  return (
    <div className="trading-page-v2">
      <p className="micro">Trading terminal — under construction</p>
    </div>
  );
}
```

- [ ] **Step 4: Re-route trading.tsx to point at the new entry**

Replace the contents of `desktop_tauri/src/routes/trading.tsx` with:

```typescript
// Re-export the new trading route. The actual implementation lives in trading/index.tsx.
export { Route } from "./trading/index";
```

- [ ] **Step 5: Type check + build**

```bash
cd desktop_tauri && npx tsc --noEmit && npm run build
```

Expected: clean. Page should render an empty "under construction" terminal background when launched.

- [ ] **Step 6: Commit**

```bash
git add desktop_tauri/src/styles/trading.css desktop_tauri/src/routes/trading/index.tsx desktop_tauri/src/routes/trading/api/types.ts desktop_tauri/src/routes/trading.tsx
git commit -m "feat(trading-ui): scaffold trading/ folder with CSS + types + stub page"
```

---

### Phase 5 — End-of-phase gates

- [ ] `npx tsc --noEmit` clean
- [ ] `npm run build --prefix desktop_tauri` clean
- [ ] Manually open the app, navigate to /trading, verify the dark terminal background shows.
- [ ] No reviewer needed (no logic changes). Proceed to Phase 6.

---

## Phase 6 — Zustand store + useTradingStream hook

### Task 6.1: Store module

**Files:**
- Create: `desktop_tauri/src/routes/trading/store.ts`

- [ ] **Step 1: Write the store**

```typescript
// desktop_tauri/src/routes/trading/store.ts
import { create } from "zustand";
import type { TradingLiveSnapshot, PickRow, EventLogLine } from "./api/types";

type SortKey = "rank" | "hit_pct" | "edge_bps" | "alloc" | "est_profit";
type SortDir = "asc" | "desc";
type FilterMode = "all" | "hittable" | "excluded" | "blocked";

type TradingState = {
  // Live data
  snapshot: TradingLiveSnapshot | null;
  streamConnected: boolean;
  lastSnapshotAt: string | null;
  // UI-only state
  sortKey: SortKey;
  sortDir: SortDir;
  filter: FilterMode;
  expandedCandidateId: string | null;
  thresholdsOpen: boolean;
  limitsModalOpen: boolean;
  // Mutators
  applySnapshot: (snapshot: TradingLiveSnapshot) => void;
  setStreamConnected: (connected: boolean) => void;
  setSort: (key: SortKey) => void;
  setFilter: (mode: FilterMode) => void;
  toggleExpand: (candidateId: string) => void;
  setThresholdsOpen: (open: boolean) => void;
  setLimitsModalOpen: (open: boolean) => void;
};

export const useTradingStore = create<TradingState>((set) => ({
  snapshot: null,
  streamConnected: false,
  lastSnapshotAt: null,
  sortKey: "rank",
  sortDir: "asc",
  filter: "all",
  expandedCandidateId: null,
  thresholdsOpen: false,
  limitsModalOpen: false,

  applySnapshot: (snapshot) =>
    set({ snapshot, lastSnapshotAt: snapshot.observed_at }),

  setStreamConnected: (connected) => set({ streamConnected: connected }),

  setSort: (key) =>
    set((state) => ({
      sortKey: key,
      sortDir: state.sortKey === key && state.sortDir === "desc" ? "asc" : "desc",
    })),

  setFilter: (filter) => set({ filter }),

  toggleExpand: (candidateId) =>
    set((state) => ({
      expandedCandidateId: state.expandedCandidateId === candidateId ? null : candidateId,
    })),

  setThresholdsOpen: (open) => set({ thresholdsOpen: open }),
  setLimitsModalOpen: (open) => set({ limitsModalOpen: open }),
}));

// Selector helpers — kept here so components import single function

export function selectVisiblePicks(state: TradingState): PickRow[] {
  if (!state.snapshot) return [];
  const all = state.snapshot.picks;
  const filtered = all.filter((row) => {
    switch (state.filter) {
      case "hittable":
        return row.state !== "blocked";
      case "excluded":
        return row.state === "excluded";
      case "blocked":
        return row.state === "blocked";
      default:
        return true;
    }
  });
  const sorted = [...filtered].sort((a, b) => {
    const dir = state.sortDir === "asc" ? 1 : -1;
    if (state.sortKey === "rank") return (a.rank - b.rank) * dir;
    return (Number(a[state.sortKey]) - Number(b[state.sortKey])) * dir;
  });
  return sorted;
}

export function selectEventLog(state: TradingState): EventLogLine[] {
  return state.snapshot?.event_log ?? [];
}
```

- [ ] **Step 2: Type check**

```bash
cd desktop_tauri && npx tsc --noEmit
```

Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add desktop_tauri/src/routes/trading/store.ts
git commit -m "feat(trading-ui): Zustand store with normalized snapshot + UI selectors"
```

---

### Task 6.2: useTradingStream hook + actions module

**Files:**
- Create: `desktop_tauri/src/routes/trading/hooks/useTradingStream.ts`
- Create: `desktop_tauri/src/routes/trading/hooks/usePulseOnChange.ts`
- Create: `desktop_tauri/src/routes/trading/api/actions.ts`

- [ ] **Step 1: Write actions.ts (typed wrappers around fetch)**

```typescript
// desktop_tauri/src/routes/trading/api/actions.ts
import { api } from "../../../api/client";
import type { TradingLiveSnapshot } from "./types";

const tradingFetch = async <T>(path: string, init?: RequestInit): Promise<T> => {
  const res = await fetch(`${api.baseUrl}${path}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      "X-App-Token": api.appToken,
      ...(init?.headers ?? {}),
    },
  });
  if (!res.ok) {
    throw new Error(`${res.status} ${res.statusText}: ${path}`);
  }
  return (await res.json()) as T;
};

export const tradingActions = {
  togglePick: (candidateId: string, included: boolean) =>
    tradingFetch<TradingLiveSnapshot>(`/api/trading/picks/${encodeURIComponent(candidateId)}/toggle`, {
      method: "POST",
      body: JSON.stringify({ included }),
    }),

  bulk: (action: "select_all_hittable" | "deselect_all" | "top_n", n?: number) =>
    tradingFetch<TradingLiveSnapshot>(`/api/trading/picks/bulk`, {
      method: "POST",
      body: JSON.stringify({ action, n }),
    }),

  setThresholds: (minHitPct: number, minEdgeBps: number) =>
    tradingFetch<TradingLiveSnapshot>(`/api/trading/thresholds`, {
      method: "POST",
      body: JSON.stringify({ min_hit_pct: minHitPct, min_edge_bps: minEdgeBps }),
    }),

  updateLimits: (body: Partial<{
    max_open_notional: number;
    daily_loss_cap: number;
    reject_cooldown_seconds: number;
    per_order_cap_override: number;
  }>) =>
    tradingFetch(`/api/trading/limits`, {
      method: "POST",
      body: JSON.stringify(body),
    }),

  readLimits: () => tradingFetch(`/api/trading/limits`),

  fetchWallet: () => tradingFetch<{ balance: number; fetched_at: string }>(`/api/trading/wallet`),
};
```

Note: `api.baseUrl` and `api.appToken` should already be exposed by the existing `client.ts`. If not exposed, add them as exports there.

- [ ] **Step 2: Write usePulseOnChange**

```typescript
// desktop_tauri/src/routes/trading/hooks/usePulseOnChange.ts
import { useEffect, useRef, useState } from "react";

export type PulseDirection = "up" | "down" | null;

export function usePulseOnChange(value: number): PulseDirection {
  const previousRef = useRef(value);
  const [pulse, setPulse] = useState<PulseDirection>(null);

  useEffect(() => {
    if (value > previousRef.current) setPulse("up");
    else if (value < previousRef.current) setPulse("down");
    previousRef.current = value;
    const timer = setTimeout(() => setPulse(null), 250);
    return () => clearTimeout(timer);
  }, [value]);

  return pulse;
}
```

- [ ] **Step 3: Write useTradingStream**

```typescript
// desktop_tauri/src/routes/trading/hooks/useTradingStream.ts
import { useEffect, useRef } from "react";
import { api } from "../../../api/client";
import { useTradingStore } from "../store";
import type { TradingLiveSnapshot } from "../api/types";

const BACKOFF_STEPS_MS = [1_000, 2_000, 4_000, 8_000];

export function useTradingStream(): void {
  const applySnapshot = useTradingStore((s) => s.applySnapshot);
  const setStreamConnected = useTradingStore((s) => s.setStreamConnected);
  const backoffIndex = useRef(0);
  const pollTimerRef = useRef<number | null>(null);
  const sourceRef = useRef<EventSource | null>(null);
  const epochRef = useRef(0);

  useEffect(() => {
    let cancelled = false;

    const stopPolling = () => {
      if (pollTimerRef.current !== null) {
        window.clearTimeout(pollTimerRef.current);
        pollTimerRef.current = null;
      }
    };

    const pollFallback = async () => {
      try {
        const res = await fetch(`${api.baseUrl}/api/trading/snapshot-live`, {
          headers: { "X-App-Token": api.appToken },
        });
        if (!res.ok) throw new Error(`${res.status}`);
        const data = (await res.json()) as TradingLiveSnapshot;
        if (cancelled) return;
        applySnapshot(data);
      } catch {
        // swallow — next poll will retry
      }
      if (cancelled) return;
      pollTimerRef.current = window.setTimeout(pollFallback, 2_000);
    };

    const connect = () => {
      if (cancelled) return;
      const epoch = ++epochRef.current;
      const url = `${api.baseUrl}/api/trading/stream?token=${encodeURIComponent(api.appToken)}`;
      const source = new EventSource(url);
      sourceRef.current = source;

      source.addEventListener("open", () => {
        if (cancelled || epochRef.current !== epoch) return;
        setStreamConnected(true);
        backoffIndex.current = 0;
        stopPolling();
      });

      source.addEventListener("snapshot", (event) => {
        if (cancelled || epochRef.current !== epoch) return;
        try {
          const data = JSON.parse((event as MessageEvent<string>).data) as TradingLiveSnapshot;
          applySnapshot(data);
        } catch {
          // ignore malformed
        }
      });

      source.onerror = () => {
        if (cancelled || epochRef.current !== epoch) return;
        setStreamConnected(false);
        source.close();
        // Start polling fallback immediately
        if (pollTimerRef.current === null) {
          pollTimerRef.current = window.setTimeout(pollFallback, 0);
        }
        // Schedule reconnect
        const delay = BACKOFF_STEPS_MS[Math.min(backoffIndex.current, BACKOFF_STEPS_MS.length - 1)];
        backoffIndex.current = Math.min(backoffIndex.current + 1, BACKOFF_STEPS_MS.length - 1);
        window.setTimeout(connect, delay);
      };
    };

    connect();

    return () => {
      cancelled = true;
      sourceRef.current?.close();
      sourceRef.current = null;
      stopPolling();
    };
  }, [applySnapshot, setStreamConnected]);
}
```

- [ ] **Step 4: Type check + build**

```bash
cd desktop_tauri && npx tsc --noEmit && npm run build
```

Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add desktop_tauri/src/routes/trading/hooks/useTradingStream.ts desktop_tauri/src/routes/trading/hooks/usePulseOnChange.ts desktop_tauri/src/routes/trading/api/actions.ts
git commit -m "feat(trading-ui): SSE stream hook + pulse-on-change + typed action wrappers"
```

---

### Phase 6 — End-of-phase gates

- [ ] `npx tsc --noEmit` clean, `npm run build --prefix desktop_tauri` clean.
- [ ] Dispatch `typescript-reviewer` on Phase 5+6 diff. Focus: SSE reconnect correctness, race conditions on rapid reconnects, store mutation safety.
- [ ] Address findings. Proceed to Phase 7.

---

## Phase 7 — KPI tiles + Control bar

### Task 7.1: KpiTile + KpiTileStrip components

**Files:**
- Create: `desktop_tauri/src/routes/trading/components/KpiTile.tsx`
- Create: `desktop_tauri/src/routes/trading/components/KpiTileStrip.tsx`

- [ ] **Step 1: KpiTile**

```typescript
// desktop_tauri/src/routes/trading/components/KpiTile.tsx
import type { ReactNode } from "react";
import { usePulseOnChange } from "../hooks/usePulseOnChange";

type Accent = "pnl" | "budget" | "picks" | "system";

type Props = {
  label: string;
  value: ReactNode;
  numericForPulse?: number;
  subline?: ReactNode;
  barProgress?: number; // 0..1
  barColor?: string;
  accent: Accent;
  rightSlot?: ReactNode;
};

export function KpiTile({ label, value, numericForPulse, subline, barProgress, barColor, accent, rightSlot }: Props) {
  const pulse = usePulseOnChange(numericForPulse ?? 0);
  const pulseClass = numericForPulse === undefined ? "" : pulse === "up" ? "trading-pulse-up" : pulse === "down" ? "trading-pulse-down" : "";
  return (
    <div className={`trading-tile accent-${accent}`}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <div className="micro">{label}</div>
        {rightSlot ?? null}
      </div>
      <div className={`trading-tile-value mono ${pulseClass}`}>{value}</div>
      {barProgress !== undefined ? (
        <div className="trading-tile-bar">
          <span style={{ width: `${Math.max(0, Math.min(1, barProgress)) * 100}%`, background: barColor ?? "var(--trading-fg-muted)" }} />
        </div>
      ) : null}
      {subline ? <div className="micro" style={{ marginTop: 4, letterSpacing: 0, textTransform: "none", fontSize: 9 }}>{subline}</div> : null}
    </div>
  );
}
```

- [ ] **Step 2: KpiTileStrip**

```typescript
// desktop_tauri/src/routes/trading/components/KpiTileStrip.tsx
import { useState } from "react";
import { useTradingStore } from "../store";
import { tradingActions } from "../api/actions";
import { KpiTile } from "./KpiTile";

function formatMoney(value: number): string {
  const sign = value >= 0 ? "+" : "";
  return `${sign}$${value.toFixed(2)}`;
}

export function KpiTileStrip() {
  const snapshot = useTradingStore((s) => s.snapshot);
  const setLimitsOpen = useTradingStore((s) => s.setLimitsModalOpen);
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState("");

  if (!snapshot) {
    return (
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 10, marginBottom: 10 }}>
        {Array.from({ length: 4 }).map((_, i) => (
          <div key={i} className="trading-tile" style={{ height: 88 }} />
        ))}
      </div>
    );
  }

  const { pnl, budget, picks, system } = snapshot.kpis;

  const submitBudget = async () => {
    const value = Number.parseFloat(draft);
    if (!Number.isFinite(value) || value <= 0) {
      setEditing(false);
      return;
    }
    try {
      await tradingActions.updateLimits({ max_open_notional: value });
    } finally {
      setEditing(false);
    }
  };

  return (
    <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 10, marginBottom: 10 }}>
      <KpiTile
        accent="pnl"
        label="Daily P&L"
        value={<span style={{ color: pnl.daily_pnl >= 0 ? "var(--trading-accent-pnl)" : "var(--trading-accent-danger)" }}>{formatMoney(pnl.daily_pnl)}</span>}
        numericForPulse={pnl.daily_pnl}
        barProgress={pnl.loss_progress}
        barColor={pnl.daily_pnl >= 0 ? "var(--trading-accent-pnl)" : "var(--trading-accent-danger)"}
        subline={`${(pnl.loss_progress * 100).toFixed(0)}% of $${pnl.loss_cap.toFixed(2)} cap · realized ${formatMoney(pnl.realized)} · unreal ${formatMoney(pnl.unrealized)}`}
      />
      <KpiTile
        accent="budget"
        label="Budget"
        value={
          editing ? (
            <input
              autoFocus
              type="number"
              step="0.01"
              defaultValue={budget.max_open_notional.toFixed(2)}
              onChange={(e) => setDraft(e.target.value)}
              onBlur={submitBudget}
              onKeyDown={(e) => {
                if (e.key === "Enter") void submitBudget();
                if (e.key === "Escape") setEditing(false);
              }}
              style={{ background: "transparent", border: "1px solid var(--trading-border-soft)", color: "var(--trading-fg)", fontFamily: "inherit", fontSize: 18, width: 100, padding: "2px 4px" }}
            />
          ) : (
            `$${budget.allocated.toFixed(2)} / $${budget.max_open_notional.toFixed(2)}`
          )
        }
        numericForPulse={budget.allocated}
        barProgress={budget.usage_progress}
        barColor="var(--trading-accent-budget)"
        subline={`$${budget.allocated.toFixed(2)} allocated · $${budget.free.toFixed(2)} free`}
        rightSlot={
          <button
            type="button"
            onClick={() => setEditing((v) => !v)}
            style={{ background: "transparent", border: "none", color: "var(--trading-accent-budget)", fontSize: 9, cursor: "pointer", textDecoration: "underline" }}
          >
            {editing ? "save" : "edit"}
          </button>
        }
      />
      <KpiTile
        accent="picks"
        label="Picks"
        value={`${picks.selected} of ${picks.available}`}
        numericForPulse={picks.selected}
        subline={
          <>
            est. profit{" "}
            <span style={{ color: "var(--trading-accent-pnl)", fontWeight: 700 }}>{formatMoney(picks.est_total_profit)}</span>{" "}
            · {picks.excluded} excluded · {picks.blocked} blocked
          </>
        }
      />
      <KpiTile
        accent="system"
        label="System"
        value={
          <span style={{ color: system.status === "ready" ? "var(--trading-accent-pnl)" : "var(--trading-accent-danger)", fontSize: 13 }}>
            ● {system.status === "ready" ? "READY" : system.status === "blocked" ? "BLOCKED" : "CHECKING"}
          </span>
        }
        subline={system.summary}
      />
    </div>
  );
}
```

- [ ] **Step 3: Type check + build**

```bash
cd desktop_tauri && npx tsc --noEmit && npm run build
```

Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add desktop_tauri/src/routes/trading/components/KpiTile.tsx desktop_tauri/src/routes/trading/components/KpiTileStrip.tsx
git commit -m "feat(trading-ui): KPI tile strip with inline budget edit + pulse animations"
```

---

### Task 7.2: ControlBar component

**Files:**
- Create: `desktop_tauri/src/routes/trading/components/ControlBar.tsx`

- [ ] **Step 1: Implementation**

```typescript
// desktop_tauri/src/routes/trading/components/ControlBar.tsx
import { useState } from "react";
import { useTradingStore } from "../store";
import { tradingActions } from "../api/actions";
import { api } from "../../../api/client";

export function ControlBar() {
  const snapshot = useTradingStore((s) => s.snapshot);
  const setLimitsOpen = useTradingStore((s) => s.setLimitsModalOpen);
  const [modeBusy, setModeBusy] = useState(false);
  const [loopBusy, setLoopBusy] = useState(false);

  if (!snapshot) return <div className="trading-tile" style={{ marginBottom: 18, height: 44 }} />;

  const { control } = snapshot;
  const setMode = async (mode: "observe" | "supervised-live") => {
    setModeBusy(true);
    try {
      await fetch(`${api.baseUrl}/api/trading/brain/sync`, {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-App-Token": api.appToken },
        body: JSON.stringify({ mode, resolve_markets: true, build_pack: true }),
      });
    } finally {
      setModeBusy(false);
    }
  };

  const startLoop = async () => {
    setLoopBusy(true);
    try {
      await fetch(`${api.baseUrl}/api/trading/loop/start`, {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-App-Token": api.appToken },
      });
    } finally {
      setLoopBusy(false);
    }
  };

  const triggerKill = async () => {
    await fetch(`${api.baseUrl}/api/trading/kill-switch`, {
      method: "POST",
      headers: { "X-App-Token": api.appToken },
    });
  };

  return (
    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "8px 12px", background: "var(--trading-surface)", borderRadius: 6, marginBottom: 18 }}>
      <div style={{ display: "flex", gap: 6 }}>
        <button
          type="button"
          className={`btn-trading ${control.mode === "supervised-live" ? "primary" : "ghost"}`}
          disabled={modeBusy}
          onClick={() => setMode("supervised-live")}
        >
          ● Live
        </button>
        <button
          type="button"
          className={`btn-trading ${control.mode === "observe" ? "primary" : "ghost"}`}
          disabled={modeBusy}
          onClick={() => setMode("observe")}
        >
          ○ Watch
        </button>
        <button type="button" className="btn-trading ghost" onClick={() => setLimitsOpen(true)}>
          ⚙ Limits
        </button>
      </div>
      <div style={{ display: "flex", gap: 8 }}>
        <button
          type="button"
          className="btn-trading primary"
          disabled={!control.can_start || loopBusy}
          onClick={startLoop}
        >
          ▶ {control.start_label}
        </button>
        <button type="button" className="btn-trading danger" disabled={control.kill_switch_active} onClick={triggerKill}>
          ⏻ {control.kill_switch_active ? "Stopped" : "Kill Switch"}
        </button>
      </div>
    </div>
  );
}
```

- [ ] **Step 2: Type check + build**

```bash
cd desktop_tauri && npx tsc --noEmit && npm run build
```

Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add desktop_tauri/src/routes/trading/components/ControlBar.tsx
git commit -m "feat(trading-ui): control bar with mode toggle, start, kill, limits gear"
```

---

### Phase 7 — End-of-phase gates

- [ ] `npx tsc --noEmit` clean, `npm run build --prefix desktop_tauri` clean.
- [ ] Manually mount these two components in the stub page to visually verify (temporary — replaced in Phase 11).
- [ ] Proceed to Phase 8.

---

## Phase 8 — Picks table + bullet interactions + bet slip

This is the largest phase. Five components + integration.

### Task 8.1: PickRow component

**Files:**
- Create: `desktop_tauri/src/routes/trading/components/PickRow.tsx`

- [ ] **Step 1: Implementation**

```typescript
// desktop_tauri/src/routes/trading/components/PickRow.tsx
import { tradingActions } from "../api/actions";
import { useTradingStore } from "../store";
import { usePulseOnChange } from "../hooks/usePulseOnChange";
import type { PickRow as PickRowType } from "../api/types";

type Props = { pick: PickRowType };

function PulseCell({ value, format }: { value: number; format: (v: number) => string }) {
  const pulse = usePulseOnChange(value);
  const cls = pulse === "up" ? "trading-pulse-up" : pulse === "down" ? "trading-pulse-down" : "";
  return <span className={cls}>{format(value)}</span>;
}

const fmtPct = (v: number) => `${(v * 100).toFixed(0)}%`;
const fmtBp = (v: number) => `${v >= 0 ? "+" : ""}${v}bp`;
const fmtUsd = (v: number) => `$${v.toFixed(2)}`;
const fmtUsdSigned = (v: number) => `${v >= 0 ? "+" : ""}$${v.toFixed(2)}`;

export function PickRow({ pick }: Props) {
  const toggleExpand = useTradingStore((s) => s.toggleExpand);
  const expandedId = useTradingStore((s) => s.expandedCandidateId);

  const onBulletClick = async (e: React.MouseEvent) => {
    e.stopPropagation();
    if (pick.state === "blocked") return;
    await tradingActions.togglePick(pick.candidate_id, !pick.selected);
  };

  const onRowClick = () => toggleExpand(pick.candidate_id);

  const bulletGlyph = pick.state === "blocked" ? "⊘" : pick.selected ? "●" : "○";
  const bulletCls = `pick-bullet ${pick.state === "blocked" ? "blocked" : pick.selected ? "included" : "excluded"}`;
  const rowCls = pick.state === "blocked" ? "blocked" : pick.state === "excluded" ? "excluded" : "";

  return (
    <tr className={rowCls} onClick={onRowClick} aria-expanded={expandedId === pick.candidate_id}>
      <td onClick={onBulletClick} style={{ width: 18 }}>
        <span
          className={bulletCls}
          title={pick.state === "blocked" ? pick.blocker_reason ?? "blocked" : ""}
        >
          {bulletGlyph}
        </span>
      </td>
      <td>
        {pick.prop_label}
        {pick.game_label ? (
          <span style={{ color: "var(--trading-fg-subtle)", fontSize: 9, marginLeft: 8 }}>· {pick.game_label}</span>
        ) : null}
      </td>
      <td style={{ color: "var(--trading-accent-pnl)" }}>
        <PulseCell value={pick.hit_pct} format={fmtPct} />
      </td>
      <td style={{ color: "var(--trading-accent-picks)" }}>
        <PulseCell value={pick.edge_bps} format={fmtBp} />
      </td>
      <td>{pick.alloc > 0 ? <PulseCell value={pick.alloc} format={fmtUsd} /> : "--"}</td>
      <td style={{ color: pick.est_profit >= 0 ? "var(--trading-accent-pnl)" : "var(--trading-accent-danger)" }}>
        {pick.est_profit !== 0 ? <PulseCell value={pick.est_profit} format={fmtUsdSigned} /> : "--"}
      </td>
      <td>
        {pick.state === "blocked" ? (
          <span style={{ color: "var(--trading-accent-danger)", fontSize: 9 }}>{pick.blocker_reason}</span>
        ) : pick.state === "queued" ? (
          <span style={{ color: "var(--trading-accent-budget)" }}>queued</span>
        ) : (
          <span style={{ color: "var(--trading-fg-subtle)" }}>{pick.state}</span>
        )}
      </td>
    </tr>
  );
}
```

- [ ] **Step 2: Type check + build, commit**

```bash
cd desktop_tauri && npx tsc --noEmit && npm run build
cd ..
git add desktop_tauri/src/routes/trading/components/PickRow.tsx
git commit -m "feat(trading-ui): PickRow with interactive bullet + pulse on numerics"
```

---

### Task 8.2: PicksTable + sort + filter pills + bulk actions

**Files:**
- Create: `desktop_tauri/src/routes/trading/components/PicksTable.tsx`
- Create: `desktop_tauri/src/routes/trading/components/FilterPills.tsx`
- Create: `desktop_tauri/src/routes/trading/components/BulkActions.tsx`
- Create: `desktop_tauri/src/routes/trading/components/ThresholdsRow.tsx`

- [ ] **Step 1: FilterPills**

```typescript
// desktop_tauri/src/routes/trading/components/FilterPills.tsx
import { useTradingStore } from "../store";

const PILLS: { id: "all" | "hittable" | "excluded" | "blocked"; label: string }[] = [
  { id: "all", label: "All" },
  { id: "hittable", label: "Hittable" },
  { id: "excluded", label: "Excluded" },
  { id: "blocked", label: "Blocked" },
];

export function FilterPills() {
  const filter = useTradingStore((s) => s.filter);
  const setFilter = useTradingStore((s) => s.setFilter);
  const snapshot = useTradingStore((s) => s.snapshot);
  const counts = {
    all: snapshot?.picks.length ?? 0,
    hittable: snapshot?.picks.filter((p) => p.state !== "blocked").length ?? 0,
    excluded: snapshot?.kpis.picks.excluded ?? 0,
    blocked: snapshot?.kpis.picks.blocked ?? 0,
  };
  return (
    <div style={{ display: "flex", gap: 6, marginBottom: 10, alignItems: "center", flexWrap: "wrap" }}>
      {PILLS.map((pill) => (
        <button
          key={pill.id}
          type="button"
          className={`filter-pill ${filter === pill.id ? "active" : ""}`}
          onClick={() => setFilter(pill.id)}
        >
          {pill.label} · {counts[pill.id]}
        </button>
      ))}
    </div>
  );
}
```

- [ ] **Step 2: BulkActions**

```typescript
// desktop_tauri/src/routes/trading/components/BulkActions.tsx
import { tradingActions } from "../api/actions";

export function BulkActions() {
  return (
    <div style={{ display: "flex", gap: 6 }}>
      <button className="btn-trading ghost" onClick={() => tradingActions.bulk("select_all_hittable")}>
        Select all hittable
      </button>
      <button className="btn-trading ghost" onClick={() => tradingActions.bulk("deselect_all")}>
        Deselect all
      </button>
      <button className="btn-trading ghost" onClick={() => tradingActions.bulk("top_n", 5)}>
        Top 5
      </button>
    </div>
  );
}
```

- [ ] **Step 3: ThresholdsRow**

```typescript
// desktop_tauri/src/routes/trading/components/ThresholdsRow.tsx
import { useState } from "react";
import { useTradingStore } from "../store";
import { tradingActions } from "../api/actions";

export function ThresholdsRow() {
  const open = useTradingStore((s) => s.thresholdsOpen);
  const setOpen = useTradingStore((s) => s.setThresholdsOpen);
  const [hit, setHit] = useState(55);
  const [edge, setEdge] = useState(50);

  return (
    <div style={{ marginBottom: 8 }}>
      <button
        type="button"
        onClick={() => setOpen(!open)}
        style={{ color: "var(--trading-fg-subtle)", fontSize: 10, background: "transparent", border: "none", cursor: "pointer" }}
      >
        {open ? "▾" : "▸"} Thresholds (min hit {hit}% · min edge +{edge}bp)
      </button>
      {open ? (
        <div style={{ display: "flex", gap: 12, padding: "8px 0", alignItems: "center" }}>
          <label className="micro" style={{ display: "flex", alignItems: "center", gap: 6 }}>
            min hit %
            <input
              type="number"
              min={0}
              max={100}
              value={hit}
              onChange={(e) => setHit(Number(e.target.value))}
              style={{ width: 60, background: "var(--trading-surface)", border: "1px solid var(--trading-border-soft)", color: "var(--trading-fg)", padding: 4 }}
            />
          </label>
          <label className="micro" style={{ display: "flex", alignItems: "center", gap: 6 }}>
            min edge bps
            <input
              type="number"
              min={0}
              max={5000}
              value={edge}
              onChange={(e) => setEdge(Number(e.target.value))}
              style={{ width: 80, background: "var(--trading-surface)", border: "1px solid var(--trading-border-soft)", color: "var(--trading-fg)", padding: 4 }}
            />
          </label>
          <button
            className="btn-trading primary"
            onClick={() => tradingActions.setThresholds(hit / 100, edge)}
          >
            Apply
          </button>
        </div>
      ) : null}
    </div>
  );
}
```

- [ ] **Step 4: PicksTable**

```typescript
// desktop_tauri/src/routes/trading/components/PicksTable.tsx
import { useTradingStore, selectVisiblePicks } from "../store";
import { PickRow } from "./PickRow";
import { FilterPills } from "./FilterPills";
import { BulkActions } from "./BulkActions";
import { ThresholdsRow } from "./ThresholdsRow";

const COLUMNS: { key: "rank" | "hit_pct" | "edge_bps" | "alloc" | "est_profit"; label: string }[] = [
  { key: "rank", label: "Prop" },
  { key: "hit_pct", label: "Hit %" },
  { key: "edge_bps", label: "Edge" },
  { key: "alloc", label: "Alloc" },
  { key: "est_profit", label: "Est. Profit" },
];

function formatUsd(n: number): string {
  return `$${n.toFixed(2)}`;
}

function formatSigned(n: number): string {
  return `${n >= 0 ? "+" : ""}$${n.toFixed(2)}`;
}

export function PicksTable() {
  const setSort = useTradingStore((s) => s.setSort);
  const visible = useTradingStore(selectVisiblePicks);
  const snapshot = useTradingStore((s) => s.snapshot);

  if (!snapshot) {
    return (
      <div className="trading-tile" style={{ height: 240, marginBottom: 18 }} />
    );
  }
  const picks = snapshot.kpis.picks;
  return (
    <section style={{ marginBottom: 24 }}>
      <header style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "8px 0 12px" }}>
        <div>
          <div style={{ fontSize: 14, fontWeight: 700 }}>Picks</div>
          <div className="micro" style={{ textTransform: "none", letterSpacing: 0, fontSize: 10, marginTop: 2 }}>
            {picks.available} picks available · {picks.selected} selected · {formatUsd(snapshot.bet_slip.total_stake)} allocated · est. profit{" "}
            <span style={{ color: "var(--trading-accent-pnl)" }}>{formatSigned(picks.est_total_profit)}</span>
          </div>
        </div>
        <BulkActions />
      </header>
      <FilterPills />
      <ThresholdsRow />
      <table className="picks-table">
        <thead>
          <tr>
            <th style={{ width: 18 }} />
            {COLUMNS.map((col) => (
              <th key={col.key} onClick={() => setSort(col.key)}>
                {col.label} ↕
              </th>
            ))}
            <th>State</th>
          </tr>
        </thead>
        <tbody>
          {visible.length === 0 ? (
            <tr>
              <td colSpan={7} style={{ color: "var(--trading-fg-subtle)", textAlign: "center", padding: 16 }}>
                No picks match the current filter.
              </td>
            </tr>
          ) : (
            visible.map((pick) => <PickRow key={pick.candidate_id} pick={pick} />)
          )}
        </tbody>
      </table>
    </section>
  );
}
```

- [ ] **Step 5: Type check + build**

```bash
cd desktop_tauri && npx tsc --noEmit && npm run build
```

Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add desktop_tauri/src/routes/trading/components/FilterPills.tsx desktop_tauri/src/routes/trading/components/BulkActions.tsx desktop_tauri/src/routes/trading/components/ThresholdsRow.tsx desktop_tauri/src/routes/trading/components/PicksTable.tsx
git commit -m "feat(trading-ui): picks table with sort, filter pills, bulk actions, thresholds"
```

---

### Task 8.3: BetSlipSidebar

**Files:**
- Create: `desktop_tauri/src/routes/trading/components/BetSlipSidebar.tsx`

- [ ] **Step 1: Implementation**

```typescript
// desktop_tauri/src/routes/trading/components/BetSlipSidebar.tsx
import { useTradingStore } from "../store";
import { usePulseOnChange } from "../hooks/usePulseOnChange";

function Money({ value }: { value: number }) {
  const pulse = usePulseOnChange(value);
  const cls = pulse === "up" ? "trading-pulse-up" : pulse === "down" ? "trading-pulse-down" : "";
  return <span className={cls}>{`${value >= 0 ? "+" : ""}$${value.toFixed(2)}`}</span>;
}

export function BetSlipSidebar() {
  const slip = useTradingStore((s) => s.snapshot?.bet_slip);
  if (!slip) return null;
  if (slip.selected.length === 0) {
    return (
      <aside className="bet-slip">
        <div className="micro">Selected Picks · 0</div>
        <p style={{ marginTop: 12, color: "var(--trading-fg-subtle)", fontSize: 10 }}>
          No picks selected · click bullets in the table to include
        </p>
      </aside>
    );
  }
  return (
    <aside className="bet-slip">
      <div className="micro">Selected Picks · {slip.selected.length}</div>
      <div style={{ marginTop: 12, display: "flex", flexDirection: "column", gap: 8 }}>
        {slip.selected.map((pick) => (
          <div key={pick.candidate_id} style={{ borderTop: "1px solid var(--trading-border)", paddingTop: 8 }}>
            <div style={{ fontSize: 11, fontWeight: 600 }}>● {pick.prop_label}</div>
            <div className="mono" style={{ color: "var(--trading-fg-subtle)", fontSize: 10, marginTop: 2 }}>
              {(pick.hit_pct * 100).toFixed(0)}% · +{pick.edge_bps}bp
            </div>
            <div className="mono" style={{ fontSize: 10, marginTop: 2 }}>
              ${pick.alloc.toFixed(2)} → <span style={{ color: "var(--trading-accent-pnl)" }}>{`+$${pick.est_profit.toFixed(2)}`}</span>
            </div>
          </div>
        ))}
      </div>
      <div style={{ borderTop: "1px solid var(--trading-border)", marginTop: 12, paddingTop: 12, fontSize: 10 }} className="mono">
        <div style={{ display: "flex", justifyContent: "space-between" }}>
          <span>Total stake</span>
          <span><Money value={slip.total_stake} /> of ${slip.cap_total.toFixed(2)} cap</span>
        </div>
        <div style={{ display: "flex", justifyContent: "space-between", marginTop: 4 }}>
          <span>Est. profit</span>
          <span style={{ color: "var(--trading-accent-pnl)" }}><Money value={slip.est_total_profit} /></span>
        </div>
        <div style={{ display: "flex", justifyContent: "space-between", marginTop: 4 }}>
          <span>Unused</span>
          <span>${slip.unused_budget.toFixed(2)}</span>
        </div>
      </div>
    </aside>
  );
}
```

- [ ] **Step 2: Build, commit**

```bash
cd desktop_tauri && npx tsc --noEmit && npm run build
cd ..
git add desktop_tauri/src/routes/trading/components/BetSlipSidebar.tsx
git commit -m "feat(trading-ui): sticky bet slip sidebar with pulse-on-change totals"
```

---

### Task 8.4: PickRowExpansion (click-to-expand panel)

**Files:**
- Create: `desktop_tauri/src/routes/trading/components/PickRowExpansion.tsx`
- Modify: `desktop_tauri/src/routes/trading/components/PicksTable.tsx` — render expansion row

- [ ] **Step 1: Implementation**

```typescript
// desktop_tauri/src/routes/trading/components/PickRowExpansion.tsx
import type { PickRow } from "../api/types";

type Props = { pick: PickRow };

export function PickRowExpansion({ pick }: Props) {
  return (
    <tr>
      <td />
      <td colSpan={6} style={{ background: "var(--trading-surface-alt)", padding: 12 }}>
        <div className="mono" style={{ fontSize: 10, display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
          <div>
            <div className="micro">Kalshi quote</div>
            <div style={{ marginTop: 4 }}>
              ticker: {pick.kalshi.ticker ?? "—"}<br />
              yes bid: {pick.kalshi.yes_bid?.toFixed(3) ?? "—"}<br />
              yes ask: {pick.kalshi.yes_ask?.toFixed(3) ?? "—"}<br />
              spread: {pick.kalshi.spread?.toFixed(3) ?? "—"}<br />
              last quote: {pick.kalshi.last_quote_at ?? "—"}
            </div>
          </div>
          <div>
            <div className="micro">Model</div>
            <div style={{ marginTop: 4 }}>
              model prob: {(pick.model_prob * 100).toFixed(1)}%<br />
              market prob: {pick.market_prob !== null ? `${(pick.market_prob * 100).toFixed(1)}%` : "—"}<br />
              edge: +{pick.edge_bps}bp<br />
              rank: #{pick.rank + 1}
            </div>
          </div>
        </div>
        {pick.blocker_reason ? (
          <p style={{ marginTop: 12, color: "var(--trading-accent-danger)", fontSize: 11 }}>
            ⊘ Blocked: {pick.blocker_reason}
          </p>
        ) : null}
      </td>
    </tr>
  );
}
```

- [ ] **Step 2: Render expansion in PicksTable**

In `PicksTable.tsx`, import `PickRowExpansion` and wrap the existing `visible.map(...)` rendering:

```typescript
import { PickRowExpansion } from "./PickRowExpansion";
// ...
{visible.map((pick) => (
  <Fragment key={pick.candidate_id}>
    <PickRow pick={pick} />
    {useTradingStore.getState().expandedCandidateId === pick.candidate_id ? (
      <PickRowExpansion pick={pick} />
    ) : null}
  </Fragment>
))}
```

Import `Fragment` from React. To make the expansion reactive (not stuck reading state.getState()), subscribe:

```typescript
const expandedId = useTradingStore((s) => s.expandedCandidateId);
// then in the map:
{expandedId === pick.candidate_id ? <PickRowExpansion pick={pick} /> : null}
```

- [ ] **Step 3: Build, commit**

```bash
cd desktop_tauri && npx tsc --noEmit && npm run build
cd ..
git add desktop_tauri/src/routes/trading/components/PickRowExpansion.tsx desktop_tauri/src/routes/trading/components/PicksTable.tsx
git commit -m "feat(trading-ui): click-to-expand pick row with Kalshi quote + model details"
```

---

### Phase 8 — End-of-phase gates

- [ ] `npx tsc --noEmit` clean, `npm run build --prefix desktop_tauri` clean
- [ ] Dispatch `typescript-reviewer` on Phase 7+8 diff. Focus: re-render cost (per-cell selectors), event handler correctness on bullet clicks, accessibility (keyboard nav, aria roles on table).
- [ ] Address findings, proceed to Phase 9.

---

## Phase 9 — Below-picks sections + event log strip

### Task 9.1: PositionsTable + FillsFeed + PnlTrendChart

**Files:**
- Create: `desktop_tauri/src/routes/trading/components/PositionsTable.tsx`
- Create: `desktop_tauri/src/routes/trading/components/FillsFeed.tsx`
- Create: `desktop_tauri/src/routes/trading/components/PnlTrendChart.tsx`

- [ ] **Step 1: PositionsTable**

```typescript
// desktop_tauri/src/routes/trading/components/PositionsTable.tsx
import { useTradingStore } from "../store";

export function PositionsTable() {
  const positions = useTradingStore((s) => s.snapshot?.positions ?? []) as Array<Record<string, unknown>>;
  if (positions.length === 0) {
    return (
      <section style={{ marginTop: 14 }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "8px 0 12px" }}>
          <div style={{ fontSize: 13, fontWeight: 700 }}>Open Positions <span style={{ color: "var(--trading-fg-subtle)", fontSize: 10, fontWeight: 400 }}>· 0</span></div>
        </div>
        <p className="micro" style={{ textTransform: "none", letterSpacing: 0 }}>No open positions.</p>
      </section>
    );
  }
  return (
    <section style={{ marginTop: 14 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "8px 0 12px" }}>
        <div style={{ fontSize: 13, fontWeight: 700 }}>
          Open Positions <span style={{ color: "var(--trading-fg-subtle)", fontSize: 10, fontWeight: 400 }}>· {positions.length}</span>
        </div>
      </div>
      <div className="mono" style={{ background: "var(--trading-surface)", borderRadius: 6, padding: 10, fontSize: 10, color: "var(--trading-fg-muted)" }}>
        {positions.map((p, idx) => (
          <div key={idx} style={{ padding: "4px 0", borderBottom: "1px solid var(--trading-border)" }}>
            {String(p.market_symbol ?? p.ticker ?? "—")} · {String(p.side ?? "—")} · {String(p.contract_count ?? "—")}
          </div>
        ))}
      </div>
    </section>
  );
}
```

- [ ] **Step 2: FillsFeed**

```typescript
// desktop_tauri/src/routes/trading/components/FillsFeed.tsx
import { useEffect, useRef, useState } from "react";
import { useTradingStore } from "../store";

type FillLike = {
  fill_id?: string;
  market?: { symbol?: string };
  side?: string;
  price?: number;
  stake?: number;
  realized_pnl?: number;
  timestamp?: string;
};

export function FillsFeed() {
  const fills = useTradingStore((s) => s.snapshot?.fills ?? []) as FillLike[];
  const previousIds = useRef<Set<string>>(new Set());
  const [flashIds, setFlashIds] = useState<Set<string>>(new Set());

  useEffect(() => {
    const current = new Set(fills.map((f) => f.fill_id ?? "").filter(Boolean));
    const novel = new Set<string>();
    for (const id of current) {
      if (!previousIds.current.has(id)) novel.add(id);
    }
    if (novel.size > 0 && previousIds.current.size > 0) {
      setFlashIds(novel);
      const timer = setTimeout(() => setFlashIds(new Set()), 220);
      previousIds.current = current;
      return () => clearTimeout(timer);
    }
    previousIds.current = current;
  }, [fills]);

  return (
    <section style={{ marginTop: 14 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "8px 0 12px" }}>
        <div style={{ fontSize: 13, fontWeight: 700 }}>
          Recent Fills <span style={{ color: "var(--trading-fg-subtle)", fontSize: 10, fontWeight: 400 }}>· {fills.length}</span>
        </div>
        <span className="micro" style={{ textTransform: "none", letterSpacing: 0 }}>live</span>
      </div>
      {fills.length === 0 ? (
        <p className="micro" style={{ textTransform: "none", letterSpacing: 0 }}>No fills recorded yet.</p>
      ) : (
        <div className="mono" style={{ background: "var(--trading-surface)", borderRadius: 6, padding: 10, fontSize: 10 }}>
          {fills.map((fill) => (
            <div
              key={fill.fill_id}
              className={flashIds.has(fill.fill_id ?? "") ? "trading-pulse-up" : ""}
              style={{ padding: "4px 0", borderBottom: "1px solid var(--trading-border)", display: "flex", justifyContent: "space-between" }}
            >
              <span>
                <span style={{ color: "var(--trading-fg)" }}>{fill.market?.symbol ?? "—"}</span> · {fill.side ?? "—"} @ {fill.price?.toFixed(3) ?? "—"} · stake ${fill.stake?.toFixed(2) ?? "—"}
              </span>
              <span style={{ color: (fill.realized_pnl ?? 0) >= 0 ? "var(--trading-accent-pnl)" : "var(--trading-accent-danger)" }}>
                {fill.realized_pnl !== undefined ? `${fill.realized_pnl >= 0 ? "+" : ""}$${fill.realized_pnl.toFixed(2)}` : "—"}
              </span>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}
```

- [ ] **Step 3: PnlTrendChart**

```typescript
// desktop_tauri/src/routes/trading/components/PnlTrendChart.tsx
import { Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { useTradingStore } from "../store";

export function PnlTrendChart() {
  const data = useTradingStore((s) => s.snapshot?.pnl_trend ?? []);
  return (
    <section style={{ marginTop: 14 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "8px 0 12px" }}>
        <div style={{ fontSize: 13, fontWeight: 700 }}>P&L Trend</div>
        <span className="micro" style={{ textTransform: "none", letterSpacing: 0 }}>today</span>
      </div>
      <div style={{ background: "var(--trading-surface)", borderRadius: 6, padding: 12, height: 80 }}>
        {data.length === 0 ? (
          <p className="micro" style={{ textTransform: "none", letterSpacing: 0, color: "var(--trading-fg-subtle)" }}>
            No fills yet — chart populates as bets settle.
          </p>
        ) : (
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={data}>
              <XAxis dataKey="index" hide />
              <YAxis hide domain={["auto", "auto"]} />
              <Tooltip
                formatter={(value: number) => [`$${value.toFixed(2)}`, "Cum P&L"]}
                labelFormatter={(label) => `Fill #${label}`}
                contentStyle={{ background: "var(--trading-surface)", border: "1px solid var(--trading-border)" }}
              />
              <Line type="monotone" dataKey="pnl" stroke="var(--trading-accent-pnl)" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        )}
      </div>
    </section>
  );
}
```

- [ ] **Step 4: Build, commit**

```bash
cd desktop_tauri && npx tsc --noEmit && npm run build
cd ..
git add desktop_tauri/src/routes/trading/components/PositionsTable.tsx desktop_tauri/src/routes/trading/components/FillsFeed.tsx desktop_tauri/src/routes/trading/components/PnlTrendChart.tsx
git commit -m "feat(trading-ui): positions table, fills feed (with flash), P&L sparkline"
```

---

### Task 9.2: CollapsedSection + EventLogStrip

**Files:**
- Create: `desktop_tauri/src/routes/trading/components/CollapsedSection.tsx`
- Create: `desktop_tauri/src/routes/trading/components/EventLogStrip.tsx`

- [ ] **Step 1: CollapsedSection**

```typescript
// desktop_tauri/src/routes/trading/components/CollapsedSection.tsx
import { useState } from "react";
import type { ReactNode } from "react";

type Props = {
  title: string;
  count: number;
  hideWhenEmpty?: boolean;
  rightLabel?: string;
  children: ReactNode;
};

export function CollapsedSection({ title, count, hideWhenEmpty, rightLabel, children }: Props) {
  const [open, setOpen] = useState(false);
  if (hideWhenEmpty && count === 0) return null;
  return (
    <div style={{ background: "var(--trading-surface-alt)", border: "1px solid var(--trading-border)", borderRadius: 4, padding: "8px 12px", marginBottom: 6 }}>
      <button
        type="button"
        onClick={() => setOpen(!open)}
        style={{ display: "flex", justifyContent: "space-between", alignItems: "center", width: "100%", background: "transparent", border: "none", color: "var(--trading-fg-muted)", fontSize: 10, fontFamily: "inherit", cursor: "pointer", padding: 0 }}
      >
        <span>
          {open ? "▾" : "▸"} {title} <span style={{ opacity: 0.5 }}>· {count}</span>
        </span>
        <span style={{ opacity: 0.5 }}>{rightLabel ?? (open ? "collapse" : "expand")}</span>
      </button>
      {open ? <div style={{ marginTop: 8 }}>{children}</div> : null}
    </div>
  );
}
```

- [ ] **Step 2: EventLogStrip**

```typescript
// desktop_tauri/src/routes/trading/components/EventLogStrip.tsx
import { useEffect, useRef } from "react";
import { useTradingStore, selectEventLog } from "../store";

export function EventLogStrip() {
  const log = useTradingStore(selectEventLog);
  const containerRef = useRef<HTMLDivElement>(null);
  const userScrolledRef = useRef(false);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    if (!userScrolledRef.current) {
      el.scrollTop = 0; // newest at top
    }
  }, [log.length]);

  return (
    <section style={{ marginTop: 18 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "8px 0 12px" }}>
        <div style={{ fontSize: 13, fontWeight: 700 }}>Event Log</div>
        <span className="micro" style={{ textTransform: "none", letterSpacing: 0 }}>errors · warnings · info</span>
      </div>
      <div
        ref={containerRef}
        className="event-log"
        onScroll={() => {
          const el = containerRef.current;
          if (!el) return;
          userScrolledRef.current = el.scrollTop > 16;
        }}
      >
        {[...log].reverse().map((line) => (
          <div key={line.cursor} className={`log-${line.level}`}>
            [{line.timestamp.slice(11, 19)}] {line.level}  {line.message}
          </div>
        ))}
      </div>
    </section>
  );
}
```

- [ ] **Step 3: Build, commit**

```bash
cd desktop_tauri && npx tsc --noEmit && npm run build
cd ..
git add desktop_tauri/src/routes/trading/components/CollapsedSection.tsx desktop_tauri/src/routes/trading/components/EventLogStrip.tsx
git commit -m "feat(trading-ui): collapsed-section pattern + terminal-style event log strip"
```

---

### Phase 9 — End-of-phase gates

- [ ] Build + type check clean
- [ ] `typescript-reviewer` on the Phase 9 diff
- [ ] Proceed to Phase 10

---

## Phase 10 — Limits modal + diagnostics

### Task 10.1: LimitsModal component

**Files:**
- Create: `desktop_tauri/src/routes/trading/components/LimitsModal.tsx`

- [ ] **Step 1: Implementation**

```typescript
// desktop_tauri/src/routes/trading/components/LimitsModal.tsx
import * as Dialog from "@radix-ui/react-dialog";
import { useEffect, useState } from "react";
import { useTradingStore } from "../store";
import { tradingActions } from "../api/actions";

type LimitsForm = {
  max_open_notional: string;
  daily_loss_cap: string;
  reject_cooldown_seconds: string;
  per_order_cap_override: string;
};

const EMPTY: LimitsForm = {
  max_open_notional: "",
  daily_loss_cap: "",
  reject_cooldown_seconds: "",
  per_order_cap_override: "",
};

export function LimitsModal() {
  const open = useTradingStore((s) => s.limitsModalOpen);
  const setOpen = useTradingStore((s) => s.setLimitsModalOpen);
  const [form, setForm] = useState<LimitsForm>(EMPTY);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (open) {
      void tradingActions.readLimits().then((data) => {
        setForm({
          max_open_notional: String(data.max_open_notional ?? ""),
          daily_loss_cap: String(data.daily_loss_cap ?? ""),
          reject_cooldown_seconds: String(data.reject_cooldown_seconds ?? ""),
          per_order_cap_override: data.per_order_cap_override !== null && data.per_order_cap_override !== undefined ? String(data.per_order_cap_override) : "",
        });
      });
    } else {
      setForm(EMPTY);
      setError(null);
    }
  }, [open]);

  const refreshWallet = async () => {
    try {
      const { balance } = await tradingActions.fetchWallet();
      setForm((f) => ({ ...f, max_open_notional: String(balance) }));
    } catch (err) {
      setError(err instanceof Error ? err.message : "wallet fetch failed");
    }
  };

  const save = async () => {
    setSaving(true);
    setError(null);
    try {
      await tradingActions.updateLimits({
        max_open_notional: form.max_open_notional ? Number(form.max_open_notional) : undefined,
        daily_loss_cap: form.daily_loss_cap ? Number(form.daily_loss_cap) : undefined,
        reject_cooldown_seconds: form.reject_cooldown_seconds ? Number(form.reject_cooldown_seconds) : undefined,
        per_order_cap_override: form.per_order_cap_override ? Number(form.per_order_cap_override) : undefined,
      });
      setOpen(false);
    } catch (err) {
      setError(err instanceof Error ? err.message : "save failed");
    } finally {
      setSaving(false);
    }
  };

  const inputStyle = {
    background: "var(--trading-surface-alt)",
    border: "1px solid var(--trading-border-soft)",
    color: "var(--trading-fg)",
    padding: "6px 8px",
    fontFamily: "var(--trading-font-mono)",
    fontSize: 12,
    width: "100%",
  };
  const labelStyle = { color: "var(--trading-fg-muted)", fontSize: 10, letterSpacing: 1.5, textTransform: "uppercase" as const };

  return (
    <Dialog.Root open={open} onOpenChange={setOpen}>
      <Dialog.Portal>
        <Dialog.Overlay style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.6)" }} />
        <Dialog.Content
          style={{
            position: "fixed",
            top: "50%",
            left: "50%",
            transform: "translate(-50%, -50%)",
            background: "var(--trading-surface)",
            color: "var(--trading-fg)",
            border: "1px solid var(--trading-border)",
            padding: 20,
            borderRadius: 8,
            minWidth: 360,
          }}
        >
          <Dialog.Title style={{ fontSize: 14, fontWeight: 700, marginBottom: 12 }}>Trading Limits</Dialog.Title>
          <div style={{ display: "grid", gap: 12 }}>
            <label style={{ display: "grid", gap: 4 }}>
              <span style={labelStyle}>Max open notional ($)</span>
              <input style={inputStyle} value={form.max_open_notional} onChange={(e) => setForm({ ...form, max_open_notional: e.target.value })} />
            </label>
            <label style={{ display: "grid", gap: 4 }}>
              <span style={labelStyle}>Daily loss cap ($)</span>
              <input style={inputStyle} value={form.daily_loss_cap} onChange={(e) => setForm({ ...form, daily_loss_cap: e.target.value })} />
            </label>
            <label style={{ display: "grid", gap: 4 }}>
              <span style={labelStyle}>Reject cooldown (seconds)</span>
              <input style={inputStyle} value={form.reject_cooldown_seconds} onChange={(e) => setForm({ ...form, reject_cooldown_seconds: e.target.value })} />
            </label>
            <label style={{ display: "grid", gap: 4 }}>
              <span style={labelStyle}>Per-order cap override ($, empty = 35% rule)</span>
              <input style={inputStyle} value={form.per_order_cap_override} onChange={(e) => setForm({ ...form, per_order_cap_override: e.target.value })} />
            </label>
          </div>
          {error ? <p style={{ color: "var(--trading-accent-danger)", marginTop: 12, fontSize: 11 }}>{error}</p> : null}
          <div style={{ display: "flex", justifyContent: "space-between", marginTop: 18 }}>
            <button className="btn-trading ghost" onClick={refreshWallet}>Refresh wallet balance</button>
            <div style={{ display: "flex", gap: 6 }}>
              <button className="btn-trading ghost" onClick={() => setOpen(false)}>Cancel</button>
              <button className="btn-trading primary" disabled={saving} onClick={save}>
                {saving ? "Saving..." : "Save"}
              </button>
            </div>
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
```

- [ ] **Step 2: Build, commit**

```bash
cd desktop_tauri && npx tsc --noEmit && npm run build
cd ..
git add desktop_tauri/src/routes/trading/components/LimitsModal.tsx
git commit -m "feat(trading-ui): Limits modal with wallet refresh + all-fields form"
```

---

### Phase 10 — End-of-phase gates

- [ ] Build + type check clean
- [ ] `typescript-reviewer` on Phase 10 diff
- [ ] Proceed to Phase 11

---

## Phase 11 — Wire-up + replace existing trading.tsx

### Task 11.1: Compose the full page

**Files:**
- Modify: `desktop_tauri/src/routes/trading/index.tsx` — full composition

- [ ] **Step 1: Replace the stub**

```typescript
// desktop_tauri/src/routes/trading/index.tsx
import { createRoute } from "@tanstack/react-router";
import { Route as rootRoute } from "../__root";
import "../../styles/trading.css";

import { useTradingStream } from "./hooks/useTradingStream";
import { KpiTileStrip } from "./components/KpiTileStrip";
import { ControlBar } from "./components/ControlBar";
import { PicksTable } from "./components/PicksTable";
import { BetSlipSidebar } from "./components/BetSlipSidebar";
import { PositionsTable } from "./components/PositionsTable";
import { FillsFeed } from "./components/FillsFeed";
import { PnlTrendChart } from "./components/PnlTrendChart";
import { CollapsedSection } from "./components/CollapsedSection";
import { EventLogStrip } from "./components/EventLogStrip";
import { LimitsModal } from "./components/LimitsModal";
import { useTradingStore } from "./store";

export const Route = createRoute({
  getParentRoute: () => rootRoute,
  path: "/trading",
  component: TradingPageV2,
});

function TradingPageV2() {
  useTradingStream();
  const snapshot = useTradingStore((s) => s.snapshot);
  const streamConnected = useTradingStore((s) => s.streamConnected);

  return (
    <div className="trading-page-v2">
      <KpiTileStrip />
      <ControlBar />

      <div style={{ display: "grid", gridTemplateColumns: "70% 30%", gap: 16, marginBottom: 18 }}>
        <PicksTable />
        <BetSlipSidebar />
      </div>

      <PositionsTable />
      <FillsFeed />
      <PnlTrendChart />

      <div style={{ marginTop: 14, display: "flex", flexDirection: "column", gap: 6 }}>
        <CollapsedSection title="Resting Orders" count={snapshot?.resting_orders.length ?? 0} hideWhenEmpty>
          <pre className="mono" style={{ fontSize: 10, color: "var(--trading-fg-muted)" }}>
            {JSON.stringify(snapshot?.resting_orders, null, 2)}
          </pre>
        </CollapsedSection>
        <CollapsedSection title="Live Kalshi Quotes" count={snapshot?.quotes.length ?? 0}>
          <pre className="mono" style={{ fontSize: 10, color: "var(--trading-fg-muted)" }}>
            {JSON.stringify(snapshot?.quotes, null, 2)}
          </pre>
        </CollapsedSection>
        <CollapsedSection title="System Diagnostics" count={snapshot?.diagnostics ? 1 : 0}>
          <pre className="mono" style={{ fontSize: 10, color: "var(--trading-fg-muted)" }}>
            {JSON.stringify(snapshot?.diagnostics, null, 2)}
          </pre>
        </CollapsedSection>
      </div>

      <EventLogStrip />
      <LimitsModal />

      {!streamConnected ? (
        <div style={{ position: "fixed", bottom: 12, right: 12, padding: "4px 10px", background: "var(--trading-surface)", border: "1px solid var(--trading-accent-system)", borderRadius: 12, fontSize: 10, color: "var(--trading-accent-system)" }}>
          ● disconnected — retrying
        </div>
      ) : null}
    </div>
  );
}
```

- [ ] **Step 2: Build + manual smoke**

```bash
cd desktop_tauri && npx tsc --noEmit && npm run build
```

Then run the app via `npm run tauri:dev --prefix desktop_tauri`. Verify:
- Page renders dark terminal background
- KPI tiles populate with real data within ~2s
- Inline budget edit works
- Bullet toggles trigger immediate sidebar update
- Filter pills filter the table
- Top 5 button works
- Event log shows recent activity

- [ ] **Step 3: Commit**

```bash
git add desktop_tauri/src/routes/trading/index.tsx
git commit -m "feat(trading-ui): compose full trading page from new components"
```

---

### Task 11.2: Remove dead code

**Files:**
- Modify: `desktop_tauri/src/routes/trading.tsx` — confirm it only re-exports
- Delete: any remaining unused exports from the old `theme.css` block for `.trading-page`, `.trading-readiness`, etc. — only if `grep -r "trading-readiness" desktop_tauri/src/` returns zero matches

- [ ] **Step 1: Confirm re-export shim**

Open `desktop_tauri/src/routes/trading.tsx` and verify it only contains the re-export line from Phase 5. If anything else is there, replace with:

```typescript
export { Route } from "./trading/index";
```

- [ ] **Step 2: Search for dead old-component imports**

Run:

```bash
cd desktop_tauri && grep -rn "DecisionBrainPanel\|ReadinessPanel\|TradingLoopPanel\|useStartupSnapshot.*trading" src/
```

Expected: zero matches. If any, follow the trail and remove them.

- [ ] **Step 3: Search for orphan CSS**

Run:

```bash
cd desktop_tauri && grep -rn "trading-readiness\|trading-card\|trading-positions" src/
```

Any matches in `theme.css` that have no consumer in `src/` should be deleted (carefully — some may be shared with other tabs).

- [ ] **Step 4: Type check, build, commit**

```bash
cd desktop_tauri && npx tsc --noEmit && npm run build
cd ..
git add -A
git commit -m "chore(trading-ui): remove dead Decision/Readiness/Loop panel references"
```

---

### Phase 11 — End-of-phase gates

- [ ] Full backend test suite: `pytest -v`
- [ ] Frontend: `npx tsc --noEmit && npm run build --prefix desktop_tauri`
- [ ] Manually exercise: mode toggle, inline budget edit, bullet click, bulk action, threshold apply, limits modal save, kill switch, start loop (without confirming order).
- [ ] `code-reviewer` (Opus 4.7) holistic review of the entire feature.
- [ ] Address findings. Proceed to Phase 12.

---

## Phase 12 — E2E smoke, brain template, cleanup

### Task 12.1: Brain template note

**Files:**
- Create: `E:/AI Brain/ClaudeBrain/04 Workflow and Systems/Project Templates/terminal-design-system.md`

- [ ] **Step 1: Write the brain template**

```markdown
---
name: Terminal Design System
description: Dark data-terminal aesthetic + live SSE + pick curation patterns for the NBA Prop Engine
type: project
---

# Terminal Design System

Captured from the 2026-05-11 trading tab redesign. Use this template when porting
the same aesthetic to other tabs (homepage, insights, parlays, players, settings).

## Visual tokens (CSS)

Located in `desktop_tauri/src/theme.css`:

- `--trading-bg`, `--trading-surface`, `--trading-surface-alt`
- `--trading-border`, `--trading-border-soft`
- `--trading-fg`, `--trading-fg-muted`, `--trading-fg-subtle`
- Accents: `--trading-accent-pnl`, `--trading-accent-budget`, `--trading-accent-picks`, `--trading-accent-system`, `--trading-accent-danger`
- Typography: `--trading-font-mono`
- Spacing: `--trading-pad-sm`, `--trading-pad-md`, `--trading-pad-lg`
- Pulse: `--trading-pulse-positive`, `--trading-pulse-negative`

## Layout primitives

1. **KPI tile strip** — 4 equal-width tiles, accent-bordered left, 12-14px padding, mini progress bars optional
2. **Control bar** — single horizontal strip, mode toggle left, primary actions right
3. **Sortable single-line table** — monospace numerics, hover-highlight rows, color-coded states
4. **Sticky right sidebar** — 30% width, sticks to viewport top
5. **Collapsible sections** — hide rare data behind one-line headers
6. **Bottom event log strip** — black background, color-coded info/warn/error lines

## Live data pattern

- **One SSE stream per page** at `/api/<area>/stream`
- Backend builds full snapshot, emits via `sse_starlette.EventSourceResponse`
- Frontend subscribes via `useEventSource` hook, feeds Zustand store
- **Per-cell reactivity**: components subscribe to slices, not the whole store
- **No skeletons after first paint** — last-known values stay on disconnect
- **Pulse on change**: `usePulseOnChange(value)` hook for numeric flash

## Allocation algorithm

For budget-splitting workflows:
- Proportional to a weight (e.g. `model_prob`)
- Soft cap per item (35% default)
- Cap-and-redistribute over `max_iterations`
- Total may be < budget when caps bind (safety property)
- Shared module: `app/trading/allocation.py`

## Subagent execution conventions

- **Implementation**: `general-purpose` on Sonnet 4.6
- **Review (Python)**: `python-reviewer` on Sonnet 4.6
- **Review (TypeScript)**: `typescript-reviewer` on Sonnet 4.6
- **Security review**: `security-reviewer` on Sonnet 4.6 (anything touching credentials, money, or input validation)
- **Build resolution**: `build-error-resolver` on Haiku 4.5
- **End-of-phase holistic**: `code-reviewer` on Opus 4.7

## Gates each phase must pass

1. `pytest` on touched modules
2. `ruff check .` clean
3. `mypy app` clean
4. `npx tsc --noEmit` clean (frontend phases)
5. `npm run build --prefix desktop_tauri` clean (frontend phases)
6. Reviewer agents report no CRITICAL/HIGH

## Decision criteria for "is this tab a candidate?"

- Has at least one live data source (prices, scores, news)
- Has at least one user action (toggle, edit, start/stop)
- Currently uses `useQuery({ refetchInterval })` that causes loading flashes
- User would benefit from dense data display

Tabs that fit: trading (done), homepage/board, insights, parlays.
Tabs that don't: settings, players (mostly static profiles).
```

- [ ] **Step 2: Save the file**

The brain directory should be auto-created if the parent exists. If not:

```bash
mkdir -p "E:/AI Brain/ClaudeBrain/04 Workflow and Systems/Project Templates/"
```

Then write the file using the Write tool with the path above.

- [ ] **Step 3: Add a pointer to MEMORY.md (optional)**

If the user's brain has a `MEMORY.md` for project templates, append a one-line pointer there. Otherwise skip.

---

### Task 12.2: E2E smoke checklist

Run through this manually with the app launched via `npm run tauri:dev --prefix desktop_tauri`:

- [ ] App opens, trading tab loads dark terminal background
- [ ] KPI tiles populate within 3s of opening the tab
- [ ] P&L value changes (or stays at 0) without flashing skeleton on refresh
- [ ] Budget tile shows wallet-initialized value (or 0 if Kalshi creds missing)
- [ ] Click "edit" on Budget tile → input appears → type 5.00 → Enter → tile updates, no page reload
- [ ] Picks list shows N rows with ●/○/⊘ bullets
- [ ] Click a ● bullet → flips to ○ within 200ms → bet slip sidebar updates total
- [ ] Click "Top 5" → 5 highest-edge bullets become ● → bet slip reflects
- [ ] Click "All" → "Hittable" → "Blocked" filter pills → table filters live
- [ ] Click a prop row → expansion panel appears below with Kalshi quote + model details
- [ ] Click ⚙ Limits → modal opens → values pre-fill → change daily_loss_cap → Save → modal closes → KPI tile reflects
- [ ] Click ● Live button → brain syncs → System tile shows "Live mode"
- [ ] Click ▶ Start Auto-Bet → loop start fires (preflight may abort if KALSHI_LIVE_TRADING not set — acceptable, just verify the request goes out)
- [ ] Click ⏻ Kill Switch → confirmation pattern → state updates
- [ ] Disconnect Kalshi (kill WS service via env) → disconnect chip appears → numbers stay on screen
- [ ] Trigger a real Kalshi price tick → relevant cells pulse without anything else re-rendering visibly
- [ ] Event log strip shows recent activity, newest at top

If anything fails, file as a follow-up issue rather than blocking the phase.

---

### Task 12.3: Dead code sweep

- [ ] **Step 1: Verify no orphan imports**

```bash
cd desktop_tauri && npx tsc --noEmit
cd .. && ruff check . && mypy app
```

- [ ] **Step 2: Run `refactor-cleaner` subagent if available**

Dispatch with prompt:

> Scan the trading-tab redesign diff (Phases 5-11) for dead code: unused exports, unreachable conditions, components imported but never rendered, CSS rules with no consumer. Report a list — do not delete anything; the user will confirm each. Skip changes outside `desktop_tauri/src/routes/trading/` and `desktop_tauri/src/styles/trading.css`.

- [ ] **Step 3: Apply suggested deletions after user confirmation, commit**

```bash
git add -A
git commit -m "chore(trading-ui): remove dead code identified by refactor-cleaner sweep"
```

---

### Phase 12 — End-of-phase gates

- [ ] All E2E checklist items pass or are filed as separate issues
- [ ] Brain template note saved
- [ ] Dead code sweep clean
- [ ] `code-reviewer` (Opus 4.7) final holistic review across the full feature
- [ ] Update `docs/superpowers/specs/2026-05-11-trading-tab-redesign-design.md` status from `draft` to `implemented`

---

## Cross-phase notes

### Token budget per subagent spawn

Each implementer spawn should target ~30k tokens of output max. If a task feels too large to fit in a single spawn, split it at a natural file boundary. Don't try to dispatch "implement Phase 8" as one prompt — dispatch Task 8.1, then 8.2, etc.

### Self-contained subagent prompts

Each subagent prompt must include:
1. The task ID (e.g. "Task 8.2")
2. The exact file paths to create/modify
3. The full code blocks from this plan
4. The expected commands and pass criteria
5. The spec reference for context: "Spec at docs/superpowers/specs/2026-05-11-trading-tab-redesign-design.md, particularly §<relevant section>"
6. The commit message for that task

### When gates fail

If `pytest`/`mypy`/`ruff`/`tsc` fails:
1. First-pass: dispatch `build-error-resolver` (Haiku 4.5) with the error output
2. If that doesn't fix, escalate to the original implementer (Sonnet) with the error context
3. Never `--no-verify` past a failing gate
4. Never delete a failing test without first proving the test is wrong

### Inter-phase dependencies

```
Phase 1 ─→ Phase 2 ─→ Phase 3 ─→ Phase 4 (backend complete)
                                    │
                                    ▼
                                 Phase 5 ─→ Phase 6 ─→ Phase 7 ─→ Phase 8
                                                                    │
                                                                    ▼
                                                                 Phase 9 ─→ Phase 10 ─→ Phase 11 ─→ Phase 12
```

Phases 1-4 can theoretically run partially in parallel (allocation helper doesn't depend on snapshot builder for instance), but for simplicity and safety the subagent runner should do them sequentially.

### Rollback path

If any phase introduces a regression that survives review, the rollback is:

```bash
git revert <commit-range>
```

The new components are isolated under `desktop_tauri/src/routes/trading/` — the old `trading.tsx` re-export means a single revert of Phase 11 step 1 returns the app to the previous component set.

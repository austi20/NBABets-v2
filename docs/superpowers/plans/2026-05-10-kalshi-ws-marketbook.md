# Kalshi WebSocket Consumer + MarketBook Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up a live, in-memory Kalshi market data feed inside the FastAPI sidecar — no trading changes — so later sub-projects can read live quotes from `MarketBook` instead of polling REST. Off by default behind `kalshi_ws_enabled=False`.

**Architecture:** Three new modules in `app/trading/`: a pure-logic frame parser, an async-safe `MarketBook` store with pub/sub fan-out, and a `KalshiWebSocketConsumer` that connects, signs the handshake with the existing RSA-PSS helper, subscribes to `ticker` + `orderbook_delta` channels, and pumps frames into the book. A `KalshiMarketService` lifecycle wrapper is started/stopped by a new FastAPI `lifespan` context manager.

**Tech Stack:** Python 3.12, asyncio, `websockets` library, `pydantic-settings`, FastAPI lifespan, pytest with `pytest-asyncio`. Reuses existing `app/providers/exchanges/kalshi_signing.sign_request`.

**Spec:** `docs/superpowers/specs/2026-05-10-kalshi-autotrader-design.md` §5.

---

## File Map

**Create:**
- `app/trading/ws_frames.py` — Pure parser functions for Kalshi WS message types. No IO. ~80 lines.
- `app/trading/market_book.py` — `MarketEntry`, `BookUpdate`, `MarketBook` (in-memory store with pub/sub). ~180 lines.
- `app/trading/ws_consumer.py` — `KalshiWebSocketConsumer` (connection lifecycle, auth, subscribe, receive loop, reconnect, ping/pong, auth-failure threshold). ~260 lines.
- `app/trading/ws_service.py` — `KalshiMarketService` lifecycle wrapper read from `config/kalshi_symbols.json`. ~120 lines.
- `tests/unit/test_market_book.py` — async unit tests for the book.
- `tests/unit/test_ws_frames.py` — pure parser tests.
- `tests/integration/test_ws_consumer.py` — consumer against in-process fake WS server.
- `tests/integration/test_ws_service.py` — service lifecycle tests.
- `tests/integration/test_ws_lifespan.py` — FastAPI lifespan integration.

**Modify:**
- `pyproject.toml` — add `websockets>=12.0` dependency.
- `app/config/settings.py` — add 5 settings fields (see Task 2).
- `app/server/main.py` — add `lifespan` async context manager wired into `create_app`.

**Untouched (verified non-goals from spec §5.2):** `TradingLoop`, `KalshiAdapter`, `ExposureRiskEngine`, `SqlPortfolioLedger`, any existing `/api/trading/*` route, any frontend file.

---

## Task 1: Add `websockets` dependency

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Inspect current dependency block**

Run: `grep -n "httpx" pyproject.toml`
Expected: line showing `"httpx>=0.28.0",` inside the `dependencies = [...]` array.

- [ ] **Step 2: Add `websockets` next to `httpx`**

Open `pyproject.toml`. In the `dependencies` array, add the line directly after the `httpx` entry:

```toml
  "websockets>=12.0",
```

- [ ] **Step 3: Install into the local venv**

Run: `.venv\Scripts\python -m pip install -e .[dev]`
Expected: `Successfully installed websockets-...` (no errors).

- [ ] **Step 4: Verify import works**

Run: `.venv\Scripts\python -c "import websockets; import websockets.asyncio.client; print(websockets.__version__)"`
Expected: prints a version `>=12.0`.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml
git commit -m "chore(deps): add websockets for Kalshi WS consumer"
```

---

## Task 2: Add settings fields

**Files:**
- Modify: `app/config/settings.py`
- Test: `tests/unit/test_settings_ws.py`

- [ ] **Step 1: Find the settings class and locate Kalshi-related fields**

Run: `grep -n "kalshi" app/config/settings.py`
Expected: lines showing existing `kalshi_api_key_id`, `kalshi_private_key_path`, `kalshi_base_url`, `kalshi_market_data_base_url`, `kalshi_live_trading` settings. Pick the line right after `kalshi_market_data_base_url` as the insertion point.

- [ ] **Step 2: Write a failing test**

Create `tests/unit/test_settings_ws.py`:

```python
import importlib

import pytest


@pytest.fixture
def reload_settings(monkeypatch):
    def _reload():
        import app.config.settings as settings_mod
        importlib.reload(settings_mod)
        return settings_mod.get_settings()
    return _reload


def test_ws_settings_have_expected_defaults(monkeypatch, reload_settings):
    for var in (
        "KALSHI_WS_ENABLED",
        "KALSHI_WS_BASE_URL",
        "KALSHI_WS_MAX_BACKOFF_SECONDS",
        "KALSHI_WS_PING_INTERVAL_SECONDS",
        "KALSHI_WS_MAX_CONSECUTIVE_AUTH_FAILURES",
    ):
        monkeypatch.delenv(var, raising=False)
    settings = reload_settings()
    assert settings.kalshi_ws_enabled is False
    assert settings.kalshi_ws_base_url == "wss://api.elections.kalshi.com/trade-api/ws/v2"
    assert settings.kalshi_ws_max_backoff_seconds == 30
    assert settings.kalshi_ws_ping_interval_seconds == 10
    assert settings.kalshi_ws_max_consecutive_auth_failures == 5


def test_ws_enabled_overridable_via_env(monkeypatch, reload_settings):
    monkeypatch.setenv("KALSHI_WS_ENABLED", "true")
    monkeypatch.setenv("KALSHI_WS_BASE_URL", "wss://demo-api.kalshi.co/trade-api/ws/v2")
    settings = reload_settings()
    assert settings.kalshi_ws_enabled is True
    assert settings.kalshi_ws_base_url == "wss://demo-api.kalshi.co/trade-api/ws/v2"
```

- [ ] **Step 3: Run test to verify it fails**

Run: `.venv\Scripts\python -m pytest tests/unit/test_settings_ws.py -v`
Expected: FAIL with `AttributeError: ... has no attribute 'kalshi_ws_enabled'`.

- [ ] **Step 4: Add the fields to `app/config/settings.py`**

Locate the Kalshi block (after `kalshi_market_data_base_url`). Insert:

```python
    kalshi_ws_enabled: bool = False
    kalshi_ws_base_url: str = "wss://api.elections.kalshi.com/trade-api/ws/v2"
    kalshi_ws_max_backoff_seconds: int = 30
    kalshi_ws_ping_interval_seconds: int = 10
    kalshi_ws_max_consecutive_auth_failures: int = 5
```

- [ ] **Step 5: Run tests, verify pass**

Run: `.venv\Scripts\python -m pytest tests/unit/test_settings_ws.py -v`
Expected: 2 passed.

- [ ] **Step 6: Commit**

```bash
git add app/config/settings.py tests/unit/test_settings_ws.py
git commit -m "feat(settings): add Kalshi WS config knobs (disabled by default)"
```

---

## Task 3: `MarketEntry` and `BookUpdate` dataclasses

**Files:**
- Create: `app/trading/market_book.py`
- Test: `tests/unit/test_market_book.py`

- [ ] **Step 1: Write failing test for dataclass shape**

Create `tests/unit/test_market_book.py`:

```python
from datetime import UTC, datetime

import pytest

from app.trading.market_book import BookUpdate, MarketEntry


def test_market_entry_is_frozen_dataclass():
    entry = MarketEntry(
        ticker="KXTEST-A",
        yes_bid=0.50,
        yes_ask=0.55,
        no_bid=0.45,
        no_ask=0.50,
        last=0.52,
        spread=0.05,
        status="open",
        updated_at=datetime(2026, 5, 10, tzinfo=UTC),
    )
    assert entry.ticker == "KXTEST-A"
    with pytest.raises(Exception):
        entry.ticker = "MUTATED"  # type: ignore[misc]


def test_market_entry_optional_prices_default_none():
    entry = MarketEntry(
        ticker="KXTEST-B",
        yes_bid=None,
        yes_ask=None,
        no_bid=None,
        no_ask=None,
        last=None,
        spread=None,
        status="unknown",
        updated_at=datetime(2026, 5, 10, tzinfo=UTC),
    )
    assert entry.yes_bid is None
    assert entry.spread is None


def test_book_update_carries_before_after():
    before = MarketEntry(
        ticker="KXTEST-C", yes_bid=0.50, yes_ask=0.55, no_bid=0.45, no_ask=0.50,
        last=None, spread=0.05, status="open",
        updated_at=datetime(2026, 5, 10, 12, 0, tzinfo=UTC),
    )
    after = MarketEntry(
        ticker="KXTEST-C", yes_bid=0.51, yes_ask=0.55, no_bid=0.45, no_ask=0.49,
        last=None, spread=0.04, status="open",
        updated_at=datetime(2026, 5, 10, 12, 0, 1, tzinfo=UTC),
    )
    update = BookUpdate(ticker="KXTEST-C", before=before, after=after)
    assert update.ticker == "KXTEST-C"
    assert update.before is before
    assert update.after is after
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv\Scripts\python -m pytest tests/unit/test_market_book.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app.trading.market_book'`.

- [ ] **Step 3: Create the module with dataclasses only**

Create `app/trading/market_book.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class MarketEntry:
    ticker: str
    yes_bid: float | None
    yes_ask: float | None
    no_bid: float | None
    no_ask: float | None
    last: float | None
    spread: float | None
    status: str
    updated_at: datetime


@dataclass(frozen=True)
class BookUpdate:
    ticker: str
    before: MarketEntry | None
    after: MarketEntry
```

- [ ] **Step 4: Run tests, verify pass**

Run: `.venv\Scripts\python -m pytest tests/unit/test_market_book.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add app/trading/market_book.py tests/unit/test_market_book.py
git commit -m "feat(trading): MarketEntry and BookUpdate dataclasses"
```

---

## Task 4: `MarketBook` store — get/update/snapshot

**Files:**
- Modify: `app/trading/market_book.py`
- Test: append to `tests/unit/test_market_book.py`

- [ ] **Step 1: Write failing tests for the core store**

Append to `tests/unit/test_market_book.py`:

```python
import asyncio

from app.trading.market_book import MarketBook


def _entry(ticker: str, yes_bid: float, ts_seconds: int = 0) -> MarketEntry:
    return MarketEntry(
        ticker=ticker,
        yes_bid=yes_bid,
        yes_ask=yes_bid + 0.02,
        no_bid=1.0 - (yes_bid + 0.02),
        no_ask=1.0 - yes_bid,
        last=yes_bid,
        spread=0.02,
        status="open",
        updated_at=datetime(2026, 5, 10, 12, 0, ts_seconds, tzinfo=UTC),
    )


def test_get_returns_none_for_unknown_ticker():
    book = MarketBook()
    assert book.get("MISSING") is None


def test_update_then_get_returns_entry():
    book = MarketBook()
    entry = _entry("KXTEST-D", 0.50)

    async def runner():
        await book.update(entry)
        return book.get("KXTEST-D")

    assert asyncio.run(runner()) == entry


def test_update_returns_book_update_with_before_none_on_first_write():
    book = MarketBook()
    entry = _entry("KXTEST-E", 0.50)

    async def runner():
        return await book.update(entry)

    update = asyncio.run(runner())
    assert update.ticker == "KXTEST-E"
    assert update.before is None
    assert update.after == entry


def test_update_returns_before_after_on_second_write():
    book = MarketBook()
    first = _entry("KXTEST-F", 0.50, ts_seconds=0)
    second = _entry("KXTEST-F", 0.55, ts_seconds=1)

    async def runner():
        await book.update(first)
        return await book.update(second)

    update = asyncio.run(runner())
    assert update.before == first
    assert update.after == second


def test_snapshot_returns_independent_copy():
    book = MarketBook()
    entry = _entry("KXTEST-G", 0.50)

    async def runner():
        await book.update(entry)
        snap = book.snapshot()
        await book.update(_entry("KXTEST-G", 0.99, ts_seconds=2))
        return snap

    snap = asyncio.run(runner())
    assert snap["KXTEST-G"].yes_bid == 0.50  # snap not mutated by later update
```

- [ ] **Step 2: Run tests to confirm they fail**

Run: `.venv\Scripts\python -m pytest tests/unit/test_market_book.py -v`
Expected: 4 new FAILs with `AttributeError: ... has no attribute 'update'` or similar.

- [ ] **Step 3: Implement `MarketBook` class**

Append to `app/trading/market_book.py`:

```python
import asyncio


class MarketBook:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._entries: dict[str, MarketEntry] = {}

    async def update(self, entry: MarketEntry) -> BookUpdate:
        async with self._lock:
            before = self._entries.get(entry.ticker)
            self._entries[entry.ticker] = entry
            return BookUpdate(ticker=entry.ticker, before=before, after=entry)

    def get(self, ticker: str) -> MarketEntry | None:
        return self._entries.get(ticker)

    def snapshot(self) -> dict[str, MarketEntry]:
        return dict(self._entries)
```

- [ ] **Step 4: Run tests, verify pass**

Run: `.venv\Scripts\python -m pytest tests/unit/test_market_book.py -v`
Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
git add app/trading/market_book.py tests/unit/test_market_book.py
git commit -m "feat(trading): MarketBook in-memory store with async update/get/snapshot"
```

---

## Task 5: `MarketBook` pub/sub — `subscribe()` with bounded queue and drop-oldest

**Files:**
- Modify: `app/trading/market_book.py`
- Test: append to `tests/unit/test_market_book.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/unit/test_market_book.py`:

```python
import pytest


@pytest.mark.asyncio
async def test_subscribe_receives_updates():
    book = MarketBook()
    received: list[BookUpdate] = []

    async def consumer():
        async for upd in book.subscribe():
            received.append(upd)
            if len(received) == 2:
                return

    task = asyncio.create_task(consumer())
    await asyncio.sleep(0.01)  # let subscribe register
    await book.update(_entry("KXTEST-H", 0.50))
    await book.update(_entry("KXTEST-H", 0.55, ts_seconds=1))
    await asyncio.wait_for(task, timeout=1.0)
    assert [u.after.yes_bid for u in received] == [0.50, 0.55]


@pytest.mark.asyncio
async def test_multiple_subscribers_each_receive_updates():
    book = MarketBook()

    async def collect(n: int) -> list[BookUpdate]:
        out: list[BookUpdate] = []
        async for upd in book.subscribe():
            out.append(upd)
            if len(out) == n:
                return out
        return out

    t1 = asyncio.create_task(collect(1))
    t2 = asyncio.create_task(collect(1))
    await asyncio.sleep(0.01)
    await book.update(_entry("KXTEST-I", 0.50))
    r1 = await asyncio.wait_for(t1, timeout=1.0)
    r2 = await asyncio.wait_for(t2, timeout=1.0)
    assert r1[0].after.ticker == "KXTEST-I"
    assert r2[0].after.ticker == "KXTEST-I"


@pytest.mark.asyncio
async def test_slow_subscriber_drops_oldest_under_backpressure():
    book = MarketBook(subscriber_queue_size=2)
    drained: list[BookUpdate] = []

    sub = book.subscribe()
    await asyncio.sleep(0.01)
    for i in range(5):
        await book.update(_entry("KXTEST-J", 0.50 + i * 0.01, ts_seconds=i))
    # consumer wakes up; should only see the last 2 updates retained
    async def drain():
        async for upd in sub:
            drained.append(upd)
            if len(drained) == 2:
                return
    await asyncio.wait_for(drain(), timeout=1.0)
    assert [round(u.after.yes_bid, 2) for u in drained] == [0.53, 0.54]
```

- [ ] **Step 2: Add `pytest-asyncio` config**

Run: `grep -n "asyncio_mode\|pytest-asyncio" pyproject.toml`
Expected: if a result shows `asyncio_mode = "auto"` or `pytest-asyncio` already configured, skip Step 3.

- [ ] **Step 3: Enable pytest-asyncio if not already**

If Step 2 returned nothing, append to `pyproject.toml` under `[tool.pytest.ini_options]` (create the section if missing, just inside the section's existing keys):

```toml
asyncio_mode = "auto"
```

And ensure `pytest-asyncio` is in dev deps. Run:

```bash
grep -n "pytest-asyncio" pyproject.toml
```

If absent, add `"pytest-asyncio>=0.23",` to the `[project.optional-dependencies] dev` array, then run:
`.venv\Scripts\python -m pip install -e .[dev]`

- [ ] **Step 4: Run tests, confirm they fail with the right error**

Run: `.venv\Scripts\python -m pytest tests/unit/test_market_book.py::test_subscribe_receives_updates -v`
Expected: FAIL with `AttributeError: ... has no attribute 'subscribe'`.

- [ ] **Step 5: Implement subscribe with bounded queue**

In `app/trading/market_book.py`, modify `MarketBook`:

```python
from collections.abc import AsyncIterator


class MarketBook:
    def __init__(self, *, subscriber_queue_size: int = 256) -> None:
        self._lock = asyncio.Lock()
        self._entries: dict[str, MarketEntry] = {}
        self._subscribers: list[asyncio.Queue[BookUpdate]] = []
        self._queue_size = max(1, int(subscriber_queue_size))

    async def update(self, entry: MarketEntry) -> BookUpdate:
        async with self._lock:
            before = self._entries.get(entry.ticker)
            self._entries[entry.ticker] = entry
            update = BookUpdate(ticker=entry.ticker, before=before, after=entry)
            for queue in self._subscribers:
                _put_drop_oldest(queue, update)
            return update

    def get(self, ticker: str) -> MarketEntry | None:
        return self._entries.get(ticker)

    def snapshot(self) -> dict[str, MarketEntry]:
        return dict(self._entries)

    async def subscribe(self) -> AsyncIterator[BookUpdate]:
        queue: asyncio.Queue[BookUpdate] = asyncio.Queue(maxsize=self._queue_size)
        self._subscribers.append(queue)
        try:
            while True:
                yield await queue.get()
        finally:
            if queue in self._subscribers:
                self._subscribers.remove(queue)


def _put_drop_oldest(queue: asyncio.Queue[BookUpdate], item: BookUpdate) -> None:
    while True:
        try:
            queue.put_nowait(item)
            return
        except asyncio.QueueFull:
            try:
                queue.get_nowait()  # drop oldest
            except asyncio.QueueEmpty:
                return
```

- [ ] **Step 6: Run tests, verify pass**

Run: `.venv\Scripts\python -m pytest tests/unit/test_market_book.py -v`
Expected: 10 passed.

- [ ] **Step 7: Commit**

```bash
git add app/trading/market_book.py tests/unit/test_market_book.py pyproject.toml
git commit -m "feat(trading): MarketBook subscribe() with bounded queue + drop-oldest"
```

---

## Task 6: Ticker frame parser

**Files:**
- Create: `app/trading/ws_frames.py`
- Test: `tests/unit/test_ws_frames.py`

- [ ] **Step 1: Write failing tests**

Create `tests/unit/test_ws_frames.py`:

```python
from datetime import UTC, datetime

from app.trading.market_book import MarketEntry
from app.trading.ws_frames import parse_frame


_FIXED_NOW = datetime(2026, 5, 10, 12, 0, 0, tzinfo=UTC)


def _now() -> datetime:
    return _FIXED_NOW


def test_parse_ticker_frame_returns_market_entry():
    frame = {
        "type": "ticker",
        "msg": {
            "market_ticker": "KXNBA-LAL-W",
            "yes_bid_dollars": 0.52,
            "yes_ask_dollars": 0.55,
            "no_bid_dollars": 0.45,
            "no_ask_dollars": 0.48,
        },
    }
    entry = parse_frame(frame, now=_now)
    assert isinstance(entry, MarketEntry)
    assert entry.ticker == "KXNBA-LAL-W"
    assert entry.yes_bid == 0.52
    assert entry.yes_ask == 0.55
    assert entry.no_bid == 0.45
    assert entry.no_ask == 0.48
    assert entry.spread == 0.03
    assert entry.status == "open"
    assert entry.updated_at == _FIXED_NOW


def test_parse_ticker_frame_with_missing_optional_prices():
    frame = {
        "type": "ticker",
        "msg": {
            "market_ticker": "KXNBA-LAL-W",
            "yes_bid_dollars": 0.52,
            "yes_ask_dollars": 0.55,
        },
    }
    entry = parse_frame(frame, now=_now)
    assert entry is not None
    assert entry.no_bid is None
    assert entry.no_ask is None
    assert entry.spread == 0.03


def test_parse_orderbook_delta_returns_none_in_v1():
    # v1 does not turn orderbook_delta into MarketEntry; it is ack-only.
    frame = {"type": "orderbook_delta", "msg": {"market_ticker": "KXNBA-LAL-W"}}
    assert parse_frame(frame, now=_now) is None


def test_parse_unknown_type_returns_none():
    assert parse_frame({"type": "weird"}, now=_now) is None


def test_parse_missing_market_ticker_returns_none():
    frame = {"type": "ticker", "msg": {"yes_bid_dollars": 0.52, "yes_ask_dollars": 0.55}}
    assert parse_frame(frame, now=_now) is None


def test_parse_non_dict_returns_none():
    assert parse_frame("not-a-dict", now=_now) is None  # type: ignore[arg-type]
    assert parse_frame(None, now=_now) is None  # type: ignore[arg-type]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv\Scripts\python -m pytest tests/unit/test_ws_frames.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app.trading.ws_frames'`.

- [ ] **Step 3: Implement the parser**

Create `app/trading/ws_frames.py`:

```python
"""Pure parsers for Kalshi WebSocket frame payloads."""
from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from app.trading.market_book import MarketEntry


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _spread(yes_bid: float | None, yes_ask: float | None) -> float | None:
    if yes_bid is None or yes_ask is None:
        return None
    return round(yes_ask - yes_bid, 4)


def parse_frame(
    frame: Any,
    *,
    now: Callable[[], datetime] = _utcnow,
) -> MarketEntry | None:
    """Convert a raw decoded JSON frame into a MarketEntry, or None to skip.

    Kalshi sends `{"type": "ticker", "msg": {...}}` envelopes. Non-ticker frames
    return None in v1; the caller must handle subscription acks, errors, etc.
    """
    if not isinstance(frame, dict):
        return None
    frame_type = frame.get("type")
    if frame_type != "ticker":
        return None
    msg = frame.get("msg")
    if not isinstance(msg, dict):
        return None
    ticker = msg.get("market_ticker")
    if not isinstance(ticker, str) or not ticker:
        return None
    yes_bid = _float_or_none(msg.get("yes_bid_dollars"))
    yes_ask = _float_or_none(msg.get("yes_ask_dollars"))
    no_bid = _float_or_none(msg.get("no_bid_dollars"))
    no_ask = _float_or_none(msg.get("no_ask_dollars"))
    return MarketEntry(
        ticker=ticker,
        yes_bid=yes_bid,
        yes_ask=yes_ask,
        no_bid=no_bid,
        no_ask=no_ask,
        last=_float_or_none(msg.get("last_dollars")),
        spread=_spread(yes_bid, yes_ask),
        status=str(msg.get("status") or "open"),
        updated_at=now(),
    )
```

- [ ] **Step 4: Run tests, verify pass**

Run: `.venv\Scripts\python -m pytest tests/unit/test_ws_frames.py -v`
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add app/trading/ws_frames.py tests/unit/test_ws_frames.py
git commit -m "feat(trading): pure parser for Kalshi ticker WS frames"
```

---

## Task 7: `KalshiWebSocketConsumer` — connect, sign, subscribe

**Files:**
- Create: `app/trading/ws_consumer.py`
- Test: `tests/integration/test_ws_consumer.py`

- [ ] **Step 1: Write failing test using an in-process fake WS server**

Create `tests/integration/test_ws_consumer.py`:

```python
from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
import websockets
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from app.trading.market_book import MarketBook
from app.trading.ws_consumer import KalshiWebSocketConsumer, KalshiWsCredentials


@pytest.fixture
def rsa_key_file(tmp_path: Path) -> Path:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    path = tmp_path / "kalshi.pem"
    path.write_bytes(pem)
    return path


class FakeKalshiServer:
    """Minimal in-process Kalshi WS server for tests."""

    def __init__(self) -> None:
        self.received_subscribes: list[dict] = []
        self.received_headers: list[dict] = []
        self._server: websockets.WebSocketServer | None = None
        self.port: int = 0
        self._connections: list[websockets.WebSocketServerProtocol] = []
        self._frames_to_send: list[dict] = []
        self._auth_reject = False

    def queue_frame(self, frame: dict) -> None:
        self._frames_to_send.append(frame)

    def set_auth_reject(self, value: bool) -> None:
        self._auth_reject = value

    async def _handler(self, ws):
        if self._auth_reject:
            await ws.close(code=4401, reason="unauthorized")
            return
        self.received_headers.append(dict(ws.request_headers))
        self._connections.append(ws)
        try:
            async for raw in ws:
                msg = json.loads(raw)
                if msg.get("cmd") == "subscribe":
                    self.received_subscribes.append(msg)
                    for frame in self._frames_to_send:
                        await ws.send(json.dumps(frame))
        except websockets.ConnectionClosed:
            pass
        finally:
            if ws in self._connections:
                self._connections.remove(ws)

    async def start(self) -> None:
        self._server = await websockets.serve(self._handler, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        for ws in list(self._connections):
            await ws.close()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()


@pytest.fixture
async def fake_server():
    server = FakeKalshiServer()
    await server.start()
    yield server
    await server.stop()


async def test_consumer_connects_signs_and_subscribes(fake_server, rsa_key_file):
    book = MarketBook()
    creds = KalshiWsCredentials(api_key_id="key-abc", private_key_path=rsa_key_file)
    fake_server.queue_frame({
        "type": "ticker",
        "msg": {
            "market_ticker": "KXNBA-LAL-W",
            "yes_bid_dollars": 0.52,
            "yes_ask_dollars": 0.55,
        },
    })
    consumer = KalshiWebSocketConsumer(
        base_url=f"ws://127.0.0.1:{fake_server.port}",
        credentials=creds,
        book=book,
        tickers=["KXNBA-LAL-W"],
        ping_interval_seconds=60,
        max_backoff_seconds=1,
        max_consecutive_auth_failures=2,
    )
    task = asyncio.create_task(consumer.run())
    # wait for first book update
    deadline = datetime.now(UTC).timestamp() + 2.0
    while datetime.now(UTC).timestamp() < deadline:
        if book.get("KXNBA-LAL-W") is not None:
            break
        await asyncio.sleep(0.02)
    await consumer.stop()
    await asyncio.wait_for(task, timeout=2.0)
    assert book.get("KXNBA-LAL-W") is not None
    assert book.get("KXNBA-LAL-W").yes_bid == 0.52
    assert len(fake_server.received_subscribes) == 1
    sub = fake_server.received_subscribes[0]
    assert sub["cmd"] == "subscribe"
    assert "ticker" in sub["params"]["channels"]
    assert sub["params"]["market_tickers"] == ["KXNBA-LAL-W"]
    headers = fake_server.received_headers[0]
    assert "KALSHI-ACCESS-KEY" in headers
    assert headers["KALSHI-ACCESS-KEY"] == "key-abc"
    assert "KALSHI-ACCESS-SIGNATURE" in headers
    assert "KALSHI-ACCESS-TIMESTAMP" in headers
```

- [ ] **Step 2: Run test, expect failure**

Run: `.venv\Scripts\python -m pytest tests/integration/test_ws_consumer.py::test_consumer_connects_signs_and_subscribes -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app.trading.ws_consumer'`.

- [ ] **Step 3: Implement `KalshiWebSocketConsumer` (connect + sign + subscribe + receive loop)**

Create `app/trading/ws_consumer.py`:

```python
"""Kalshi WebSocket consumer that pumps frames into a MarketBook.

Connection lifecycle:
  1. Sign handshake with RSA-PSS over (timestamp + "GET" + path).
  2. Open ws connection with KALSHI-ACCESS-* headers.
  3. Send subscribe command for ("ticker",) on the configured tickers.
  4. Receive frames, parse, MarketBook.update().
  5. On disconnect: backoff and reconnect (Task 9). On 401: increment auth
     failure counter; stop after threshold (Task 10).

This task implements steps 1-4. Reconnect/auth/ping are added in Tasks 9-11.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse

import websockets
from websockets.asyncio.client import ClientConnection, connect

from app.providers.exchanges.kalshi_signing import sign_request
from app.trading.market_book import MarketBook
from app.trading.ws_frames import parse_frame

_LOG = logging.getLogger(__name__)
_DEFAULT_CHANNELS: tuple[str, ...] = ("ticker",)


@dataclass(frozen=True)
class KalshiWsCredentials:
    api_key_id: str
    private_key_path: Path


@dataclass
class _ConsumerState:
    is_connected: bool = False
    last_message_at: float = 0.0
    reconnect_count: int = 0
    consecutive_auth_failures: int = 0
    stopped: bool = False
    fatal_auth_failure: bool = False


class KalshiWebSocketConsumer:
    def __init__(
        self,
        *,
        base_url: str,
        credentials: KalshiWsCredentials,
        book: MarketBook,
        tickers: list[str],
        ping_interval_seconds: int = 10,
        max_backoff_seconds: int = 30,
        max_consecutive_auth_failures: int = 5,
        channels: tuple[str, ...] = _DEFAULT_CHANNELS,
    ) -> None:
        self._base_url = base_url
        self._creds = credentials
        self._book = book
        self._tickers = list(tickers)
        self._ping_interval = ping_interval_seconds
        self._max_backoff = max_backoff_seconds
        self._max_auth_failures = max_consecutive_auth_failures
        self._channels = tuple(channels) or _DEFAULT_CHANNELS
        self._state = _ConsumerState()
        self._stop_event = asyncio.Event()
        self._next_subscribe_id = 1

    @property
    def is_connected(self) -> bool:
        return self._state.is_connected

    @property
    def reconnect_count(self) -> int:
        return self._state.reconnect_count

    @property
    def consecutive_auth_failures(self) -> int:
        return self._state.consecutive_auth_failures

    @property
    def last_message_at(self) -> float:
        return self._state.last_message_at

    async def stop(self) -> None:
        self._state.stopped = True
        self._stop_event.set()

    async def run(self) -> None:
        # Single-iteration runner; reconnect added in Task 9.
        if not self._tickers:
            _LOG.info("Kalshi WS consumer started with no tickers; idling")
            await self._stop_event.wait()
            return
        try:
            await self._run_once()
        finally:
            self._state.is_connected = False

    async def _run_once(self) -> None:
        headers = self._signed_headers()
        async with connect(self._base_url, additional_headers=headers) as ws:
            self._state.is_connected = True
            await self._send_subscribe(ws)
            await self._receive_loop(ws)

    def _signed_headers(self) -> dict[str, str]:
        ts = str(int(time.time() * 1000))
        path = urlparse(self._base_url).path or "/trade-api/ws/v2"
        signature = sign_request(self._creds.private_key_path, ts, "GET", path)
        return {
            "KALSHI-ACCESS-KEY": self._creds.api_key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": ts,
        }

    async def _send_subscribe(self, ws: ClientConnection) -> None:
        msg = {
            "id": self._next_subscribe_id,
            "cmd": "subscribe",
            "params": {
                "channels": list(self._channels),
                "market_tickers": list(self._tickers),
            },
        }
        self._next_subscribe_id += 1
        await ws.send(json.dumps(msg))

    async def _receive_loop(self, ws: ClientConnection) -> None:
        async for raw in ws:
            if self._state.stopped:
                break
            self._state.last_message_at = time.time()
            try:
                frame = json.loads(raw)
            except json.JSONDecodeError:
                _LOG.warning("Malformed JSON frame; skipping")
                continue
            entry = parse_frame(frame)
            if entry is None:
                _LOG.debug("Frame skipped (non-ticker or invalid): %s", frame.get("type"))
                continue
            await self._book.update(entry)
```

- [ ] **Step 4: Run the test, verify pass**

Run: `.venv\Scripts\python -m pytest tests/integration/test_ws_consumer.py::test_consumer_connects_signs_and_subscribes -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/trading/ws_consumer.py tests/integration/test_ws_consumer.py
git commit -m "feat(trading): KalshiWebSocketConsumer connects, signs, subscribes (single iteration)"
```

---

## Task 8: Reconnect with exponential backoff

**Files:**
- Modify: `app/trading/ws_consumer.py`
- Test: append to `tests/integration/test_ws_consumer.py`

- [ ] **Step 1: Write failing test**

Append to `tests/integration/test_ws_consumer.py`:

```python
async def test_consumer_reconnects_after_server_close(fake_server, rsa_key_file):
    book = MarketBook()
    creds = KalshiWsCredentials(api_key_id="key-abc", private_key_path=rsa_key_file)
    fake_server.queue_frame({
        "type": "ticker",
        "msg": {
            "market_ticker": "KXNBA-LAL-W",
            "yes_bid_dollars": 0.50,
            "yes_ask_dollars": 0.55,
        },
    })
    consumer = KalshiWebSocketConsumer(
        base_url=f"ws://127.0.0.1:{fake_server.port}",
        credentials=creds,
        book=book,
        tickers=["KXNBA-LAL-W"],
        ping_interval_seconds=60,
        max_backoff_seconds=1,
        max_consecutive_auth_failures=5,
    )
    task = asyncio.create_task(consumer.run())
    # wait for first connect + frame
    for _ in range(100):
        if consumer.is_connected and book.get("KXNBA-LAL-W") is not None:
            break
        await asyncio.sleep(0.02)
    assert consumer.is_connected

    # force-close server side, expect reconnect
    for ws in list(fake_server._connections):
        await ws.close()
    for _ in range(200):
        if consumer.reconnect_count >= 1 and consumer.is_connected:
            break
        await asyncio.sleep(0.02)
    assert consumer.reconnect_count >= 1
    assert consumer.is_connected

    await consumer.stop()
    await asyncio.wait_for(task, timeout=2.0)
```

- [ ] **Step 2: Run test, expect failure**

Run: `.venv\Scripts\python -m pytest tests/integration/test_ws_consumer.py::test_consumer_reconnects_after_server_close -v`
Expected: FAIL (task exits after first iteration; reconnect_count stays 0).

- [ ] **Step 3: Replace `run()` with reconnect loop**

In `app/trading/ws_consumer.py`, replace the body of `run()`:

```python
    async def run(self) -> None:
        if not self._tickers:
            _LOG.info("Kalshi WS consumer started with no tickers; idling")
            await self._stop_event.wait()
            return
        backoff = 1.0
        while not self._state.stopped and not self._state.fatal_auth_failure:
            try:
                await self._run_once()
                backoff = 1.0  # reset on clean exit
            except websockets.InvalidStatus as exc:
                if _is_auth_status(exc):
                    self._state.consecutive_auth_failures += 1
                    _LOG.error(
                        "Kalshi WS auth rejected (%d/%d)",
                        self._state.consecutive_auth_failures,
                        self._max_auth_failures,
                    )
                    if self._state.consecutive_auth_failures >= self._max_auth_failures:
                        self._state.fatal_auth_failure = True
                        break
                else:
                    _LOG.warning("Kalshi WS handshake failed: %s", exc)
            except (websockets.ConnectionClosed, OSError) as exc:
                _LOG.info("Kalshi WS disconnected: %s", exc)
            except Exception:
                _LOG.exception("Unexpected Kalshi WS error")
            finally:
                self._state.is_connected = False
            if self._state.stopped or self._state.fatal_auth_failure:
                break
            self._state.reconnect_count += 1
            sleep_for = min(backoff, float(self._max_backoff))
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=sleep_for)
                break  # stopped while sleeping
            except asyncio.TimeoutError:
                pass
            backoff = min(backoff * 2.0, float(self._max_backoff))


def _is_auth_status(exc: "websockets.InvalidStatus") -> bool:
    status = getattr(getattr(exc, "response", None), "status_code", None)
    return status in (401, 403)
```

Add the import at top of file:

```python
import websockets
```

(already imported in Task 7).

- [ ] **Step 4: Reset auth failures on successful connect**

In `_run_once`, after `self._state.is_connected = True`, add:

```python
            self._state.consecutive_auth_failures = 0
```

- [ ] **Step 5: Run tests, verify pass**

Run: `.venv\Scripts\python -m pytest tests/integration/test_ws_consumer.py -v`
Expected: 2 passed.

- [ ] **Step 6: Commit**

```bash
git add app/trading/ws_consumer.py tests/integration/test_ws_consumer.py
git commit -m "feat(trading): WS consumer reconnect with exponential backoff"
```

---

## Task 9: Auth failure threshold halts retries

**Files:**
- Test: append to `tests/integration/test_ws_consumer.py`

- [ ] **Step 1: Write test**

Append to `tests/integration/test_ws_consumer.py`:

```python
async def test_consumer_stops_after_max_auth_failures(fake_server, rsa_key_file):
    book = MarketBook()
    creds = KalshiWsCredentials(api_key_id="bad-key", private_key_path=rsa_key_file)
    fake_server.set_auth_reject(True)
    consumer = KalshiWebSocketConsumer(
        base_url=f"ws://127.0.0.1:{fake_server.port}",
        credentials=creds,
        book=book,
        tickers=["KXNBA-LAL-W"],
        ping_interval_seconds=60,
        max_backoff_seconds=1,
        max_consecutive_auth_failures=2,
    )
    task = asyncio.create_task(consumer.run())
    await asyncio.wait_for(task, timeout=5.0)
    assert consumer.consecutive_auth_failures >= 2
    assert not consumer.is_connected
```

- [ ] **Step 2: Tighten fake server to produce a real handshake auth rejection**

The current `FakeKalshiServer._handler` returns a close frame, but for `InvalidStatus` to fire client-side we need the upgrade itself to return non-101. Replace `set_auth_reject` machinery: convert the server to use `process_request` to reject the upgrade.

Modify the `FakeKalshiServer` class in `tests/integration/test_ws_consumer.py`:

```python
class FakeKalshiServer:
    def __init__(self) -> None:
        self.received_subscribes: list[dict] = []
        self.received_headers: list[dict] = []
        self._server: websockets.WebSocketServer | None = None
        self.port: int = 0
        self._connections: list = []
        self._frames_to_send: list[dict] = []
        self._auth_reject = False

    def queue_frame(self, frame: dict) -> None:
        self._frames_to_send.append(frame)

    def set_auth_reject(self, value: bool) -> None:
        self._auth_reject = value

    async def _process_request(self, connection, request):
        if self._auth_reject:
            from websockets.http11 import Response
            return Response(401, "Unauthorized", connection.protocol.headers_factory(), b"unauthorized")
        return None

    async def _handler(self, ws):
        self.received_headers.append(dict(ws.request.headers))
        self._connections.append(ws)
        try:
            async for raw in ws:
                msg = json.loads(raw)
                if msg.get("cmd") == "subscribe":
                    self.received_subscribes.append(msg)
                    for frame in self._frames_to_send:
                        await ws.send(json.dumps(frame))
        except websockets.ConnectionClosed:
            pass
        finally:
            if ws in self._connections:
                self._connections.remove(ws)

    async def start(self) -> None:
        self._server = await websockets.serve(
            self._handler,
            "127.0.0.1",
            0,
            process_request=self._process_request,
        )
        self.port = self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        for ws in list(self._connections):
            try:
                await ws.close()
            except Exception:
                pass
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
```

Note: the `process_request` callback signature in `websockets>=12` is `(connection, request) -> Response | None`. Tasks 7 and 8 fixtures keep working because `_auth_reject` defaults to False.

- [ ] **Step 3: Run all WS tests, verify pass**

Run: `.venv\Scripts\python -m pytest tests/integration/test_ws_consumer.py -v`
Expected: 3 passed.

- [ ] **Step 4: Commit**

```bash
git add tests/integration/test_ws_consumer.py
git commit -m "test(trading): WS consumer halts after max consecutive auth failures"
```

---

## Task 10: Ping/pong keepalive

**Files:**
- Modify: `app/trading/ws_consumer.py`
- Test: append to `tests/integration/test_ws_consumer.py`

The `websockets` library handles ping/pong automatically via its `ping_interval` and `ping_timeout` parameters. This task surfaces that as our `ping_interval_seconds` setting.

- [ ] **Step 1: Write a test that confirms ping_interval is wired**

Append to `tests/integration/test_ws_consumer.py`:

```python
async def test_consumer_uses_ping_interval(fake_server, rsa_key_file, monkeypatch):
    book = MarketBook()
    creds = KalshiWsCredentials(api_key_id="key-abc", private_key_path=rsa_key_file)
    captured: dict = {}

    real_connect = websockets.asyncio.client.connect

    def spy_connect(*args, **kwargs):
        captured.update(kwargs)
        return real_connect(*args, **kwargs)

    monkeypatch.setattr("app.trading.ws_consumer.connect", spy_connect)

    consumer = KalshiWebSocketConsumer(
        base_url=f"ws://127.0.0.1:{fake_server.port}",
        credentials=creds,
        book=book,
        tickers=["KXNBA-LAL-W"],
        ping_interval_seconds=7,
        max_backoff_seconds=1,
        max_consecutive_auth_failures=5,
    )
    task = asyncio.create_task(consumer.run())
    for _ in range(50):
        if "ping_interval" in captured:
            break
        await asyncio.sleep(0.02)
    await consumer.stop()
    await asyncio.wait_for(task, timeout=2.0)
    assert captured.get("ping_interval") == 7
```

- [ ] **Step 2: Run test, expect failure**

Run: `.venv\Scripts\python -m pytest tests/integration/test_ws_consumer.py::test_consumer_uses_ping_interval -v`
Expected: FAIL — `ping_interval` not passed to `connect`.

- [ ] **Step 3: Pass `ping_interval` to `connect()`**

In `app/trading/ws_consumer.py`, update `_run_once`:

```python
    async def _run_once(self) -> None:
        headers = self._signed_headers()
        async with connect(
            self._base_url,
            additional_headers=headers,
            ping_interval=self._ping_interval,
        ) as ws:
            self._state.is_connected = True
            self._state.consecutive_auth_failures = 0
            await self._send_subscribe(ws)
            await self._receive_loop(ws)
```

- [ ] **Step 4: Run tests, verify pass**

Run: `.venv\Scripts\python -m pytest tests/integration/test_ws_consumer.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add app/trading/ws_consumer.py tests/integration/test_ws_consumer.py
git commit -m "feat(trading): WS consumer respects ping_interval_seconds setting"
```

---

## Task 11: `KalshiMarketService` lifecycle wrapper

**Files:**
- Create: `app/trading/ws_service.py`
- Test: `tests/integration/test_ws_service.py`

- [ ] **Step 1: Inspect the symbols config layout**

Run: `head -40 config/kalshi_symbols.json` (or open it). Confirm the file has the shape `{ "symbols": [ { "kalshi_ticker": "...", "recommendation": "...", ... } ] }`. The `live_pack_builder.pick_executable_entries` function already walks this shape.

- [ ] **Step 2: Write tests**

Create `tests/integration/test_ws_service.py`:

```python
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
import websockets
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from app.trading.market_book import MarketBook
from app.trading.ws_consumer import KalshiWsCredentials
from app.trading.ws_service import KalshiMarketService


@pytest.fixture
def rsa_key_file(tmp_path: Path) -> Path:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    path = tmp_path / "kalshi.pem"
    path.write_bytes(pem)
    return path


def _write_symbols(path: Path, tickers: list[str]) -> None:
    payload = {
        "symbols": [
            {
                "kalshi_ticker": t,
                "recommendation": "buy_yes",
                "line_value": 24.5,
                "market_key": "points",
            }
            for t in tickers
        ]
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


async def test_service_starts_with_empty_tickers_and_does_not_connect(tmp_path, rsa_key_file):
    symbols = tmp_path / "kalshi_symbols.json"
    _write_symbols(symbols, [])
    creds = KalshiWsCredentials(api_key_id="key", private_key_path=rsa_key_file)
    book = MarketBook()
    service = KalshiMarketService(
        symbols_path=symbols,
        credentials=creds,
        book=book,
        base_url="ws://127.0.0.1:1",  # would fail to connect if attempted
        ping_interval_seconds=60,
        max_backoff_seconds=1,
        max_consecutive_auth_failures=5,
    )
    await service.start()
    await asyncio.sleep(0.05)
    assert service.is_connected is False
    assert service.tickers == ()
    await service.stop()


async def test_service_subscribes_to_tickers_from_symbols_file(tmp_path, rsa_key_file):
    from tests.integration.test_ws_consumer import FakeKalshiServer  # reuse

    server = FakeKalshiServer()
    await server.start()
    try:
        symbols = tmp_path / "kalshi_symbols.json"
        _write_symbols(symbols, ["KXA", "KXB"])
        creds = KalshiWsCredentials(api_key_id="key", private_key_path=rsa_key_file)
        book = MarketBook()
        service = KalshiMarketService(
            symbols_path=symbols,
            credentials=creds,
            book=book,
            base_url=f"ws://127.0.0.1:{server.port}",
            ping_interval_seconds=60,
            max_backoff_seconds=1,
            max_consecutive_auth_failures=5,
        )
        await service.start()
        for _ in range(100):
            if server.received_subscribes:
                break
            await asyncio.sleep(0.02)
        assert server.received_subscribes
        assert set(server.received_subscribes[0]["params"]["market_tickers"]) == {"KXA", "KXB"}
        await service.stop()
    finally:
        await server.stop()
```

- [ ] **Step 3: Run tests, expect failure**

Run: `.venv\Scripts\python -m pytest tests/integration/test_ws_service.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app.trading.ws_service'`.

- [ ] **Step 4: Implement the service**

Create `app/trading/ws_service.py`:

```python
"""Lifecycle wrapper for the Kalshi WebSocket consumer + MarketBook."""
from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

from app.trading.market_book import MarketBook
from app.trading.ws_consumer import KalshiWebSocketConsumer, KalshiWsCredentials

_LOG = logging.getLogger(__name__)
_EXECUTABLE_REC = frozenset({"buy_yes", "buy_no", "over", "under", "yes", "no"})


def _load_tickers(symbols_path: Path) -> list[str]:
    if not symbols_path.is_file():
        return []
    try:
        payload = json.loads(symbols_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        _LOG.warning("kalshi_symbols.json unreadable at %s", symbols_path)
        return []
    raw = payload.get("symbols", []) if isinstance(payload, dict) else []
    out: list[str] = []
    for row in raw:
        if not isinstance(row, dict):
            continue
        rec = str(row.get("recommendation") or "").strip().lower()
        ticker = row.get("kalshi_ticker")
        if rec in _EXECUTABLE_REC and isinstance(ticker, str) and ticker:
            out.append(ticker)
    # de-duplicate, preserve order
    seen: set[str] = set()
    unique: list[str] = []
    for t in out:
        if t not in seen:
            seen.add(t)
            unique.append(t)
    return unique


class KalshiMarketService:
    def __init__(
        self,
        *,
        symbols_path: Path,
        credentials: KalshiWsCredentials,
        book: MarketBook,
        base_url: str,
        ping_interval_seconds: int,
        max_backoff_seconds: int,
        max_consecutive_auth_failures: int,
    ) -> None:
        self._symbols_path = symbols_path
        self._creds = credentials
        self._book = book
        self._base_url = base_url
        self._ping = ping_interval_seconds
        self._backoff = max_backoff_seconds
        self._max_auth = max_consecutive_auth_failures
        self._tickers: tuple[str, ...] = ()
        self._consumer: KalshiWebSocketConsumer | None = None
        self._task: asyncio.Task[None] | None = None

    @property
    def tickers(self) -> tuple[str, ...]:
        return self._tickers

    @property
    def book(self) -> MarketBook:
        return self._book

    @property
    def is_connected(self) -> bool:
        return bool(self._consumer and self._consumer.is_connected)

    @property
    def reconnect_count(self) -> int:
        return self._consumer.reconnect_count if self._consumer else 0

    @property
    def consecutive_auth_failures(self) -> int:
        return self._consumer.consecutive_auth_failures if self._consumer else 0

    @property
    def last_message_at(self) -> float:
        return self._consumer.last_message_at if self._consumer else 0.0

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._tickers = tuple(_load_tickers(self._symbols_path))
        if not self._tickers:
            _LOG.info("Kalshi market service: no executable tickers; idling")
            return
        self._consumer = KalshiWebSocketConsumer(
            base_url=self._base_url,
            credentials=self._creds,
            book=self._book,
            tickers=list(self._tickers),
            ping_interval_seconds=self._ping,
            max_backoff_seconds=self._backoff,
            max_consecutive_auth_failures=self._max_auth,
        )
        self._task = asyncio.create_task(self._consumer.run(), name="kalshi-ws-consumer")

    async def stop(self) -> None:
        if self._consumer is not None:
            await self._consumer.stop()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=2.0)
            except asyncio.TimeoutError:
                self._task.cancel()
                try:
                    await self._task
                except (asyncio.CancelledError, Exception):
                    pass
        self._consumer = None
        self._task = None
```

- [ ] **Step 5: Run tests, verify pass**

Run: `.venv\Scripts\python -m pytest tests/integration/test_ws_service.py -v`
Expected: 2 passed.

- [ ] **Step 6: Commit**

```bash
git add app/trading/ws_service.py tests/integration/test_ws_service.py
git commit -m "feat(trading): KalshiMarketService lifecycle wrapper around WS consumer"
```

---

## Task 12: FastAPI `lifespan` integration

**Files:**
- Modify: `app/server/main.py`
- Test: `tests/integration/test_ws_lifespan.py`

- [ ] **Step 1: Confirm current `create_app` does not yet use lifespan**

Run: `grep -n "lifespan\|@asynccontextmanager" app/server/main.py`
Expected: no results.

- [ ] **Step 2: Write failing test**

Create `tests/integration/test_ws_lifespan.py`:

```python
from __future__ import annotations

import importlib
import os
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient


def _write_key(tmp_path: Path) -> Path:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    path = tmp_path / "kalshi.pem"
    path.write_bytes(pem)
    return path


@pytest.fixture
def reset_settings():
    import app.config.settings as settings_mod
    yield
    importlib.reload(settings_mod)


def test_lifespan_does_not_start_service_when_ws_disabled(tmp_path, reset_settings, monkeypatch):
    monkeypatch.delenv("KALSHI_WS_ENABLED", raising=False)
    monkeypatch.setenv("KALSHI_API_KEY_ID", "key")
    monkeypatch.setenv("KALSHI_PRIVATE_KEY_PATH", str(_write_key(tmp_path)))
    import app.config.settings as settings_mod
    importlib.reload(settings_mod)
    from app.server.main import create_app

    app = create_app()
    with TestClient(app) as client:
        client.get("/api/health")
        service = getattr(app.state, "market_service", None)
        assert service is not None
        assert service.is_connected is False
        assert service.tickers == ()


def test_lifespan_starts_service_when_ws_enabled_with_no_tickers(tmp_path, reset_settings, monkeypatch):
    monkeypatch.setenv("KALSHI_WS_ENABLED", "true")
    monkeypatch.setenv("KALSHI_API_KEY_ID", "key")
    monkeypatch.setenv("KALSHI_PRIVATE_KEY_PATH", str(_write_key(tmp_path)))
    # symbols file does not exist; service should idle, not crash
    monkeypatch.setenv("KALSHI_SYMBOLS_PATH", str(tmp_path / "missing.json"))
    import app.config.settings as settings_mod
    importlib.reload(settings_mod)
    from app.server.main import create_app

    app = create_app()
    with TestClient(app) as client:
        client.get("/api/health")
        service = app.state.market_service
        assert service.tickers == ()
        assert service.is_connected is False
```

- [ ] **Step 3: Run test, expect failure**

Run: `.venv\Scripts\python -m pytest tests/integration/test_ws_lifespan.py -v`
Expected: FAIL with `AttributeError: ... no attribute 'market_service'`.

- [ ] **Step 4: Add lifespan to `app/server/main.py`**

At top of `app/server/main.py`, add:

```python
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from app.trading.market_book import MarketBook
from app.trading.ws_consumer import KalshiWsCredentials
from app.trading.ws_service import KalshiMarketService
```

Add a builder function above `create_app`:

```python
def _build_market_service() -> KalshiMarketService:
    settings = get_settings()
    creds = KalshiWsCredentials(
        api_key_id=settings.kalshi_api_key_id or "",
        private_key_path=Path(settings.kalshi_private_key_path)
        if settings.kalshi_private_key_path
        else Path(""),
    )
    return KalshiMarketService(
        symbols_path=Path(settings.kalshi_symbols_path),
        credentials=creds,
        book=MarketBook(),
        base_url=settings.kalshi_ws_base_url,
        ping_interval_seconds=settings.kalshi_ws_ping_interval_seconds,
        max_backoff_seconds=settings.kalshi_ws_max_backoff_seconds,
        max_consecutive_auth_failures=settings.kalshi_ws_max_consecutive_auth_failures,
    )
```

Add a lifespan factory near the top of `create_app`, then pass it to `FastAPI(...)`:

```python
    market_service = _build_market_service()

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        settings = get_settings()
        if settings.kalshi_ws_enabled:
            await market_service.start()
        try:
            yield
        finally:
            await market_service.stop()

    app = FastAPI(
        title="NBA Prop Probability Engine API",
        version=_app_version(),
        lifespan=lifespan,
    )
```

After `app = FastAPI(...)`, store the service on app state:

```python
    app.state.market_service = market_service
```

Add `Path` import if missing:

```python
from pathlib import Path
```

- [ ] **Step 5: Run tests, verify pass**

Run: `.venv\Scripts\python -m pytest tests/integration/test_ws_lifespan.py -v`
Expected: 2 passed.

- [ ] **Step 6: Run the full unit + integration suites to confirm nothing broke**

Run: `.venv\Scripts\python -m pytest tests/unit tests/integration -x -q`
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add app/server/main.py tests/integration/test_ws_lifespan.py
git commit -m "feat(server): wire Kalshi market service into FastAPI lifespan (off by default)"
```

---

## Task 13: Project-wide lint + type check

- [ ] **Step 1: Lint**

Run: `.venv\Scripts\python -m ruff check app/trading app/server/main.py app/config/settings.py tests/unit tests/integration`
Expected: no errors. Fix any reported.

- [ ] **Step 2: Type check**

Run: `.venv\Scripts\python -m mypy app/trading app/server/main.py`
Expected: no new errors compared to baseline. If baseline already has errors elsewhere, ensure new files clean.

- [ ] **Step 3: Combined check (project convention)**

Run: `powershell -File scripts/check.ps1`
Expected: passes.

- [ ] **Step 4: Commit any lint/type fixes**

```bash
git add -A
git commit -m "chore(trading): satisfy ruff + mypy on new WS modules"
```

(Skip if nothing changed.)

---

## Task 14: Manual smoke test against Kalshi demo

This is the only manual step. It verifies real-network behavior against `wss://demo-api.kalshi.co/trade-api/ws/v2`. Skip if you do not have demo credentials yet; the suite still proves correctness against the fake server.

- [ ] **Step 1: Set up env**

In a shell where `KALSHI_API_KEY_ID` and `KALSHI_PRIVATE_KEY_PATH` point at a Kalshi demo key (not prod), set:

```bash
export KALSHI_WS_ENABLED=true
export KALSHI_WS_BASE_URL=wss://demo-api.kalshi.co/trade-api/ws/v2
```

(Windows PowerShell: `$env:KALSHI_WS_ENABLED="true"`, etc.)

- [ ] **Step 2: Ensure `config/kalshi_symbols.json` has at least one demo ticker**

The file should already exist with prod tickers. For the smoke test, add a demo ticker as the first executable row. After the smoke is done, revert.

- [ ] **Step 3: Start the sidecar**

Run: `.venv\Scripts\python -m app.server.main`
Expected: logs show `Kalshi market service` starting and `Kalshi WS consumer started` with the demo ticker.

- [ ] **Step 4: Verify the book is populated**

In another shell:

```bash
.venv\Scripts\python -c "from urllib.request import urlopen; import json; print(urlopen('http://127.0.0.1:8765/api/health').read())"
```

The health endpoint still returns OK. Then add a `/api/trading/ws-debug` temporary endpoint OR call into the running service via the Python debugger; for v1, simply check log output that frames are arriving (`DEBUG` level shows `Frame skipped (non-ticker or invalid)`; `INFO` shows reconnects). A proper readiness surface is added in sub-project #6.

- [ ] **Step 5: Force a reconnect**

Kill the network temporarily (Wi-Fi off / on). Expected: consumer logs disconnect + reconnect within `max_backoff_seconds`. `reconnect_count` increments.

- [ ] **Step 6: Stop**

`Ctrl-C` the sidecar. Expected: clean shutdown, no traceback.

- [ ] **Step 7: Revert demo ticker in `config/kalshi_symbols.json`** if you added one.

---

## Acceptance verification (sub-project 1)

Run through spec §5.10 one item at a time:

- [ ] With `kalshi_ws_enabled=true` and at least one executable ticker, sidecar starts a WS connection on boot → **proven by Task 14 smoke + Task 11 integration test.**
- [ ] `MarketBook.snapshot()` returns live data within 5 seconds of sidecar start → **proven by Task 14 smoke.**
- [ ] Force-disconnecting WS server triggers auto-reconnect within `max_backoff_seconds` → **proven by Task 8 integration test.**
- [ ] All unit tests pass; integration tests pass against fake WS server → **proven by Task 12 Step 6.**
- [ ] No changes to existing trading endpoints, paper trading, or live trading guards → **proven by full suite green and zero diff in those files.**
- [ ] Sidecar shutdown cleanly closes the WS connection → **proven by Task 12 test + Task 14 Step 6.**
- [ ] Setting flag off restores current behavior exactly → **proven by Task 12 first test (no service when disabled).**

If every box is checked, sub-project 1 is done. Sub-project 2 (SSE push channel) is next; it adds an `EventBus` that subscribes to `MarketBook` and fans events to an `EventSource` endpoint.

---

## Self-Review Notes (resolved)

- **Spec coverage:** Every §5 requirement maps to a task — components (Tasks 3-7, 11), data flow (Task 11 service wires consumer → book), lifecycle (Task 12 lifespan), settings (Task 2), error handling (Tasks 8, 9, parser in 6), tests (every task), build order (Tasks 3→11 follow spec §5.9), acceptance criteria (final checklist mirrors §5.10).
- **Type consistency:** `KalshiWsCredentials`, `KalshiWebSocketConsumer`, `KalshiMarketService`, `MarketEntry`, `BookUpdate`, `MarketBook` referenced by identical names across all tasks. `parse_frame(frame, *, now)` signature stable.
- **No placeholders:** every code step is concrete. The smoke test (Task 14) is the only step with a "you may skip" gate; that's explicit and bounded.
- **One open dependency:** Task 9's `process_request` callback uses websockets≥12 API. The Task 1 pin `>=12.0` matches.

from __future__ import annotations

import asyncio
import statistics
from collections import deque
from dataclasses import dataclass
from datetime import datetime

_VOL_WINDOW_MAXLEN = 2048


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


class MarketBook:
    def __init__(self, *, subscriber_queue_size: int = 256) -> None:
        self._lock = asyncio.Lock()
        self._entries: dict[str, MarketEntry] = {}
        self._subscribers: list[asyncio.Queue[BookUpdate]] = []
        self._queue_size = max(1, int(subscriber_queue_size))
        self._vol_windows: dict[str, deque[tuple[float, float]]] = {}

    async def update(self, entry: MarketEntry) -> BookUpdate:
        async with self._lock:
            before = self._entries.get(entry.ticker)
            self._entries[entry.ticker] = entry
            if entry.yes_bid is not None:
                window = self._vol_windows.get(entry.ticker)
                if window is None:
                    window = deque(maxlen=_VOL_WINDOW_MAXLEN)
                    self._vol_windows[entry.ticker] = window
                window.append((entry.updated_at.timestamp(), float(entry.yes_bid)))
            update = BookUpdate(ticker=entry.ticker, before=before, after=entry)
            subs = list(self._subscribers)
            for queue in subs:
                _put_drop_oldest(queue, update)
            return update

    def get(self, ticker: str) -> MarketEntry | None:
        return self._entries.get(ticker)

    def snapshot(self) -> dict[str, MarketEntry]:
        return dict(self._entries)

    def stdev_cents(self, ticker: str, window_seconds: float = 60.0) -> float | None:
        """Population stdev (in cents) of yes_bid samples within the last
        `window_seconds`.

        Returns None if there are no samples in the window. Returns 0.0 for a
        single sample or all-identical samples.

        Sync (not async) for parity with `get`/`snapshot`. Deque append +
        iteration are GIL-safe in CPython, and this read is advisory (used by
        the vol-veto), so a torn read missing the very latest sample is fine.
        """
        window = self._vol_windows.get(ticker)
        if not window:
            return None
        # Snapshot to a local list so concurrent appends do not perturb iteration.
        samples = list(window)
        if not samples:
            return None
        latest_ts = samples[-1][0]
        cutoff = latest_ts - float(window_seconds)
        prices = [price for ts, price in samples if ts >= cutoff]
        # `prices` is always non-empty here: the latest sample's own timestamp
        # satisfies ts >= latest_ts - window for any non-negative window.
        if len(prices) == 1:
            return 0.0
        return statistics.pstdev(prices) * 100.0

    def subscribe(self) -> _SubscriptionIterator:
        # Callers must either call aclose() or use `async with` to unsubscribe; iterating to exhaustion is also acceptable.
        queue: asyncio.Queue[BookUpdate] = asyncio.Queue(maxsize=self._queue_size)
        self._subscribers.append(queue)
        return _SubscriptionIterator(self, queue)


class _SubscriptionIterator:
    def __init__(self, book: MarketBook, queue: asyncio.Queue[BookUpdate]) -> None:
        self._book = book
        self._queue = queue
        self._closed = False

    def __aiter__(self) -> _SubscriptionIterator:
        return self

    async def __anext__(self) -> BookUpdate:
        if self._closed:
            raise StopAsyncIteration
        return await self._queue.get()

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._queue in self._book._subscribers:
            self._book._subscribers.remove(self._queue)

    async def __aenter__(self) -> _SubscriptionIterator:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.aclose()


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

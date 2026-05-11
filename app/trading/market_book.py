from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
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

    def subscribe(self) -> AsyncIterator[BookUpdate]:
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

    def __del__(self) -> None:
        if not self._closed and self._queue in self._book._subscribers:
            try:
                self._book._subscribers.remove(self._queue)
            except ValueError:
                pass


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

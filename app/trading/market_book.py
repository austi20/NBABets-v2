from __future__ import annotations

import asyncio
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

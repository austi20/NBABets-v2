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

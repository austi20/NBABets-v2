"""Fallback poller that refreshes MarketBook entries when the WS feed is down.

During a WebSocket disconnect, new entries stop arriving. This module provides
poll_held_tickers_once() to manually refresh quotes for held positions, and a
run_disconnect_poller() background task that calls it on a fixed cadence
whenever the book reports disconnected.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

from app.trading.market_book import MarketBook, MarketEntry

_log = logging.getLogger(__name__)


async def poll_held_tickers_once(
    *,
    book: MarketBook,
    held_tickers: list[str],
    fetch_entry: Callable[[str], Awaitable[MarketEntry | None]],
) -> None:
    """Fetch fresh quotes for each held ticker and push them into the book.

    No-op when the book is connected - this is a fallback only.
    """
    if book.is_connected():
        return
    for ticker in held_tickers:
        try:
            entry = await fetch_entry(ticker)
            if entry is not None:
                await book.update(entry)
        except Exception:  # noqa: BLE001
            _log.warning("disconnect poller: failed to fetch %s", ticker, exc_info=True)


async def run_disconnect_poller(
    *,
    book: MarketBook,
    held_tickers_source: Callable[[], list[str]],
    fetch_entry: Callable[[str], Awaitable[MarketEntry | None]],
    poll_interval_seconds: float = 5.0,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    """Background task: poll held tickers every poll_interval_seconds when disconnected.

    Cancel the returned coroutine to stop. Designed to run alongside
    ActiveTrader.run() - when the book reconnects, polls become no-ops.
    """
    while True:
        await sleep(poll_interval_seconds)
        if not book.is_connected():
            tickers = held_tickers_source()
            if tickers:
                await poll_held_tickers_once(
                    book=book,
                    held_tickers=tickers,
                    fetch_entry=fetch_entry,
                )

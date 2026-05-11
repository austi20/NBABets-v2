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
            except TimeoutError:
                self._task.cancel()
                try:
                    await self._task
                except (asyncio.CancelledError, Exception):
                    pass
        self._consumer = None
        self._task = None

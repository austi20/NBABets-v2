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
        self._update_event: asyncio.Event | None = None

    def _get_event(self) -> asyncio.Event:
        if self._update_event is None:
            self._update_event = asyncio.Event()
        return self._update_event

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
        try:
            self._get_event().set()
        except RuntimeError:
            # No running event loop (e.g. called from sync context during tests)
            pass

    async def wait_for_update(self, *, timeout: float | None = None) -> bool:
        event = self._get_event()
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
        except TimeoutError:
            return False
        finally:
            event.clear()
        return True

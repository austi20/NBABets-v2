# app/trading/brain_auto_resync.py
from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable

from app.trading.stream_publisher import TradingStreamPublisher

_log = logging.getLogger("nba.trading.brain_resync")


class BrainAutoResync:
    """Periodically re-syncs the decision brain while in supervised-live mode.

    Pause-while-observing semantics: the timer keeps ticking but the sync is
    skipped when mode_fn returns anything other than "supervised-live".
    """

    def __init__(
        self,
        *,
        interval_seconds: float,
        sync_fn: Callable[[], object],
        mode_fn: Callable[[], str],
        publisher: TradingStreamPublisher,
    ) -> None:
        self._interval = interval_seconds
        self._sync_fn = sync_fn
        self._mode_fn = mode_fn
        self._publisher = publisher
        self._task: asyncio.Task[None] | None = None
        self._stop_event: asyncio.Event | None = None

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stop_event = asyncio.Event()
        self._task = asyncio.create_task(self._loop(), name="brain-auto-resync")

    async def stop(self) -> None:
        if self._task is None:
            return
        if self._stop_event is not None:
            self._stop_event.set()
        self._task.cancel()
        try:
            await self._task
        except (asyncio.CancelledError, Exception):  # noqa: BLE001
            pass
        finally:
            self._task = None
            self._stop_event = None

    async def _loop(self) -> None:
        stop = self._stop_event
        assert stop is not None
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=self._interval)
                return
            except TimeoutError:
                pass
            if self._mode_fn() != "supervised-live":
                continue
            try:
                result = await asyncio.to_thread(self._sync_fn)
                state = getattr(result, "state", "unknown")
                ticker = getattr(result, "selected_ticker", None) or "-"
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

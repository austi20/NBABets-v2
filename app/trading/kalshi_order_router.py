from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any, TypeVar
from uuid import uuid4

from app.providers.exchanges.kalshi_client import KalshiClient
from app.providers.exchanges.kalshi_errors import KalshiRateLimited, KalshiServerError
from app.trading.portfolio_manager import OrderHandle

_log = logging.getLogger(__name__)

_T = TypeVar("_T")

_MAX_RETRIES = 3
_BACKOFF_BASE_SECONDS = 1.0
_BACKOFF_CAP_SECONDS = 30.0


class SellExhaustedError(RuntimeError):
    """Raised when a sell order exhausts all rate-limit retries.

    Sells are critical — a position that cannot be exited must be escalated
    immediately rather than silently dropped.
    """


class KalshiOrderRouter:
    """Async OrderRouter wrapping the synchronous KalshiClient.

    Blocking client calls are dispatched via asyncio.to_thread so the event
    loop is never stalled. All write operations share an RPS semaphore and
    are wrapped in retry logic for 429 and 5xx responses.
    """

    def __init__(
        self,
        client: KalshiClient,
        *,
        rps_cap: int = 5,
        fill_poll_attempts: int = 5,
        fill_poll_interval_seconds: float = 0.5,
    ) -> None:
        self._client = client
        self._rps_sem = asyncio.Semaphore(max(1, int(rps_cap)))
        self._fill_poll_attempts = int(fill_poll_attempts)
        self._fill_poll_interval = float(fill_poll_interval_seconds)

    # ------------------------------------------------------------------ write ops

    async def place_buy(self, ticker: str, qty: int, limit_price_cents: int) -> OrderHandle:
        client_order_id = str(uuid4())

        async def _call() -> dict[str, Any]:
            return await asyncio.to_thread(
                self._client.create_order,
                ticker=ticker,
                side="bid",
                count=qty,
                price_dollars=limit_price_cents / 100,
                client_order_id=client_order_id,
                time_in_force="immediate_or_cancel",
            )

        resp = await self._guarded(_call, is_sell=False)
        return OrderHandle(order_id=_extract_order_id(resp), ticker=ticker, side="buy")

    async def place_sell(self, ticker: str, qty: int, limit_price_cents: int) -> OrderHandle:
        client_order_id = str(uuid4())

        async def _call() -> dict[str, Any]:
            return await asyncio.to_thread(
                self._client.create_exit_order,
                ticker=ticker,
                held_side="yes",
                count=qty,
                exit_price_dollars=limit_price_cents / 100,
                client_order_id=client_order_id,
            )

        resp = await self._guarded(_call, is_sell=True)
        return OrderHandle(order_id=_extract_order_id(resp), ticker=ticker, side="sell")

    async def cancel(self, handle: OrderHandle) -> None:
        await asyncio.to_thread(self._client.cancel_order, handle.order_id)

    # ------------------------------------------------------------------ fills

    async def deliver_fills(
        self,
        handle: OrderHandle,
        on_fill: Callable[[OrderHandle, int, int], Awaitable[None]],
    ) -> None:
        """Poll get_fills and drive on_fill for each new fill record."""
        seen: set[str] = set()
        for _ in range(self._fill_poll_attempts):
            if self._fill_poll_interval > 0:
                await asyncio.sleep(self._fill_poll_interval)
            resp: dict[str, Any] = await asyncio.to_thread(
                self._client.get_fills, order_id=handle.order_id
            )
            for fill in resp.get("fills", []):
                fill_id = str(fill.get("fill_id", ""))
                if fill_id in seen:
                    continue
                seen.add(fill_id)
                filled_qty = int(fill.get("count", 0))
                if filled_qty <= 0:
                    continue
                avg_price_cents = round(float(fill.get("price", 0)) * 100)
                await on_fill(handle, filled_qty, avg_price_cents)

    # ------------------------------------------------------------------ internals

    async def _guarded(
        self,
        call: Callable[[], Coroutine[Any, Any, _T]],
        *,
        is_sell: bool,
    ) -> _T:
        """Acquire RPS token per attempt, retry on 429/5xx/timeout up to _MAX_RETRIES times.

        The semaphore is held only for the duration of each individual call via
        ``async with``, guaranteeing symmetric release even on exception.
        Backoff sleeps happen outside the semaphore scope so the permit is not
        held while waiting.
        """
        last_exc: Exception | None = None
        wait: float = 0.0
        for attempt in range(_MAX_RETRIES + 1):
            if wait > 0:
                await asyncio.sleep(wait)
            async with self._rps_sem:
                try:
                    return await call()
                except KalshiRateLimited as exc:
                    last_exc = exc
                    if attempt >= _MAX_RETRIES:
                        break
                    wait = max(float(exc.retry_after), 0.0)
                    _log.warning("rate limited (attempt %d/%d), sleeping %.1fs", attempt + 1, _MAX_RETRIES, wait)
                except KalshiServerError as exc:
                    last_exc = exc
                    if attempt >= _MAX_RETRIES:
                        break
                    wait = min(_BACKOFF_CAP_SECONDS, _BACKOFF_BASE_SECONDS * (2**attempt) * (0.5 + random.random()))
                    _log.warning("5xx error (attempt %d/%d), backoff %.1fs", attempt + 1, _MAX_RETRIES, wait)
                except TimeoutError as exc:
                    last_exc = exc
                    if attempt >= _MAX_RETRIES:
                        break
                    wait = min(_BACKOFF_CAP_SECONDS, _BACKOFF_BASE_SECONDS * (2**attempt) * (0.5 + random.random()))
                    _log.warning("timeout (attempt %d/%d), backoff %.1fs", attempt + 1, _MAX_RETRIES, wait)

        assert last_exc is not None
        if is_sell:
            _log.error("sell order exhausted retries: %s", last_exc)
            raise SellExhaustedError(f"sell exhausted after {_MAX_RETRIES} retries") from last_exc
        raise last_exc


def _extract_order_id(resp: dict[str, Any]) -> str:
    order_id = resp.get("order_id") or resp.get("order", {}).get("order_id")
    if not order_id:
        raise ValueError(f"no order_id in Kalshi response: {resp!r}")
    return str(order_id)

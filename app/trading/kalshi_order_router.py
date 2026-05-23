from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import uuid4

from app.providers.exchanges.kalshi_client import KalshiClient
from app.trading.portfolio_manager import OrderHandle


class KalshiOrderRouter:
    """Async OrderRouter wrapping the synchronous KalshiClient.

    Blocking client calls are dispatched via asyncio.to_thread so the event
    loop is never stalled.
    """

    def __init__(
        self,
        client: KalshiClient,
        *,
        fill_poll_attempts: int = 5,
        fill_poll_interval_seconds: float = 0.5,
    ) -> None:
        self._client = client
        self._fill_poll_attempts = int(fill_poll_attempts)
        self._fill_poll_interval = float(fill_poll_interval_seconds)

    async def place_buy(self, ticker: str, qty: int, limit_price_cents: int) -> OrderHandle:
        resp = await asyncio.to_thread(
            self._client.create_order,
            ticker=ticker,
            side="bid",
            count=qty,
            price_dollars=limit_price_cents / 100,
            client_order_id=str(uuid4()),
            time_in_force="immediate_or_cancel",
        )
        return OrderHandle(order_id=_extract_order_id(resp), ticker=ticker, side="buy")

    async def place_sell(self, ticker: str, qty: int, limit_price_cents: int) -> OrderHandle:
        resp = await asyncio.to_thread(
            self._client.create_exit_order,
            ticker=ticker,
            held_side="yes",
            count=qty,
            exit_price_dollars=limit_price_cents / 100,
            client_order_id=str(uuid4()),
        )
        return OrderHandle(order_id=_extract_order_id(resp), ticker=ticker, side="sell")

    async def cancel(self, handle: OrderHandle) -> None:
        await asyncio.to_thread(self._client.cancel_order, handle.order_id)

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


def _extract_order_id(resp: dict[str, Any]) -> str:
    order_id = resp.get("order_id") or resp.get("order", {}).get("order_id")
    if not order_id:
        raise ValueError(f"no order_id in Kalshi response: {resp!r}")
    return str(order_id)

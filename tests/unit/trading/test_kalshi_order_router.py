from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from app.trading.kalshi_order_router import KalshiOrderRouter
from app.trading.portfolio_manager import OrderHandle


def _mock_client(
    *,
    create_order_resp: dict[str, Any] | None = None,
    create_exit_resp: dict[str, Any] | None = None,
    cancel_resp: dict[str, Any] | None = None,
    fills_resp: dict[str, Any] | None = None,
) -> MagicMock:
    client = MagicMock()
    client.create_order.return_value = create_order_resp or {"order": {"order_id": "buy-001"}}
    client.create_exit_order.return_value = create_exit_resp or {"order": {"order_id": "sell-001"}}
    client.cancel_order.return_value = cancel_resp or {}
    client.get_fills.return_value = fills_resp or {"fills": []}
    return client


# ---------------------------------------------------------------------------
# P3.1a: place_buy
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_place_buy_calls_create_order_with_bid_and_ioc():
    client = _mock_client()
    router = KalshiOrderRouter(client, fill_poll_attempts=0)

    await router.place_buy("NBA-TEST-OVER", qty=2, limit_price_cents=55)

    client.create_order.assert_called_once()
    kwargs = client.create_order.call_args.kwargs
    assert kwargs["ticker"] == "NBA-TEST-OVER"
    assert kwargs["side"] == "bid"
    assert kwargs["count"] == 2
    assert abs(float(kwargs["price_dollars"]) - 0.55) < 1e-9
    assert kwargs["time_in_force"] == "immediate_or_cancel"


@pytest.mark.asyncio
async def test_place_buy_returns_correct_handle():
    client = _mock_client(create_order_resp={"order": {"order_id": "abc-123"}})
    router = KalshiOrderRouter(client, fill_poll_attempts=0)

    handle = await router.place_buy("TICKER", qty=1, limit_price_cents=40)

    assert handle == OrderHandle(order_id="abc-123", ticker="TICKER", side="buy")


@pytest.mark.asyncio
async def test_place_buy_accepts_flat_order_id_response():
    client = _mock_client(create_order_resp={"order_id": "flat-456"})
    router = KalshiOrderRouter(client, fill_poll_attempts=0)

    handle = await router.place_buy("T", qty=1, limit_price_cents=30)
    assert handle.order_id == "flat-456"


# ---------------------------------------------------------------------------
# P3.1b: place_sell
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_place_sell_calls_create_exit_order_with_held_yes():
    client = _mock_client()
    router = KalshiOrderRouter(client, fill_poll_attempts=0)

    await router.place_sell("NBA-TEST-OVER", qty=2, limit_price_cents=70)

    client.create_exit_order.assert_called_once()
    kwargs = client.create_exit_order.call_args.kwargs
    assert kwargs["ticker"] == "NBA-TEST-OVER"
    assert kwargs["held_side"] == "yes"
    assert kwargs["count"] == 2
    assert abs(float(kwargs["exit_price_dollars"]) - 0.70) < 1e-9


@pytest.mark.asyncio
async def test_place_sell_returns_correct_handle():
    client = _mock_client(create_exit_resp={"order": {"order_id": "sell-789"}})
    router = KalshiOrderRouter(client, fill_poll_attempts=0)

    handle = await router.place_sell("TICKER", qty=1, limit_price_cents=65)

    assert handle == OrderHandle(order_id="sell-789", ticker="TICKER", side="sell")


# ---------------------------------------------------------------------------
# P3.1c: cancel
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancel_calls_cancel_order_with_handle_order_id():
    client = _mock_client()
    router = KalshiOrderRouter(client)
    handle = OrderHandle(order_id="ord-999", ticker="T", side="buy")

    await router.cancel(handle)

    client.cancel_order.assert_called_once_with("ord-999")


# ---------------------------------------------------------------------------
# P3.1d: fill delivery
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_deliver_fills_calls_on_fill_with_cents():
    fills_resp = {
        "fills": [
            {"fill_id": "f1", "count": 2, "price": 0.55},
        ]
    }
    client = _mock_client(fills_resp=fills_resp)
    router = KalshiOrderRouter(client, fill_poll_attempts=1, fill_poll_interval_seconds=0.0)
    handle = OrderHandle(order_id="ord-1", ticker="T", side="buy")

    calls: list[tuple[OrderHandle, int, int]] = []

    async def on_fill(h: OrderHandle, qty: int, price_cents: int) -> None:
        calls.append((h, qty, price_cents))

    await router.deliver_fills(handle, on_fill)

    assert len(calls) == 1
    assert calls[0] == (handle, 2, 55)


@pytest.mark.asyncio
async def test_deliver_fills_deduplicates_by_fill_id():
    fills_resp = {
        "fills": [
            {"fill_id": "f1", "count": 1, "price": 0.40},
        ]
    }
    client = _mock_client(fills_resp=fills_resp)
    # two polling attempts — same fill should only fire once
    router = KalshiOrderRouter(client, fill_poll_attempts=2, fill_poll_interval_seconds=0.0)
    handle = OrderHandle(order_id="ord-2", ticker="T", side="buy")

    calls: list[int] = []

    async def on_fill(h: OrderHandle, qty: int, price_cents: int) -> None:
        calls.append(qty)

    await router.deliver_fills(handle, on_fill)

    assert len(calls) == 1


@pytest.mark.asyncio
async def test_deliver_fills_skips_zero_count_fills():
    fills_resp = {"fills": [{"fill_id": "f0", "count": 0, "price": 0.50}]}
    client = _mock_client(fills_resp=fills_resp)
    router = KalshiOrderRouter(client, fill_poll_attempts=1, fill_poll_interval_seconds=0.0)
    handle = OrderHandle(order_id="ord-3", ticker="T", side="buy")
    calls: list[int] = []

    async def on_fill(h: OrderHandle, qty: int, price_cents: int) -> None:
        calls.append(qty)

    await router.deliver_fills(handle, on_fill)
    assert calls == []


@pytest.mark.asyncio
async def test_deliver_fills_no_fills_does_not_call_on_fill():
    client = _mock_client(fills_resp={"fills": []})
    router = KalshiOrderRouter(client, fill_poll_attempts=1, fill_poll_interval_seconds=0.0)
    handle = OrderHandle(order_id="ord-4", ticker="T", side="buy")
    called = False

    async def on_fill(h: OrderHandle, qty: int, price_cents: int) -> None:
        nonlocal called
        called = True

    await router.deliver_fills(handle, on_fill)
    assert not called


# ---------------------------------------------------------------------------
# CRITICAL fixes: symmetric semaphore + idempotent client_order_id
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rps_permit_released_in_finally_even_on_error():
    """Semaphore must be released even when the call raises."""
    client = MagicMock()
    client.create_order = MagicMock(side_effect=RuntimeError("boom"))
    router = KalshiOrderRouter(client=client, rps_cap=2)
    with pytest.raises(RuntimeError):
        await router.place_buy(ticker="X", qty=1, limit_price_cents=10)
    # After the failed call, both permits must be available immediately.
    assert router._rps_sem._value == 2


@pytest.mark.asyncio
async def test_retry_reuses_same_client_order_id():
    """client_order_id must be stable across retries."""
    client = MagicMock()
    seen_ids: list = []
    call_count = 0

    def stub(**kwargs):
        nonlocal call_count
        seen_ids.append(kwargs.get("client_order_id"))
        call_count += 1
        if call_count == 1:
            raise TimeoutError("first call timeout")
        return {"order_id": "abc"}

    client.create_order = MagicMock(side_effect=stub)
    router = KalshiOrderRouter(client=client, rps_cap=4)
    await router.place_buy(ticker="X", qty=1, limit_price_cents=10)
    assert len(seen_ids) == 2
    assert seen_ids[0] == seen_ids[1], "client_order_id must be stable across retries"

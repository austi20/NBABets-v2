from __future__ import annotations

import asyncio
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.providers.exchanges.kalshi_errors import KalshiRateLimited, KalshiServerError
from app.trading.kalshi_order_router import KalshiOrderRouter, SellExhaustedError


def _make_client(**overrides: Any) -> MagicMock:
    client = MagicMock()
    client.create_order.return_value = overrides.get("create_order", {"order": {"order_id": "o1"}})
    client.create_exit_order.return_value = overrides.get("create_exit_order", {"order": {"order_id": "o2"}})
    client.cancel_order.return_value = {}
    client.get_fills.return_value = {"fills": []}
    return client


def _rate_limited(retry_after: int = 1) -> KalshiRateLimited:
    return KalshiRateLimited(429, b"rate limited", retry_after)


def _server_error() -> KalshiServerError:
    return KalshiServerError(500, b"server error")


# ---------------------------------------------------------------------------
# P3.2a: retry on KalshiRateLimited
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_place_buy_retries_on_rate_limit_then_succeeds():
    client = _make_client()
    client.create_order.side_effect = [
        _rate_limited(retry_after=0),
        {"order": {"order_id": "ok-1"}},
    ]
    router = KalshiOrderRouter(client, rps_cap=10)

    with patch("app.trading.kalshi_order_router.asyncio.sleep", new_callable=AsyncMock):
        handle = await router.place_buy("T", qty=1, limit_price_cents=50)

    assert handle.order_id == "ok-1"
    assert client.create_order.call_count == 2


@pytest.mark.asyncio
async def test_place_buy_honors_retry_after_sleep():
    client = _make_client()
    client.create_order.side_effect = [
        _rate_limited(retry_after=3),
        {"order": {"order_id": "ok-2"}},
    ]
    router = KalshiOrderRouter(client, rps_cap=10)

    sleep_calls: list[float] = []

    async def fake_sleep(secs: float) -> None:
        sleep_calls.append(secs)

    with patch("app.trading.kalshi_order_router.asyncio.sleep", side_effect=fake_sleep):
        await router.place_buy("T", qty=1, limit_price_cents=50)

    assert any(s >= 3 for s in sleep_calls)


@pytest.mark.asyncio
async def test_buy_exhausts_retries_and_raises():
    client = _make_client()
    client.create_order.side_effect = _rate_limited(retry_after=0)
    router = KalshiOrderRouter(client, rps_cap=10)

    with patch("app.trading.kalshi_order_router.asyncio.sleep", new_callable=AsyncMock):
        with pytest.raises(KalshiRateLimited):
            await router.place_buy("T", qty=1, limit_price_cents=50)


@pytest.mark.asyncio
async def test_sell_exhausts_retries_and_raises_sell_exhausted():
    client = _make_client()
    client.create_exit_order.side_effect = _rate_limited(retry_after=0)
    router = KalshiOrderRouter(client, rps_cap=10)

    with patch("app.trading.kalshi_order_router.asyncio.sleep", new_callable=AsyncMock):
        with pytest.raises(SellExhaustedError):
            await router.place_sell("T", qty=1, limit_price_cents=50)


# ---------------------------------------------------------------------------
# P3.2b: exponential backoff on 5xx
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_place_buy_retries_on_5xx_with_backoff():
    client = _make_client()
    client.create_order.side_effect = [
        _server_error(),
        _server_error(),
        {"order": {"order_id": "ok-3"}},
    ]
    router = KalshiOrderRouter(client, rps_cap=10)

    sleep_calls: list[float] = []

    async def fake_sleep(secs: float) -> None:
        sleep_calls.append(secs)

    with patch("app.trading.kalshi_order_router.asyncio.sleep", side_effect=fake_sleep):
        handle = await router.place_buy("T", qty=1, limit_price_cents=50)

    assert handle.order_id == "ok-3"
    assert client.create_order.call_count == 3
    # backoff grows: second sleep >= first sleep
    assert len(sleep_calls) >= 2
    assert sleep_calls[1] >= sleep_calls[0]


# ---------------------------------------------------------------------------
# P3.2c: RPS cap serialises writes
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rps_cap_serialises_concurrent_writes():
    """With rps_cap=1 two concurrent buys must stagger by ~1 s."""
    client = _make_client()
    router = KalshiOrderRouter(client, rps_cap=1)

    start_times: list[float] = []

    real_to_thread = asyncio.to_thread

    async def recording_to_thread(fn, *args, **kwargs):  # type: ignore[override]
        start_times.append(time.monotonic())
        return await real_to_thread(fn, *args, **kwargs)

    with patch("app.trading.kalshi_order_router.asyncio.to_thread", side_effect=recording_to_thread):
        await asyncio.gather(
            router.place_buy("A", qty=1, limit_price_cents=50),
            router.place_buy("B", qty=1, limit_price_cents=50),
        )

    assert len(start_times) == 2
    gap = abs(start_times[1] - start_times[0])
    assert gap >= 0.9, f"Expected >= 0.9s gap between writes, got {gap:.3f}s"

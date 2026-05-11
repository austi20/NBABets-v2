# tests/unit/trading/test_stream_publisher.py
from __future__ import annotations

import asyncio

import pytest

from app.trading.stream_publisher import TradingStreamPublisher


def test_ring_buffer_caps_at_250_lines() -> None:
    pub = TradingStreamPublisher(event_log_capacity=250)
    for i in range(300):
        pub.log_event(level="info", message=f"line {i}")
    log = pub.event_log_snapshot()
    assert len(log) == 250
    assert log[0].message == "line 50"
    assert log[-1].message == "line 299"


def test_monotonic_cursor() -> None:
    pub = TradingStreamPublisher()
    pub.log_event(level="info", message="a")
    pub.log_event(level="warn", message="b")
    log = pub.event_log_snapshot()
    assert log[0].cursor < log[1].cursor


def test_event_log_since_cursor_returns_only_new() -> None:
    pub = TradingStreamPublisher()
    pub.log_event(level="info", message="a")
    pub.log_event(level="info", message="b")
    cursor_after_first = pub.event_log_snapshot()[0].cursor
    new_lines = pub.event_log_since(cursor_after_first)
    assert len(new_lines) == 1
    assert new_lines[0].message == "b"


@pytest.mark.asyncio
async def test_notify_wakes_waiting_subscribers() -> None:
    pub = TradingStreamPublisher()
    woken = asyncio.Event()

    async def waiter() -> None:
        await pub.wait_for_update(timeout=1.0)
        woken.set()

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0.05)
    pub.notify()
    await asyncio.wait_for(woken.wait(), timeout=1.0)
    task.cancel()

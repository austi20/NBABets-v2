import asyncio
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime

import pytest

from app.trading.market_book import BookUpdate, MarketBook, MarketEntry


def test_market_entry_is_frozen_dataclass():
    entry = MarketEntry(
        ticker="KXTEST-A",
        yes_bid=0.50,
        yes_ask=0.55,
        no_bid=0.45,
        no_ask=0.50,
        last=0.52,
        spread=0.05,
        status="open",
        updated_at=datetime(2026, 5, 10, tzinfo=UTC),
    )
    assert entry.ticker == "KXTEST-A"
    with pytest.raises(FrozenInstanceError):
        entry.ticker = "MUTATED"  # type: ignore[misc]


def test_market_entry_optional_prices_default_none():
    entry = MarketEntry(
        ticker="KXTEST-B",
        yes_bid=None,
        yes_ask=None,
        no_bid=None,
        no_ask=None,
        last=None,
        spread=None,
        status="unknown",
        updated_at=datetime(2026, 5, 10, tzinfo=UTC),
    )
    assert entry.yes_bid is None
    assert entry.spread is None


def test_book_update_carries_before_after():
    before = MarketEntry(
        ticker="KXTEST-C",
        yes_bid=0.50,
        yes_ask=0.55,
        no_bid=0.45,
        no_ask=0.50,
        last=None,
        spread=0.05,
        status="open",
        updated_at=datetime(2026, 5, 10, 12, 0, tzinfo=UTC),
    )
    after = MarketEntry(
        ticker="KXTEST-C",
        yes_bid=0.51,
        yes_ask=0.55,
        no_bid=0.45,
        no_ask=0.49,
        last=None,
        spread=0.04,
        status="open",
        updated_at=datetime(2026, 5, 10, 12, 0, 1, tzinfo=UTC),
    )
    update = BookUpdate(ticker="KXTEST-C", before=before, after=after)
    assert update.ticker == "KXTEST-C"
    assert update.before is before
    assert update.after is after


def _entry(ticker: str, yes_bid: float, ts_seconds: int = 0) -> MarketEntry:
    return MarketEntry(
        ticker=ticker,
        yes_bid=yes_bid,
        yes_ask=yes_bid + 0.02,
        no_bid=1.0 - (yes_bid + 0.02),
        no_ask=1.0 - yes_bid,
        last=yes_bid,
        spread=0.02,
        status="open",
        updated_at=datetime(2026, 5, 10, 12, 0, ts_seconds, tzinfo=UTC),
    )


def test_get_returns_none_for_unknown_ticker():
    book = MarketBook()
    assert book.get("MISSING") is None


def test_update_then_get_returns_entry():
    book = MarketBook()
    entry = _entry("KXTEST-D", 0.50)

    async def runner():
        await book.update(entry)
        return book.get("KXTEST-D")

    assert asyncio.run(runner()) == entry


def test_update_returns_book_update_with_before_none_on_first_write():
    book = MarketBook()
    entry = _entry("KXTEST-E", 0.50)

    async def runner():
        return await book.update(entry)

    update = asyncio.run(runner())
    assert update.ticker == "KXTEST-E"
    assert update.before is None
    assert update.after == entry


def test_update_returns_before_after_on_second_write():
    book = MarketBook()
    first = _entry("KXTEST-F", 0.50, ts_seconds=0)
    second = _entry("KXTEST-F", 0.55, ts_seconds=1)

    async def runner():
        await book.update(first)
        return await book.update(second)

    update = asyncio.run(runner())
    assert update.before == first
    assert update.after == second


def test_snapshot_returns_independent_copy():
    book = MarketBook()
    entry = _entry("KXTEST-G", 0.50)

    async def runner():
        await book.update(entry)
        snap = book.snapshot()
        await book.update(_entry("KXTEST-G", 0.99, ts_seconds=2))
        return snap

    snap = asyncio.run(runner())
    assert snap["KXTEST-G"].yes_bid == 0.50  # snap not mutated by later update


@pytest.mark.asyncio
async def test_subscribe_receives_updates():
    book = MarketBook()
    received: list[BookUpdate] = []

    async def consumer():
        async for upd in book.subscribe():
            received.append(upd)
            if len(received) == 2:
                return

    task = asyncio.create_task(consumer())
    await asyncio.sleep(0.01)  # let subscribe register
    await book.update(_entry("KXTEST-H", 0.50))
    await book.update(_entry("KXTEST-H", 0.55, ts_seconds=1))
    await asyncio.wait_for(task, timeout=1.0)
    assert [u.after.yes_bid for u in received] == [0.50, 0.55]


@pytest.mark.asyncio
async def test_multiple_subscribers_each_receive_updates():
    book = MarketBook()

    async def collect(n: int) -> list[BookUpdate]:
        out: list[BookUpdate] = []
        async for upd in book.subscribe():
            out.append(upd)
            if len(out) == n:
                return out
        return out

    t1 = asyncio.create_task(collect(1))
    t2 = asyncio.create_task(collect(1))
    await asyncio.sleep(0.01)
    await book.update(_entry("KXTEST-I", 0.50))
    r1 = await asyncio.wait_for(t1, timeout=1.0)
    r2 = await asyncio.wait_for(t2, timeout=1.0)
    assert r1[0].after.ticker == "KXTEST-I"
    assert r2[0].after.ticker == "KXTEST-I"


@pytest.mark.asyncio
async def test_subscribe_works_as_async_context_manager():
    book = MarketBook()
    received: list[BookUpdate] = []

    async def consume():
        async with book.subscribe() as sub:
            async for upd in sub:
                received.append(upd)
                if len(received) == 1:
                    return

    task = asyncio.create_task(consume())
    await asyncio.sleep(0.01)
    await book.update(_entry("KXTEST-K", 0.50))
    await asyncio.wait_for(task, timeout=1.0)
    assert received[0].after.ticker == "KXTEST-K"
    # subscriber should have been removed on __aexit__
    assert len(book._subscribers) == 0


@pytest.mark.asyncio
async def test_slow_subscriber_drops_oldest_under_backpressure():
    book = MarketBook(subscriber_queue_size=2)
    drained: list[BookUpdate] = []

    sub = book.subscribe()
    await asyncio.sleep(0.01)
    for i in range(5):
        await book.update(_entry("KXTEST-J", 0.50 + i * 0.01, ts_seconds=i))
    # consumer wakes up; should only see the last 2 updates retained
    async def drain():
        async for upd in sub:
            drained.append(upd)
            if len(drained) == 2:
                return
    await asyncio.wait_for(drain(), timeout=1.0)
    assert [round(u.after.yes_bid, 2) for u in drained] == [0.53, 0.54]

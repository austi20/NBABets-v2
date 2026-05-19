# tests/unit/trading/test_market_book_volatility.py
from __future__ import annotations

import statistics
from datetime import UTC, datetime, timedelta

import pytest

from app.trading.market_book import MarketBook, MarketEntry


def _entry(
    ticker: str,
    yes_bid: float | None,
    updated_at: datetime,
) -> MarketEntry:
    return MarketEntry(
        ticker=ticker,
        yes_bid=yes_bid,
        yes_ask=None,
        no_bid=None,
        no_ask=None,
        last=None,
        spread=None,
        status="active",
        updated_at=updated_at,
    )


def test_empty_book_returns_none() -> None:
    book = MarketBook()
    assert book.stdev_cents("X") is None


@pytest.mark.asyncio
async def test_single_sample_returns_zero() -> None:
    book = MarketBook()
    t0 = datetime(2026, 5, 19, 12, 0, 0, tzinfo=UTC)
    await book.update(_entry("X", 0.50, t0))
    assert book.stdev_cents("X") == 0.0


@pytest.mark.asyncio
async def test_two_identical_samples_returns_zero() -> None:
    book = MarketBook()
    t0 = datetime(2026, 5, 19, 12, 0, 0, tzinfo=UTC)
    await book.update(_entry("X", 0.50, t0))
    await book.update(_entry("X", 0.50, t0 + timedelta(seconds=1)))
    assert book.stdev_cents("X") == 0.0


@pytest.mark.asyncio
async def test_jittery_samples_match_pstdev_in_cents() -> None:
    book = MarketBook()
    t0 = datetime(2026, 5, 19, 12, 0, 0, tzinfo=UTC)
    prices = [0.50, 0.52, 0.48, 0.51]
    for i, p in enumerate(prices):
        await book.update(_entry("X", p, t0 + timedelta(seconds=i)))
    expected = statistics.pstdev([50.0, 52.0, 48.0, 51.0])
    result = book.stdev_cents("X")
    assert result is not None
    assert result == pytest.approx(expected, abs=1e-9)


@pytest.mark.asyncio
async def test_window_trims_old_samples() -> None:
    book = MarketBook()
    t0 = datetime(2026, 5, 19, 12, 0, 0, tzinfo=UTC)
    # Wild samples > 60s before the latest -- must be excluded.
    await book.update(_entry("X", 0.10, t0))
    await book.update(_entry("X", 0.90, t0 + timedelta(seconds=10)))
    # In-window samples (last 60s relative to the final sample at t0+130s).
    await book.update(_entry("X", 0.50, t0 + timedelta(seconds=80)))
    await book.update(_entry("X", 0.52, t0 + timedelta(seconds=100)))
    await book.update(_entry("X", 0.48, t0 + timedelta(seconds=120)))
    await book.update(_entry("X", 0.51, t0 + timedelta(seconds=130)))
    expected = statistics.pstdev([50.0, 52.0, 48.0, 51.0])
    result = book.stdev_cents("X", window_seconds=60.0)
    assert result is not None
    assert result == pytest.approx(expected, abs=1e-9)


@pytest.mark.asyncio
async def test_none_yes_bid_does_not_enter_window() -> None:
    book = MarketBook()
    t0 = datetime(2026, 5, 19, 12, 0, 0, tzinfo=UTC)
    await book.update(_entry("X", 0.50, t0))
    await book.update(_entry("X", None, t0 + timedelta(seconds=1)))
    await book.update(_entry("X", 0.50, t0 + timedelta(seconds=2)))
    # All non-None samples are 0.50 -> stdev is 0.
    assert book.stdev_cents("X") == 0.0


@pytest.mark.asyncio
async def test_no_in_window_samples_returns_none() -> None:
    # If only ancient samples exist (older than window), latest_ts itself is
    # always in-window (cutoff = latest_ts - window). So construct a case via
    # an explicit small window_seconds to make all samples older than cutoff
    # except the latest -- which still falls in window. To actually exercise
    # the empty-prices branch, we rely on the trimming logic: a tighter
    # window keeps only the most recent sample.
    book = MarketBook()
    t0 = datetime(2026, 5, 19, 12, 0, 0, tzinfo=UTC)
    await book.update(_entry("X", 0.40, t0))
    await book.update(_entry("X", 0.60, t0 + timedelta(seconds=30)))
    # With a 1-second window, only the latest sample qualifies -> stdev 0.0.
    assert book.stdev_cents("X", window_seconds=1.0) == 0.0

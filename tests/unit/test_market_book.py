from dataclasses import FrozenInstanceError
from datetime import UTC, datetime

import pytest
from app.trading.market_book import BookUpdate, MarketEntry


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

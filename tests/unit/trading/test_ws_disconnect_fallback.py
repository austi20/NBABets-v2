"""P3.7 tests: WS disconnect fallback - pause entries, poll held positions."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.trading.active_trader import ActiveCandidate, ActiveTrader
from app.trading.live_limits import ACTIVE_TRADER_DEFAULTS
from app.trading.market_book import MarketBook, MarketEntry
from app.trading.portfolio_manager import PortfolioManager
from app.trading.risk import RiskLimits

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime(2026, 5, 23, 12, 0, 0, tzinfo=UTC)


def _entry(ticker: str = "NBA-A", *, bid: float = 0.48, ask: float = 0.52) -> MarketEntry:
    return MarketEntry(
        ticker=ticker, yes_bid=bid, yes_ask=ask,
        no_bid=None, no_ask=None, last=None, spread=None,
        status="active", updated_at=_now(),
    )


def _cand(ticker: str = "NBA-A") -> ActiveCandidate:
    return ActiveCandidate(
        ticker=ticker, model_prob=0.75, edge_bps=300.0,
        line_value=24.5, market_close_at=None, stake_cents=500,
    )


def _build(book: MarketBook | None = None) -> tuple[ActiveTrader, MarketBook, MagicMock]:
    cfg = replace(ACTIVE_TRADER_DEFAULTS, enabled=True)
    clock = _now
    router = MagicMock()
    router.place_buy = AsyncMock(return_value=MagicMock(order_id="b1", ticker="NBA-A", side="buy"))
    router.place_sell = AsyncMock(return_value=MagicMock(order_id="s1", ticker="NBA-A", side="sell"))
    router.cancel = AsyncMock()
    book = book or MarketBook()
    pm = PortfolioManager(config=cfg, router=router, clock=clock)
    src = MagicMock()
    src.candidates = AsyncMock(return_value=[_cand()])
    trader = ActiveTrader(
        config=cfg,
        risk_limits=RiskLimits(),
        book=book,
        portfolio=pm,
        candidates_source=src,
    )
    return trader, book, router


# ---------------------------------------------------------------------------
# P3.7a: MarketBook connectivity state
# ---------------------------------------------------------------------------


def test_market_book_connected_by_default() -> None:
    book = MarketBook()
    assert book.is_connected() is True


def test_market_book_mark_disconnected() -> None:
    book = MarketBook()
    book.mark_connected(False)
    assert book.is_connected() is False


def test_market_book_reconnect() -> None:
    book = MarketBook()
    book.mark_connected(False)
    book.mark_connected(True)
    assert book.is_connected() is True


# ---------------------------------------------------------------------------
# P3.7b: ActiveTrader skips entries when book is disconnected
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_consider_skips_entry_when_book_disconnected() -> None:
    trader, book, router = _build()
    await book.update(_entry())
    book.mark_connected(False)

    await trader._consider(_cand())

    router.place_buy.assert_not_called()


@pytest.mark.asyncio
async def test_consider_allows_entry_when_book_reconnected() -> None:
    trader, book, router = _build()
    await book.update(_entry())
    book.mark_connected(False)
    book.mark_connected(True)

    await trader._consider(_cand())

    router.place_buy.assert_called_once()


# ---------------------------------------------------------------------------
# P3.7c: polling fallback updates book during disconnect
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_poll_held_tickers_updates_book_when_disconnected() -> None:
    from app.trading.ws_disconnect_poller import poll_held_tickers_once

    book = MarketBook()
    book.mark_connected(False)

    fetched: list[str] = []

    async def fake_fetch(ticker: str) -> MarketEntry | None:
        fetched.append(ticker)
        return _entry(ticker, bid=0.50, ask=0.55)

    await poll_held_tickers_once(
        book=book,
        held_tickers=["NBA-A", "NBA-B"],
        fetch_entry=fake_fetch,
    )

    assert fetched == ["NBA-A", "NBA-B"]
    assert book.get("NBA-A") is not None
    assert book.get("NBA-B") is not None


@pytest.mark.asyncio
async def test_poll_held_tickers_noop_when_connected() -> None:
    from app.trading.ws_disconnect_poller import poll_held_tickers_once

    book = MarketBook()
    book.mark_connected(True)

    fetched: list[str] = []

    async def fake_fetch(ticker: str) -> MarketEntry | None:
        fetched.append(ticker)
        return _entry(ticker)

    await poll_held_tickers_once(
        book=book,
        held_tickers=["NBA-A"],
        fetch_entry=fake_fetch,
    )

    assert fetched == []

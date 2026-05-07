from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.base import Base
from app.db.models.trading import (
    TradingDailyPnL,
    TradingFill,
    TradingKillSwitch,
    TradingOrder,
    TradingPosition,
)
from app.trading.sql_ledger import SqlPortfolioLedger
from app.trading.types import Fill, MarketRef, OrderEvent


@pytest.fixture()
def session_factory():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(
        engine,
        tables=[
            TradingOrder.__table__,
            TradingFill.__table__,
            TradingPosition.__table__,
            TradingKillSwitch.__table__,
            TradingDailyPnL.__table__,
        ],
    )
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def _market(symbol: str = "kalshi:points:over:25.5:g0:p237") -> MarketRef:
    return MarketRef(
        exchange="kalshi",
        symbol=symbol,
        market_key="points",
        side="OVER",
        line_value=25.5,
    )


def test_record_fill_creates_position(session_factory) -> None:
    ledger = SqlPortfolioLedger(session_factory)
    fill = Fill(
        fill_id="f1",
        intent_id="i1",
        market=_market(),
        side="buy",
        stake=0.25,
        price=0.40,
        fee=0.0,
        timestamp=datetime.now(UTC),
    )
    ledger.record_fill(fill)
    positions = ledger.open_positions()
    assert len(positions) == 1
    assert positions[0].open_stake == pytest.approx(0.25)
    assert positions[0].avg_price == pytest.approx(0.40)


def test_record_fill_idempotent(session_factory) -> None:
    ledger = SqlPortfolioLedger(session_factory)
    fill = Fill(
        fill_id="f1",
        intent_id="i1",
        market=_market(),
        side="buy",
        stake=0.25,
        price=0.40,
        timestamp=datetime.now(UTC),
    )
    ledger.record_fill(fill)
    ledger.record_fill(fill)  # second call must be a no-op
    assert ledger.open_positions()[0].open_stake == pytest.approx(0.25)


def test_market_exposure_and_open_notional(session_factory) -> None:
    ledger = SqlPortfolioLedger(session_factory)
    ledger.record_fill(
        Fill(fill_id="f1", intent_id="i1", market=_market("a"), side="buy", stake=0.25, price=0.40, timestamp=datetime.now(UTC))
    )
    ledger.record_fill(
        Fill(fill_id="f2", intent_id="i2", market=_market("b"), side="buy", stake=0.50, price=0.50, timestamp=datetime.now(UTC))
    )
    assert ledger.market_exposure("a") == pytest.approx(0.25)
    assert ledger.open_notional() == pytest.approx(0.75)


def test_record_order_event_persists(session_factory) -> None:
    ledger = SqlPortfolioLedger(session_factory)
    event = OrderEvent(intent_id="i1", event_type="rejected", status="blocked", message="kill switch")
    ledger.record_order_event(event)
    # Re-instantiate to prove persistence
    ledger2 = SqlPortfolioLedger(session_factory)
    # No public read API for events in Spec 1, but the order row should exist with status=blocked
    with session_factory() as session:
        order = session.get(TradingOrder, "i1")
        assert order is not None
        assert order.status == "blocked"
        assert "kill switch" in order.message
    _ = ledger2  # silence unused warning


def test_recent_fills_returns_in_reverse_order(session_factory) -> None:
    ledger = SqlPortfolioLedger(session_factory)
    base = datetime.now(UTC)
    for idx in range(3):
        ledger.record_fill(
            Fill(
                fill_id=f"f{idx}",
                intent_id=f"i{idx}",
                market=_market(),
                side="buy",
                stake=0.25,
                price=0.40,
                timestamp=base.replace(microsecond=idx),
            )
        )
    fills = ledger.recent_fills(limit=2)
    assert [f.fill_id for f in fills] == ["f2", "f1"]
    assert fills[0].market.side == "OVER"
    assert fills[0].market.line_value == pytest.approx(25.5)


def test_record_fill_backfills_existing_order_event(session_factory) -> None:
    ledger = SqlPortfolioLedger(session_factory)
    ledger.record_order_event(
        OrderEvent(
            intent_id="i1",
            event_type="accepted",
            status="ok",
            message="kalshi order ord1",
            exchange_order_id="ord1",
        )
    )
    ledger.record_fill(
        Fill(
            fill_id="f1",
            intent_id="i1",
            market=_market(),
            side="buy",
            stake=0.25,
            price=0.40,
            exchange_order_id="ord1",
            exchange_trade_id="trade1",
            timestamp=datetime.now(UTC),
        )
    )
    with session_factory() as session:
        order = session.get(TradingOrder, "i1")
        assert order is not None
        assert order.kalshi_order_id == "ord1"
        assert order.market_symbol == "kalshi:points:over:25.5:g0:p237"
        assert order.market_key == "points"
        assert order.side == "buy"
        assert order.stake == pytest.approx(0.25)
        assert order.status == "filled"
        db_fill = session.get(TradingFill, "f1")
        assert db_fill is not None
        assert db_fill.kalshi_trade_id == "trade1"


def test_positions_keep_yes_no_exposure_separate_for_same_ticker(session_factory) -> None:
    ledger = SqlPortfolioLedger(session_factory)
    now = datetime.now(UTC)
    ledger.record_fill(
        Fill(
            fill_id="yes",
            intent_id="i-yes",
            market=_market("kalshi:points:over:25.5:g1:p237"),
            side="buy",
            stake=0.25,
            price=0.40,
            timestamp=now,
        )
    )
    ledger.record_fill(
        Fill(
            fill_id="no",
            intent_id="i-no",
            market=_market("kalshi:points:under:25.5:g1:p237"),
            side="buy",
            stake=0.25,
            price=0.60,
            timestamp=now,
        )
    )
    positions = sorted(ledger.open_positions(), key=lambda position: position.market_symbol)
    assert len(positions) == 2
    assert positions[0].market_symbol != positions[1].market_symbol


def test_sell_fill_no_double_count_realized_pnl(session_factory) -> None:
    """fill.realized_pnl must not be added to AVCO-computed realized; only AVCO is used."""
    ledger = SqlPortfolioLedger(session_factory)
    market = _market()
    now = datetime.now(UTC)
    ledger.record_fill(Fill(fill_id="b1", intent_id="i1", market=market, side="buy", stake=1.0, price=0.40, timestamp=now))
    # Sell with a non-zero fill.realized_pnl to verify it is NOT added to position pnl.
    sell = Fill(
        fill_id="s1",
        intent_id="i2",
        market=market,
        side="sell",
        stake=1.0,
        price=0.50,
        fee=0.0,
        realized_pnl=99.0,  # exchange-reported value that must be ignored
        timestamp=now,
    )
    ledger.record_fill(sell)
    # Expected: (0.50 - 0.40) * 1.0 = 0.10, NOT 99.10
    assert ledger.daily_realized_pnl() == pytest.approx(0.10)


def test_open_stake_no_negative_float(session_factory) -> None:
    """Selling exactly the open stake must leave open_stake at 0.0, not -epsilon."""
    ledger = SqlPortfolioLedger(session_factory)
    market = _market()
    now = datetime.now(UTC)
    ledger.record_fill(Fill(fill_id="b1", intent_id="i1", market=market, side="buy", stake=0.25, price=0.40, timestamp=now))
    ledger.record_fill(Fill(fill_id="s1", intent_id="i2", market=market, side="sell", stake=0.25, price=0.50, timestamp=now))
    assert ledger.open_positions() == []
    assert ledger.market_exposure(market.symbol) == pytest.approx(0.0)


def test_daily_realized_pnl_sums_today(session_factory) -> None:
    ledger = SqlPortfolioLedger(session_factory)
    market = _market()
    now = datetime.now(UTC)
    ledger.record_fill(Fill(fill_id="b1", intent_id="i1", market=market, side="buy", stake=1.0, price=0.40, timestamp=now))
    ledger.record_fill(Fill(fill_id="s1", intent_id="i2", market=market, side="sell", stake=1.0, price=0.50, timestamp=now))
    # 1.0 stake * (0.50 - 0.40) = 0.10 realized
    assert ledger.daily_realized_pnl() == pytest.approx(0.10)

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from app.trading.sql_ledger import SqlPortfolioLedger
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
                market=_market("a"),
                side="buy",
                stake=0.25,
                price=0.40,
                timestamp=base.replace(microsecond=idx),
            )
        )
    fills = ledger.recent_fills(limit=2)
    assert [f.fill_id for f in fills] == ["f2", "f1"]


def test_daily_realized_pnl_sums_today(session_factory) -> None:
    ledger = SqlPortfolioLedger(session_factory)
    market = _market()
    now = datetime.now(UTC)
    ledger.record_fill(Fill(fill_id="b1", intent_id="i1", market=market, side="buy", stake=1.0, price=0.40, timestamp=now))
    ledger.record_fill(Fill(fill_id="s1", intent_id="i2", market=market, side="sell", stake=1.0, price=0.50, timestamp=now))
    # 1.0 stake * (0.50 - 0.40) = 0.10 realized
    assert ledger.daily_realized_pnl() == pytest.approx(0.10)

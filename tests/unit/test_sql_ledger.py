"""P3.5 tests: SqlPortfolioLedger persistence + restart recovery."""

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
def sf():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine, tables=[
        TradingOrder.__table__,
        TradingFill.__table__,
        TradingPosition.__table__,
        TradingKillSwitch.__table__,
        TradingDailyPnL.__table__,
    ])
    return sessionmaker(engine, autoflush=False, autocommit=False, future=True)


def _market(symbol: str = "NBA-A", side: str = "buy") -> MarketRef:
    return MarketRef(exchange="kalshi", symbol=symbol, market_key="points", side=side, line_value=25.5)


def _fill(
    fill_id: str,
    *,
    symbol: str = "NBA-A",
    side: str = "buy",
    stake: float = 10.0,
    price: float = 0.55,
    ts: datetime | None = None,
) -> Fill:
    return Fill(
        fill_id=fill_id,
        intent_id=f"intent-{fill_id}",
        market=_market(symbol, side),
        side=side,
        stake=stake,
        price=price,
        timestamp=ts or datetime.now(UTC),
    )


# ---------------------------------------------------------------------------
# P3.5a: fill persistence
# ---------------------------------------------------------------------------


def test_record_fill_persists_to_db(sf) -> None:
    ledger = SqlPortfolioLedger(sf)
    ledger.record_fill(_fill("f1"))

    with sf() as session:
        row = session.get(TradingFill, "f1")
    assert row is not None
    assert row.market_symbol == "NBA-A"
    assert row.stake == 10.0


def test_record_fill_updates_market_exposure(sf) -> None:
    ledger = SqlPortfolioLedger(sf)
    ledger.record_fill(_fill("f1", stake=10.0))
    ledger.record_fill(_fill("f2", stake=5.0))
    assert ledger.market_exposure("NBA-A") == pytest.approx(15.0)


def test_record_fill_sell_reduces_exposure(sf) -> None:
    ledger = SqlPortfolioLedger(sf)
    ledger.record_fill(_fill("f1", side="buy", stake=10.0))
    ledger.record_fill(_fill("f2", symbol="NBA-A", side="sell", stake=10.0))
    assert ledger.market_exposure("NBA-A") == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# P3.5b: order event persistence
# ---------------------------------------------------------------------------


def test_record_order_event_persists_to_db(sf) -> None:
    ledger = SqlPortfolioLedger(sf)
    event = OrderEvent(
        intent_id="intent-1",
        event_type="accepted",
        status="open",
        message="ok",
        market=_market(),
        side="buy",
        stake=10.0,
        timestamp=datetime.now(UTC),
    )
    ledger.record_order_event(event)

    with sf() as session:
        row = session.get(TradingOrder, "intent-1")
    assert row is not None
    assert row.status == "open"


def test_record_order_event_upserts_on_second_event(sf) -> None:
    ledger = SqlPortfolioLedger(sf)
    t0 = datetime(2026, 5, 23, 10, 0, 0, tzinfo=UTC)
    t1 = datetime(2026, 5, 23, 10, 0, 5, tzinfo=UTC)
    ledger.record_order_event(OrderEvent(
        intent_id="intent-1", event_type="accepted", status="open",
        message="ok", timestamp=t0,
    ))
    ledger.record_order_event(OrderEvent(
        intent_id="intent-1", event_type="filled", status="filled",
        message="done", timestamp=t1,
    ))

    with sf() as session:
        rows = session.query(TradingOrder).filter_by(intent_id="intent-1").all()
    assert len(rows) == 1
    assert rows[0].status == "filled"


# ---------------------------------------------------------------------------
# P3.5c: restart recovery
# ---------------------------------------------------------------------------


def test_restart_recovery_rebuilds_open_positions(sf) -> None:
    first = SqlPortfolioLedger(sf)
    first.record_fill(_fill("f1", symbol="NBA-A", stake=10.0, price=0.55))

    # New ledger instance from same session_factory restores from DB
    second = SqlPortfolioLedger(sf)
    positions = second.open_positions()

    assert len(positions) == 1
    assert positions[0].market_symbol == "NBA-A"
    assert positions[0].open_stake == pytest.approx(10.0)


def test_restart_recovery_excludes_closed_positions(sf) -> None:
    first = SqlPortfolioLedger(sf)
    first.record_fill(_fill("f1", symbol="NBA-A", side="buy", stake=10.0))
    first.record_fill(_fill("f2", symbol="NBA-A", side="sell", stake=10.0))

    second = SqlPortfolioLedger(sf)
    assert second.open_positions() == []


def test_restart_recovery_restores_daily_pnl(sf) -> None:
    ts = datetime.now(UTC)
    first = SqlPortfolioLedger(sf)
    first.record_fill(_fill("f1", symbol="NBA-A", side="buy", stake=10.0, price=0.55, ts=ts))
    first.record_fill(_fill("f2", symbol="NBA-A", side="sell", stake=10.0, price=0.70, ts=ts))

    second = SqlPortfolioLedger(sf)
    pnl = second.daily_realized_pnl()
    assert pnl == pytest.approx((0.70 - 0.55) * 10.0)

"""Regression guard: InMemoryPortfolioLedger and SqlPortfolioLedger must produce
identical daily_realized_pnl for the same sequence of fills.

Both ledgers MUST include fill.realized_pnl in the effective realized amount in
addition to position-computed realized PnL from closing trades.
"""
from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.trading.ledger import InMemoryPortfolioLedger
from app.trading.types import Fill, MarketRef


def _market(symbol: str = "paper:pts:over:24.5") -> MarketRef:
    return MarketRef(
        exchange="paper",
        symbol=symbol,
        market_key="pts",
        side="OVER",
        line_value=24.5,
    )


def _fill(
    *,
    fill_id: str,
    side: str = "buy",
    stake: float = 10.0,
    price: float = 0.5,
    fee: float = 0.0,
    realized_pnl: float = 0.0,
    symbol: str = "paper:pts:over:24.5",
) -> Fill:
    return Fill(
        fill_id=fill_id,
        intent_id="intent-1",
        market=_market(symbol),
        side=side,  # type: ignore[arg-type]
        stake=stake,
        price=price,
        fee=fee,
        realized_pnl=realized_pnl,
        timestamp=datetime.now(UTC),
    )


# ---------------------------------------------------------------------------
# InMemory tests
# ---------------------------------------------------------------------------

def test_inmemory_buy_fill_with_realized_pnl_included() -> None:
    """A buy fill with fill.realized_pnl=5 must contribute +5 to daily pnl."""
    ledger = InMemoryPortfolioLedger()
    ledger.record_fill(_fill(fill_id="f1", realized_pnl=5.0))
    assert ledger.daily_realized_pnl() == pytest.approx(5.0)


def test_inmemory_fee_subtracted_from_daily_pnl() -> None:
    ledger = InMemoryPortfolioLedger()
    ledger.record_fill(_fill(fill_id="f1", realized_pnl=5.0, fee=1.0))
    assert ledger.daily_realized_pnl() == pytest.approx(4.0)


def test_inmemory_close_trade_computed_realized() -> None:
    """Selling at a gain produces correct computed_realized without double-counting."""
    ledger = InMemoryPortfolioLedger()
    ledger.record_fill(_fill(fill_id="f1", side="buy", stake=10.0, price=0.4))
    # sell 10 contracts at 0.6 -> gain = (0.6-0.4)*10 = 2.0
    ledger.record_fill(_fill(fill_id="f2", side="sell", stake=10.0, price=0.6))
    assert ledger.daily_realized_pnl() == pytest.approx(2.0)


# ---------------------------------------------------------------------------
# SqlPortfolioLedger parity tests via SQLite in-memory
# ---------------------------------------------------------------------------

def _make_sql_ledger():
    """Return a SqlPortfolioLedger backed by a fresh SQLite in-memory database."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    from app.db.models.trading import Base
    from app.trading.sql_ledger import SqlPortfolioLedger

    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return SqlPortfolioLedger(session_factory=Session)


def test_sql_buy_fill_with_realized_pnl_included() -> None:
    """SqlPortfolioLedger: fill.realized_pnl=5 must contribute to daily_realized_pnl."""
    ledger = _make_sql_ledger()
    ledger.record_fill(_fill(fill_id="f1", realized_pnl=5.0))
    assert ledger.daily_realized_pnl() == pytest.approx(5.0)


def test_sql_fee_subtracted_from_daily_pnl() -> None:
    ledger = _make_sql_ledger()
    ledger.record_fill(_fill(fill_id="f1", realized_pnl=5.0, fee=1.0))
    assert ledger.daily_realized_pnl() == pytest.approx(4.0)


def test_sql_close_trade_computed_realized() -> None:
    ledger = _make_sql_ledger()
    ledger.record_fill(_fill(fill_id="f1", side="buy", stake=10.0, price=0.4))
    ledger.record_fill(_fill(fill_id="f2", side="sell", stake=10.0, price=0.6))
    assert ledger.daily_realized_pnl() == pytest.approx(2.0)


# ---------------------------------------------------------------------------
# Parity assertion: both ledgers agree for same fill sequence
# ---------------------------------------------------------------------------

def test_both_ledgers_agree_on_daily_pnl_with_realized_pnl_field() -> None:
    """Core parity check: fill.realized_pnl=5 must produce same result in both."""
    mem_ledger = InMemoryPortfolioLedger()
    sql_ledger = _make_sql_ledger()

    fill = _fill(fill_id="f1", realized_pnl=5.0, fee=0.5)
    mem_ledger.record_fill(fill)
    sql_ledger.record_fill(fill)

    assert mem_ledger.daily_realized_pnl() == pytest.approx(sql_ledger.daily_realized_pnl())

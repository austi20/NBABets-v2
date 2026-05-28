"""Regression guard: InMemoryPortfolioLedger and SqlPortfolioLedger must produce
identical daily_realized_pnl for the same sequence of fills.

Both ledgers use AVCO-computed realized PnL only — fill.realized_pnl from the
exchange is intentionally excluded to prevent double-counting.  The Kalshi adapter
always sets realized_pnl=0.0 on emitted fills; this value is never load-bearing.
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

def test_inmemory_buy_only_no_realized() -> None:
    """A buy fill has no AVCO realized; daily_pnl is 0."""
    ledger = InMemoryPortfolioLedger()
    ledger.record_fill(_fill(fill_id="f1", side="buy", stake=10.0, price=0.4))
    assert ledger.daily_realized_pnl() == pytest.approx(0.0)


def test_inmemory_fee_subtracted_from_daily_pnl() -> None:
    """Fee is applied even when no AVCO gain on a sell."""
    ledger = InMemoryPortfolioLedger()
    ledger.record_fill(_fill(fill_id="f1", side="buy", stake=10.0, price=0.4))
    ledger.record_fill(_fill(fill_id="f2", side="sell", stake=10.0, price=0.4, fee=1.0))
    assert ledger.daily_realized_pnl() == pytest.approx(-1.0)


def test_inmemory_close_trade_computed_realized() -> None:
    """Selling at a gain produces correct AVCO realized without fill.realized_pnl."""
    ledger = InMemoryPortfolioLedger()
    ledger.record_fill(_fill(fill_id="f1", side="buy", stake=10.0, price=0.4))
    # sell 10 contracts at 0.6 -> gain = (0.6-0.4)*10 = 2.0
    ledger.record_fill(_fill(fill_id="f2", side="sell", stake=10.0, price=0.6))
    assert ledger.daily_realized_pnl() == pytest.approx(2.0)


def test_inmemory_sell_fill_realized_pnl_field_is_not_double_counted() -> None:
    """fill.realized_pnl from exchange must NOT be added to AVCO-computed realized."""
    ledger = InMemoryPortfolioLedger()
    ledger.record_fill(_fill(fill_id="f1", side="buy", stake=1.0, price=0.40))
    # sell with artificial exchange-reported realized_pnl=99 to detect double-count
    ledger.record_fill(_fill(fill_id="f2", side="sell", stake=1.0, price=0.50, realized_pnl=99.0))
    # AVCO: (0.50-0.40)*1.0 = 0.10, NOT 99.10
    assert ledger.daily_realized_pnl() == pytest.approx(0.10)


# ---------------------------------------------------------------------------
# SqlPortfolioLedger parity tests via SQLite in-memory
# ---------------------------------------------------------------------------

def _make_sql_ledger():
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    from app.db.models.trading import Base
    from app.trading.sql_ledger import SqlPortfolioLedger

    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return SqlPortfolioLedger(session_factory=Session)


def test_sql_buy_only_no_realized() -> None:
    ledger = _make_sql_ledger()
    ledger.record_fill(_fill(fill_id="f1", side="buy", stake=10.0, price=0.4))
    assert ledger.daily_realized_pnl() == pytest.approx(0.0)


def test_sql_fee_subtracted_from_daily_pnl() -> None:
    ledger = _make_sql_ledger()
    ledger.record_fill(_fill(fill_id="f1", side="buy", stake=10.0, price=0.4))
    ledger.record_fill(_fill(fill_id="f2", side="sell", stake=10.0, price=0.4, fee=1.0))
    assert ledger.daily_realized_pnl() == pytest.approx(-1.0)


def test_sql_close_trade_computed_realized() -> None:
    ledger = _make_sql_ledger()
    ledger.record_fill(_fill(fill_id="f1", side="buy", stake=10.0, price=0.4))
    ledger.record_fill(_fill(fill_id="f2", side="sell", stake=10.0, price=0.6))
    assert ledger.daily_realized_pnl() == pytest.approx(2.0)


def test_sql_sell_fill_realized_pnl_field_is_not_double_counted() -> None:
    ledger = _make_sql_ledger()
    ledger.record_fill(_fill(fill_id="f1", side="buy", stake=1.0, price=0.40))
    ledger.record_fill(_fill(fill_id="f2", side="sell", stake=1.0, price=0.50, realized_pnl=99.0))
    assert ledger.daily_realized_pnl() == pytest.approx(0.10)


# ---------------------------------------------------------------------------
# Parity assertion: both ledgers agree on identical sequences
# ---------------------------------------------------------------------------

def test_both_ledgers_agree_on_buy_sell_sequence() -> None:
    """Core parity: buy then sell at gain gives same daily_realized_pnl in both."""
    mem_ledger = InMemoryPortfolioLedger()
    sql_ledger = _make_sql_ledger()

    buy = _fill(fill_id="f1", side="buy", stake=5.0, price=0.40, fee=0.0)
    sell = _fill(fill_id="f2", side="sell", stake=5.0, price=0.60, fee=0.10)
    for ledger in (mem_ledger, sql_ledger):
        ledger.record_fill(buy)
        ledger.record_fill(sell)

    assert mem_ledger.daily_realized_pnl() == pytest.approx(sql_ledger.daily_realized_pnl())


def test_both_ledgers_agree_exchange_realized_pnl_excluded() -> None:
    """fill.realized_pnl must be excluded identically in both ledgers."""
    mem_ledger = InMemoryPortfolioLedger()
    sql_ledger = _make_sql_ledger()

    buy = _fill(fill_id="f1", side="buy", stake=1.0, price=0.40)
    sell = _fill(fill_id="f2", side="sell", stake=1.0, price=0.50, realized_pnl=99.0)
    for ledger in (mem_ledger, sql_ledger):
        ledger.record_fill(buy)
        ledger.record_fill(sell)

    mem_result = mem_ledger.daily_realized_pnl()
    sql_result = sql_ledger.daily_realized_pnl()
    assert mem_result == pytest.approx(sql_result)
    assert mem_result == pytest.approx(0.10)

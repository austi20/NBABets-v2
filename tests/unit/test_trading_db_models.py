from __future__ import annotations

from datetime import UTC, datetime

from app.db.models.trading import (
    TradingDailyPnL,
    TradingFill,
    TradingKillSwitch,
    TradingOrder,
    TradingPosition,
)
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db.base import Base


def test_trading_models_create_tables() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine, tables=[
        TradingOrder.__table__,
        TradingFill.__table__,
        TradingPosition.__table__,
        TradingKillSwitch.__table__,
        TradingDailyPnL.__table__,
    ])
    with Session(engine, future=True) as session:
        order = TradingOrder(
            intent_id="i1",
            kalshi_order_id=None,
            market_symbol="kalshi:points:over:25.5:g0:p237",
            market_key="points",
            side="OVER",
            stake=0.25,
            status="pending",
            message="created",
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
        )
        session.add(order)
        session.commit()
        loaded = session.get(TradingOrder, "i1")
        assert loaded is not None
        assert loaded.market_key == "points"


def test_kill_switch_singleton_row() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine, tables=[TradingKillSwitch.__table__])
    with Session(engine, future=True) as session:
        switch = TradingKillSwitch(id=1, killed=False, set_at=datetime.now(UTC), set_by="test")
        session.add(switch)
        session.commit()
        loaded = session.get(TradingKillSwitch, 1)
        assert loaded is not None
        assert loaded.killed is False

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session

from app.db.base import Base
from app.db.models.trading import (
    TradingDailyPnL,
    TradingFill,
    TradingKillSwitch,
    TradingOrder,
    TradingPosition,
)
from app.server.main import _ensure_trading_tables


def test_trading_models_create_tables() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine, tables=[
        TradingOrder.__table__,
        TradingFill.__table__,
        TradingPosition.__table__,
        TradingKillSwitch.__table__,
        TradingDailyPnL.__table__,
    ])
    with Session(engine) as session:
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
    with Session(engine) as session:
        switch = TradingKillSwitch(id=1, killed=False, set_at=datetime.now(UTC), set_by="test")
        session.add(switch)
        session.commit()
        loaded = session.get(TradingKillSwitch, 1)
        assert loaded is not None
        assert loaded.killed is False


def test_ensure_trading_tables_adds_flatten_to_legacy_kill_switch(monkeypatch) -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE trading_kill_switch (
                    id INTEGER PRIMARY KEY,
                    killed BOOLEAN,
                    set_at DATETIME,
                    set_by VARCHAR(64),
                    CONSTRAINT ck_kill_switch_singleton CHECK (id = 1)
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO trading_kill_switch (id, killed, set_at, set_by)
                VALUES (1, 0, :set_at, 'legacy')
                """
            ),
            {"set_at": datetime.now(UTC)},
        )

    monkeypatch.setattr("app.server.main.get_engine", lambda: engine)

    _ensure_trading_tables()

    columns = {column["name"] for column in inspect(engine).get_columns("trading_kill_switch")}
    assert "flatten" in columns
    with engine.connect() as connection:
        flatten = connection.execute(
            text("SELECT flatten FROM trading_kill_switch WHERE id = 1")
        ).scalar_one()
    assert flatten in (False, 0)

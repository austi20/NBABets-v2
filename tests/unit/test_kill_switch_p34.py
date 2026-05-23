"""P3.4 tests: set_kill_switch flatten flag + make_kill_switch_callable factory."""

from __future__ import annotations

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
from app.trading.loop import make_kill_switch_callable, set_kill_switch


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


# ---------------------------------------------------------------------------
# P3.4a: set_kill_switch stores flatten flag
# ---------------------------------------------------------------------------


def test_set_kill_switch_stores_flatten_true(sf) -> None:
    set_kill_switch(sf, killed=True, set_by="test", flatten=True)
    with sf() as session:
        row = session.get(TradingKillSwitch, 1)
    assert row is not None
    assert row.flatten is True


def test_set_kill_switch_stores_flatten_false_by_default(sf) -> None:
    set_kill_switch(sf, killed=True, set_by="test")
    with sf() as session:
        row = session.get(TradingKillSwitch, 1)
    assert row is not None
    assert row.flatten is False


def test_set_kill_switch_updates_flatten_on_second_call(sf) -> None:
    set_kill_switch(sf, killed=True, set_by="test", flatten=False)
    set_kill_switch(sf, killed=True, set_by="test", flatten=True)
    with sf() as session:
        row = session.get(TradingKillSwitch, 1)
    assert row is not None
    assert row.flatten is True


# ---------------------------------------------------------------------------
# P3.4b: make_kill_switch_callable factory
# ---------------------------------------------------------------------------


def test_make_kill_switch_callable_returns_false_when_no_row(sf) -> None:
    fn = make_kill_switch_callable(sf)
    assert fn() is False


def test_make_kill_switch_callable_returns_false_when_not_killed(sf) -> None:
    set_kill_switch(sf, killed=False, set_by="test")
    fn = make_kill_switch_callable(sf)
    assert fn() is False


def test_make_kill_switch_callable_returns_true_when_killed(sf) -> None:
    set_kill_switch(sf, killed=True, set_by="test")
    fn = make_kill_switch_callable(sf)
    assert fn() is True


def test_make_kill_switch_callable_reflects_live_db_changes(sf) -> None:
    fn = make_kill_switch_callable(sf)
    assert fn() is False
    set_kill_switch(sf, killed=True, set_by="test")
    assert fn() is True

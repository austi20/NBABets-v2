"""P3.6 smoke tests: build_active_trader factory wiring."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

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
from app.trading.active_trader import ActiveCandidate, ActiveTrader
from app.trading.active_trader_factory import (
    StaticCandidateProvider,
    build_active_trader,
)
from app.trading.live_limits import ACTIVE_TRADER_DEFAULTS
from app.trading.live_stats_poller import LiveStatsPoller
from app.trading.risk import RiskLimits


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


def _mock_router() -> MagicMock:
    router = MagicMock()
    router.place_buy = AsyncMock(return_value=MagicMock(order_id="b1", ticker="T", side="buy"))
    router.place_sell = AsyncMock(return_value=MagicMock(order_id="s1", ticker="T", side="sell"))
    router.cancel = AsyncMock()
    return router


async def _fake_fetch(player_ids: list[int]) -> dict:
    return {"data": []}


def _cand(ticker: str = "NBA-A") -> ActiveCandidate:
    return ActiveCandidate(
        ticker=ticker,
        model_prob=0.70,
        edge_bps=200.0,
        line_value=24.5,
        market_close_at=None,
        stake_cents=500,
        player_id=42,
        stat_type="points",
    )


# ---------------------------------------------------------------------------
# P3.6a: factory returns correct types
# ---------------------------------------------------------------------------


def test_build_active_trader_returns_correct_types(sf) -> None:
    trader, poller, provider = build_active_trader(
        at_config=ACTIVE_TRADER_DEFAULTS,
        risk_limits=RiskLimits(),
        router=_mock_router(),
        session_factory=sf,
        live_stats_fetch=_fake_fetch,
        base_candidates=[_cand()],
    )
    assert isinstance(trader, ActiveTrader)
    assert isinstance(poller, LiveStatsPoller)
    assert isinstance(provider, StaticCandidateProvider)


def test_build_active_trader_static_provider_holds_candidates(sf) -> None:
    cand = _cand("NBA-B")
    _, _, provider = build_active_trader(
        at_config=ACTIVE_TRADER_DEFAULTS,
        risk_limits=RiskLimits(),
        router=_mock_router(),
        session_factory=sf,
        live_stats_fetch=_fake_fetch,
        base_candidates=[cand],
    )
    assert provider._candidates == [cand]


@pytest.mark.asyncio
async def test_build_active_trader_static_provider_update_refreshes_candidates(sf) -> None:
    _, _, provider = build_active_trader(
        at_config=ACTIVE_TRADER_DEFAULTS,
        risk_limits=RiskLimits(),
        router=_mock_router(),
        session_factory=sf,
        live_stats_fetch=_fake_fetch,
        base_candidates=[_cand("NBA-A")],
    )
    new_cand = _cand("NBA-C")
    provider.update([new_cand])
    result = await provider.candidates()
    assert result == [new_cand]


# ---------------------------------------------------------------------------
# P3.6b: kill switch callable reads from same DB
# ---------------------------------------------------------------------------


def test_kill_switch_callable_reflects_db(sf) -> None:
    from app.trading.loop import set_kill_switch

    trader, _, _ = build_active_trader(
        at_config=ACTIVE_TRADER_DEFAULTS,
        risk_limits=RiskLimits(),
        router=_mock_router(),
        session_factory=sf,
        live_stats_fetch=_fake_fetch,
        base_candidates=[],
    )
    assert trader._kill_switch() is False
    set_kill_switch(sf, killed=True, set_by="test")
    assert trader._kill_switch() is True

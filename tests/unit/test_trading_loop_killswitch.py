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
from app.evaluation.prop_decision import PropDecision
from app.trading.ledger import InMemoryPortfolioLedger
from app.trading.loop import TradingLoop, set_kill_switch
from app.trading.paper_adapter import FakePaperAdapter
from app.trading.risk import ExposureRiskEngine


@pytest.fixture()
def session_factory():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine, tables=[
        TradingOrder.__table__, TradingFill.__table__, TradingPosition.__table__,
        TradingKillSwitch.__table__, TradingDailyPnL.__table__,
    ])
    return sessionmaker(engine, autoflush=False, autocommit=False, future=True)


def _decision(market_key: str = "points") -> PropDecision:
    return PropDecision(
        model_prob=0.6, market_prob=0.5, no_vig_market_prob=0.5,
        ev=0.05, recommendation="OVER", confidence="high", driver="test",
        market_key=market_key, line_value=25.5, over_odds=-110, under_odds=-110,
        player_id=237, game_id=1, game_date="2026-05-06",
    )


def test_loop_halts_when_kill_switch_set_in_db(session_factory) -> None:
    set_kill_switch(session_factory, killed=True, set_by="test")
    loop = TradingLoop(
        risk_engine=ExposureRiskEngine(),
        ledger=InMemoryPortfolioLedger(),
        adapter=FakePaperAdapter(),
        session_factory=session_factory,
    )
    result = loop.run_decisions([_decision(), _decision("rebounds")], stake=1.0)
    assert result.accepted == 0
    assert result.rejected == 2


def test_loop_runs_normally_when_kill_switch_off(session_factory) -> None:
    set_kill_switch(session_factory, killed=False, set_by="test")
    loop = TradingLoop(
        risk_engine=ExposureRiskEngine(),
        ledger=InMemoryPortfolioLedger(),
        adapter=FakePaperAdapter(),
        session_factory=session_factory,
    )
    result = loop.run_decisions([_decision()], stake=1.0)
    assert result.accepted == 1


def test_decision_to_signal_preserves_real_resolution_metadata() -> None:
    loop = TradingLoop(
        risk_engine=ExposureRiskEngine(),
        ledger=InMemoryPortfolioLedger(),
        adapter=FakePaperAdapter(),
    )
    sig = loop._decision_to_signal(_decision())  # type: ignore[attr-defined]
    assert sig.metadata["player_id"] == 237
    assert sig.metadata["game_id"] == 1
    assert sig.metadata["game_date"] == "2026-05-06"


def test_decision_to_signal_does_not_invent_resolution_metadata() -> None:
    loop = TradingLoop(
        risk_engine=ExposureRiskEngine(),
        ledger=InMemoryPortfolioLedger(),
        adapter=FakePaperAdapter(),
    )
    decision = PropDecision(
        model_prob=0.6, market_prob=0.5, no_vig_market_prob=0.5,
        ev=0.05, recommendation="OVER", confidence="high", driver="test",
        market_key="points", line_value=25.5, over_odds=-110, under_odds=-110,
    )
    sig = loop._decision_to_signal(decision)  # type: ignore[attr-defined]
    assert "player_id" not in sig.metadata
    assert "game_id" not in sig.metadata
    assert "game_date" not in sig.metadata

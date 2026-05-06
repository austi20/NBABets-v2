from __future__ import annotations

from datetime import UTC, datetime

from app.trading.ledger import InMemoryPortfolioLedger
from app.trading.risk import ExposureRiskEngine, RiskLimits
from app.trading.types import ExecutionIntent, Fill, MarketRef, Signal


def _make_intent(*, stake: float, market_symbol: str = "paper:points:over:24.5") -> ExecutionIntent:
    signal = Signal(
        signal_id="sig",
        created_at=datetime.now(UTC),
        market_key="points",
        side="OVER",
        confidence="solid",
        edge=0.03,
        model_probability=0.56,
        line_value=24.5,
    )
    market = MarketRef(exchange="paper", symbol=market_symbol, market_key="points", side="OVER", line_value=24.5)
    return ExecutionIntent(intent_id=f"intent-{stake}", signal=signal, market=market, side="buy", stake=stake)


def test_exposure_risk_accepts_valid_order() -> None:
    ledger = InMemoryPortfolioLedger()
    engine = ExposureRiskEngine(
        RiskLimits(
            per_order_cap=25.0,
            per_market_cap=75.0,
            max_open_notional=150.0,
            daily_loss_cap=100.0,
            reject_cooldown_seconds=0,
        )
    )
    accepted, reason = engine.evaluate(_make_intent(stake=10.0), ledger)
    assert accepted is True
    assert reason == "accepted"


def test_exposure_risk_rejects_market_cap_hit() -> None:
    ledger = InMemoryPortfolioLedger()
    market = MarketRef(exchange="paper", symbol="paper:points:over:24.5", market_key="points", side="OVER", line_value=24.5)
    ledger.record_fill(
        Fill(
            fill_id="f1",
            intent_id="i1",
            market=market,
            side="buy",
            stake=20.0,
            price=0.5,
            timestamp=datetime.now(UTC),
        )
    )
    engine = ExposureRiskEngine(
        RiskLimits(
            per_order_cap=25.0,
            per_market_cap=25.0,
            max_open_notional=200.0,
            daily_loss_cap=100.0,
            reject_cooldown_seconds=0,
        )
    )
    accepted, reason = engine.evaluate(_make_intent(stake=10.0), ledger)
    assert accepted is False
    assert "per-market cap" in reason


def test_exposure_risk_rejects_daily_loss_cap_hit() -> None:
    ledger = InMemoryPortfolioLedger()
    market = MarketRef(exchange="paper", symbol="paper:points:over:24.5", market_key="points", side="OVER", line_value=24.5)
    ledger.record_fill(
        Fill(
            fill_id="f2",
            intent_id="i2",
            market=market,
            side="buy",
            stake=10.0,
            price=0.5,
            realized_pnl=-80.0,
            timestamp=datetime.now(UTC),
        )
    )
    engine = ExposureRiskEngine(
        RiskLimits(
            per_order_cap=25.0,
            per_market_cap=75.0,
            max_open_notional=200.0,
            daily_loss_cap=50.0,
            reject_cooldown_seconds=0,
        )
    )
    accepted, reason = engine.evaluate(_make_intent(stake=5.0), ledger)
    assert accepted is False
    assert "daily loss cap" in reason


def test_exposure_risk_reject_cooldown_blocks_followup() -> None:
    ledger = InMemoryPortfolioLedger()
    engine = ExposureRiskEngine(
        RiskLimits(
            per_order_cap=5.0,
            per_market_cap=75.0,
            max_open_notional=200.0,
            daily_loss_cap=100.0,
            reject_cooldown_seconds=60,
        )
    )
    first_ok, first_reason = engine.evaluate(_make_intent(stake=10.0), ledger)
    assert first_ok is False
    assert "per-order cap" in first_reason

    second_ok, second_reason = engine.evaluate(_make_intent(stake=1.0), ledger)
    assert second_ok is False
    assert "cooldown active" in second_reason

from __future__ import annotations

from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from app.trading.ledger import InMemoryPortfolioLedger
from app.trading.risk import ExposureRiskEngine, RiskLimits
from app.trading.sql_ledger import _trading_date_key
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
    # Buy 80 contracts at 0.90, then sell at 0.20 -> AVCO loss = (0.20-0.90)*80 = -56 > cap=50
    ledger.record_fill(
        Fill(fill_id="f1", intent_id="i1", market=market, side="buy", stake=80.0, price=0.9, timestamp=datetime.now(UTC))
    )
    ledger.record_fill(
        Fill(fill_id="f2", intent_id="i2", market=market, side="sell", stake=80.0, price=0.2, timestamp=datetime.now(UTC))
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


# ---- Task 2.7: unrealized PnL + trading-date key --------------------------

class _LedgerWithUnrealized(InMemoryPortfolioLedger):
    """Stub that injects a fixed unrealized loss."""
    def __init__(self, unrealized: float) -> None:
        super().__init__()
        self._unrealized = unrealized

    def unrealized_pnl(self) -> float:
        return self._unrealized


def test_exposure_risk_blocks_when_unrealized_exceeds_cap() -> None:
    # realized=0, unrealized=-60 => total=-60 > cap=50
    ledger = _LedgerWithUnrealized(unrealized=-60.0)
    engine = ExposureRiskEngine(
        RiskLimits(
            per_order_cap=25.0,
            per_market_cap=75.0,
            max_open_notional=200.0,
            daily_loss_cap=50.0,
            reject_cooldown_seconds=0,
        )
    )
    ok, reason = engine.evaluate(_make_intent(stake=5.0), ledger)
    assert ok is False
    assert "daily loss cap" in reason


def test_trading_date_key_uses_eastern_timezone() -> None:
    """11pm UTC on Dec 31 = 6pm ET = same slate, not Jan 1."""
    # 23:00 UTC = 18:00 ET (EST = UTC-5) on same calendar day
    dt_utc = datetime(2026, 12, 31, 23, 0, 0, tzinfo=UTC)
    et_date = _trading_date_key(dt_utc)
    eastern = ZoneInfo("America/New_York")
    expected = dt_utc.astimezone(eastern).date()
    assert et_date == expected


def test_trading_date_key_crosses_midnight_in_utc() -> None:
    """2am UTC Jan 1 = 9pm ET Dec 31 = previous day slate."""
    dt_utc = datetime(2027, 1, 1, 2, 0, 0, tzinfo=UTC)
    eastern = ZoneInfo("America/New_York")
    expected = dt_utc.astimezone(eastern).date()
    result = _trading_date_key(dt_utc)
    assert result == expected


# ---- Task 2.12: explicit per_order_cap / max_open_notional / cooldown-expiry ----

def test_per_order_cap_rejects() -> None:
    """stake > per_order_cap → blocked, 'per_order_cap' reflected in reason."""
    ledger = InMemoryPortfolioLedger()
    engine = ExposureRiskEngine(
        RiskLimits(
            per_order_cap=10.0,
            per_market_cap=75.0,
            max_open_notional=200.0,
            daily_loss_cap=100.0,
            reject_cooldown_seconds=0,
        )
    )
    ok, reason = engine.evaluate(_make_intent(stake=11.0), ledger)
    assert ok is False
    assert "per-order cap" in reason


def test_max_open_notional_rejects() -> None:
    """open notional near limit + new stake → blocked with exposure cap reason."""
    ledger = InMemoryPortfolioLedger()
    market = MarketRef(
        exchange="paper", symbol="paper:points:over:24.5",
        market_key="points", side="OVER", line_value=24.5,
    )
    # Open position that uses most of notional
    ledger.record_fill(
        Fill(
            fill_id="f1",
            intent_id="i1",
            market=market,
            side="buy",
            stake=90.0,
            price=0.5,
            timestamp=datetime.now(UTC),
        )
    )
    engine = ExposureRiskEngine(
        RiskLimits(
            per_order_cap=25.0,
            per_market_cap=150.0,
            max_open_notional=100.0,
            daily_loss_cap=200.0,
            reject_cooldown_seconds=0,
        )
    )
    # 90 open + 15 new = 105 > 100 notional cap
    ok, reason = engine.evaluate(_make_intent(stake=15.0), ledger)
    assert ok is False
    assert "open exposure cap" in reason


def test_cooldown_clears_after_window_passes() -> None:
    """Record a triggering rejection → engine is in cooldown → advance clock past
    the cooldown window → next evaluation is NOT rejected by cooldown."""
    ledger = InMemoryPortfolioLedger()
    engine = ExposureRiskEngine(
        RiskLimits(
            per_order_cap=5.0,  # intentionally low to trigger rejection
            per_market_cap=75.0,
            max_open_notional=200.0,
            daily_loss_cap=100.0,
            reject_cooldown_seconds=60,
        )
    )
    # Trigger rejection → sets cooldown
    ok, reason = engine.evaluate(_make_intent(stake=10.0), ledger)
    assert ok is False
    assert "per-order cap" in reason

    # Immediately after: cooldown blocks even a valid stake
    ok2, reason2 = engine.evaluate(_make_intent(stake=1.0), ledger)
    assert ok2 is False
    assert "cooldown active" in reason2

    # Advance the engine's internal cooldown_until to the past
    from datetime import timedelta
    engine._cooldown_until = datetime.now(UTC) - timedelta(seconds=1)

    # Now a valid order should be accepted
    ok3, reason3 = engine.evaluate(_make_intent(stake=1.0), ledger)
    assert ok3 is True
    assert reason3 == "accepted"

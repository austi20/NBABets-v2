"""P3.3 tests: ExposureRiskEngine wired into ActiveTrader._risk_ok."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from unittest.mock import MagicMock

from app.trading.active_trader import ActiveCandidate, ActiveTrader
from app.trading.live_limits import ACTIVE_TRADER_DEFAULTS
from app.trading.market_book import MarketBook
from app.trading.portfolio_manager import PortfolioManager
from app.trading.risk import RiskLimits

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _mock_router() -> MagicMock:
    router = MagicMock()
    router.place_buy.return_value = MagicMock(order_id="b1", ticker="T", side="buy")
    router.place_sell.return_value = MagicMock(order_id="s1", ticker="T", side="sell")
    router.cancel.return_value = None
    return router


def _mock_ledger(
    *,
    market_exp: float = 0.0,
    open_notional: float = 0.0,
    daily_pnl: float = 0.0,
) -> MagicMock:
    ledger = MagicMock()
    ledger.market_exposure.return_value = market_exp
    ledger.open_notional.return_value = open_notional
    ledger.daily_realized_pnl.return_value = daily_pnl
    return ledger


def _cand(ticker: str = "KX-A", *, stake_cents: int = 500) -> ActiveCandidate:
    return ActiveCandidate(
        ticker=ticker,
        model_prob=0.70,
        edge_bps=200.0,
        line_value=24.5,
        market_close_at=None,
        stake_cents=stake_cents,
    )


def _build_trader(*, risk_engine=None, ledger=None) -> ActiveTrader:
    cfg = replace(ACTIVE_TRADER_DEFAULTS, enabled=True)
    clock = lambda: datetime(2026, 5, 23, 12, 0, 0, tzinfo=UTC)  # noqa: E731
    router = _mock_router()
    book = MarketBook()
    pm = PortfolioManager(config=cfg, router=router, clock=clock)
    src = MagicMock()
    src.candidates.return_value = []
    return ActiveTrader(
        config=cfg,
        risk_limits=RiskLimits(),
        book=book,
        portfolio=pm,
        candidates_source=src,
        risk_engine=risk_engine,
        ledger=ledger,
    )


# ---------------------------------------------------------------------------
# P3.3a: kill switch via engine
# ---------------------------------------------------------------------------


def test_risk_ok_returns_false_when_engine_reports_killed():
    engine = MagicMock()
    engine.evaluate.return_value = (False, "kill switch is active")
    ledger = _mock_ledger()

    trader = _build_trader(risk_engine=engine, ledger=ledger)
    cand = _cand()

    result = trader._risk_ok(cand)

    assert result is False
    engine.evaluate.assert_called_once()


# ---------------------------------------------------------------------------
# P3.3b: reject cooldown via engine
# ---------------------------------------------------------------------------


def test_risk_ok_returns_false_when_engine_reports_cooldown():
    engine = MagicMock()
    engine.evaluate.return_value = (False, "reject cooldown active (90s remaining)")
    ledger = _mock_ledger()

    trader = _build_trader(risk_engine=engine, ledger=ledger)
    result = trader._risk_ok(_cand())

    assert result is False


# ---------------------------------------------------------------------------
# P3.3c: daily loss cap via engine
# ---------------------------------------------------------------------------


def test_risk_ok_returns_false_when_engine_reports_daily_loss_cap():
    engine = MagicMock()
    engine.evaluate.return_value = (False, "daily loss cap reached (-120.00)")
    ledger = _mock_ledger(daily_pnl=-120.0)

    trader = _build_trader(risk_engine=engine, ledger=ledger)
    result = trader._risk_ok(_cand())

    assert result is False


# ---------------------------------------------------------------------------
# P3.3d: engine accepts
# ---------------------------------------------------------------------------


def test_risk_ok_returns_true_when_engine_accepts():
    engine = MagicMock()
    engine.evaluate.return_value = (True, "accepted")
    ledger = _mock_ledger()

    trader = _build_trader(risk_engine=engine, ledger=ledger)
    result = trader._risk_ok(_cand())

    assert result is True


# ---------------------------------------------------------------------------
# P3.3e: intent fields forwarded to engine
# ---------------------------------------------------------------------------


def test_risk_ok_passes_correct_stake_to_engine():
    engine = MagicMock()
    engine.evaluate.return_value = (True, "accepted")
    ledger = _mock_ledger()

    trader = _build_trader(risk_engine=engine, ledger=ledger)
    trader._risk_ok(_cand(stake_cents=750))

    _intent, _ledger = engine.evaluate.call_args.args
    assert abs(_intent.stake - 7.50) < 1e-6


def test_risk_ok_passes_correct_ticker_to_engine():
    engine = MagicMock()
    engine.evaluate.return_value = (True, "accepted")
    ledger = _mock_ledger()

    trader = _build_trader(risk_engine=engine, ledger=ledger)
    trader._risk_ok(_cand(ticker="NBA-PLAYER-PTS-OVER-24.5"))

    _intent, _ledger = engine.evaluate.call_args.args
    assert _intent.market.symbol == "NBA-PLAYER-PTS-OVER-24.5"


# ---------------------------------------------------------------------------
# P3.3f: no engine → manual fallback (backward compat)
# ---------------------------------------------------------------------------


def test_risk_ok_manual_fallback_rejects_when_per_order_cap_exceeded():
    # RiskLimits.per_order_cap = 25.0; stake = 50.0 -> reject
    trader = _build_trader(risk_engine=None, ledger=None)
    cand = _cand(stake_cents=5000)  # $50
    assert trader._risk_ok(cand) is False


def test_risk_ok_manual_fallback_accepts_within_limits():
    trader = _build_trader(risk_engine=None, ledger=None)
    cand = _cand(stake_cents=500)  # $5 — well within defaults
    assert trader._risk_ok(cand) is True

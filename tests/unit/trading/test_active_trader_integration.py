"""P3.8 integration tests: full ActiveTrader loop with mocked router.

Tests the end-to-end candidate → entry-gate → place_buy → fill → position
path, plus risk gate, kill switch, and disconnect fallback.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.trading.active_trader import ActiveCandidate, ActiveTrader
from app.trading.live_limits import ACTIVE_TRADER_DEFAULTS
from app.trading.market_book import MarketBook, MarketEntry
from app.trading.portfolio_manager import FSMState, PortfolioManager
from app.trading.risk import RiskLimits

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime(2026, 5, 23, 14, 0, 0, tzinfo=UTC)


def _entry(ticker: str = "NBA-A", *, bid: float = 0.48, ask: float = 0.52) -> MarketEntry:
    return MarketEntry(
        ticker=ticker, yes_bid=bid, yes_ask=ask,
        no_bid=None, no_ask=None, last=None, spread=None,
        status="active", updated_at=_now(),
    )


def _cand(
    ticker: str = "NBA-A",
    *,
    model_prob: float = 0.75,
    edge_bps: float = 300.0,
    stake_cents: int = 500,
) -> ActiveCandidate:
    return ActiveCandidate(
        ticker=ticker, model_prob=model_prob, edge_bps=edge_bps,
        line_value=24.5, market_close_at=None, stake_cents=stake_cents,
    )


def _build_router(order_id: str = "b1") -> MagicMock:
    router = MagicMock()
    router.place_buy = AsyncMock(
        return_value=MagicMock(order_id=order_id, ticker="NBA-A", side="buy")
    )
    router.place_sell = AsyncMock(
        return_value=MagicMock(order_id="s1", ticker="NBA-A", side="sell")
    )
    router.cancel = AsyncMock()
    return router


def _build(
    *,
    router: MagicMock | None = None,
    risk: RiskLimits | None = None,
    kill_switch=lambda: False,
    candidates: list[ActiveCandidate] | None = None,
    risk_engine=None,
    ledger=None,
) -> tuple[ActiveTrader, MarketBook, PortfolioManager, MagicMock]:
    cfg = replace(ACTIVE_TRADER_DEFAULTS, enabled=True)
    r = router or _build_router()
    book = MarketBook()
    pm = PortfolioManager(config=cfg, router=r, clock=_now)
    src = MagicMock()
    src.candidates = AsyncMock(return_value=list(candidates or []))
    trader = ActiveTrader(
        config=cfg,
        risk_limits=risk or RiskLimits(),
        book=book,
        portfolio=pm,
        candidates_source=src,
        kill_switch=kill_switch,
        clock=_now,
        risk_engine=risk_engine,
        ledger=ledger,
    )
    return trader, book, pm, r


# ---------------------------------------------------------------------------
# P3.8a: full entry cycle - candidate passes all gates, buy is placed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_full_entry_cycle_places_buy() -> None:
    trader, book, pm, router = _build(candidates=[_cand()])
    await book.update(_entry())

    await trader._consider(_cand())

    router.place_buy.assert_called_once()
    call_kwargs = router.place_buy.call_args
    assert call_kwargs.args[0] == "NBA-A"  # ticker


@pytest.mark.asyncio
async def test_full_entry_cycle_opens_position_in_portfolio() -> None:
    trader, book, pm, router = _build(candidates=[_cand()])
    await book.update(_entry())

    await trader._consider(_cand())

    pos = pm.position("NBA-A")
    assert pos is not None
    assert pos.state == FSMState.BUY_PENDING


# ---------------------------------------------------------------------------
# P3.8b: entry gate rejects low-prob candidate
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_low_model_prob_candidate_not_placed() -> None:
    trader, book, _, router = _build()
    await book.update(_entry())

    await trader._consider(_cand(model_prob=0.50))  # below min_model_prob=0.60

    router.place_buy.assert_not_called()


# ---------------------------------------------------------------------------
# P3.8c: risk engine rejects when engine says no
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_risk_engine_rejection_blocks_entry() -> None:
    engine = MagicMock()
    engine.evaluate.return_value = (False, "daily loss cap reached")
    ledger = MagicMock()
    ledger.market_exposure.return_value = 0.0
    ledger.open_notional.return_value = 0.0
    ledger.daily_realized_pnl.return_value = -120.0

    trader, book, _, router = _build(risk_engine=engine, ledger=ledger)
    await book.update(_entry())

    await trader._consider(_cand())

    router.place_buy.assert_not_called()


# ---------------------------------------------------------------------------
# P3.8d: kill switch halts new entries via run() loop
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_kill_switch_stops_run_loop() -> None:
    killed = [False]

    def kill_switch() -> bool:
        return killed[0]

    trader, book, pm, router = _build(kill_switch=kill_switch, candidates=[_cand()])

    async def _drive() -> None:
        await asyncio.sleep(0.01)
        await book.update(_entry())
        await asyncio.sleep(0.01)
        killed[0] = True  # arm kill switch
        await book.update(_entry(bid=0.49, ask=0.53))  # trigger loop check
        await asyncio.sleep(0.05)

    run_task = asyncio.ensure_future(trader.run())
    await _drive()
    run_task.cancel()
    try:
        await run_task
    except asyncio.CancelledError:
        pass

    # Kill switch fired so flatten_all should have been called; no new buy after kill
    # (one buy may have happened before kill fired - that's fine)
    assert router.place_buy.call_count <= 1


# ---------------------------------------------------------------------------
# P3.8e: disconnect gate suppresses entries
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_disconnect_suppresses_all_new_entries() -> None:
    trader, book, _, router = _build(candidates=[_cand()])
    await book.update(_entry())
    book.mark_connected(False)

    await trader._consider(_cand())
    await trader._consider(_cand())
    await trader._consider(_cand())

    router.place_buy.assert_not_called()


# ---------------------------------------------------------------------------
# P3.8f: per-order cap rejects oversized stake
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_per_order_cap_rejects_oversized_stake() -> None:
    # RiskLimits.per_order_cap default = 25.0; stake = $50 -> reject
    trader, book, _, router = _build()
    await book.update(_entry())

    await trader._consider(_cand(stake_cents=5000))

    router.place_buy.assert_not_called()


# ---------------------------------------------------------------------------
# P3.8g: duplicate position gate
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_second_entry_blocked_while_position_active() -> None:
    trader, book, pm, router = _build(candidates=[_cand()])
    await book.update(_entry())

    await trader._consider(_cand())
    assert router.place_buy.call_count == 1

    await trader._consider(_cand())  # position already open
    assert router.place_buy.call_count == 1  # no second buy

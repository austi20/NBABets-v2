from __future__ import annotations

import asyncio
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta

import pytest

from app.trading.active_trader import ActiveCandidate, ActiveTrader
from app.trading.live_limits import ACTIVE_TRADER_DEFAULTS, ActiveTraderConfig
from app.trading.market_book import MarketBook, MarketEntry
from app.trading.portfolio_manager import (
    FSMState,
    OrderHandle,
    PortfolioManager,
)
from app.trading.risk import RiskLimits


# ----------------------------------------------------------------------- fakes
@dataclass
class _PlacedOrder:
    handle: OrderHandle
    qty: int
    price_cents: int


@dataclass
class _FakeRouter:
    buys: list[_PlacedOrder] = field(default_factory=list)
    sells: list[_PlacedOrder] = field(default_factory=list)
    cancels: list[OrderHandle] = field(default_factory=list)
    _seq: int = 0

    def _next_id(self, side: str) -> str:
        self._seq += 1
        return f"{side}-{self._seq}"

    async def place_buy(self, ticker: str, qty: int, limit_price_cents: int) -> OrderHandle:
        h = OrderHandle(order_id=self._next_id("b"), ticker=ticker, side="buy")
        self.buys.append(_PlacedOrder(h, qty, limit_price_cents))
        return h

    async def place_sell(self, ticker: str, qty: int, limit_price_cents: int) -> OrderHandle:
        h = OrderHandle(order_id=self._next_id("s"), ticker=ticker, side="sell")
        self.sells.append(_PlacedOrder(h, qty, limit_price_cents))
        return h

    async def cancel(self, handle: OrderHandle) -> None:
        self.cancels.append(handle)


class _Clock:
    def __init__(self, start: datetime | None = None) -> None:
        self._now = start or datetime(2026, 5, 19, 19, 0, 0, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now = self._now + timedelta(seconds=seconds)


@dataclass
class _FakeCandidates:
    items: list[ActiveCandidate] = field(default_factory=list)

    async def candidates(self) -> list[ActiveCandidate]:
        return list(self.items)


def _config(**overrides: object) -> ActiveTraderConfig:
    overrides.setdefault("enabled", True)
    return replace(ACTIVE_TRADER_DEFAULTS, **overrides)  # type: ignore[arg-type]


def _entry(ticker: str, bid: float | None, ask: float | None, clock: _Clock) -> MarketEntry:
    return MarketEntry(
        ticker=ticker,
        yes_bid=bid,
        yes_ask=ask,
        no_bid=None,
        no_ask=None,
        last=None,
        spread=None,
        status="active",
        updated_at=clock(),
    )


def _cand(
    ticker: str = "KX-A",
    *,
    model_prob: float = 0.70,
    edge_bps: float = 200.0,
    line_value: float = 24.5,
    stake_cents: int = 1000,
    market_close_at: datetime | None = None,
) -> ActiveCandidate:
    return ActiveCandidate(
        ticker=ticker,
        model_prob=model_prob,
        edge_bps=edge_bps,
        line_value=line_value,
        market_close_at=market_close_at,
        stake_cents=stake_cents,
    )


def _build(
    *,
    config: ActiveTraderConfig | None = None,
    risk: RiskLimits | None = None,
    candidates: list[ActiveCandidate] | None = None,
    kill_switch=lambda: False,
) -> tuple[ActiveTrader, MarketBook, PortfolioManager, _FakeRouter, _Clock, _FakeCandidates]:
    cfg = config or _config()
    clock = _Clock()
    router = _FakeRouter()
    book = MarketBook()
    pm = PortfolioManager(config=cfg, router=router, clock=clock)
    source = _FakeCandidates(candidates or [])
    trader = ActiveTrader(
        config=cfg,
        risk_limits=risk or RiskLimits(),
        book=book,
        portfolio=pm,
        candidates_source=source,
        kill_switch=kill_switch,
        clock=clock,
    )
    return trader, book, pm, router, clock, source


async def _pump(book: MarketBook, entry: MarketEntry) -> None:
    """Push one update and let the loop drain it."""
    await asyncio.sleep(0.01)  # let run() subscribe before we publish
    await book.update(entry)
    await asyncio.sleep(0.01)  # let the loop process the update


def _buys_for(router: _FakeRouter, ticker: str) -> list[_PlacedOrder]:
    return [o for o in router.buys if o.handle.ticker == ticker]


async def _set_holding(
    pm: PortfolioManager, ticker: str, *, qty: int, price_cents: int, model_prob: float
) -> None:
    await pm.open_position(ticker, qty, price_cents, model_prob)
    pos = pm.position(ticker)
    assert pos is not None and pos.buy_handle is not None
    await pm.on_fill(pos.buy_handle, filled_qty=qty, avg_price_cents=price_cents)
    assert pm.position(ticker).state is FSMState.HOLDING  # type: ignore[union-attr]


# ----------------------------------------------------------------------- tests
@pytest.mark.asyncio
async def test_disabled_processes_quotes_but_never_opens() -> None:
    trader, book, pm, router, clock, _ = _build(
        config=_config(enabled=False), candidates=[_cand()]
    )
    task = asyncio.create_task(trader.run())
    await _pump(book, _entry("KX-A", 0.48, 0.50, clock))
    # Quote was processed (no position exists, so on_quote is a no-op, but no buy).
    assert router.buys == []
    assert pm.position("KX-A") is None
    await trader.stop()
    await task


@pytest.mark.asyncio
async def test_happy_path_entry_opens_once() -> None:
    trader, book, pm, router, clock, _ = _build(candidates=[_cand(stake_cents=1000)])
    task = asyncio.create_task(trader.run())
    await _pump(book, _entry("KX-A", 0.48, 0.50, clock))
    assert len(router.buys) == 1
    assert router.buys[0].qty == 20  # 1000 // 50
    assert router.buys[0].price_cents == 50
    assert pm.position("KX-A").state is FSMState.BUY_PENDING  # type: ignore[union-attr]
    await trader.stop()
    await task


@pytest.mark.asyncio
async def test_skip_low_model_prob() -> None:
    trader, book, _, router, clock, _ = _build(candidates=[_cand(model_prob=0.55)])
    task = asyncio.create_task(trader.run())
    await _pump(book, _entry("KX-A", 0.48, 0.50, clock))
    assert router.buys == []
    await trader.stop()
    await task


@pytest.mark.asyncio
async def test_skip_ask_above_max_entry_price() -> None:
    trader, book, _, router, clock, _ = _build(candidates=[_cand()])
    task = asyncio.create_task(trader.run())
    await _pump(book, _entry("KX-A", 0.83, 0.85, clock))  # ask 0.85 > 0.80
    assert router.buys == []
    await trader.stop()
    await task


@pytest.mark.asyncio
async def test_skip_spread_too_wide() -> None:
    trader, book, _, router, clock, _ = _build(candidates=[_cand()])
    task = asyncio.create_task(trader.run())
    await _pump(book, _entry("KX-A", 0.40, 0.50, clock))  # spread 10c > 6c
    assert router.buys == []
    await trader.stop()
    await task


@pytest.mark.asyncio
async def test_skip_edge_below_minimum() -> None:
    trader, book, _, router, clock, _ = _build(candidates=[_cand(edge_bps=50)])
    task = asyncio.create_task(trader.run())
    await _pump(book, _entry("KX-A", 0.48, 0.50, clock))
    assert router.buys == []
    await trader.stop()
    await task


@pytest.mark.asyncio
async def test_volatility_veto_then_override() -> None:
    # Start with no candidate so seeding volatility cannot trigger an entry.
    trader, book, _, router, clock, source = _build(candidates=[])
    task = asyncio.create_task(trader.run())
    # Seed >5c stdev of yes_bid via several samples (all within the 60s window).
    for bid, ask in ((0.40, 0.41), (0.60, 0.61), (0.40, 0.41)):
        await _pump(book, _entry("KX-A", bid, ask, clock))
    assert book.stdev_cents("KX-A", 60) > 5  # type: ignore[operator]
    # Low-prob candidate is vetoed by the high volatility.
    source.items = [_cand(model_prob=0.70)]
    await _pump(book, _entry("KX-A", 0.48, 0.49, clock))
    assert router.buys == []
    # model_prob 0.85 >= 0.80 override -> entry despite high volatility.
    source.items = [_cand(model_prob=0.85)]
    await _pump(book, _entry("KX-A", 0.48, 0.49, clock))
    assert len(router.buys) == 1
    await trader.stop()
    await task


@pytest.mark.asyncio
async def test_skip_double_down_when_holding() -> None:
    trader, book, pm, router, clock, _ = _build(candidates=[_cand()])
    await _set_holding(pm, "KX-A", qty=20, price_cents=50, model_prob=0.70)
    task = asyncio.create_task(trader.run())
    await _pump(book, _entry("KX-A", 0.48, 0.50, clock))
    assert len(_buys_for(router, "KX-A")) == 1  # only the setup buy, no double-down
    await trader.stop()
    await task


@pytest.mark.asyncio
async def test_skip_when_in_cooldown() -> None:
    cfg = _config(cooldown_after_exit_seconds=30)
    trader, book, pm, router, clock, _ = _build(config=cfg, candidates=[_cand()])
    # Drive a position to CLOSED so cooldown is active.
    await _set_holding(pm, "KX-A", qty=20, price_cents=50, model_prob=0.70)
    await pm.on_quote("KX-A", yes_bid_cents=80, yes_ask_cents=81)  # TP -> SELL_PENDING
    sh = pm.position("KX-A").sell_handle  # type: ignore[union-attr]
    assert sh is not None
    await pm.on_fill(sh, filled_qty=20, avg_price_cents=80)
    assert pm.is_in_cooldown("KX-A") is True
    task = asyncio.create_task(trader.run())
    await _pump(book, _entry("KX-A", 0.48, 0.50, clock))
    assert len(_buys_for(router, "KX-A")) == 1  # only the original setup buy
    await trader.stop()
    await task


@pytest.mark.asyncio
async def test_position_cap_blocks_third() -> None:
    cfg = _config(max_open_positions=2)
    trader, book, pm, router, clock, _ = _build(config=cfg, candidates=[_cand("KX-C")])
    await _set_holding(pm, "KX-A", qty=10, price_cents=50, model_prob=0.70)
    await _set_holding(pm, "KX-B", qty=10, price_cents=50, model_prob=0.70)
    task = asyncio.create_task(trader.run())
    await _pump(book, _entry("KX-C", 0.48, 0.50, clock))
    assert _buys_for(router, "KX-C") == []  # cap of 2 already met
    await trader.stop()
    await task


@pytest.mark.asyncio
async def test_risk_per_order_cap_blocks() -> None:
    risk = RiskLimits(per_order_cap=5.0)  # $5
    trader, book, _, router, clock, _ = _build(
        risk=risk, candidates=[_cand(stake_cents=1000)]  # $10 > $5
    )
    task = asyncio.create_task(trader.run())
    await _pump(book, _entry("KX-A", 0.48, 0.50, clock))
    assert router.buys == []
    await trader.stop()
    await task


@pytest.mark.asyncio
async def test_risk_max_open_notional_blocks() -> None:
    # Existing holding ~$10; cap $15; new $10 stake pushes total to $20 > $15.
    risk = RiskLimits(per_order_cap=25.0, per_market_cap=75.0, max_open_notional=15.0)
    trader, book, pm, router, clock, _ = _build(
        risk=risk, candidates=[_cand("KX-B", stake_cents=1000)]
    )
    await _set_holding(pm, "KX-A", qty=20, price_cents=50, model_prob=0.70)  # $10 open
    task = asyncio.create_task(trader.run())
    await _pump(book, _entry("KX-B", 0.48, 0.50, clock))
    assert _buys_for(router, "KX-B") == []
    await trader.stop()
    await task


@pytest.mark.asyncio
async def test_kill_switch_flattens_and_exits() -> None:
    state = {"kill": False}
    trader, book, pm, router, clock, _ = _build(
        candidates=[_cand()], kill_switch=lambda: state["kill"]
    )
    await _set_holding(pm, "KX-A", qty=20, price_cents=50, model_prob=0.90)
    await pm.on_quote("KX-A", yes_bid_cents=55, yes_ask_cents=56)  # seed last_bid
    task = asyncio.create_task(trader.run())
    state["kill"] = True
    await _pump(book, _entry("KX-A", 0.55, 0.56, clock))
    await asyncio.wait_for(task, timeout=1.0)  # loop returned on kill
    assert len(router.sells) == 1  # flatten_all sold the holding
    assert task.done()


@pytest.mark.asyncio
async def test_stop_terminates_loop_quickly() -> None:
    trader, book, _, _, _, _ = _build(candidates=[])
    task = asyncio.create_task(trader.run())
    await asyncio.sleep(0)  # let run() block on the empty queue
    await trader.stop()
    await asyncio.wait_for(task, timeout=0.1)
    assert task.done()


@pytest.mark.asyncio
async def test_stop_is_idempotent() -> None:
    trader, _, _, _, _, _ = _build(candidates=[])
    task = asyncio.create_task(trader.run())
    await asyncio.sleep(0)
    await trader.stop()
    await trader.stop()
    await asyncio.wait_for(task, timeout=0.1)
    assert task.done()

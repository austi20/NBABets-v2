from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta

import pytest

from app.trading.live_limits import ACTIVE_TRADER_DEFAULTS, ActiveTraderConfig
from app.trading.portfolio_manager import (
    PARTIAL_BUY_CANCEL_AFTER_SECONDS,
    FSMState,
    OrderHandle,
    PortfolioManager,
    PositionState,
)


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

    async def place_buy(
        self, ticker: str, qty: int, limit_price_cents: int
    ) -> OrderHandle:
        h = OrderHandle(order_id=self._next_id("b"), ticker=ticker, side="buy")
        self.buys.append(_PlacedOrder(h, qty, limit_price_cents))
        return h

    async def place_sell(
        self, ticker: str, qty: int, limit_price_cents: int
    ) -> OrderHandle:
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


def _config(**overrides: object) -> ActiveTraderConfig:
    base = ACTIVE_TRADER_DEFAULTS
    return replace(base, **overrides)  # type: ignore[arg-type]


def _mgr(
    *,
    config: ActiveTraderConfig | None = None,
    on_pace: object = None,
) -> tuple[PortfolioManager, _FakeRouter, _Clock]:
    router = _FakeRouter()
    clock = _Clock()
    pm = PortfolioManager(
        config=config or _config(),
        router=router,
        clock=clock,
        on_pace_callable=on_pace,  # type: ignore[arg-type]
    )
    return pm, router, clock


# ----------------------------------------------------------------------- tests
@pytest.mark.asyncio
async def test_open_position_creates_buy_pending() -> None:
    pm, router, _ = _mgr()
    ok = await pm.open_position("KX-A", qty=10, limit_price_cents=50, model_prob=0.7)
    assert ok is True
    pos = pm.position("KX-A")
    assert pos is not None
    assert pos.state is FSMState.BUY_PENDING
    assert pos.requested_qty == 10
    assert pos.qty == 0
    assert len(router.buys) == 1
    assert router.buys[0].qty == 10


@pytest.mark.asyncio
async def test_full_fill_transitions_to_holding_with_brackets() -> None:
    pm, _router, _ = _mgr(config=_config(tp_pct=0.5, sl_pct=0.5))
    await pm.open_position("KX-A", qty=10, limit_price_cents=50, model_prob=0.7)
    pos = pm.position("KX-A")
    assert pos is not None and pos.buy_handle is not None
    await pm.on_fill(pos.buy_handle, filled_qty=10, avg_price_cents=50)
    pos2 = pm.position("KX-A")
    assert pos2 is not None
    assert pos2.state is FSMState.HOLDING
    assert pos2.qty == 10
    assert pos2.entry_price_cents == 50
    assert pos2.tp_price_cents == 75
    assert pos2.sl_price_cents == 25


@pytest.mark.asyncio
async def test_tp_triggers_sell_when_low_conviction() -> None:
    pm, router, _ = _mgr(config=_config(tp_pct=0.5, sl_pct=0.5, conviction_hold=0.85))
    await pm.open_position("KX-A", qty=10, limit_price_cents=50, model_prob=0.70)
    h = pm.position("KX-A").buy_handle  # type: ignore[union-attr]
    assert h is not None
    await pm.on_fill(h, filled_qty=10, avg_price_cents=50)
    await pm.on_quote("KX-A", yes_bid_cents=80, yes_ask_cents=82)
    pos = pm.position("KX-A")
    assert pos is not None and pos.state is FSMState.SELL_PENDING
    assert len(router.sells) == 1
    assert router.sells[0].price_cents == 80


@pytest.mark.asyncio
async def test_sl_triggers_sell_when_low_conviction() -> None:
    pm, router, _ = _mgr(config=_config(tp_pct=0.5, sl_pct=0.5, conviction_hold=0.85))
    await pm.open_position("KX-A", qty=10, limit_price_cents=50, model_prob=0.70)
    h = pm.position("KX-A").buy_handle  # type: ignore[union-attr]
    assert h is not None
    await pm.on_fill(h, filled_qty=10, avg_price_cents=50)
    await pm.on_quote("KX-A", yes_bid_cents=20, yes_ask_cents=22)
    pos = pm.position("KX-A")
    assert pos is not None and pos.state is FSMState.SELL_PENDING
    assert len(router.sells) == 1
    assert router.sells[0].price_cents == 20


@pytest.mark.asyncio
async def test_conviction_override_no_auto_sell() -> None:
    pm, router, _ = _mgr(config=_config(tp_pct=0.5, sl_pct=0.5, conviction_hold=0.85))
    await pm.open_position("KX-A", qty=10, limit_price_cents=50, model_prob=0.90)
    h = pm.position("KX-A").buy_handle  # type: ignore[union-attr]
    assert h is not None
    await pm.on_fill(h, filled_qty=10, avg_price_cents=50)
    # TP-side
    await pm.on_quote("KX-A", yes_bid_cents=99, yes_ask_cents=99)
    # SL-side
    await pm.on_quote("KX-A", yes_bid_cents=5, yes_ask_cents=6)
    assert pm.position("KX-A").state is FSMState.HOLDING  # type: ignore[union-attr]
    assert router.sells == []


@pytest.mark.asyncio
async def test_partial_buy_fill_recomputes_brackets_and_cancels_remainder() -> None:
    pm, router, clock = _mgr(config=_config(tp_pct=0.5, sl_pct=0.5))
    await pm.open_position("KX-A", qty=10, limit_price_cents=50, model_prob=0.7)
    h = pm.position("KX-A").buy_handle  # type: ignore[union-attr]
    assert h is not None
    # Partial fill at avg 40 cents for 6 of 10 shares.
    await pm.on_fill(h, filled_qty=6, avg_price_cents=40)
    pos = pm.position("KX-A")
    assert pos is not None
    assert pos.state is FSMState.BUY_PENDING
    assert pos.qty == 6
    assert pos.entry_price_cents == 40
    assert pos.tp_price_cents == 60
    assert pos.sl_price_cents == 20
    assert pos.buy_partial_cancel_at is not None
    # Advance past the cancel deadline; next quote drives the finalize.
    clock.advance(PARTIAL_BUY_CANCEL_AFTER_SECONDS + 1)
    await pm.on_quote("KX-A", yes_bid_cents=42, yes_ask_cents=44)
    pos2 = pm.position("KX-A")
    assert pos2 is not None
    assert pos2.state is FSMState.HOLDING
    assert pos2.qty == 6
    assert router.cancels and router.cancels[0] == h


@pytest.mark.asyncio
async def test_settle_window_sells_when_not_on_pace() -> None:
    captured: list[PositionState] = []

    def on_pace(pos: PositionState) -> bool:
        captured.append(pos)
        return False

    cfg = _config(tp_pct=0.5, sl_pct=0.5, settle_hold_minutes=15)
    pm, router, clock = _mgr(config=cfg, on_pace=on_pace)
    close_at = clock() + timedelta(minutes=30)
    await pm.open_position(
        "KX-A", qty=10, limit_price_cents=50, model_prob=0.70, market_close_at=close_at
    )
    h = pm.position("KX-A").buy_handle  # type: ignore[union-attr]
    assert h is not None
    await pm.on_fill(h, filled_qty=10, avg_price_cents=50)
    # Outside settle window: no sell triggered.
    await pm.on_quote("KX-A", yes_bid_cents=55, yes_ask_cents=56)
    assert pm.position("KX-A").state is FSMState.HOLDING  # type: ignore[union-attr]
    assert router.sells == []
    # Step into the settle window.
    clock.advance(20 * 60)
    await pm.on_quote("KX-A", yes_bid_cents=55, yes_ask_cents=56)
    assert pm.position("KX-A").state is FSMState.SELL_PENDING  # type: ignore[union-attr]
    assert captured and captured[0].ticker == "KX-A"


@pytest.mark.asyncio
async def test_settle_window_holds_when_on_pace_true() -> None:
    def on_pace(_pos: PositionState) -> bool:
        return True

    cfg = _config(tp_pct=0.5, sl_pct=0.5, settle_hold_minutes=15)
    pm, router, clock = _mgr(config=cfg, on_pace=on_pace)
    close_at = clock() + timedelta(minutes=10)  # already inside settle window
    await pm.open_position(
        "KX-A", qty=10, limit_price_cents=50, model_prob=0.70, market_close_at=close_at
    )
    h = pm.position("KX-A").buy_handle  # type: ignore[union-attr]
    assert h is not None
    await pm.on_fill(h, filled_qty=10, avg_price_cents=50)
    # Even a TP-level quote should not sell because settle-window+on_pace=True holds.
    await pm.on_quote("KX-A", yes_bid_cents=99, yes_ask_cents=99)
    assert pm.position("KX-A").state is FSMState.HOLDING  # type: ignore[union-attr]
    assert router.sells == []


@pytest.mark.asyncio
async def test_settle_window_holds_when_no_pace_callable() -> None:
    cfg = _config(tp_pct=0.5, sl_pct=0.5, settle_hold_minutes=15)
    pm, router, clock = _mgr(config=cfg, on_pace=None)
    close_at = clock() + timedelta(minutes=5)  # already inside settle window
    await pm.open_position(
        "KX-A", qty=10, limit_price_cents=50, model_prob=0.70, market_close_at=close_at
    )
    h = pm.position("KX-A").buy_handle  # type: ignore[union-attr]
    assert h is not None
    await pm.on_fill(h, filled_qty=10, avg_price_cents=50)
    await pm.on_quote("KX-A", yes_bid_cents=50, yes_ask_cents=51)
    assert pm.position("KX-A").state is FSMState.HOLDING  # type: ignore[union-attr]
    assert router.sells == []


@pytest.mark.asyncio
async def test_cooldown_blocks_reentry_until_window_elapses() -> None:
    cfg = _config(tp_pct=0.5, sl_pct=0.5, cooldown_after_exit_seconds=30)
    pm, _router, clock = _mgr(config=cfg)
    await pm.open_position("KX-A", qty=10, limit_price_cents=50, model_prob=0.70)
    bh = pm.position("KX-A").buy_handle  # type: ignore[union-attr]
    assert bh is not None
    await pm.on_fill(bh, filled_qty=10, avg_price_cents=50)
    # Drive a TP exit.
    await pm.on_quote("KX-A", yes_bid_cents=80, yes_ask_cents=81)
    sh = pm.position("KX-A").sell_handle  # type: ignore[union-attr]
    assert sh is not None
    await pm.on_fill(sh, filled_qty=10, avg_price_cents=80)
    assert pm.position("KX-A").state is FSMState.CLOSED  # type: ignore[union-attr]
    assert pm.is_in_cooldown("KX-A") is True

    ok = await pm.open_position("KX-A", qty=5, limit_price_cents=50, model_prob=0.70)
    assert ok is False

    clock.advance(31)
    assert pm.is_in_cooldown("KX-A") is False
    ok2 = await pm.open_position("KX-A", qty=5, limit_price_cents=50, model_prob=0.70)
    assert ok2 is True


@pytest.mark.asyncio
async def test_multi_position_independence() -> None:
    cfg = _config(tp_pct=0.5, sl_pct=0.5, conviction_hold=0.85)
    pm, router, _ = _mgr(config=cfg)
    # Position A: conviction override
    await pm.open_position("KX-A", qty=10, limit_price_cents=50, model_prob=0.90)
    await pm.on_fill(
        pm.position("KX-A").buy_handle, filled_qty=10, avg_price_cents=50  # type: ignore[union-attr,arg-type]
    )
    # Position B: low conviction
    await pm.open_position("KX-B", qty=10, limit_price_cents=50, model_prob=0.70)
    await pm.on_fill(
        pm.position("KX-B").buy_handle, filled_qty=10, avg_price_cents=50  # type: ignore[union-attr,arg-type]
    )
    # SL fires for both. Only B should sell.
    await pm.on_quote("KX-A", yes_bid_cents=10, yes_ask_cents=11)
    await pm.on_quote("KX-B", yes_bid_cents=10, yes_ask_cents=11)
    assert pm.position("KX-A").state is FSMState.HOLDING  # type: ignore[union-attr]
    assert pm.position("KX-B").state is FSMState.SELL_PENDING  # type: ignore[union-attr]
    assert len(router.sells) == 1
    assert router.sells[0].handle.ticker == "KX-B"


@pytest.mark.asyncio
async def test_flatten_all_sells_every_holding_at_last_bid() -> None:
    pm, router, _ = _mgr()
    for ticker, bid in (("KX-A", 60), ("KX-B", 55), ("KX-C", 70)):
        await pm.open_position(ticker, qty=5, limit_price_cents=50, model_prob=0.90)
        h = pm.position(ticker).buy_handle  # type: ignore[union-attr]
        assert h is not None
        await pm.on_fill(h, filled_qty=5, avg_price_cents=50)
        # Seed last_bid via on_quote (conviction override keeps it in HOLDING).
        await pm.on_quote(ticker, yes_bid_cents=bid, yes_ask_cents=bid + 1)

    count = await pm.flatten_all()
    assert count == 3
    assert len(router.sells) == 3
    by_ticker = {s.handle.ticker: s.price_cents for s in router.sells}
    assert by_ticker == {"KX-A": 60, "KX-B": 55, "KX-C": 70}


@pytest.mark.asyncio
async def test_no_double_down_in_active_states() -> None:
    pm, _router, _ = _mgr()
    assert (
        await pm.open_position("KX-A", qty=5, limit_price_cents=50, model_prob=0.70)
        is True
    )
    # BUY_PENDING
    assert (
        await pm.open_position("KX-A", qty=5, limit_price_cents=50, model_prob=0.70)
        is False
    )
    # HOLDING
    h = pm.position("KX-A").buy_handle  # type: ignore[union-attr]
    assert h is not None
    await pm.on_fill(h, filled_qty=5, avg_price_cents=50)
    assert pm.position("KX-A").state is FSMState.HOLDING  # type: ignore[union-attr]
    assert (
        await pm.open_position("KX-A", qty=5, limit_price_cents=50, model_prob=0.70)
        is False
    )
    # SELL_PENDING
    await pm.on_quote("KX-A", yes_bid_cents=80, yes_ask_cents=81)
    assert pm.position("KX-A").state is FSMState.SELL_PENDING  # type: ignore[union-attr]
    assert (
        await pm.open_position("KX-A", qty=5, limit_price_cents=50, model_prob=0.70)
        is False
    )


# --- Task 2.8: no 1c dump when bid is missing --------------------------------

@pytest.mark.asyncio
async def test_partial_sell_retry_holds_when_no_bid() -> None:
    """Partial sell retry must NOT place a 1c order when last_bid_cents is None."""
    pm, router, _ = _mgr(config=_config(tp_pct=0.5, sl_pct=0.5))
    await pm.open_position("KX-A", qty=10, limit_price_cents=50, model_prob=0.7)
    bh = pm.position("KX-A").buy_handle  # type: ignore[union-attr]
    assert bh is not None
    await pm.on_fill(bh, filled_qty=10, avg_price_cents=50)
    # Trigger a sell
    await pm.on_quote("KX-A", yes_bid_cents=80, yes_ask_cents=82)
    sh = pm.position("KX-A").sell_handle  # type: ignore[union-attr]
    assert sh is not None
    # Partial fill (5 of 10) - position still sell-pending with no bid known
    # Force last_bid_cents to None to simulate no quote seen after partial
    from dataclasses import replace as dc_replace
    pm._positions["KX-A"] = dc_replace(pm._positions["KX-A"], last_bid_cents=None)  # type: ignore[attr-defined]
    router.sells.clear()
    await pm.on_fill(sh, filled_qty=5, avg_price_cents=80)
    # Retry should NOT have placed a 1c sell
    assert all(s.price_cents != 1 for s in router.sells), "must not sell at 1c when bid is missing"


@pytest.mark.asyncio
async def test_flatten_all_skips_positions_with_no_bid() -> None:
    """flatten_all must NOT issue sells at 1c when last_bid_cents is None."""
    pm, router, _ = _mgr(config=_config(tp_pct=0.5, sl_pct=0.5))
    await pm.open_position("KX-A", qty=10, limit_price_cents=50, model_prob=0.7)
    bh = pm.position("KX-A").buy_handle  # type: ignore[union-attr]
    assert bh is not None
    await pm.on_fill(bh, filled_qty=10, avg_price_cents=50)
    # Ensure last_bid_cents is None (no quote received yet)
    from dataclasses import replace as dc_replace
    pm._positions["KX-A"] = dc_replace(pm._positions["KX-A"], last_bid_cents=None)  # type: ignore[attr-defined]
    issued = await pm.flatten_all()
    # Should issue 0 sells (skip the position) instead of selling at 1c
    assert issued == 0
    assert router.sells == []

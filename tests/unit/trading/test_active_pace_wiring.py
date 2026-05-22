from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta

import pytest

from app.trading.active_trader import (
    ActiveCandidate,
    ActiveTrader,
    make_on_pace_callable,
)
from app.trading.live_limits import ACTIVE_TRADER_DEFAULTS, ActiveTraderConfig
from app.trading.live_stats_poller import LivePlayerStats, LiveStatsPoller
from app.trading.market_book import MarketBook
from app.trading.portfolio_manager import (
    FSMState,
    OrderHandle,
    PortfolioManager,
    PositionState,
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
class _CapturePortfolio:
    """Minimal portfolio stub that records open_position kwargs."""

    opened: list[dict[str, object]] = field(default_factory=list)

    def position(self, ticker: str) -> None:
        return None

    def positions(self) -> dict[str, PositionState]:
        return {}

    def is_in_cooldown(self, ticker: str) -> bool:
        return False

    async def open_position(
        self,
        ticker: str,
        qty: int,
        limit_price_cents: int,
        model_prob: float,
        line_value: float | None = None,
        market_close_at: datetime | None = None,
        player_id: int | None = None,
        stat_type: str | None = None,
    ) -> bool:
        self.opened.append(
            {
                "ticker": ticker,
                "qty": qty,
                "player_id": player_id,
                "stat_type": stat_type,
                "line_value": line_value,
            }
        )
        return True


def _config(**overrides: object) -> ActiveTraderConfig:
    return replace(ACTIVE_TRADER_DEFAULTS, **overrides)  # type: ignore[arg-type]


def _stat(
    *,
    player_id: int = 1,
    points: int = 0,
    minutes_played: float = 0.0,
    period: int = 3,
    clock_seconds_remaining: float = 0.0,
) -> LivePlayerStats:
    return LivePlayerStats(
        player_id=player_id,
        points=points,
        rebounds=0,
        assists=0,
        threes=0,
        turnovers=0,
        minutes_played=minutes_played,
        period=period,
        clock_seconds_remaining=clock_seconds_remaining,
        last_updated=datetime.now(UTC),
    )


def _seeded_poller(*stats: LivePlayerStats) -> LiveStatsPoller:
    async def fetch(ids: list[int]) -> dict[str, object]:
        return {"data": []}

    poller = LiveStatsPoller(
        config=ACTIVE_TRADER_DEFAULTS,
        player_ids_source=set,
        fetch=fetch,
    )
    for stat in stats:
        poller._view[stat.player_id] = stat
    return poller


def _pos(**overrides: object) -> PositionState:
    base = PositionState(
        ticker="KX-A",
        state=FSMState.HOLDING,
        player_id=1,
        stat_type="points",
        line_value=25.5,
    )
    return replace(base, **overrides)  # type: ignore[arg-type]


# ----------------------------------------------------- ActiveCandidate / threading
def test_active_candidate_carries_pace_inputs() -> None:
    cand = ActiveCandidate(
        ticker="KX-A",
        model_prob=0.7,
        edge_bps=200.0,
        line_value=24.5,
        market_close_at=None,
        stake_cents=1000,
        player_id=42,
        stat_type="points",
    )
    assert cand.player_id == 42
    assert cand.stat_type == "points"


def test_active_candidate_pace_inputs_default_none() -> None:
    cand = ActiveCandidate(
        ticker="KX-A",
        model_prob=0.7,
        edge_bps=200.0,
        line_value=24.5,
        market_close_at=None,
        stake_cents=1000,
    )
    assert cand.player_id is None
    assert cand.stat_type is None


@pytest.mark.asyncio
async def test_open_position_threads_pace_inputs_into_state() -> None:
    pm = PortfolioManager(config=_config(), router=_FakeRouter(), clock=_Clock())
    await pm.open_position(
        "KX-A",
        qty=10,
        limit_price_cents=50,
        model_prob=0.7,
        player_id=99,
        stat_type="rebounds",
    )
    pos = pm.position("KX-A")
    assert pos is not None
    assert pos.player_id == 99
    assert pos.stat_type == "rebounds"


@pytest.mark.asyncio
async def test_place_forwards_pace_inputs_to_open_position() -> None:
    portfolio = _CapturePortfolio()
    trader = ActiveTrader(
        config=_config(enabled=True),
        risk_limits=RiskLimits(),
        book=MarketBook(),
        portfolio=portfolio,  # type: ignore[arg-type]
        candidates_source=_CapturePortfolio(),  # type: ignore[arg-type]
    )
    cand = ActiveCandidate(
        ticker="KX-A",
        model_prob=0.7,
        edge_bps=200.0,
        line_value=24.5,
        market_close_at=None,
        stake_cents=1000,
        player_id=7,
        stat_type="assists",
    )
    await trader._place(cand, yes_ask=0.50)
    assert portfolio.opened == [
        {
            "ticker": "KX-A",
            "qty": 20,
            "player_id": 7,
            "stat_type": "assists",
            "line_value": 24.5,
        }
    ]


# ----------------------------------------------------------------- adapter unit
def test_adapter_returns_on_pace_result() -> None:
    poller = _seeded_poller(
        _stat(player_id=1, points=30, minutes_played=30.0, period=3, clock_seconds_remaining=330)
    )
    on_pace = make_on_pace_callable(poller)
    assert on_pace(_pos(player_id=1, stat_type="points", line_value=25.5)) is True


def test_adapter_returns_false_when_off_pace() -> None:
    poller = _seeded_poller(
        _stat(player_id=1, points=5, minutes_played=30.0, period=4, clock_seconds_remaining=60)
    )
    on_pace = make_on_pace_callable(poller)
    assert on_pace(_pos(player_id=1, stat_type="points", line_value=25.5)) is False


def test_adapter_raises_when_pace_inputs_missing() -> None:
    poller = _seeded_poller(_stat(player_id=1, points=30, minutes_played=30.0))
    on_pace = make_on_pace_callable(poller)
    with pytest.raises(ValueError):
        on_pace(_pos(player_id=None, stat_type="points", line_value=25.5))
    with pytest.raises(ValueError):
        on_pace(_pos(player_id=1, stat_type=None, line_value=25.5))
    with pytest.raises(ValueError):
        on_pace(_pos(player_id=1, stat_type="points", line_value=None))


def test_adapter_raises_when_no_live_stats() -> None:
    poller = _seeded_poller()  # empty view -> outage
    on_pace = make_on_pace_callable(poller)
    with pytest.raises(LookupError):
        on_pace(_pos(player_id=1, stat_type="points", line_value=25.5))


# --------------------------------------------------- FSM settle-window integration
async def _hold_in_settle_window(
    poller: LiveStatsPoller, *, player_id: int | None, stat_type: str | None
) -> tuple[PortfolioManager, _FakeRouter, _Clock]:
    router = _FakeRouter()
    clock = _Clock()
    cfg = _config(tp_pct=0.5, sl_pct=0.5, settle_hold_minutes=15)
    pm = PortfolioManager(
        config=cfg,
        router=router,
        clock=clock,
        on_pace_callable=make_on_pace_callable(poller),
    )
    close_at = clock() + timedelta(minutes=10)  # inside the 15-min settle window
    await pm.open_position(
        "KX-A",
        qty=10,
        limit_price_cents=50,
        model_prob=0.70,
        line_value=25.5,
        market_close_at=close_at,
        player_id=player_id,
        stat_type=stat_type,
    )
    h = pm.position("KX-A").buy_handle  # type: ignore[union-attr]
    assert h is not None
    await pm.on_fill(h, filled_qty=10, avg_price_cents=50)
    return pm, router, clock


@pytest.mark.asyncio
async def test_settle_window_holds_when_pace_true() -> None:
    poller = _seeded_poller(
        _stat(player_id=1, points=30, minutes_played=30.0, period=3, clock_seconds_remaining=330)
    )
    pm, router, _ = await _hold_in_settle_window(poller, player_id=1, stat_type="points")
    # SL-level quote would normally sell, but on_pace=True holds through settle.
    await pm.on_quote("KX-A", yes_bid_cents=20, yes_ask_cents=22)
    assert pm.position("KX-A").state is FSMState.HOLDING  # type: ignore[union-attr]
    assert router.sells == []


@pytest.mark.asyncio
async def test_settle_window_sells_when_pace_false() -> None:
    poller = _seeded_poller(
        _stat(player_id=1, points=5, minutes_played=30.0, period=4, clock_seconds_remaining=60)
    )
    pm, router, _ = await _hold_in_settle_window(poller, player_id=1, stat_type="points")
    await pm.on_quote("KX-A", yes_bid_cents=55, yes_ask_cents=56)
    assert pm.position("KX-A").state is FSMState.SELL_PENDING  # type: ignore[union-attr]
    assert len(router.sells) == 1


@pytest.mark.asyncio
async def test_settle_window_holds_on_bdl_outage() -> None:
    poller = _seeded_poller()  # no live data -> adapter raises -> FSM holds
    pm, router, _ = await _hold_in_settle_window(poller, player_id=1, stat_type="points")
    await pm.on_quote("KX-A", yes_bid_cents=20, yes_ask_cents=22)  # SL-level
    assert pm.position("KX-A").state is FSMState.HOLDING  # type: ignore[union-attr]
    assert router.sells == []

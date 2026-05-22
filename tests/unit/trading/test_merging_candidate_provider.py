from __future__ import annotations

import asyncio
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime

import pytest

from app.trading.active_trader import (
    ActiveCandidate,
    ActiveTrader,
    MergingCandidateProvider,
)
from app.trading.live_limits import ACTIVE_TRADER_DEFAULTS, ActiveTraderConfig
from app.trading.live_stats_poller import LivePlayerStats, LiveStatsPoller
from app.trading.market_book import MarketBook, MarketEntry
from app.trading.portfolio_manager import FSMState, OrderHandle, PortfolioManager
from app.trading.risk import RiskLimits


# ----------------------------------------------------------------------- fakes
@dataclass
class _FakeBase:
    items: list[ActiveCandidate] = field(default_factory=list)

    async def candidates(self) -> list[ActiveCandidate]:
        return list(self.items)


@dataclass
class _PlacedOrder:
    handle: OrderHandle
    qty: int
    price_cents: int


@dataclass
class _FakeRouter:
    buys: list[_PlacedOrder] = field(default_factory=list)
    sells: list[_PlacedOrder] = field(default_factory=list)
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
        return None


class _Clock:
    def __init__(self) -> None:
        self._now = datetime(2026, 5, 19, 19, 0, 0, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self._now


def _config(**overrides: object) -> ActiveTraderConfig:
    return replace(ACTIVE_TRADER_DEFAULTS, **overrides)  # type: ignore[arg-type]


def _cand(
    ticker: str,
    *,
    player_id: int | None = 1,
    stat_type: str | None = "points",
    line_value: float = 20.0,
    model_prob: float = 0.70,
    edge_bps: float = 200.0,
    stake_cents: int = 1000,
) -> ActiveCandidate:
    return ActiveCandidate(
        ticker=ticker,
        model_prob=model_prob,
        edge_bps=edge_bps,
        line_value=line_value,
        market_close_at=None,
        stake_cents=stake_cents,
        player_id=player_id,
        stat_type=stat_type,
    )


def _signaling_stat(player_id: int) -> LivePlayerStats:
    # 15 pts == line(20) * 0.75 threshold; Q3 + 2:24 -> 14.4 min == 30% of 48.
    return LivePlayerStats(
        player_id=player_id,
        points=15,
        rebounds=0,
        assists=0,
        threes=0,
        turnovers=0,
        minutes_played=20.0,
        period=3,
        clock_seconds_remaining=144,
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


# ------------------------------------------------------------------ merge logic
@pytest.mark.asyncio
async def test_base_candidates_pass_through() -> None:
    base = _FakeBase([_cand("KX-A"), _cand("KX-B")])
    provider = MergingCandidateProvider(
        base=base, poller=_seeded_poller(), preemptive_source=list
    )
    out = await provider.candidates()
    assert [c.ticker for c in out] == ["KX-A", "KX-B"]


@pytest.mark.asyncio
async def test_preemptive_added_when_signaling() -> None:
    poller = _seeded_poller(_signaling_stat(1))
    provider = MergingCandidateProvider(
        base=_FakeBase([]),
        poller=poller,
        preemptive_source=lambda: [_cand("KX-PRE", player_id=1)],
    )
    out = await provider.candidates()
    assert [c.ticker for c in out] == ["KX-PRE"]


@pytest.mark.asyncio
async def test_preemptive_skipped_when_not_signaling() -> None:
    # Player has no live data -> is_signaling_entry False.
    provider = MergingCandidateProvider(
        base=_FakeBase([]),
        poller=_seeded_poller(),
        preemptive_source=lambda: [_cand("KX-PRE", player_id=1)],
    )
    out = await provider.candidates()
    assert out == []


@pytest.mark.asyncio
async def test_preemptive_deduped_against_base_ticker() -> None:
    poller = _seeded_poller(_signaling_stat(1))
    base = _FakeBase([_cand("KX-A", model_prob=0.9)])
    provider = MergingCandidateProvider(
        base=base,
        poller=poller,
        preemptive_source=lambda: [_cand("KX-A", player_id=1, model_prob=0.6)],
    )
    out = await provider.candidates()
    assert len(out) == 1
    assert out[0].model_prob == 0.9  # base candidate kept, pre-emptive dup dropped


@pytest.mark.asyncio
async def test_preemptive_skipped_without_player_mapping() -> None:
    poller = _seeded_poller(_signaling_stat(1))
    provider = MergingCandidateProvider(
        base=_FakeBase([]),
        poller=poller,
        preemptive_source=lambda: [_cand("KX-PRE", player_id=None, stat_type=None)],
    )
    out = await provider.candidates()
    assert out == []


# ----------------------------------------------------------- end-to-end through gates
@pytest.mark.asyncio
async def test_preemptive_candidate_flows_through_entry_gates() -> None:
    poller = _seeded_poller(_signaling_stat(1))
    provider = MergingCandidateProvider(
        base=_FakeBase([]),
        poller=poller,
        preemptive_source=lambda: [_cand("KX-PRE", player_id=1, stat_type="points")],
    )
    cfg = _config(enabled=True)
    clock = _Clock()
    router = _FakeRouter()
    book = MarketBook()
    pm = PortfolioManager(config=cfg, router=router, clock=clock)
    trader = ActiveTrader(
        config=cfg,
        risk_limits=RiskLimits(),
        book=book,
        portfolio=pm,
        candidates_source=provider,
        clock=clock,
    )
    task = asyncio.create_task(trader.run())
    await asyncio.sleep(0.01)  # let run() subscribe
    await book.update(
        MarketEntry(
            ticker="KX-PRE",
            yes_bid=0.48,
            yes_ask=0.50,
            no_bid=None,
            no_ask=None,
            last=None,
            spread=None,
            status="active",
            updated_at=clock(),
        )
    )
    await asyncio.sleep(0.01)  # let the loop process the update
    assert len(router.buys) == 1  # pre-emptive candidate passed all entry gates
    pos = pm.position("KX-PRE")
    assert pos is not None
    assert pos.state is FSMState.BUY_PENDING
    assert pos.player_id == 1 and pos.stat_type == "points"
    await trader.stop()
    await task

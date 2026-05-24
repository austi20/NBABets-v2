"""ActiveTrader loop for the paper-mode active trader.

Wires a ``MarketBook`` subscription to a ``PortfolioManager``, applying entry
gates to candidates supplied by an injected ``CandidateProvider``. No live I/O:
the router lives behind ``PortfolioManager`` and candidates are supplied
externally.

Spec: docs/plans/specs/profit-machine-design.md section 3.1.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from uuid import uuid4

from app.trading.live_limits import ActiveTraderConfig
from app.trading.live_stats_poller import LiveStatsPoller
from app.trading.market_book import MarketBook
from app.trading.portfolio_manager import FSMState, PortfolioManager, PositionState
from app.trading.protocols import PortfolioLedger, RiskEngine
from app.trading.risk import RiskLimits
from app.trading.types import ExecutionIntent, MarketRef, Signal

logger = logging.getLogger(__name__)

_ACTIVE_STATES = (FSMState.BUY_PENDING, FSMState.HOLDING, FSMState.SELL_PENDING)


def _candidate_to_intent(cand: ActiveCandidate) -> ExecutionIntent:
    """Build a minimal ExecutionIntent from an ActiveCandidate for risk evaluation."""
    now = datetime.now(UTC)
    signal = Signal(
        signal_id=str(uuid4()),
        created_at=now,
        market_key=cand.ticker,
        side="yes",
        confidence="high",
        edge=cand.edge_bps / 10_000,
        model_probability=cand.model_prob,
        line_value=cand.line_value,
    )
    market = MarketRef(
        exchange="kalshi",
        symbol=cand.ticker,
        market_key=cand.ticker,
        side="yes",
        line_value=cand.line_value,
    )
    return ExecutionIntent(
        intent_id=str(uuid4()),
        signal=signal,
        market=market,
        side="buy",
        stake=cand.stake_cents / 100,
    )


@dataclass(frozen=True)
class ActiveCandidate:
    ticker: str
    model_prob: float
    edge_bps: float
    line_value: float
    market_close_at: datetime | None
    stake_cents: int  # already sized upstream
    # Live-pace inputs (Phase 2); None for candidates without a live mapping.
    player_id: int | None = None
    stat_type: str | None = None


class CandidateProvider(Protocol):
    async def candidates(self) -> Iterable[ActiveCandidate]: ...


class MergingCandidateProvider:
    """Merge decision-pack candidates with pre-emptive live-pace signals.

    Base (decision-pack) candidates always pass through. A pre-emptive candidate
    from ``preemptive_source`` is added only when it is not already present (dedup
    by ticker) and the live poller reports ``is_signaling_entry``. All merged
    candidates still flow through ``ActiveTrader._consider``, so the entry gates
    remain the single chokepoint ("pounce before the market reprices").

    Spec: docs/plans/specs/profit-machine-design.md §1 (live-stats edge, use 2).
    """

    def __init__(
        self,
        *,
        base: CandidateProvider,
        poller: LiveStatsPoller,
        preemptive_source: Callable[[], Iterable[ActiveCandidate]],
    ) -> None:
        self._base = base
        self._poller = poller
        self._preemptive_source = preemptive_source

    async def candidates(self) -> list[ActiveCandidate]:
        merged = list(await self._base.candidates())
        seen = {cand.ticker for cand in merged}
        for cand in self._preemptive_source():
            if cand.ticker in seen:
                continue
            if cand.player_id is None or cand.stat_type is None:
                continue
            if self._poller.is_signaling_entry(
                cand.player_id, cand.stat_type, cand.line_value
            ):
                merged.append(cand)
                seen.add(cand.ticker)
        return merged


def make_on_pace_callable(
    poller: LiveStatsPoller,
) -> Callable[[PositionState], bool]:
    """Adapt a PositionState to ``poller.on_pace_to_hit`` for the FSM settle window.

    Raises when pace inputs are missing or live stats are unavailable (BDL
    outage). ``PortfolioManager`` swallows the exception and holds the position,
    which is the desired default at the 15-minute settle lockout.
    """

    def on_pace(pos: PositionState) -> bool:
        if pos.player_id is None or pos.stat_type is None or pos.line_value is None:
            raise ValueError(f"position {pos.ticker} missing pace inputs")
        if poller.current_stat(pos.player_id, pos.stat_type) is None:
            raise LookupError(f"no live stats for player {pos.player_id}")
        return poller.on_pace_to_hit(pos.player_id, pos.stat_type, pos.line_value)

    return on_pace


def _to_cents(price: float | None) -> int:
    return int(round(price * 100)) if price is not None else 0


class ActiveTrader:
    """Drives entry decisions off a MarketBook subscription (paper mode)."""

    def __init__(
        self,
        *,
        config: ActiveTraderConfig,
        risk_limits: RiskLimits,
        book: MarketBook,
        portfolio: PortfolioManager,
        candidates_source: CandidateProvider,
        kill_switch: Callable[[], bool] = lambda: False,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        risk_engine: RiskEngine | None = None,
        ledger: PortfolioLedger | None = None,
    ) -> None:
        self._config = config
        self._risk = risk_limits
        self._book = book
        self._portfolio = portfolio
        self._candidates = candidates_source
        self._kill_switch = kill_switch
        self._clock = clock
        self._risk_engine = risk_engine
        self._ledger = ledger
        self._stopping = False
        self._stop_event = asyncio.Event()

    # ------------------------------------------------------------------ lifecycle
    async def run(self) -> None:
        sub = self._book.subscribe()
        try:
            async for update in self._iter_until_stop(sub):
                if self._stopping:
                    break
                entry = update.after
                if self._kill_switch():
                    await self._portfolio.flatten_all()
                    return
                await self._portfolio.on_quote(
                    entry.ticker, _to_cents(entry.yes_bid), _to_cents(entry.yes_ask)
                )
                await self._maybe_enter_new_positions()
        finally:
            await sub.aclose()

    async def stop(self) -> None:
        self._stopping = True
        self._stop_event.set()

    async def _iter_until_stop(self, sub):  # type: ignore[no-untyped-def]
        """Yield book updates, but bail promptly when stop() is called.

        A blocked ``queue.get()`` must not pin the loop, so we race the next
        update against the stop event and clean up the pending task on stop.
        """
        stop_task = asyncio.ensure_future(self._stop_event.wait())
        try:
            while not self._stopping:
                anext_task = asyncio.ensure_future(sub.__anext__())
                done, _pending = await asyncio.wait(
                    {anext_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
                )
                if anext_task in done:
                    try:
                        yield anext_task.result()
                    except StopAsyncIteration:
                        return
                else:
                    anext_task.cancel()
                    try:
                        await anext_task
                    except (asyncio.CancelledError, StopAsyncIteration):
                        pass
                    return
        finally:
            stop_task.cancel()

    # ------------------------------------------------------------------ entries
    async def _maybe_enter_new_positions(self) -> None:
        if not self._config.enabled:
            return
        cands = await self._candidates.candidates()
        for cand in cands:
            try:
                await self._consider(cand)
            except Exception as exc:  # noqa: BLE001 - isolate one bad candidate
                logger.warning(
                    "active-trader: candidate %s raised during consideration: %s",
                    cand.ticker, exc,
                )
                continue

    async def _consider(self, cand: ActiveCandidate) -> None:
        if not self._book.is_connected():
            logger.debug("skip %s: market book disconnected", cand.ticker)
            return
        cfg = self._config
        if cand.model_prob < cfg.min_model_prob:
            logger.debug("skip %s: model_prob %.3f < %.3f", cand.ticker, cand.model_prob, cfg.min_model_prob)
            return
        entry = self._book.get(cand.ticker)
        if entry is None or entry.yes_ask is None or entry.yes_bid is None:
            logger.debug("skip %s: no two-sided quote", cand.ticker)
            return
        if entry.yes_ask > cfg.max_entry_price:
            logger.debug("skip %s: yes_ask %.2f > %.2f", cand.ticker, entry.yes_ask, cfg.max_entry_price)
            return
        if (entry.yes_ask - entry.yes_bid) * 100 > cfg.spread_max_cents:
            logger.debug("skip %s: spread > %d cents", cand.ticker, cfg.spread_max_cents)
            return
        if cand.edge_bps < cfg.min_edge_bps:
            logger.debug("skip %s: edge_bps %.0f < %d", cand.ticker, cand.edge_bps, cfg.min_edge_bps)
            return
        vol = self._book.stdev_cents(cand.ticker, 60)
        if vol is not None and vol > cfg.volatility_max_cents and cand.model_prob < cfg.volatility_override:
            logger.debug("skip %s: volatility %.2f cents > %d", cand.ticker, vol, cfg.volatility_max_cents)
            return
        existing = self._portfolio.position(cand.ticker)
        if existing is not None and existing.state in _ACTIVE_STATES:
            logger.debug("skip %s: already active (%s)", cand.ticker, existing.state)
            return
        if self._portfolio.is_in_cooldown(cand.ticker):
            logger.debug("skip %s: in cooldown", cand.ticker)
            return
        if self._open_position_count() >= cfg.max_open_positions:
            logger.debug("skip %s: position cap %d reached", cand.ticker, cfg.max_open_positions)
            return
        if not self._risk_ok(cand):
            return
        await self._place(cand, entry.yes_ask)

    # ------------------------------------------------------------------ helpers
    def _open_position_count(self) -> int:
        return sum(
            1 for p in self._portfolio.positions().values() if p.state != FSMState.CLOSED
        )

    def _open_stake_dollars(self, ticker: str | None = None) -> float:
        """Sum open stake (dollars) across non-CLOSED positions.

        BUY_PENDING positions have qty=0 until filled, so use requested_qty;
        in both cases entry_price_cents holds the relevant per-share price.
        """
        total = 0.0
        for pos in self._portfolio.positions().values():
            if pos.state == FSMState.CLOSED:
                continue
            if ticker is not None and pos.ticker != ticker:
                continue
            shares = pos.qty if pos.state != FSMState.BUY_PENDING else pos.requested_qty
            total += shares * pos.entry_price_cents / 100
        return total

    def _risk_ok(self, cand: ActiveCandidate) -> bool:
        if self._risk_engine is not None and self._ledger is not None:
            intent = _candidate_to_intent(cand)
            ok, reason = self._risk_engine.evaluate(intent, self._ledger)
            if not ok:
                logger.debug("skip %s: risk engine rejected: %s", cand.ticker, reason)
            return ok
        # Fallback: manual cap checks (no engine injected).
        stake = cand.stake_cents / 100
        if stake > self._risk.per_order_cap:
            logger.debug("skip %s: per-order cap %.2f exceeded", cand.ticker, self._risk.per_order_cap)
            return False
        if self._open_stake_dollars(cand.ticker) + stake > self._risk.per_market_cap:
            logger.debug("skip %s: per-market cap %.2f exceeded", cand.ticker, self._risk.per_market_cap)
            return False
        if self._open_stake_dollars() + stake > self._risk.max_open_notional:
            logger.debug("skip %s: open-notional cap %.2f exceeded", cand.ticker, self._risk.max_open_notional)
            return False
        return True

    async def _place(self, cand: ActiveCandidate, yes_ask: float) -> None:
        limit_price_cents = int(round(yes_ask * 100))
        # Gate 11 (affordability): if the sized stake can't buy a single share at
        # the limit price, skip rather than over-spend the risk-approved stake.
        qty = cand.stake_cents // limit_price_cents
        if qty < 1:
            logger.debug(
                "skip %s: stake %dc cannot afford 1 share at %dc",
                cand.ticker, cand.stake_cents, limit_price_cents,
            )
            return
        ok = await self._portfolio.open_position(
            cand.ticker,
            qty,
            limit_price_cents,
            cand.model_prob,
            line_value=cand.line_value,
            market_close_at=cand.market_close_at,
            player_id=cand.player_id,
            stat_type=cand.stat_type,
        )
        if ok:
            logger.info(
                "entry %s qty=%d limit=%dc edge=%.0fbps prob=%.3f",
                cand.ticker, qty, limit_price_cents, cand.edge_bps, cand.model_prob,
            )
        else:
            logger.debug("open_position rejected for %s (race/double-down)", cand.ticker)

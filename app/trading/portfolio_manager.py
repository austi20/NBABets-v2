"""Multi-position FSM for the active-trader paper mode (Phase 1).

This module is intentionally I/O-free except through the injected
``OrderRouter`` protocol. All time math goes through an injected ``clock``
callable so tests can step time deterministically. Risk-gate evaluation is
the caller's job (see ``app/trading/risk.py``); this manager only enforces
the per-ticker "no double-down" rule and per-session cooldown.

Spec: docs/plans/specs/profit-machine-design.md §3.2 and §6.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Protocol

from app.trading.live_limits import ActiveTraderConfig

logger = logging.getLogger(__name__)

PARTIAL_BUY_CANCEL_AFTER_SECONDS = 10
PARTIAL_SELL_RETRY_WINDOW_SECONDS = 30
PARTIAL_SELL_MAX_ATTEMPTS = 3


class FSMState(StrEnum):
    BUY_PENDING = "buy_pending"
    HOLDING = "holding"
    SELL_PENDING = "sell_pending"
    CLOSED = "closed"


@dataclass(frozen=True)
class OrderHandle:
    """Opaque identifier the router returns for each placed order."""

    order_id: str
    ticker: str
    side: str  # "buy" or "sell"


class OrderRouter(Protocol):
    async def place_buy(
        self, ticker: str, qty: int, limit_price_cents: int
    ) -> OrderHandle: ...

    async def place_sell(
        self, ticker: str, qty: int, limit_price_cents: int
    ) -> OrderHandle: ...

    async def cancel(self, handle: OrderHandle) -> None: ...


@dataclass(frozen=True)
class PositionState:
    ticker: str
    state: FSMState
    qty: int = 0
    requested_qty: int = 0
    entry_price_cents: int = 0
    tp_price_cents: int = 0
    sl_price_cents: int = 0
    model_prob: float = 0.0
    line_value: float | None = None
    market_close_at: datetime | None = None
    last_bid_cents: int | None = None
    opened_at: datetime | None = None
    closed_at: datetime | None = None
    recently_exited_until: datetime | None = None
    buy_handle: OrderHandle | None = None
    sell_handle: OrderHandle | None = None
    # Deadline for cancelling the unfilled remainder of a partial buy.
    buy_partial_cancel_at: datetime | None = None
    # Sell-retry bookkeeping for partial fills.
    sell_remaining_qty: int = 0
    sell_attempts: int = 0
    sell_first_attempt_at: datetime | None = None
    partial_sell_residual_qty: int = 0


def _bracket_prices(entry_cents: int, tp_pct: float, sl_pct: float) -> tuple[int, int]:
    tp = int(round(entry_cents * (1.0 + tp_pct)))
    sl = int(round(entry_cents * (1.0 - sl_pct)))
    return tp, sl


class PortfolioManager:
    """Per-ticker FSM driver for the active-trader paper mode."""

    def __init__(
        self,
        *,
        config: ActiveTraderConfig,
        router: OrderRouter,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        on_pace_callable: Callable[[PositionState], bool] | None = None,
    ) -> None:
        self._config = config
        self._router = router
        self._clock = clock
        self._on_pace = on_pace_callable
        self._positions: dict[str, PositionState] = {}
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------ accessors
    def position(self, ticker: str) -> PositionState | None:
        return self._positions.get(ticker)

    def positions(self) -> Mapping[str, PositionState]:
        return dict(self._positions)

    def is_in_cooldown(self, ticker: str) -> bool:
        pos = self._positions.get(ticker)
        if pos is None or pos.recently_exited_until is None:
            return False
        return self._clock() < pos.recently_exited_until

    # ------------------------------------------------------------------ entry
    async def open_position(
        self,
        ticker: str,
        qty: int,
        limit_price_cents: int,
        model_prob: float,
        line_value: float | None = None,
        market_close_at: datetime | None = None,
    ) -> bool:
        async with self._lock:
            existing = self._positions.get(ticker)
            if existing is not None:
                if existing.state in (FSMState.BUY_PENDING, FSMState.HOLDING, FSMState.SELL_PENDING):
                    return False
                if existing.state == FSMState.CLOSED and self.is_in_cooldown(ticker):
                    return False
            handle = await self._router.place_buy(ticker, qty, limit_price_cents)
            now = self._clock()
            tp, sl = _bracket_prices(limit_price_cents, self._config.tp_pct, self._config.sl_pct)
            self._positions[ticker] = PositionState(
                ticker=ticker,
                state=FSMState.BUY_PENDING,
                qty=0,
                requested_qty=qty,
                entry_price_cents=limit_price_cents,
                tp_price_cents=tp,
                sl_price_cents=sl,
                model_prob=model_prob,
                line_value=line_value,
                market_close_at=market_close_at,
                opened_at=now,
                buy_handle=handle,
            )
            return True

    # ------------------------------------------------------------------ quotes
    async def on_quote(self, ticker: str, yes_bid_cents: int, yes_ask_cents: int) -> None:
        async with self._lock:
            pos = self._positions.get(ticker)
            if pos is None:
                return
            now = self._clock()
            pos = replace(pos, last_bid_cents=yes_bid_cents)
            self._positions[ticker] = pos
            # Cancel-remainder deadline for partial buys.
            if (
                pos.state == FSMState.BUY_PENDING
                and pos.buy_partial_cancel_at is not None
                and now >= pos.buy_partial_cancel_at
            ):
                await self._finalize_partial_buy(pos)
                pos = self._positions[ticker]
            if pos.state != FSMState.HOLDING:
                return
            high_conviction = pos.model_prob >= self._config.conviction_hold
            # Settle-window check first - it can flatten conviction holds via on_pace=False.
            if self._settle_window_active(pos, now):
                decision = self._evaluate_on_pace(pos)
                if decision is False:
                    await self._enqueue_sell_at_bid(ticker, yes_bid_cents)
                    return
                # decision True or None -> hold.
                return
            if high_conviction:
                return
            if yes_bid_cents >= pos.tp_price_cents:
                await self._enqueue_sell_at_bid(ticker, yes_bid_cents)
                return
            if yes_bid_cents <= pos.sl_price_cents:
                await self._enqueue_sell_at_bid(ticker, yes_bid_cents)
                return

    def _settle_window_active(self, pos: PositionState, now: datetime) -> bool:
        if pos.market_close_at is None:
            return False
        window = timedelta(minutes=self._config.settle_hold_minutes)
        return now >= pos.market_close_at - window

    def _evaluate_on_pace(self, pos: PositionState) -> bool | None:
        if self._on_pace is None:
            return None
        try:
            return bool(self._on_pace(pos))
        except Exception:  # noqa: BLE001 - never let pace callable break the FSM
            logger.warning("on_pace_callable raised for %s; treating as hold", pos.ticker)
            return None

    # ------------------------------------------------------------------ fills
    async def on_fill(
        self, handle: OrderHandle, filled_qty: int, avg_price_cents: int
    ) -> None:
        async with self._lock:
            pos = self._positions.get(handle.ticker)
            if pos is None:
                return
            if handle.side == "buy":
                await self._handle_buy_fill(pos, filled_qty, avg_price_cents)
            elif handle.side == "sell":
                await self._handle_sell_fill(pos, filled_qty)

    async def _handle_buy_fill(
        self, pos: PositionState, filled_qty: int, avg_price_cents: int
    ) -> None:
        if pos.state != FSMState.BUY_PENDING:
            return
        total_filled = pos.qty + filled_qty
        if total_filled >= pos.requested_qty:
            # Full fill - finalize entry at the actual avg price.
            tp, sl = _bracket_prices(
                avg_price_cents, self._config.tp_pct, self._config.sl_pct
            )
            self._positions[pos.ticker] = replace(
                pos,
                state=FSMState.HOLDING,
                qty=total_filled,
                entry_price_cents=avg_price_cents,
                tp_price_cents=tp,
                sl_price_cents=sl,
                buy_partial_cancel_at=None,
            )
            return
        # Partial fill - record the partial and arm the cancel deadline.
        now = self._clock()
        deadline = pos.buy_partial_cancel_at or (
            now + timedelta(seconds=PARTIAL_BUY_CANCEL_AFTER_SECONDS)
        )
        tp, sl = _bracket_prices(
            avg_price_cents, self._config.tp_pct, self._config.sl_pct
        )
        self._positions[pos.ticker] = replace(
            pos,
            qty=total_filled,
            entry_price_cents=avg_price_cents,
            tp_price_cents=tp,
            sl_price_cents=sl,
            buy_partial_cancel_at=deadline,
        )

    async def _finalize_partial_buy(self, pos: PositionState) -> None:
        # Cancel any unfilled remainder and move to HOLDING with what we have.
        if pos.buy_handle is not None:
            try:
                await self._router.cancel(pos.buy_handle)
            except Exception:  # noqa: BLE001
                logger.warning("cancel of partial buy failed for %s", pos.ticker)
        if pos.qty <= 0:
            # Nothing filled - go back to no-position.
            self._positions.pop(pos.ticker, None)
            return
        self._positions[pos.ticker] = replace(
            pos,
            state=FSMState.HOLDING,
            buy_partial_cancel_at=None,
            buy_handle=None,
        )

    async def _handle_sell_fill(self, pos: PositionState, filled_qty: int) -> None:
        if pos.state != FSMState.SELL_PENDING:
            return
        remaining = max(0, pos.sell_remaining_qty - filled_qty)
        if remaining <= 0:
            self._mark_closed(pos.ticker, residual_qty=0)
            return
        # Partial sell - retry within window, up to N attempts.
        now = self._clock()
        first = pos.sell_first_attempt_at or now
        window_elapsed = (now - first).total_seconds() >= PARTIAL_SELL_RETRY_WINDOW_SECONDS
        if pos.sell_attempts >= PARTIAL_SELL_MAX_ATTEMPTS or window_elapsed:
            logger.warning(
                "partial sell exhausted for %s; %d residual shares left to settle",
                pos.ticker,
                remaining,
            )
            self._mark_closed(pos.ticker, residual_qty=remaining)
            return
        bid = pos.last_bid_cents or 1
        handle = await self._router.place_sell(pos.ticker, remaining, bid)
        self._positions[pos.ticker] = replace(
            pos,
            sell_remaining_qty=remaining,
            sell_attempts=pos.sell_attempts + 1,
            sell_first_attempt_at=first,
            sell_handle=handle,
        )

    # ------------------------------------------------------------------ exits
    async def _enqueue_sell_at_bid(self, ticker: str, bid_cents: int) -> None:
        pos = self._positions.get(ticker)
        if pos is None or pos.state != FSMState.HOLDING:
            return
        handle = await self._router.place_sell(ticker, pos.qty, bid_cents)
        now = self._clock()
        self._positions[ticker] = replace(
            pos,
            state=FSMState.SELL_PENDING,
            sell_handle=handle,
            sell_remaining_qty=pos.qty,
            sell_attempts=1,
            sell_first_attempt_at=now,
        )

    async def flatten_all(self) -> int:
        """Sell every HOLDING position at last-known bid. Returns count issued."""
        async with self._lock:
            issued = 0
            for ticker, pos in list(self._positions.items()):
                if pos.state != FSMState.HOLDING:
                    continue
                if pos.last_bid_cents is None:
                    logger.warning(
                        "flatten_all: no bid observed for %s, falling back to 1c",
                        ticker,
                    )
                    bid = 1
                else:
                    bid = pos.last_bid_cents
                await self._enqueue_sell_at_bid(ticker, bid)
                issued += 1
            return issued

    def _mark_closed(self, ticker: str, *, residual_qty: int) -> None:
        pos = self._positions.get(ticker)
        if pos is None:
            return
        now = self._clock()
        cooldown_until = now + timedelta(
            seconds=self._config.cooldown_after_exit_seconds
        )
        self._positions[ticker] = replace(
            pos,
            state=FSMState.CLOSED,
            closed_at=now,
            recently_exited_until=cooldown_until,
            partial_sell_residual_qty=residual_qty,
            sell_handle=None,
        )

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, date, datetime

from app.trading.protocols import PortfolioLedger
from app.trading.types import Fill, OrderEvent, Position


@dataclass
class _PositionState:
    market_key: str
    side: str
    open_stake: float = 0.0
    weighted_price_total: float = 0.0
    realized_pnl: float = 0.0
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def avg_price(self) -> float:
        if self.open_stake <= 0:
            return 0.0
        return self.weighted_price_total / self.open_stake


class InMemoryPortfolioLedger(PortfolioLedger):
    def __init__(self) -> None:
        self._positions: dict[str, _PositionState] = {}
        self._fills: list[Fill] = []
        self._events: list[OrderEvent] = []
        self._daily_realized: defaultdict[date, float] = defaultdict(float)

    def record_order_event(self, event: OrderEvent) -> None:
        self._events.append(event)

    def record_fill(self, fill: Fill) -> None:
        self._fills.append(fill)
        position = self._positions.get(fill.market.symbol)
        if position is None:
            position = _PositionState(market_key=fill.market.market_key, side=fill.market.side)
            self._positions[fill.market.symbol] = position
        fill_stake = float(fill.stake)
        computed_realized = 0.0
        if fill.side == "sell":
            if fill_stake > position.open_stake + 1e-9:
                raise ValueError(
                    f"cannot sell stake {fill_stake:.4f} with only {position.open_stake:.4f} open on {fill.market.symbol}"
                )
            closing = min(fill_stake, position.open_stake)
            avg_price = position.avg_price
            if closing > 0:
                position.open_stake -= closing
                position.weighted_price_total = avg_price * position.open_stake
                computed_realized = (float(fill.price) - avg_price) * closing
        else:
            position.open_stake += fill_stake
            position.weighted_price_total += float(fill.price) * fill_stake
        effective_realized = float(fill.realized_pnl) + computed_realized
        position.realized_pnl += effective_realized - float(fill.fee)
        position.updated_at = fill.timestamp
        fill_day = fill.timestamp.astimezone(UTC).date()
        self._daily_realized[fill_day] += effective_realized - float(fill.fee)

    def open_positions(self) -> list[Position]:
        rows: list[Position] = []
        for market_symbol, state in self._positions.items():
            if state.open_stake <= 0:
                continue
            rows.append(
                Position(
                    market_symbol=market_symbol,
                    market_key=state.market_key,
                    side=state.side,
                    open_stake=round(state.open_stake, 4),
                    avg_price=round(state.avg_price, 6),
                    realized_pnl=round(state.realized_pnl, 4),
                    updated_at=state.updated_at,
                )
            )
        rows.sort(key=lambda row: row.updated_at, reverse=True)
        return rows

    def recent_fills(self, limit: int = 20) -> list[Fill]:
        bounded = max(0, int(limit))
        if bounded == 0:
            return []
        return list(reversed(self._fills[-bounded:]))

    def market_exposure(self, market_symbol: str) -> float:
        state = self._positions.get(market_symbol)
        if state is None:
            return 0.0
        return float(state.open_stake)

    def open_notional(self) -> float:
        return float(sum(state.open_stake for state in self._positions.values()))

    def daily_realized_pnl(self) -> float:
        today = datetime.now(UTC).date()
        return float(self._daily_realized.get(today, 0.0))

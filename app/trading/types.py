from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal

OrderSide = Literal["buy", "sell"]


@dataclass(frozen=True)
class Signal:
    signal_id: str
    created_at: datetime
    market_key: str
    side: str
    confidence: str
    edge: float
    model_probability: float
    line_value: float
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MarketRef:
    exchange: str
    symbol: str
    market_key: str
    side: str
    line_value: float


@dataclass(frozen=True)
class ExecutionIntent:
    intent_id: str
    signal: Signal
    market: MarketRef
    side: OrderSide
    stake: float
    max_slippage_bps: int = 100
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(frozen=True)
class OrderEvent:
    intent_id: str
    event_type: str
    status: str
    message: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(frozen=True)
class Fill:
    fill_id: str
    intent_id: str
    market: MarketRef
    side: OrderSide
    stake: float
    price: float
    fee: float = 0.0
    realized_pnl: float = 0.0
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(frozen=True)
class Position:
    market_symbol: str
    market_key: str
    side: str
    open_stake: float
    avg_price: float
    realized_pnl: float
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

from app.trading.types import Fill, Position


class PositionModel(BaseModel):
    market_symbol: str
    market_key: str
    side: str
    open_stake: float
    avg_price: float
    realized_pnl: float
    updated_at: datetime

    @classmethod
    def from_dataclass(cls, value: Position) -> PositionModel:
        return cls(**value.__dict__)


class FillModel(BaseModel):
    fill_id: str
    intent_id: str
    market: dict[str, object]
    side: str
    stake: float
    price: float
    fee: float
    realized_pnl: float
    timestamp: datetime

    @classmethod
    def from_dataclass(cls, value: Fill) -> FillModel:
        return cls(
            fill_id=value.fill_id,
            intent_id=value.intent_id,
            market={
                "exchange": value.market.exchange,
                "symbol": value.market.symbol,
                "market_key": value.market.market_key,
                "side": value.market.side,
                "line_value": value.market.line_value,
            },
            side=value.side,
            stake=value.stake,
            price=value.price,
            fee=value.fee,
            realized_pnl=value.realized_pnl,
            timestamp=value.timestamp,
        )


class ActiveLimitsModel(BaseModel):
    per_order_cap: float
    per_market_cap: float
    max_open_notional: float
    daily_loss_cap: float
    reject_cooldown_seconds: int


class TradingPnlModel(BaseModel):
    daily_realized_pnl: float
    kill_switch_active: bool
    active_limits: ActiveLimitsModel | None = None


class TradingIntentRequestModel(BaseModel):
    game_id: int | None = None
    player_id: int
    market: str
    line: float
    side: str
    sportsbook_key: str
    stake: float


class TradingIntentResponseModel(BaseModel):
    accepted: bool
    intent_id: str | None
    message: str


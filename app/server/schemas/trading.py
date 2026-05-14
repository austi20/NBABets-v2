from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, Field

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


class TradingQuoteModel(BaseModel):
    ticker: str
    market_key: str
    side: str | None
    line_value: float | None
    player_id: str | None
    game_date: str | None
    title: str | None
    status: str | None
    yes_bid: float | None
    yes_ask: float | None
    no_bid: float | None
    no_ask: float | None
    last_price: float | None
    entry_price: float | None
    exit_price: float | None
    spread: float | None
    observed_at: datetime
    error: str | None = None


class LivePositionModel(BaseModel):
    market_symbol: str
    market_key: str
    side: str
    ticker: str | None
    open_stake: float
    contract_count: float
    avg_price: float
    current_exit_price: float | None
    current_value: float | None
    unrealized_pnl: float | None
    unrealized_pnl_pct: float | None
    realized_pnl: float
    updated_at: datetime
    quote: TradingQuoteModel | None = None


class ExchangePositionModel(BaseModel):
    ticker: str
    side: str
    contract_count: float
    net_position: float
    market_exposure: float | None
    fees_paid: float | None
    realized_pnl: float | None
    current_exit_price: float | None
    current_value: float | None
    updated_at: datetime | None
    quote: TradingQuoteModel | None = None


class RestingOrderModel(BaseModel):
    order_id: str
    client_order_id: str | None
    ticker: str | None
    side: str | None
    status: str | None
    remaining_count: float | None
    price: float | None
    created_at: datetime | None


class TradingSnapshotModel(BaseModel):
    observed_at: datetime
    daily_realized_pnl: float
    daily_unrealized_pnl: float
    total_daily_pnl: float
    open_notional: float
    budget_used: float
    budget_remaining: float
    max_open_notional: float
    daily_loss_cap: float
    loss_progress: float
    kill_switch_active: bool
    positions: list[LivePositionModel]
    quotes: list[TradingQuoteModel]
    account_positions: list[ExchangePositionModel]
    resting_orders: list[RestingOrderModel]
    errors: list[str]


class TradingReadinessCheckModel(BaseModel):
    key: str
    label: str
    status: str
    detail: str


class TradingReadinessModel(BaseModel):
    observed_at: datetime
    state: str
    summary: str
    live_trading_enabled: bool
    credentials_configured: bool
    account_sync_enabled: bool
    decisions_path: str
    symbols_path: str
    decision_id: str | None
    ticker: str | None
    game_date: str | None
    market_status: str | None
    executable_symbol_count: int
    unresolved_symbol_count: int
    brain_state: str | None = None
    brain_policy_version: str | None = None
    brain_selected_candidate_id: str | None = None
    brain_last_sync_at: datetime | None = None
    brain_snapshot_dir: str | None = None
    checks: list[TradingReadinessCheckModel]


class TradingBrainCheckModel(BaseModel):
    key: str
    label: str
    status: str
    detail: str


class TradingBrainSyncRequestModel(BaseModel):
    board_date: date | None = None
    mode: Literal["observe", "supervised-live"] = "observe"
    candidate_limit: int | None = Field(default=None, ge=1, le=250)
    resolve_markets: bool = True
    build_pack: bool = True


class TradingBrainSyncModel(BaseModel):
    state: str
    policy_version: str | None
    policy_hash: str | None
    board_date: str
    mode: str
    generated_candidate_count: int
    manual_candidate_count: int
    exported_target_count: int
    resolved_symbol_count: int
    unresolved_symbol_count: int
    selected_candidate_id: str | None
    selected_ticker: str | None
    selected_candidate_ids: list[str] = Field(default_factory=list)
    selected_tickers: list[str] = Field(default_factory=list)
    live_candidate_count: int = 0
    targets_path: str
    symbols_path: str
    decisions_path: str
    snapshot_dir: str | None
    checks: list[TradingBrainCheckModel]
    synced_at: datetime


class TradingLoopStatusModel(BaseModel):
    state: str
    message: str
    pid: int | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    return_code: int | None = None
    command: list[str] | None = None
    log_path: str | None = None
    preflight_output: str | None = None
    brain_state: str | None = None
    selected_candidate_id: str | None = None
    selected_ticker: str | None = None


class TradingLoopStartRequestModel(BaseModel):
    board_date: date | None = None


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


# ---- Live snapshot models (Phase 2) ----

class KpiPnlModel(BaseModel):
    daily_pnl: float
    realized: float
    unrealized: float
    loss_cap: float
    loss_progress: float  # 0..1


class KpiBudgetModel(BaseModel):
    max_open_notional: float
    allocated: float
    free: float
    usage_progress: float  # 0..1


class KpiPicksModel(BaseModel):
    available: int
    selected: int
    excluded: int
    blocked: int
    est_total_profit: float


class KpiSystemModel(BaseModel):
    status: Literal["ready", "blocked", "checking"]
    mode: Literal["observe", "supervised-live"]
    gates_passed: int
    gates_total: int
    ws_connected: bool
    summary: str


class KpiTilesModel(BaseModel):
    pnl: KpiPnlModel
    budget: KpiBudgetModel
    picks: KpiPicksModel
    system: KpiSystemModel


class ControlBarStateModel(BaseModel):
    mode: Literal["observe", "supervised-live"]
    loop_state: Literal["idle", "starting", "running", "killed", "exited", "failed", "blocked"]
    can_start: bool
    start_label: str
    kill_switch_active: bool


class PickKalshiModel(BaseModel):
    ticker: str | None
    yes_bid: float | None
    yes_ask: float | None
    spread: float | None
    last_quote_at: datetime | None


class PickRowModel(BaseModel):
    candidate_id: str
    rank: int
    prop_label: str
    game_label: str | None
    hit_pct: float
    edge_bps: int
    model_prob: float
    market_prob: float | None
    alloc: float
    est_profit: float
    state: Literal["queued", "excluded", "blocked", "filled", "partial"]
    selected: bool
    blocker_reason: str | None
    kalshi: PickKalshiModel


class BetSlipPickModel(BaseModel):
    candidate_id: str
    prop_label: str
    hit_pct: float
    edge_bps: int
    alloc: float
    est_profit: float


class BetSlipModel(BaseModel):
    selected: list[BetSlipPickModel]
    total_stake: float
    cap_total: float
    est_total_profit: float
    unused_budget: float


class SystemDiagnosticsModel(BaseModel):
    readiness: TradingReadinessModel | None
    brain: TradingBrainSyncModel | None


class EventLogLineModel(BaseModel):
    cursor: int
    timestamp: datetime
    level: Literal["info", "warn", "error"]
    message: str


class PnlPointModel(BaseModel):
    index: int
    pnl: float


class TradingLiveSnapshotModel(BaseModel):
    observed_at: datetime
    kpis: KpiTilesModel
    control: ControlBarStateModel
    picks: list[PickRowModel]
    bet_slip: BetSlipModel
    positions: list[LivePositionModel]
    fills: list[FillModel]
    quotes: list[TradingQuoteModel]
    resting_orders: list[RestingOrderModel]
    diagnostics: SystemDiagnosticsModel
    event_log: list[EventLogLineModel]
    pnl_trend: list[PnlPointModel]
    errors: list[str]
    stream_cursor: int


# ---- Limits + wallet request models (Phase 3) ----

class LimitsUpdateRequestModel(BaseModel):
    max_open_notional: float | None = Field(default=None, gt=0.0, le=1_000_000.0)
    daily_loss_cap: float | None = Field(default=None, ge=0.0, le=1_000_000.0)
    reject_cooldown_seconds: int | None = Field(default=None, ge=0, le=86_400)
    per_order_cap_override: float | None = Field(default=None, ge=0.0)


class LimitsResponseModel(BaseModel):
    max_open_notional: float
    per_market_cap: float
    daily_loss_cap: float
    reject_cooldown_seconds: int
    per_order_cap_override: float | None
    wallet_init_done_at: datetime | None


class WalletBalanceResponseModel(BaseModel):
    balance: float
    fetched_at: datetime


class PickToggleRequestModel(BaseModel):
    included: bool


class PickBulkRequestModel(BaseModel):
    action: Literal["select_all_hittable", "deselect_all", "top_n"]
    n: int | None = Field(default=None, ge=1, le=50)


class ThresholdsUpdateRequestModel(BaseModel):
    min_hit_pct: float = Field(ge=0.0, le=1.0)
    min_edge_bps: int = Field(ge=0, le=5_000)

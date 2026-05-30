// desktop_tauri/src/routes/trading/api/types.ts

export type KpiPnl = {
  daily_pnl: number;
  realized: number;
  unrealized: number;
  loss_cap: number;
  loss_progress: number;
};

export type KpiBudget = {
  max_open_notional: number;
  allocated: number;
  free: number;
  usage_progress: number;
};

export type KpiPicks = {
  available: number;
  selected: number;
  excluded: number;
  blocked: number;
  est_total_profit: number;
};

export type KpiSystem = {
  status: "ready" | "blocked" | "checking";
  mode: "observe" | "supervised-live";
  gates_passed: number;
  gates_total: number;
  ws_connected: boolean;
  summary: string;
};

export type KpiTiles = {
  pnl: KpiPnl;
  budget: KpiBudget;
  picks: KpiPicks;
  system: KpiSystem;
};

export type ControlBarState = {
  mode: "observe" | "supervised-live";
  loop_state: "idle" | "starting" | "running" | "killed" | "exited" | "failed" | "blocked";
  can_start: boolean;
  start_label: string;
  kill_switch_active: boolean;
};

export type PickKalshi = {
  ticker: string | null;
  yes_bid: number | null;
  yes_ask: number | null;
  spread: number | null;
  last_quote_at: string | null;
};

export type PickState = "queued" | "excluded" | "blocked" | "filled" | "partial";

export type PickRow = {
  candidate_id: string;
  stat_projection_id?: string | number | null;
  prop_line_evaluation_id?: string | number | null;
  primary_line_evaluation_id?: string | number | null;
  probability_source?: string | null;
  rank: number;
  prop_label: string;
  game_label: string | null;
  hit_pct: number;
  edge_bps: number;
  model_prob: number;
  market_prob: number | null;
  alloc: number;
  est_profit: number;
  state: PickState;
  selected: boolean;
  blocker_reason: string | null;
  kalshi: PickKalshi;
};

export type BetSlipPick = {
  candidate_id: string;
  prop_label: string;
  hit_pct: number;
  edge_bps: number;
  alloc: number;
  est_profit: number;
};

export type BetSlip = {
  selected: BetSlipPick[];
  total_stake: number;
  cap_total: number;
  est_total_profit: number;
  unused_budget: number;
};

export type EventLogLine = {
  cursor: number;
  timestamp: string;
  level: "info" | "warn" | "error";
  message: string;
};

export type PnlPoint = { index: number; pnl: number };

export type TradingQuote = {
  prop_line_evaluation_id?: string | number | null;
  stat_projection_id?: string | number | null;
  probability_source?: string | null;
  ticker: string;
  market_key: string;
  side: string | null;
  line_value: number | null;
  player_id: string | null;
  game_date: string | null;
  title: string | null;
  status: string | null;
  yes_bid: number | null;
  yes_ask: number | null;
  no_bid: number | null;
  no_ask: number | null;
  last_price: number | null;
  entry_price: number | null;
  exit_price: number | null;
  spread: number | null;
  observed_at: string;
  error?: string | null;
};

export type TradingLivePosition = {
  market_symbol: string;
  market_key: string;
  side: string;
  ticker: string | null;
  open_stake: number;
  contract_count: number;
  avg_price: number;
  current_exit_price: number | null;
  current_value: number | null;
  unrealized_pnl: number | null;
  unrealized_pnl_pct: number | null;
  realized_pnl: number;
  updated_at: string;
  quote: TradingQuote | null;
};

export type TradingFill = {
  fill_id: string;
  intent_id: string;
  market: {
    exchange?: string;
    symbol?: string;
    market_key?: string;
    side?: string;
    line_value?: number;
  };
  side: string;
  stake: number;
  price: number;
  fee: number;
  realized_pnl: number;
  timestamp: string;
};

export type TradingRestingOrder = {
  order_id: string;
  client_order_id: string | null;
  ticker: string | null;
  side: string | null;
  status: string | null;
  remaining_count: number | null;
  price: number | null;
  created_at: string | null;
};

export type TradingLiveSnapshot = {
  observed_at: string;
  kpis: KpiTiles;
  control: ControlBarState;
  picks: PickRow[];
  bet_slip: BetSlip;
  positions: TradingLivePosition[];
  fills: TradingFill[];
  quotes: TradingQuote[];
  resting_orders: TradingRestingOrder[];
  diagnostics: { readiness: unknown; brain: unknown };
  event_log: EventLogLine[];
  pnl_trend: PnlPoint[];
  errors: string[];
  stream_cursor: number;
};

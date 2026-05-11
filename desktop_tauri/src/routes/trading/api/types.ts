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

export type TradingLiveSnapshot = {
  observed_at: string;
  kpis: KpiTiles;
  control: ControlBarState;
  picks: PickRow[];
  bet_slip: BetSlip;
  positions: unknown[];
  fills: unknown[];
  quotes: unknown[];
  resting_orders: unknown[];
  diagnostics: { readiness: unknown; brain: unknown };
  event_log: EventLogLine[];
  pnl_trend: PnlPoint[];
  errors: string[];
  stream_cursor: number;
};

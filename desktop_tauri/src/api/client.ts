// Base URL for the Python sidecar API.
// In browser dev, keep this empty and use Vite's /api proxy.
// In production (Tauri), Rust injects the sidecar URL via window globals.
const DEFAULT_API_BASE = "";

type AppWindowGlobals = Window & {
  __NBA_API_BASE__?: string;
  __APP_TOKEN__?: string;
};

function resolveApiBase(): string {
  const fromWindow = (window as AppWindowGlobals).__NBA_API_BASE__;
  const fromEnv = import.meta.env.VITE_API_BASE as string | undefined;
  return fromWindow ?? fromEnv ?? DEFAULT_API_BASE;
}

function resolveAppToken(): string | null {
  const fromWindow = (window as AppWindowGlobals).__APP_TOKEN__;
  const fromEnv = import.meta.env.VITE_APP_TOKEN as string | undefined;
  return fromWindow ?? fromEnv ?? null;
}

function buildHeaders(initHeaders?: HeadersInit): Headers {
  const headers = new Headers(initHeaders);
  if (!headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }
  const token = resolveAppToken();
  if (token) {
    headers.set("X-App-Token", token);
  }
  return headers;
}

export type HealthResponse = {
  ok: boolean;
  version: string;
  db_path: string;
};

export type StartupStep = {
  key: string;
  label: string;
  status: string;
  message: string;
  progress_fraction: number;
};

export type StartupSnapshot = {
  progress_percent: number;
  eta_seconds: number | null;
  current_step: string;
  current_detail: string;
  database_message: string;
  board_date_message: string;
  completed: boolean;
  failed: boolean;
  error_message: string | null;
  steps: StartupStep[];
  log_lines: string[];
};

export type StartupRunResponse = {
  run_id: string;
};

export type BoardSummary = {
  board_date: string | null;
  game_count: number;
  opportunity_count: number;
  sportsbook_count: number;
  alt_line_count: number;
  latest_quote_at: string | null;
};

export type BoardAvailability = {
  board_date: string;
  scheduled_games: number;
  live_games: number;
  final_games: number;
  has_pregame_options: boolean;
};

export type TradingPnl = {
  daily_realized_pnl: number;
  kill_switch_active: boolean;
};

export type TradingIntentRequest = {
  player_id: number;
  market: string;
  line: number;
  side: "over" | "under";
  sportsbook_key: string;
  stake: number;
};

export type TradingIntentResponse = {
  accepted: boolean;
  intent_id: string | null;
  message: string;
};

export type TradingPosition = {
  market_symbol: string;
  market_key: string;
  side: string;
  open_stake: number;
  avg_price: number;
  realized_pnl: number;
  updated_at: string;
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

export type SportsbookQuote = {
  game_id: number;
  sportsbook_key: string;
  sportsbook_name: string;
  icon: string;
  market_key: string;
  line_value: number;
  over_odds: number | null;
  under_odds: number | null;
  timestamp: string;
  is_live_quote: boolean;
  verification_status: string;
  odds_source_provider: string;
  over_probability: number;
  under_probability: number;
  push_probability: number;
  calibrated_over_probability: number;
  calibrated_under_probability: number;
  recommended_side: string;
  hit_probability: number;
  no_vig_market_probability: number;
  source_market_key: string;
  is_alternate_line: boolean;
};

export type PropOpportunity = {
  rank: number;
  game_id: number;
  player_id: number;
  player_name: string;
  player_icon: string;
  market_key: string;
  consensus_line: number;
  projected_mean: number;
  recommended_side: string;
  hit_probability: number;
  likelihood_score: number;
  calibrated_over_probability: number;
  sportsbooks_summary: string;
  top_features: string[];
  quotes: SportsbookQuote[];
  projected_variance: number | null;
  confidence_interval_low: number | null;
  confidence_interval_high: number | null;
  predicted_at: string | null;
  data_sufficiency_tier: string;
  data_confidence_score: number;
  player_team_abbreviation: string | null;
  player_position: string | null;
  game_label: string | null;
  game_start_time: string | null;
};

export type PropInsight = {
  best_quote: SportsbookQuote;
  recommended_odds: number | null;
  implied_probability: number | null;
  fair_american_odds: number | null;
  edge: number;
  expected_profit_per_unit: number;
  confidence_score: number;
  confidence_tier: string;
  freshness_label: string;
  market_width: number;
  injury_label: string;
  injury_detail: string;
  reason_lines: string[];
  warnings: string[];
};

export type PropWithInsight = {
  opportunity: PropOpportunity;
  insight: PropInsight;
};

export type PropListResponse = {
  items: PropWithInsight[];
  total: number;
  page: number;
  page_size: number;
};

export type PropsQuery = {
  confidence?: string;
  market?: string;
  sort?: string;
  book?: string;
  page?: number;
  page_size?: number;
};

export type ParlayLeg = {
  game_id: number;
  matchup: string;
  player_name: string;
  market_key: string;
  recommended_side: string;
  line_value: number;
  american_odds: number;
  hit_probability: number;
  likelihood_score: number;
  is_live_quote: boolean;
  verification_status: string;
  odds_source_provider: string;
};

export type ParlayInsight = {
  confidence_score: number;
  confidence_tier: string;
  fragility_label: string;
  reason_lines: string[];
  warnings: string[];
};

export type ParlayRecommendation = {
  rank: number;
  game_id: number;
  matchup: string;
  sportsbook_key: string;
  sportsbook_name: string;
  sportsbook_icon: string;
  leg_count: number;
  game_count: number;
  game_ids: number[];
  game_labels: string[];
  joint_probability: number;
  combined_decimal_odds: number;
  combined_american_odds: number;
  expected_profit_per_unit: number;
  implied_probability: number;
  edge: number;
  all_legs_live: boolean;
  verification_status: string;
  odds_source_provider: string;
  correlation_penalty: number;
  average_leg_hit_probability: number;
  weakest_leg_hit_probability: number;
  legs: ParlayLeg[];
};

export type ParlayWithInsight = {
  parlay: ParlayRecommendation;
  insight: ParlayInsight;
};

export type SameGameParlaysResponse = {
  sections: Record<string, Record<string, Record<string, ParlayWithInsight[]>>>;
};

export type MultiGameParlaysResponse = {
  sections: Record<string, Record<string, ParlayWithInsight[]>>;
};

export type SameGameParlaysQuery = {
  game_id?: number;
  book?: string;
};

export type MultiGameParlaysQuery = {
  book?: string;
};

export type ProviderStatus = {
  provider_type: string;
  provider_name: string;
  endpoint: string;
  fetched_at: string | null;
  freshness_label: string;
  status_label: string;
  detail: string;
};

export type InjuryStatusBadge = {
  label: string;
  detail: string;
  updated_at: string | null;
  severity: number;
};

export type LocalAgentPolicy = "enable" | "disable" | "safe_auto_enable" | "safe_auto_disable";

export type LocalAgentStatus = {
  enabled: boolean;
  auto_execute_safe: boolean;
  updated_at: string;
  updated_by: string;
  note: string;
  last_run_status: string;
  last_run_at: string | null;
  last_summary: string;
  last_confidence: number | null;
};

function withQuery(path: string, params: Record<string, string | number | undefined>): string {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== "") {
      search.set(key, String(value));
    }
  }
  const query = search.toString();
  return query ? `${path}?${query}` : path;
}

async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${resolveApiBase()}${path}`, {
    ...init,
    headers: buildHeaders(init?.headers),
  });
  if (!res.ok) {
    throw new Error(`API ${path} returned ${res.status}`);
  }
  return res.json() as Promise<T>;
}

export function startupStreamUrl(): string {
  return `${resolveApiBase()}/api/startup/stream`;
}

export function localAgentTerminalStreamUrl(): string {
  return `${resolveApiBase()}/api/local-agent/terminal`;
}

export const api = {
  health: () => apiFetch<HealthResponse>("/api/health"),
  settings: () => apiFetch<Record<string, unknown>>("/api/settings"),
  startupSnapshot: () => apiFetch<StartupSnapshot>("/api/startup/snapshot"),
  runStartup: () => apiFetch<StartupRunResponse>("/api/startup/run", { method: "POST" }),
  boardAvailability: () => apiFetch<BoardAvailability>("/api/board/availability"),
  boardSummary: () => apiFetch<BoardSummary>("/api/board/summary"),
  props: (query: PropsQuery = {}) => apiFetch<PropListResponse>(withQuery("/api/props", query)),
  propDetail: (params: { playerId: number; market: string; line: number }) =>
    apiFetch<PropWithInsight>(`/api/props/${params.playerId}/${encodeURIComponent(params.market)}/${params.line}`),
  sameGameParlays: (query: SameGameParlaysQuery = {}) =>
    apiFetch<SameGameParlaysResponse>(withQuery("/api/parlays/sgp", query)),
  multiGameParlays: (query: MultiGameParlaysQuery = {}) =>
    apiFetch<MultiGameParlaysResponse>(withQuery("/api/parlays/multi", query)),
  insightsProviders: () => apiFetch<ProviderStatus[]>("/api/insights/providers"),
  insightsInjuries: (playerIds?: number[]) =>
    apiFetch<Record<string, InjuryStatusBadge>>(
      withQuery("/api/insights/injuries", {
        player_ids: playerIds && playerIds.length > 0 ? playerIds.join(",") : undefined,
      }),
    ),
  localAgentStatus: () => apiFetch<LocalAgentStatus>("/api/local-agent/status"),
  localAgentPolicy: (policy: LocalAgentPolicy) =>
    apiFetch<LocalAgentStatus>("/api/local-agent/policy", {
      method: "POST",
      body: JSON.stringify({ policy }),
    }),
  tradingPositions: () => apiFetch<TradingPosition[]>("/api/trading/positions"),
  tradingPnl: () => apiFetch<TradingPnl>("/api/trading/pnl"),
  tradingRecentFills: (limit = 50) => apiFetch<TradingFill[]>(withQuery("/api/trading/fills/recent", { limit })),
  tradingKillSwitch: () => apiFetch<TradingPnl>("/api/trading/kill-switch", { method: "POST" }),
  tradingIntent: (body: TradingIntentRequest) =>
    apiFetch<TradingIntentResponse>("/api/trading/intent", {
      method: "POST",
      body: JSON.stringify(body),
    }),
};

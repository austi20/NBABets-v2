// desktop_tauri/src/routes/trading/api/actions.ts
import { getAppToken, tradingSnapshotUrl } from "../../../api/client";
import type { TradingRiskLimits } from "../../../api/client";
import type { TradingLiveSnapshot } from "./types";

type LimitsResponse = TradingRiskLimits & {
  per_order_cap_override: number | null;
  wallet_init_done_at: string | null;
};

type ThresholdsResponse = {
  min_hit_pct: number;
  min_edge_bps: number;
};

function resolveBase(): string {
  const url = new URL(tradingSnapshotUrl());
  return url.origin;
}

function buildAuthHeaders(): Record<string, string> {
  const token = getAppToken();
  return token ? { "X-App-Token": token } : {};
}

const tradingFetch = async <T>(path: string, init?: RequestInit): Promise<T> => {
  const base = resolveBase();
  const res = await fetch(`${base}${path}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...buildAuthHeaders(),
      ...(init?.headers ?? {}),
    },
  });
  if (!res.ok) {
    throw new Error(`${res.status} ${res.statusText}: ${path}`);
  }
  try {
    return (await res.json()) as T;
  } catch (err) {
    const detail = err instanceof Error ? err.message : "unknown parse error";
    throw new Error(`Failed to parse response from ${path}: ${detail}`);
  }
};

export const tradingActions = {
  togglePick: (candidateId: string, included: boolean) =>
    tradingFetch<TradingLiveSnapshot>(
      `/api/trading/picks/${encodeURIComponent(candidateId)}/toggle`,
      { method: "POST", body: JSON.stringify({ included }) }
    ),

  bulk: (action: "select_all_hittable" | "deselect_all" | "top_n", n?: number) =>
    tradingFetch<TradingLiveSnapshot>("/api/trading/picks/bulk", {
      method: "POST",
      body: JSON.stringify({ action, n }),
    }),

  setThresholds: (minHitPct: number, minEdgeBps: number) =>
    tradingFetch<TradingLiveSnapshot>("/api/trading/thresholds", {
      method: "POST",
      body: JSON.stringify({ min_hit_pct: minHitPct, min_edge_bps: minEdgeBps }),
    }),

  readThresholds: () => tradingFetch<ThresholdsResponse>("/api/trading/thresholds"),

  updateLimits: (body: Partial<{
    max_open_notional: number;
    daily_loss_cap: number;
    reject_cooldown_seconds: number;
    per_order_cap_override: number;
  }>) =>
    tradingFetch<LimitsResponse>("/api/trading/limits", {
      method: "POST",
      body: JSON.stringify(body),
    }),

  readLimits: () => tradingFetch<LimitsResponse>("/api/trading/limits"),

  fetchWallet: () =>
    tradingFetch<{ balance: number; fetched_at: string }>("/api/trading/wallet"),
};

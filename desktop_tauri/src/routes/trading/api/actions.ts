// desktop_tauri/src/routes/trading/api/actions.ts
import { getAppToken, tradingSnapshotUrl } from "../../../api/client";
import type { TradingLiveSnapshot } from "./types";

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
  return (await res.json()) as T;
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

  updateLimits: (body: Partial<{
    max_open_notional: number;
    daily_loss_cap: number;
    reject_cooldown_seconds: number;
    per_order_cap_override: number;
  }>) =>
    tradingFetch("/api/trading/limits", {
      method: "POST",
      body: JSON.stringify(body),
    }),

  readLimits: () => tradingFetch("/api/trading/limits"),

  fetchWallet: () =>
    tradingFetch<{ balance: number; fetched_at: string }>("/api/trading/wallet"),
};

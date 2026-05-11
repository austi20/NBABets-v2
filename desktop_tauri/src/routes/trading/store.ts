// desktop_tauri/src/routes/trading/store.ts
import { create } from "zustand";
import type { TradingLiveSnapshot, PickRow, EventLogLine } from "./api/types";

type SortKey = "rank" | "hit_pct" | "edge_bps" | "alloc" | "est_profit";
type SortDir = "asc" | "desc";
type FilterMode = "all" | "hittable" | "excluded" | "blocked";

type TradingState = {
  // Live data
  snapshot: TradingLiveSnapshot | null;
  streamConnected: boolean;
  lastSnapshotAt: string | null;
  // UI-only state
  sortKey: SortKey;
  sortDir: SortDir;
  filter: FilterMode;
  expandedCandidateId: string | null;
  thresholdsOpen: boolean;
  limitsModalOpen: boolean;
  // Mutators
  applySnapshot: (snapshot: TradingLiveSnapshot) => void;
  setStreamConnected: (connected: boolean) => void;
  setSort: (key: SortKey) => void;
  setFilter: (mode: FilterMode) => void;
  toggleExpand: (candidateId: string) => void;
  setThresholdsOpen: (open: boolean) => void;
  setLimitsModalOpen: (open: boolean) => void;
};

export const useTradingStore = create<TradingState>((set) => ({
  snapshot: null,
  streamConnected: false,
  lastSnapshotAt: null,
  sortKey: "rank",
  sortDir: "asc",
  filter: "all",
  expandedCandidateId: null,
  thresholdsOpen: false,
  limitsModalOpen: false,

  applySnapshot: (snapshot) =>
    set({ snapshot, lastSnapshotAt: snapshot.observed_at }),

  setStreamConnected: (connected) => set({ streamConnected: connected }),

  setSort: (key) =>
    set((state) => ({
      sortKey: key,
      sortDir: state.sortKey === key && state.sortDir === "desc" ? "asc" : "desc",
    })),

  setFilter: (filter) => set({ filter }),

  toggleExpand: (candidateId) =>
    set((state) => ({
      expandedCandidateId: state.expandedCandidateId === candidateId ? null : candidateId,
    })),

  setThresholdsOpen: (open) => set({ thresholdsOpen: open }),
  setLimitsModalOpen: (open) => set({ limitsModalOpen: open }),
}));

// Selector helpers

export function selectVisiblePicks(state: TradingState): PickRow[] {
  if (!state.snapshot) return [];
  const all = state.snapshot.picks;
  const filtered = all.filter((row) => {
    switch (state.filter) {
      case "hittable":
        return row.state !== "blocked";
      case "excluded":
        return row.state === "excluded";
      case "blocked":
        return row.state === "blocked";
      default:
        return true;
    }
  });
  const sorted = [...filtered].sort((a, b) => {
    const dir = state.sortDir === "asc" ? 1 : -1;
    if (state.sortKey === "rank") return (a.rank - b.rank) * dir;
    return (Number(a[state.sortKey]) - Number(b[state.sortKey])) * dir;
  });
  return sorted;
}

export function selectEventLog(state: TradingState): EventLogLine[] {
  return state.snapshot?.event_log ?? [];
}

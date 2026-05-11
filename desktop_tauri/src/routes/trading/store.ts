// desktop_tauri/src/routes/trading/store.ts
import { create } from "zustand";
import type { TradingLiveSnapshot, PickRow, EventLogLine } from "./api/types";

type SortKey = "candidate_id" | "model_prob" | "edge_bps" | "alloc";
type SortDir = "asc" | "desc";
type FilterMode = "all" | "queued" | "excluded" | "blocked";

type TradingState = {
  // Live data
  snapshot: TradingLiveSnapshot | null;
  streamConnected: boolean;
  lastSnapshotAt: number | null;
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
  sortKey: "model_prob",
  sortDir: "desc",
  filter: "all",
  expandedCandidateId: null,
  thresholdsOpen: false,
  limitsModalOpen: false,

  applySnapshot: (snapshot) =>
    set({ snapshot, lastSnapshotAt: Date.now() }),

  setStreamConnected: (connected) => set({ streamConnected: connected }),

  setSort: (key) =>
    set((state) => ({
      sortKey: key,
      sortDir: state.sortKey === key && state.sortDir === "asc" ? "desc" : "asc",
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
      case "queued":
        return row.state === "queued";
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
    const key = state.sortKey;
    if (key === "candidate_id") {
      return a.candidate_id.localeCompare(b.candidate_id) * dir;
    }
    return (Number(a[key]) - Number(b[key])) * dir;
  });
  return sorted;
}

export function selectEventLog(state: TradingState): EventLogLine[] {
  return state.snapshot?.event_log ?? [];
}

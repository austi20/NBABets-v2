// desktop_tauri/src/routes/trading/store.ts
import { create } from "zustand";
import type { TradingLiveSnapshot, EventLogLine } from "./api/types";

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

const EMPTY_LOG: EventLogLine[] = [];

export function selectEventLog(state: TradingState): EventLogLine[] {
  return state.snapshot?.event_log ?? EMPTY_LOG;
}

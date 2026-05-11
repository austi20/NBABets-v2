// desktop_tauri/src/routes/trading/components/PicksTable.tsx
import { Fragment } from "react";
import { useTradingStore, selectVisiblePicks } from "../store";
import { PickRow } from "./PickRow";
import { FilterPills } from "./FilterPills";
import { BulkActions } from "./BulkActions";
import { ThresholdsRow } from "./ThresholdsRow";
import { PickRowExpansion } from "./PickRowExpansion";

// Only columns with valid SortKey values
const COLUMNS: { key: "candidate_id" | "model_prob" | "edge_bps" | "alloc"; label: string }[] = [
  { key: "candidate_id", label: "Prop" },
  { key: "model_prob", label: "Hit %" },
  { key: "edge_bps", label: "Edge" },
  { key: "alloc", label: "Alloc" },
];

function formatUsd(n: number): string {
  return `$${n.toFixed(2)}`;
}

function formatSigned(n: number): string {
  return `${n >= 0 ? "+" : ""}$${n.toFixed(2)}`;
}

export function PicksTable() {
  const setSort = useTradingStore((s) => s.setSort);
  const expandedId = useTradingStore((s) => s.expandedCandidateId);
  const visible = useTradingStore(selectVisiblePicks);
  const snapshot = useTradingStore((s) => s.snapshot);

  if (!snapshot) {
    return (
      <div className="trading-tile" style={{ height: 240, marginBottom: 18 }} />
    );
  }
  const picks = snapshot.kpis.picks;
  return (
    <section style={{ marginBottom: 24 }}>
      <header style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "8px 0 12px" }}>
        <div>
          <div style={{ fontSize: 14, fontWeight: 700 }}>Picks</div>
          <div className="micro" style={{ textTransform: "none", letterSpacing: 0, fontSize: 10, marginTop: 2 }}>
            {picks.available} picks available · {picks.selected} selected · {formatUsd(snapshot.bet_slip.total_stake)} allocated · est. profit{" "}
            <span style={{ color: "var(--trading-accent-pnl)" }}>{formatSigned(picks.est_total_profit)}</span>
          </div>
        </div>
        <BulkActions />
      </header>
      <FilterPills />
      <ThresholdsRow />
      <table className="picks-table">
        <thead>
          <tr>
            <th style={{ width: 18 }} />
            {COLUMNS.map((col) => (
              <th key={col.key} onClick={() => setSort(col.key)} style={{ cursor: "pointer" }}>
                {col.label} ↕
              </th>
            ))}
            <th>Est. Profit</th>
            <th>State</th>
          </tr>
        </thead>
        <tbody>
          {visible.length === 0 ? (
            <tr>
              <td colSpan={7} style={{ color: "var(--trading-fg-subtle)", textAlign: "center", padding: 16 }}>
                No picks match the current filter.
              </td>
            </tr>
          ) : (
            visible.map((pick) => (
              <Fragment key={pick.candidate_id}>
                <PickRow pick={pick} />
                {expandedId === pick.candidate_id ? (
                  <PickRowExpansion pick={pick} />
                ) : null}
              </Fragment>
            ))
          )}
        </tbody>
      </table>
    </section>
  );
}

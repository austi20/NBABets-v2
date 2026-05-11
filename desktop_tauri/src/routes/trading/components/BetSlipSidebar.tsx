// desktop_tauri/src/routes/trading/components/BetSlipSidebar.tsx
import { useTradingStore } from "../store";
import { usePulseOnChange } from "../hooks/usePulseOnChange";

function Money({ value }: { value: number }) {
  const pulse = usePulseOnChange(value);
  const cls = pulse === "up" ? "trading-pulse-up" : pulse === "down" ? "trading-pulse-down" : "";
  return <span className={cls}>{`${value >= 0 ? "+" : ""}$${value.toFixed(2)}`}</span>;
}

export function BetSlipSidebar() {
  const slip = useTradingStore((s) => s.snapshot?.bet_slip);
  if (!slip) return null;
  if (slip.selected.length === 0) {
    return (
      <aside className="bet-slip">
        <div className="micro">Selected Picks · 0</div>
        <p style={{ marginTop: 12, color: "var(--trading-fg-subtle)", fontSize: 10 }}>
          No picks selected · click bullets in the table to include
        </p>
      </aside>
    );
  }
  return (
    <aside className="bet-slip">
      <div className="micro">Selected Picks · {slip.selected.length}</div>
      <div style={{ marginTop: 12, display: "flex", flexDirection: "column", gap: 8 }}>
        {slip.selected.map((pick) => (
          <div key={pick.candidate_id} style={{ borderTop: "1px solid var(--trading-border)", paddingTop: 8 }}>
            <div style={{ fontSize: 11, fontWeight: 600 }}>● {pick.prop_label}</div>
            <div className="mono" style={{ color: "var(--trading-fg-subtle)", fontSize: 10, marginTop: 2 }}>
              {(pick.hit_pct * 100).toFixed(0)}% · +{pick.edge_bps}bp
            </div>
            <div className="mono" style={{ fontSize: 10, marginTop: 2 }}>
              ${pick.alloc.toFixed(2)} → <span style={{ color: "var(--trading-accent-pnl)" }}>{`+$${pick.est_profit.toFixed(2)}`}</span>
            </div>
          </div>
        ))}
      </div>
      <div style={{ borderTop: "1px solid var(--trading-border)", marginTop: 12, paddingTop: 12, fontSize: 10 }} className="mono">
        <div style={{ display: "flex", justifyContent: "space-between" }}>
          <span>Total stake</span>
          <span><Money value={slip.total_stake} /> of ${slip.cap_total.toFixed(2)} cap</span>
        </div>
        <div style={{ display: "flex", justifyContent: "space-between", marginTop: 4 }}>
          <span>Est. profit</span>
          <span style={{ color: "var(--trading-accent-pnl)" }}><Money value={slip.est_total_profit} /></span>
        </div>
        <div style={{ display: "flex", justifyContent: "space-between", marginTop: 4 }}>
          <span>Unused</span>
          <span>${slip.unused_budget.toFixed(2)}</span>
        </div>
      </div>
    </aside>
  );
}

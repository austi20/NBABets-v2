// desktop_tauri/src/routes/trading/components/PositionsTable.tsx
import { useTradingStore } from "../store";

const EMPTY: never[] = [];

export function PositionsTable() {
  const positions = (useTradingStore((s) => s.snapshot?.positions) ?? EMPTY) as Array<Record<string, unknown>>;
  if (positions.length === 0) {
    return (
      <section style={{ marginTop: 14 }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "8px 0 12px" }}>
          <div style={{ fontSize: 13, fontWeight: 700 }}>Open Positions <span style={{ color: "var(--trading-fg-subtle)", fontSize: 10, fontWeight: 400 }}>· 0</span></div>
        </div>
        <p className="micro" style={{ textTransform: "none", letterSpacing: 0 }}>No open positions.</p>
      </section>
    );
  }
  return (
    <section style={{ marginTop: 14 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "8px 0 12px" }}>
        <div style={{ fontSize: 13, fontWeight: 700 }}>
          Open Positions <span style={{ color: "var(--trading-fg-subtle)", fontSize: 10, fontWeight: 400 }}>· {positions.length}</span>
        </div>
      </div>
      <div className="mono" style={{ background: "var(--trading-surface)", borderRadius: 6, padding: 10, fontSize: 10, color: "var(--trading-fg-muted)" }}>
        {positions.map((p, idx) => (
          <div key={idx} style={{ padding: "4px 0", borderBottom: "1px solid var(--trading-border)" }}>
            {String(p.market_symbol ?? p.ticker ?? "—")} · {String(p.side ?? "—")} · {String(p.contract_count ?? "—")}
          </div>
        ))}
      </div>
    </section>
  );
}

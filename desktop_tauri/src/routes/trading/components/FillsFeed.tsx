// desktop_tauri/src/routes/trading/components/FillsFeed.tsx
import { useEffect, useRef, useState } from "react";
import { useTradingStore } from "../store";
import { fmtDollarSigned } from "../format";
import type { TradingFill } from "../api/types";

const EMPTY: TradingFill[] = [];

export function FillsFeed() {
  const fills = useTradingStore((s) => s.snapshot?.fills) ?? EMPTY;
  const previousIds = useRef<Set<string>>(new Set());
  const [flashIds, setFlashIds] = useState<Set<string>>(new Set());

  useEffect(() => {
    const current = new Set(fills.map((f) => f.fill_id ?? "").filter(Boolean));
    const novel = new Set<string>();
    for (const id of current) {
      if (!previousIds.current.has(id)) novel.add(id);
    }
    const hadPrevious = previousIds.current.size > 0;
    previousIds.current = current;
    if (novel.size === 0 || !hadPrevious) {
      return () => {};
    }
    setFlashIds(novel);
    const timer = setTimeout(() => setFlashIds(new Set()), 220);
    return () => clearTimeout(timer);
  }, [fills]);

  return (
    <section style={{ marginTop: 14 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "8px 0 12px" }}>
        <div style={{ fontSize: 13, fontWeight: 700 }}>
          Recent Fills <span style={{ color: "var(--trading-fg-subtle)", fontSize: 10, fontWeight: 400 }}>· {fills.length}</span>
        </div>
        <span className="micro" style={{ textTransform: "none", letterSpacing: 0 }}>live</span>
      </div>
      {fills.length === 0 ? (
        <p className="micro" style={{ textTransform: "none", letterSpacing: 0 }}>No fills recorded yet.</p>
      ) : (
        <div className="mono" style={{ background: "var(--trading-surface)", borderRadius: 6, padding: 10, fontSize: 10 }}>
          {fills.map((fill) => (
            <div
              key={fill.fill_id || `${fill.market?.symbol ?? ""}-${fill.timestamp}`}
              className={flashIds.has(fill.fill_id ?? "") ? "trading-pulse-up" : ""}
              style={{ padding: "4px 0", borderBottom: "1px solid var(--trading-border)", display: "flex", justifyContent: "space-between" }}
            >
              <span>
                <span style={{ color: "var(--trading-fg)" }}>{fill.market?.symbol ?? "—"}</span> · {fill.side ?? "—"} @ {fill.price?.toFixed(3) ?? "—"} · stake ${fill.stake?.toFixed(2) ?? "—"}
              </span>
              <span style={{ color: fill.realized_pnl >= 0 ? "var(--trading-accent-pnl)" : "var(--trading-accent-danger)" }}>
                {fmtDollarSigned(fill.realized_pnl)}
              </span>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}

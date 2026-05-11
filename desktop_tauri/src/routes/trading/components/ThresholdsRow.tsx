// desktop_tauri/src/routes/trading/components/ThresholdsRow.tsx
import { useState } from "react";
import { useTradingStore } from "../store";
import { tradingActions } from "../api/actions";

export function ThresholdsRow() {
  const open = useTradingStore((s) => s.thresholdsOpen);
  const setOpen = useTradingStore((s) => s.setThresholdsOpen);
  const [hit, setHit] = useState(55);
  const [edge, setEdge] = useState(50);

  return (
    <div style={{ marginBottom: 8 }}>
      <button
        type="button"
        onClick={() => setOpen(!open)}
        style={{ color: "var(--trading-fg-subtle)", fontSize: 10, background: "transparent", border: "none", cursor: "pointer" }}
      >
        {open ? "▾" : "▸"} Thresholds (min hit {hit}% · min edge +{edge}bp)
      </button>
      {open ? (
        <div style={{ display: "flex", gap: 12, padding: "8px 0", alignItems: "center" }}>
          <label className="micro" style={{ display: "flex", alignItems: "center", gap: 6 }}>
            min hit %
            <input
              type="number"
              min={0}
              max={100}
              value={hit}
              onChange={(e) => setHit(Number(e.target.value))}
              style={{ width: 60, background: "var(--trading-surface)", border: "1px solid var(--trading-border-soft)", color: "var(--trading-fg)", padding: 4 }}
            />
          </label>
          <label className="micro" style={{ display: "flex", alignItems: "center", gap: 6 }}>
            min edge bps
            <input
              type="number"
              min={0}
              max={5000}
              value={edge}
              onChange={(e) => setEdge(Number(e.target.value))}
              style={{ width: 80, background: "var(--trading-surface)", border: "1px solid var(--trading-border-soft)", color: "var(--trading-fg)", padding: 4 }}
            />
          </label>
          <button
            type="button"
            className="btn-trading primary"
            onClick={() => void tradingActions.setThresholds(hit / 100, edge)}
          >
            Apply
          </button>
        </div>
      ) : null}
    </div>
  );
}

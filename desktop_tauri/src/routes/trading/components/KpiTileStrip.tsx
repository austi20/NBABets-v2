// desktop_tauri/src/routes/trading/components/KpiTileStrip.tsx
import { useState } from "react";
import { useTradingStore } from "../store";
import { tradingActions } from "../api/actions";
import { KpiTile } from "./KpiTile";

function formatMoney(value: number): string {
  const sign = value >= 0 ? "+" : "";
  return `${sign}$${value.toFixed(2)}`;
}

export function KpiTileStrip() {
  const snapshot = useTradingStore((s) => s.snapshot);
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState("");

  if (!snapshot) {
    return (
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 10, marginBottom: 10 }}>
        {Array.from({ length: 4 }).map((_, i) => (
          <div key={i} className="trading-tile" style={{ height: 88 }} />
        ))}
      </div>
    );
  }

  const { pnl, budget, picks, system } = snapshot.kpis;

  const submitBudget = async () => {
    const value = Number.parseFloat(draft);
    if (!Number.isFinite(value) || value <= 0) {
      setEditing(false);
      return;
    }
    try {
      await tradingActions.updateLimits({ max_open_notional: value });
    } finally {
      setEditing(false);
    }
  };

  return (
    <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 10, marginBottom: 10 }}>
      <KpiTile
        accent="pnl"
        label="Daily P&L"
        value={<span style={{ color: pnl.daily_pnl >= 0 ? "var(--trading-accent-pnl)" : "var(--trading-accent-danger)" }}>{formatMoney(pnl.daily_pnl)}</span>}
        numericForPulse={pnl.daily_pnl}
        barProgress={pnl.loss_progress}
        barColor={pnl.daily_pnl >= 0 ? "var(--trading-accent-pnl)" : "var(--trading-accent-danger)"}
        subline={`${(pnl.loss_progress * 100).toFixed(0)}% of $${pnl.loss_cap.toFixed(2)} cap · realized ${formatMoney(pnl.realized)} · unreal ${formatMoney(pnl.unrealized)}`}
      />
      <KpiTile
        accent="budget"
        label="Budget"
        value={
          editing ? (
            <input
              autoFocus
              type="number"
              step="0.01"
              defaultValue={budget.max_open_notional.toFixed(2)}
              onChange={(e) => setDraft(e.target.value)}
              onBlur={submitBudget}
              onKeyDown={(e) => {
                if (e.key === "Enter") void submitBudget();
                if (e.key === "Escape") setEditing(false);
              }}
              style={{ background: "transparent", border: "1px solid var(--trading-border-soft)", color: "var(--trading-fg)", fontFamily: "inherit", fontSize: 18, width: 100, padding: "2px 4px" }}
            />
          ) : (
            `$${budget.allocated.toFixed(2)} / $${budget.max_open_notional.toFixed(2)}`
          )
        }
        numericForPulse={budget.allocated}
        barProgress={budget.usage_progress}
        barColor="var(--trading-accent-budget)"
        subline={`$${budget.allocated.toFixed(2)} allocated · $${budget.free.toFixed(2)} free`}
        rightSlot={
          <button
            type="button"
            onClick={() => setEditing((v) => !v)}
            style={{ background: "transparent", border: "none", color: "var(--trading-accent-budget)", fontSize: 9, cursor: "pointer", textDecoration: "underline" }}
          >
            {editing ? "save" : "edit"}
          </button>
        }
      />
      <KpiTile
        accent="picks"
        label="Picks"
        value={`${picks.selected} of ${picks.available}`}
        numericForPulse={picks.selected}
        subline={
          <>
            est. profit{" "}
            <span style={{ color: "var(--trading-accent-pnl)", fontWeight: 700 }}>{formatMoney(picks.est_total_profit)}</span>{" "}
            · {picks.excluded} excluded · {picks.blocked} blocked
          </>
        }
      />
      <KpiTile
        accent="system"
        label="System"
        value={
          <span style={{ color: system.status === "ready" ? "var(--trading-accent-pnl)" : "var(--trading-accent-danger)", fontSize: 13 }}>
            ● {system.status === "ready" ? "READY" : system.status === "blocked" ? "BLOCKED" : "CHECKING"}
          </span>
        }
        subline={system.summary}
      />
    </div>
  );
}

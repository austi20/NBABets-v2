// desktop_tauri/src/routes/trading/components/KpiTile.tsx
import type { ReactNode } from "react";
import { usePulseOnChange } from "../hooks/usePulseOnChange";

type Accent = "pnl" | "budget" | "picks" | "system";

type Props = {
  label: string;
  value: ReactNode;
  numericForPulse?: number;
  subline?: ReactNode;
  barProgress?: number; // 0..1
  barColor?: string;
  accent: Accent;
  rightSlot?: ReactNode;
};

export function KpiTile({ label, value, numericForPulse, subline, barProgress, barColor, accent, rightSlot }: Props) {
  const pulse = usePulseOnChange(numericForPulse ?? 0);
  const pulseClass = numericForPulse === undefined ? "" : pulse === "up" ? "trading-pulse-up" : pulse === "down" ? "trading-pulse-down" : "";
  return (
    <div className={`trading-tile accent-${accent}`}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <div className="micro">{label}</div>
        {rightSlot ?? null}
      </div>
      <div className={`trading-tile-value mono ${pulseClass}`}>{value}</div>
      {barProgress !== undefined ? (
        <div className="trading-tile-bar">
          <span style={{ width: `${Math.max(0, Math.min(1, barProgress)) * 100}%`, background: barColor ?? "var(--trading-fg-muted)" }} />
        </div>
      ) : null}
      {subline ? <div className="micro" style={{ marginTop: 4, letterSpacing: 0, textTransform: "none", fontSize: 9 }}>{subline}</div> : null}
    </div>
  );
}

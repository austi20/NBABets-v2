// desktop_tauri/src/routes/trading/components/PnlTrendChart.tsx
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer } from "recharts";
import { useTradingStore } from "../store";
import type { PnlPoint } from "../api/types";

const EMPTY: never[] = [];

export function PnlTrendChart() {
  const data = (useTradingStore((s) => s.snapshot?.pnl_trend) ?? EMPTY) as PnlPoint[];

  return (
    <section style={{ marginTop: 14 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "8px 0 12px" }}>
        <div style={{ fontSize: 13, fontWeight: 700 }}>P&amp;L Trend</div>
        <span className="micro" style={{ textTransform: "none", letterSpacing: 0 }}>today</span>
      </div>
      <div style={{ background: "var(--trading-surface)", borderRadius: 6, padding: 12, height: 80 }}>
        {data.length === 0 ? (
          <p className="micro" style={{ textTransform: "none", letterSpacing: 0, color: "var(--trading-fg-subtle)" }}>
            No fills yet — chart populates as bets settle.
          </p>
        ) : (
          <ResponsiveContainer width="100%" height={56}>
            <LineChart data={data} margin={{ top: 0, right: 0, bottom: 0, left: 0 }}>
              <XAxis dataKey="index" hide />
              <YAxis hide domain={["auto", "auto"]} />
              <Tooltip
                contentStyle={{ background: "var(--trading-surface-alt)", border: "1px solid var(--trading-border)", fontSize: 10 }}
                formatter={(value: number) => [`$${value.toFixed(2)}`, "P&L"]}
                labelFormatter={() => ""}
              />
              <Line
                type="monotone"
                dataKey="pnl"
                stroke="var(--trading-accent-pnl)"
                strokeWidth={1.5}
                dot={false}
                isAnimationActive={false}
              />
            </LineChart>
          </ResponsiveContainer>
        )}
      </div>
    </section>
  );
}

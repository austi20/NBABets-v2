// desktop_tauri/src/routes/trading/index.tsx
import { createRoute } from "@tanstack/react-router";
import { Route as rootRoute } from "../__root";
import "../../styles/trading.css";

import { useTradingStream } from "./hooks/useTradingStream";
import { KpiTileStrip } from "./components/KpiTileStrip";
import { ControlBar } from "./components/ControlBar";
import { PicksTable } from "./components/PicksTable";
import { BetSlipSidebar } from "./components/BetSlipSidebar";
import { PositionsTable } from "./components/PositionsTable";
import { FillsFeed } from "./components/FillsFeed";
import { PnlTrendChart } from "./components/PnlTrendChart";
import { CollapsedSection } from "./components/CollapsedSection";
import { EventLogStrip } from "./components/EventLogStrip";
import { LimitsModal } from "./components/LimitsModal";
import { useTradingStore } from "./store";

export const Route = createRoute({
  getParentRoute: () => rootRoute,
  path: "/trading",
  component: TradingPageV2,
});

function TradingPageV2() {
  useTradingStream();
  const snapshot = useTradingStore((s) => s.snapshot);
  const streamConnected = useTradingStore((s) => s.streamConnected);

  return (
    <div className="trading-page-v2">
      <KpiTileStrip />
      <ControlBar />

      <div style={{ display: "grid", gridTemplateColumns: "70% 30%", gap: 16, marginBottom: 18 }}>
        <PicksTable />
        <BetSlipSidebar />
      </div>

      <PositionsTable />
      <FillsFeed />
      <PnlTrendChart />

      <div style={{ marginTop: 14, display: "flex", flexDirection: "column", gap: 6 }}>
        <CollapsedSection title="Resting Orders" count={snapshot?.resting_orders.length ?? 0} hideWhenEmpty>
          <DebugJson value={snapshot?.resting_orders} />
        </CollapsedSection>
        <CollapsedSection title="Live Kalshi Quotes" count={snapshot?.quotes.length ?? 0}>
          <DebugJson value={snapshot?.quotes} />
        </CollapsedSection>
        <CollapsedSection title="System Diagnostics" count={snapshot?.diagnostics ? 1 : 0}>
          <DebugJson value={snapshot?.diagnostics} />
        </CollapsedSection>
      </div>

      <EventLogStrip />
      <LimitsModal />

      {!streamConnected ? (
        <div style={{ position: "fixed", bottom: 12, right: 12, padding: "4px 10px", background: "var(--trading-surface)", border: "1px solid var(--trading-accent-system)", borderRadius: 12, fontSize: 10, color: "var(--trading-accent-system)" }}>
          ● disconnected — retrying
        </div>
      ) : null}
    </div>
  );
}

// Raw JSON dump kept for development inspection only; in production builds the
// section renders a count summary without the unstructured payload.
function DebugJson({ value }: { value: unknown }) {
  if (!import.meta.env.DEV) {
    return (
      <p className="micro" style={{ textTransform: "none", letterSpacing: 0, color: "var(--trading-fg-subtle)" }}>
        Raw payload hidden outside development.
      </p>
    );
  }
  return (
    <pre className="mono" style={{ fontSize: 10, color: "var(--trading-fg-muted)" }}>
      {JSON.stringify(value, null, 2)}
    </pre>
  );
}

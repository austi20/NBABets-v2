import { createRoute } from "@tanstack/react-router";
import { useState } from "react";
import { Route as rootRoute } from "./__root";
import { AnalystPanel } from "../components/primitives/AnalystPanel";
import { ConfidenceBar } from "../components/primitives/ConfidenceBar";
import { EdgeBadge } from "../components/primitives/EdgeBadge";
import { FilterStrip } from "../components/primitives/FilterStrip";
import { FreshnessBadge } from "../components/primitives/FreshnessBadge";
import { InjuryPill } from "../components/primitives/InjuryPill";
import { MetricCard } from "../components/primitives/MetricCard";
import { PlayerCard } from "../components/primitives/PlayerCard";

export const Route = createRoute({
  getParentRoute: () => rootRoute,
  path: "/dev/components",
  component: DevComponentsPage,
});

function DevComponentsPage() {
  const [confidenceFilter, setConfidenceFilter] = useState("all");
  return (
    <div className="dev-components-page">
      <h1>Component Playground</h1>
      <p className="dev-note">Hidden route for primitive visual checks. Remove in T7.</p>

      <section className="dev-block">
        <h2>EdgeBadge + ConfidenceBar + Injury/Freshness</h2>
        <div className="dev-inline">
          <EdgeBadge edge={12.4} />
          <EdgeBadge edge={2.6} />
          <EdgeBadge edge={-1.3} />
          <InjuryPill status="Q" />
          <InjuryPill status="D" />
          <InjuryPill status="Healthy" />
          <FreshnessBadge fetchedAt={new Date().toISOString()} />
          <FreshnessBadge fetchedAt={new Date(Date.now() - 20 * 60_000).toISOString()} />
        </div>
        <div className="dev-inline confidence-stack">
          <ConfidenceBar value={84} />
          <ConfidenceBar value={52} />
          <ConfidenceBar value={18} />
        </div>
      </section>

      <section className="dev-block">
        <h2>MetricCard</h2>
        <div className="dev-metric-grid">
          <MetricCard label="Board" value="2026-05-05" />
          <MetricCard label="Games" value="8" />
          <MetricCard label="Props" value="214" />
          <MetricCard label="Books" value="5" />
        </div>
      </section>

      <section className="dev-block">
        <h2>FilterStrip</h2>
        <FilterStrip
          label="Confidence"
          value={confidenceFilter}
          onValueChange={setConfidenceFilter}
          options={[
            { value: "all", label: "All" },
            { value: "solid", label: "Solid+" },
            { value: "best", label: "Best" },
          ]}
        />
      </section>

      <section className="dev-block">
        <h2>PlayerCard</h2>
        <div className="dev-player-grid">
          <PlayerCard
            playerName="Jayson Tatum"
            team="BOS"
            position="F"
            marketLabel="Points"
            lineLabel="O 28.5"
            edge={8.2}
            confidence={79}
            injuryStatus="Healthy"
            fetchedAt={new Date().toISOString()}
          />
          <PlayerCard
            playerName="Donovan Mitchell"
            team="CLE"
            position="G"
            marketLabel="Assists"
            lineLabel="U 6.5"
            edge={2.1}
            confidence={48}
            injuryStatus="Q"
            fetchedAt={new Date(Date.now() - 10 * 60_000).toISOString()}
            selected
          />
        </div>
      </section>

      <section className="dev-block">
        <h2>AnalystPanel</h2>
        <AnalystPanel
          lines={[
            "Model confidence stabilized after injury ingest.",
            "Top edge currently BOS F points over 28.5 at +8.2%.",
            "Awaiting new quote cycle from provider feed.",
          ]}
        />
      </section>
    </div>
  );
}

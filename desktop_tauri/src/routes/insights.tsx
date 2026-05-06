import * as Tabs from "@radix-ui/react-tabs";
import { createRoute } from "@tanstack/react-router";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useMemo, useState } from "react";
import {
  api,
  localAgentTerminalStreamUrl,
  type LocalAgentPolicy,
  type ProviderStatus,
} from "../api/client";
import { AnalystPanel } from "../components/primitives/AnalystPanel";
import { FreshnessBadge } from "../components/primitives/FreshnessBadge";
import { InjuryPill } from "../components/primitives/InjuryPill";
import { Route as rootRoute } from "./__root";

export const Route = createRoute({
  getParentRoute: () => rootRoute,
  path: "/insights",
  component: InsightsPage,
});

type InsightsTab = "providers" | "injuries" | "local-agent";

function InsightsPage() {
  const queryClient = useQueryClient();
  const [tab, setTab] = useState<InsightsTab>("providers");
  const [terminalLines, setTerminalLines] = useState<string[]>([]);
  const [terminalError, setTerminalError] = useState<string | null>(null);
  const [policyOverride, setPolicyOverride] = useState<LocalAgentPolicy | null>(null);
  const [updatingPolicy, setUpdatingPolicy] = useState(false);

  const providersQuery = useQuery({
    queryKey: ["insights", "providers"],
    queryFn: api.insightsProviders,
    staleTime: 30_000,
    refetchInterval: 30_000,
  });

  const injuriesQuery = useQuery({
    queryKey: ["insights", "injuries"],
    queryFn: () => api.insightsInjuries(),
    staleTime: 30_000,
    refetchInterval: 30_000,
  });

  const localAgentStatusQuery = useQuery({
    queryKey: ["local-agent", "status"],
    queryFn: api.localAgentStatus,
    staleTime: 10_000,
    refetchInterval: 10_000,
  });

  useEffect(() => {
    if (tab !== "local-agent") {
      return;
    }
    const source = new EventSource(localAgentTerminalStreamUrl());
    source.onmessage = (event) => {
      if (!event.data) {
        return;
      }
      const nextLines = event.data.split(/\r?\n/);
      setTerminalLines(nextLines.slice(-500));
      setTerminalError(null);
    };
    source.onerror = () => {
      setTerminalError("Terminal stream disconnected. Reconnecting...");
    };
    return () => source.close();
  }, [tab]);

  const providers = providersQuery.data ?? [];
  const staleProviderCount = useMemo(
    () =>
      providers.filter((provider) => {
        const status = provider.status_label.toLowerCase();
        return status.includes("stale") || status.includes("lag");
      }).length,
    [providers],
  );

  const injuryRows = useMemo(() => {
    const entries = Object.entries(injuriesQuery.data ?? {});
    return entries
      .map(([playerId, status]) => ({ playerId: Number.parseInt(playerId, 10), status }))
      .sort((left, right) => right.status.severity - left.status.severity || left.playerId - right.playerId);
  }, [injuriesQuery.data]);

  const resolvedPolicy = policyOverride ?? policyFromStatus(localAgentStatusQuery.data?.note, localAgentStatusQuery.data?.enabled);

  const updatePolicy = async (policy: LocalAgentPolicy) => {
    setUpdatingPolicy(true);
    setPolicyOverride(policy);
    try {
      await api.localAgentPolicy(policy);
      await queryClient.invalidateQueries({ queryKey: ["local-agent", "status"] });
    } finally {
      setUpdatingPolicy(false);
      setPolicyOverride(null);
    }
  };

  return (
    <div className="insights-page">
      <div className="insights-header">
        <h1>Insights</h1>
        <p>Provider freshness, injury snapshots, and local agent controls in one view.</p>
      </div>

      <Tabs.Root value={tab} onValueChange={(value) => setTab(value as InsightsTab)} className="insights-tabs-root">
        <Tabs.List className="parlays-tabs-list" aria-label="Insights categories">
          <Tabs.Trigger className="parlays-tab-trigger" value="providers">
            Providers
          </Tabs.Trigger>
          <Tabs.Trigger className="parlays-tab-trigger" value="injuries">
            Injuries
          </Tabs.Trigger>
          <Tabs.Trigger className="parlays-tab-trigger" value="local-agent">
            Local Agent
          </Tabs.Trigger>
        </Tabs.List>

        <Tabs.Content value="providers" className="insights-tab-content">
          <div className="insights-kpi-row">
            <span className="insights-pill">Providers: {providers.length}</span>
            <span className={`insights-pill ${staleProviderCount > 0 ? "warning" : ""}`}>
              {staleProviderCount > 0 ? `${staleProviderCount} stale sources` : "All sources fresh"}
            </span>
          </div>
          {providersQuery.isLoading ? (
            <div className="slate-skeleton-stack">
              {Array.from({ length: 4 }, (_, index) => (
                <div key={index} className="slate-skeleton-card" />
              ))}
            </div>
          ) : providersQuery.error ? (
            <div className="dashboard-state error-state">
              Unable to load provider insights: {providersQuery.error instanceof Error ? providersQuery.error.message : "unknown error"}
            </div>
          ) : (
            <div className="provider-grid">
              {providers.map((provider) => (
                <ProviderCard key={`${provider.provider_type}:${provider.provider_name}`} provider={provider} />
              ))}
            </div>
          )}
        </Tabs.Content>

        <Tabs.Content value="injuries" className="insights-tab-content">
          {injuriesQuery.isLoading ? (
            <div className="slate-skeleton-stack">
              {Array.from({ length: 4 }, (_, index) => (
                <div key={index} className="slate-skeleton-card" />
              ))}
            </div>
          ) : injuriesQuery.error ? (
            <div className="dashboard-state error-state">
              Unable to load injuries: {injuriesQuery.error instanceof Error ? injuriesQuery.error.message : "unknown error"}
            </div>
          ) : injuryRows.length === 0 ? (
            <div className="dashboard-state">
              <h3>No injury statuses available</h3>
              <p>Run startup refresh to repopulate the cached injury board.</p>
            </div>
          ) : (
            <div className="injury-list" role="list" aria-label="Injury statuses">
              {injuryRows.map((row) => (
                <article key={row.playerId} className="injury-row" role="listitem">
                  <div>
                    <p className="micro-label">Player #{row.playerId}</p>
                    <p>{row.status.detail}</p>
                  </div>
                  <InjuryPill status={injuryPillStatus(row.status.label)} />
                </article>
              ))}
            </div>
          )}
        </Tabs.Content>

        <Tabs.Content value="local-agent" className="insights-tab-content">
          <section className="local-agent-card">
            <div className="local-agent-head">
              <div>
                <p className="micro-label">Policy</p>
                <h3>Local autonomy controls</h3>
              </div>
              <label className="sort-control">
                <span className="micro-label">Execution policy</span>
                <select
                  value={resolvedPolicy}
                  disabled={updatingPolicy}
                  onChange={(event) => void updatePolicy(event.target.value as LocalAgentPolicy)}
                >
                  <option value="enable">Enable</option>
                  <option value="disable">Disable</option>
                  <option value="safe_auto_enable">Safe auto enable</option>
                  <option value="safe_auto_disable">Safe auto disable</option>
                </select>
              </label>
            </div>

            <div className="local-agent-stats">
              <span className="insights-pill">{localAgentStatusQuery.data?.enabled ? "Agent enabled" : "Agent disabled"}</span>
              <span className="insights-pill">
                {localAgentStatusQuery.data?.auto_execute_safe ? "Safe auto: on" : "Safe auto: off"}
              </span>
              <span className="insights-pill">Last run: {localAgentStatusQuery.data?.last_run_status ?? "--"}</span>
            </div>

            <p className="local-agent-summary">
              {localAgentStatusQuery.data?.last_summary || "No local agent summary available yet."}
            </p>
          </section>

          <section className="local-agent-terminal">
            <div className="local-agent-terminal-head">
              <p className="micro-label">Terminal tail</p>
              <button
                type="button"
                className="decision-sort-btn"
                onClick={() => void navigator.clipboard.writeText(terminalLines.join("\n"))}
              >
                Copy
              </button>
            </div>
            {terminalError ? <p className="decision-send-note error">{terminalError}</p> : null}
            <AnalystPanel title="Local Agent Terminal" lines={terminalLines} />
          </section>
        </Tabs.Content>
      </Tabs.Root>
    </div>
  );
}

function ProviderCard({ provider }: { provider: ProviderStatus }) {
  return (
    <article className="provider-card">
      <div className="provider-card-head">
        <div>
          <p className="micro-label">{provider.provider_type}</p>
          <h3>{provider.provider_name}</h3>
        </div>
        <FreshnessBadge fetchedAt={provider.fetched_at} />
      </div>
      <p className="provider-endpoint">{provider.endpoint}</p>
      <p className="provider-detail">{provider.detail}</p>
      <p className="provider-status">{provider.status_label}</p>
    </article>
  );
}

function injuryPillStatus(label: string): "Q" | "D" | "O" | "Healthy" {
  const lowered = label.toLowerCase();
  if (lowered.includes("out")) {
    return "O";
  }
  if (lowered.includes("doubtful")) {
    return "D";
  }
  if (lowered.includes("questionable") || lowered.includes("probable") || lowered.includes("game")) {
    return "Q";
  }
  return "Healthy";
}

function policyFromStatus(note?: string, enabled?: boolean): LocalAgentPolicy {
  if (note === "safe_auto_enable") {
    return "safe_auto_enable";
  }
  if (note === "safe_auto_disable") {
    return "safe_auto_disable";
  }
  return enabled ? "enable" : "disable";
}

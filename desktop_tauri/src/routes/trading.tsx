import * as Dialog from "@radix-ui/react-dialog";
import { createRoute } from "@tanstack/react-router";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useMemo, useRef, useState } from "react";
import { Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { api, type TradingFill } from "../api/client";
import { Route as rootRoute } from "./__root";

export const Route = createRoute({
  getParentRoute: () => rootRoute,
  path: "/trading",
  component: TradingPage,
});

const LOSS_CAP_UNITS = 5;

function TradingPage() {
  const queryClient = useQueryClient();
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [newFillIds, setNewFillIds] = useState<Set<string>>(new Set());
  const previousFillIdsRef = useRef<Set<string>>(new Set());

  const positionsQuery = useQuery({
    queryKey: ["trading", "positions"],
    queryFn: api.tradingPositions,
    staleTime: 5_000,
    refetchInterval: 5_000,
  });

  const pnlQuery = useQuery({
    queryKey: ["trading", "pnl"],
    queryFn: api.tradingPnl,
    staleTime: 5_000,
    refetchInterval: 5_000,
  });

  const fillsQuery = useQuery({
    queryKey: ["trading", "fills"],
    queryFn: () => api.tradingRecentFills(50),
    staleTime: 5_000,
    refetchInterval: 5_000,
  });

  useEffect(() => {
    const fills = fillsQuery.data ?? [];
    const prev = previousFillIdsRef.current;
    const nextIds = new Set<string>();
    for (const fill of fills) {
      if (!prev.has(fill.fill_id)) {
        nextIds.add(fill.fill_id);
      }
    }
    if (nextIds.size > 0 && prev.size > 0) {
      setNewFillIds(nextIds);
      const timeout = window.setTimeout(() => setNewFillIds(new Set()), 220);
      previousFillIdsRef.current = new Set(fills.map((fill) => fill.fill_id));
      return () => window.clearTimeout(timeout);
    }
    previousFillIdsRef.current = new Set(fills.map((fill) => fill.fill_id));
    return;
  }, [fillsQuery.data]);

  const pnl = pnlQuery.data?.daily_realized_pnl ?? 0;
  const killSwitchActive = pnlQuery.data?.kill_switch_active ?? false;
  const lossProgress = Math.max(0, Math.min(1, Math.abs(Math.min(pnl, 0)) / LOSS_CAP_UNITS));
  const lossState = lossProgress >= 1 ? "danger" : lossProgress >= 0.8 ? "warning" : "normal";

  const sparkData = useMemo(() => buildPnlSparkData(fillsQuery.data ?? []), [fillsQuery.data]);

  const triggerKillSwitch = async () => {
    await api.tradingKillSwitch();
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: ["trading", "pnl"] }),
      queryClient.invalidateQueries({ queryKey: ["trading", "positions"] }),
      queryClient.invalidateQueries({ queryKey: ["trading", "fills"] }),
    ]);
    setConfirmOpen(false);
  };

  return (
    <div className="trading-page">
      <div className="trading-header">
        <h1>Trading</h1>
        <p>Paper execution monitor with open positions, daily P&L context, and recent fills.</p>
      </div>

      <section className="loss-cap-panel">
        <div className="loss-cap-top">
          <span className="micro-label">Daily loss cap</span>
          <span className="tabular">
            {formatSignedMoney(pnl)} / -{LOSS_CAP_UNITS.toFixed(2)}
          </span>
        </div>
        <div className="loss-cap-track" role="progressbar" aria-valuemin={0} aria-valuemax={100} aria-valuenow={Math.round(lossProgress * 100)}>
          <div className={`loss-cap-fill ${lossState}`} style={{ width: `${lossProgress * 100}%` }} />
        </div>
      </section>

      <div className="trading-grid">
        <section className="trading-card trading-positions">
          <div className="trading-card-head">
            <h2>Open Positions</h2>
            <span className="tabular">{positionsQuery.data?.length ?? 0}</span>
          </div>
          {positionsQuery.isLoading ? (
            <div className="slate-skeleton-stack">
              {Array.from({ length: 3 }, (_, index) => (
                <div key={index} className="slate-skeleton-card" />
              ))}
            </div>
          ) : positionsQuery.error ? (
            <p className="decision-send-note error">
              Unable to load positions: {positionsQuery.error instanceof Error ? positionsQuery.error.message : "unknown error"}
            </p>
          ) : (positionsQuery.data?.length ?? 0) === 0 ? (
            <p className="trading-empty">No open positions yet.</p>
          ) : (
            <div className="trading-table-wrap">
              <table className="trading-table tabular">
                <thead>
                  <tr>
                    <th>Market</th>
                    <th>Side</th>
                    <th>Stake</th>
                    <th>Avg Price</th>
                    <th>Realized P&L</th>
                    <th>Updated</th>
                  </tr>
                </thead>
                <tbody>
                  {positionsQuery.data?.map((position) => (
                    <tr key={`${position.market_symbol}:${position.market_key}:${position.side}`}>
                      <td>{position.market_symbol}</td>
                      <td>{position.side}</td>
                      <td>{position.open_stake.toFixed(2)}</td>
                      <td>{position.avg_price.toFixed(3)}</td>
                      <td className={position.realized_pnl >= 0 ? "pnl-positive" : "pnl-negative"}>
                        {formatSignedMoney(position.realized_pnl)}
                      </td>
                      <td>{formatTime(position.updated_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>

        <section className="trading-card trading-pnl">
          <div className="trading-card-head">
            <h2>Daily P&L</h2>
            <span className={`trading-kill ${killSwitchActive ? "active" : ""}`}>
              {killSwitchActive ? "Kill switch active" : "Kill switch off"}
            </span>
          </div>

          <p className={`trading-pnl-value tabular ${pnl >= 0 ? "pnl-positive" : "pnl-negative"}`}>{formatSignedMoney(pnl)}</p>

          <div className="trading-sparkline">
            <ResponsiveContainer width="100%" height={140}>
              <LineChart data={sparkData}>
                <XAxis dataKey="index" hide />
                <YAxis hide domain={["auto", "auto"]} />
                <Tooltip
                  formatter={(value: number) => [formatSignedMoney(value), "Cum P&L"]}
                  labelFormatter={(label) => `Fill #${label}`}
                  contentStyle={{ background: "var(--color-surface-1)", border: "1px solid var(--color-smoke)" }}
                />
                <Line type="monotone" dataKey="pnl" stroke="var(--color-info)" strokeWidth={2} dot={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>

          <button type="button" className="kill-switch-btn" disabled={killSwitchActive} onClick={() => setConfirmOpen(true)}>
            {killSwitchActive ? "Kill Switch Engaged" : "Trigger Kill Switch"}
          </button>
        </section>
      </div>

      <section className="trading-card trading-fills">
        <div className="trading-card-head">
          <h2>Recent Fills</h2>
          <span className="micro-label">5s refresh</span>
        </div>
        {fillsQuery.isLoading ? (
          <div className="slate-skeleton-stack">
            {Array.from({ length: 4 }, (_, index) => (
              <div key={index} className="slate-skeleton-card" />
            ))}
          </div>
        ) : fillsQuery.error ? (
          <p className="decision-send-note error">
            Unable to load fills: {fillsQuery.error instanceof Error ? fillsQuery.error.message : "unknown error"}
          </p>
        ) : (fillsQuery.data?.length ?? 0) === 0 ? (
          <p className="trading-empty">No fills recorded yet.</p>
        ) : (
          <div className="fills-feed" role="log" aria-live="polite">
            {fillsQuery.data?.map((fill) => (
              <article
                key={fill.fill_id}
                className={`fill-row ${fill.realized_pnl >= 0 ? "positive" : "negative"} ${newFillIds.has(fill.fill_id) ? "flash" : ""}`}
              >
                <div>
                  <p className="micro-label">{fill.market.symbol ?? fill.market.market_key ?? "Market"}</p>
                  <p className="fill-line">
                    {fill.side.toUpperCase()} {fill.market.line_value ?? "--"} @ {fill.price.toFixed(3)} | Stake {fill.stake.toFixed(2)}
                  </p>
                </div>
                <div className="fill-right tabular">
                  <span className={fill.realized_pnl >= 0 ? "pnl-positive" : "pnl-negative"}>{formatSignedMoney(fill.realized_pnl)}</span>
                  <span>{formatTime(fill.timestamp)}</span>
                </div>
              </article>
            ))}
          </div>
        )}
      </section>

      <KillSwitchConfirm
        open={confirmOpen}
        onOpenChange={setConfirmOpen}
        onConfirm={() => void triggerKillSwitch()}
      />
    </div>
  );
}

function KillSwitchConfirm({
  open,
  onOpenChange,
  onConfirm,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onConfirm: () => void;
}) {
  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className="decision-overlay" />
        <Dialog.Content className="kill-confirm-dialog">
          <Dialog.Title className="decision-title">Disable new bets for this session?</Dialog.Title>
          <p className="decision-subtitle">This sets the risk engine kill switch and blocks new trading intents.</p>
          <div className="kill-confirm-actions">
            <button type="button" className="decision-sort-btn" onClick={() => onOpenChange(false)}>
              Cancel
            </button>
            <button type="button" className="kill-switch-btn" onClick={onConfirm}>
              Confirm Kill Switch
            </button>
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

function formatSignedMoney(value: number): string {
  const signed = value >= 0 ? "+" : "";
  return `${signed}${value.toFixed(2)}`;
}

function formatTime(value: string): string {
  const parsed = new Date(value);
  if (!Number.isFinite(parsed.getTime())) {
    return "--";
  }
  return new Intl.DateTimeFormat("en-US", {
    hour: "numeric",
    minute: "2-digit",
    second: "2-digit",
  }).format(parsed);
}

function buildPnlSparkData(fills: TradingFill[]) {
  const ordered = [...fills].sort((left, right) => left.timestamp.localeCompare(right.timestamp));
  let running = 0;
  return ordered.map((fill, index) => {
    running += fill.realized_pnl;
    return {
      index: index + 1,
      pnl: Number(running.toFixed(4)),
    };
  });
}

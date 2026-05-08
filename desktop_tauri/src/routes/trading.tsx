import * as Dialog from "@radix-ui/react-dialog";
import { createRoute } from "@tanstack/react-router";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useMemo, useRef, useState } from "react";
import { Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import {
  api,
  type TradingExchangePosition,
  type TradingFill,
  type TradingLivePosition,
  type TradingQuote,
  type TradingReadiness,
  type TradingReadinessCheck,
  type TradingRestingOrder,
} from "../api/client";
import { Route as rootRoute } from "./__root";

export const Route = createRoute({
  getParentRoute: () => rootRoute,
  path: "/trading",
  component: TradingPage,
});

const FALLBACK_LOSS_CAP = 10;

function TradingPage() {
  const queryClient = useQueryClient();
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [newFillIds, setNewFillIds] = useState<Set<string>>(new Set());
  const previousFillIdsRef = useRef<Set<string>>(new Set());

  const pnlQuery = useQuery({
    queryKey: ["trading", "pnl"],
    queryFn: api.tradingPnl,
    staleTime: 5_000,
    refetchInterval: 5_000,
  });

  const snapshotQuery = useQuery({
    queryKey: ["trading", "snapshot"],
    queryFn: api.tradingSnapshot,
    staleTime: 2_000,
    refetchInterval: 3_000,
  });

  const readinessQuery = useQuery({
    queryKey: ["trading", "readiness"],
    queryFn: api.tradingReadiness,
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

  const snapshot = snapshotQuery.data;
  const pnl = snapshot?.total_daily_pnl ?? pnlQuery.data?.daily_realized_pnl ?? 0;
  const realizedPnl = snapshot?.daily_realized_pnl ?? pnlQuery.data?.daily_realized_pnl ?? 0;
  const unrealizedPnl = snapshot?.daily_unrealized_pnl ?? 0;
  const killSwitchActive = snapshot?.kill_switch_active ?? pnlQuery.data?.kill_switch_active ?? false;
  const lossCap = snapshot?.daily_loss_cap ?? pnlQuery.data?.active_limits?.daily_loss_cap ?? FALLBACK_LOSS_CAP;
  const pnlProgress = Math.max(0, Math.min(1, Math.abs(pnl) / Math.max(lossCap, 0.01)));
  const pnlState = pnl < 0 && pnlProgress >= 1 ? "danger" : pnl < 0 && pnlProgress >= 0.8 ? "warning" : pnl > 0 ? "positive" : "normal";
  const budgetMax = snapshot?.max_open_notional ?? pnlQuery.data?.active_limits?.max_open_notional ?? 0;
  const budgetUsed = snapshot?.budget_used ?? 0;
  const budgetProgress = budgetMax > 0 ? Math.max(0, Math.min(1, budgetUsed / budgetMax)) : 0;
  const livePositions = snapshot?.positions ?? [];
  const liveQuotes = snapshot?.quotes ?? [];
  const accountPositions = snapshot?.account_positions ?? [];
  const restingOrders = snapshot?.resting_orders ?? [];
  const positionRowCount = livePositions.length + accountPositions.length;

  const sparkData = useMemo(() => buildPnlSparkData(fillsQuery.data ?? []), [fillsQuery.data]);

  const triggerKillSwitch = async () => {
    await api.tradingKillSwitch();
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: ["trading", "pnl"] }),
      queryClient.invalidateQueries({ queryKey: ["trading", "snapshot"] }),
      queryClient.invalidateQueries({ queryKey: ["trading", "fills"] }),
    ]);
    setConfirmOpen(false);
  };

  return (
    <div className="trading-page">
      <div className="trading-header">
        <h1>Trading</h1>
        <p>Execution monitor with open positions, daily P&L context, and recent fills.</p>
      </div>

      <section className="loss-cap-panel">
        <div className="loss-cap-top">
          <span className="micro-label">Daily P&L</span>
          <span className="tabular">
            {formatSignedMoney(pnl)} / -{lossCap.toFixed(2)}
          </span>
        </div>
        <div className="loss-cap-track" role="progressbar" aria-valuemin={0} aria-valuemax={100} aria-valuenow={Math.round(pnlProgress * 100)}>
          <div className={`loss-cap-fill ${pnlState}`} style={{ width: `${pnlProgress * 100}%` }} />
        </div>
        <div className="trading-progress-meta tabular">
          <span>Realized {formatSignedMoney(realizedPnl)}</span>
          <span>Unrealized {formatSignedMoney(unrealizedPnl)}</span>
          <span>Budget {budgetUsed.toFixed(2)} / {budgetMax.toFixed(2)}</span>
        </div>
        <div className="budget-track" role="progressbar" aria-valuemin={0} aria-valuemax={100} aria-valuenow={Math.round(budgetProgress * 100)}>
          <div className="budget-fill" style={{ width: `${budgetProgress * 100}%` }} />
        </div>
      </section>

      <ReadinessPanel
        readiness={readinessQuery.data}
        loading={readinessQuery.isLoading}
        error={readinessQuery.error}
      />

      <div className="trading-grid">
        <section className="trading-card trading-positions">
          <div className="trading-card-head">
            <h2>Open Positions</h2>
            <span className="tabular">{positionRowCount}</span>
          </div>
          {snapshotQuery.isLoading ? (
            <div className="slate-skeleton-stack">
              {Array.from({ length: 3 }, (_, index) => (
                <div key={index} className="slate-skeleton-card" />
              ))}
            </div>
          ) : snapshotQuery.error ? (
            <p className="decision-send-note error">
              Unable to load live positions: {snapshotQuery.error instanceof Error ? snapshotQuery.error.message : "unknown error"}
            </p>
          ) : positionRowCount === 0 ? (
            <p className="trading-empty">
              No open positions. Engine rows come from fills recorded in this app; Kalshi rows require API credentials on the sidecar. Check monitor alerts below if sync is disabled.
            </p>
          ) : (
            <div className="trading-positions-stack">
              {livePositions.length > 0 ? (
                <div className="trading-subtable">
                  <h3 className="micro-label trading-subtable-title">Tracked in this app (ledger)</h3>
                  <div className="trading-table-wrap">
                    <table className="trading-table tabular">
                      <thead>
                        <tr>
                          <th>Market</th>
                          <th>Side</th>
                          <th>Contracts</th>
                          <th>Entry</th>
                          <th>Live Exit</th>
                          <th>Value</th>
                          <th>Unrealized</th>
                          <th>Updated</th>
                        </tr>
                      </thead>
                      <tbody>
                        {livePositions.map((position) => (
                          <tr key={`ledger:${position.market_symbol}:${position.market_key}:${position.side}`}>
                            <td>{formatPositionMarket(position)}</td>
                            <td>{position.side}</td>
                            <td>{position.contract_count.toFixed(2)}</td>
                            <td>{formatPrice(position.avg_price)}</td>
                            <td>{formatOptionalPrice(position.current_exit_price)}</td>
                            <td>{formatOptionalMoney(position.current_value)}</td>
                            <td className={(position.unrealized_pnl ?? 0) >= 0 ? "pnl-positive" : "pnl-negative"}>
                              {formatOptionalSignedMoney(position.unrealized_pnl)}
                            </td>
                            <td>{formatTime(position.updated_at)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              ) : null}
              {accountPositions.length > 0 ? (
                <div className="trading-subtable">
                  <h3 className="micro-label trading-subtable-title">Held on Kalshi (exchange)</h3>
                  <div className="trading-table-wrap">
                    <table className="trading-table tabular">
                      <thead>
                        <tr>
                          <th>Ticker</th>
                          <th>Side</th>
                          <th>Contracts</th>
                          <th>Exposure</th>
                          <th>Live Exit</th>
                          <th>Value</th>
                          <th>Realized</th>
                          <th>Updated</th>
                        </tr>
                      </thead>
                      <tbody>
                        {accountPositions.map((position) => (
                          <tr key={`kalshi:${position.ticker}:${position.side}:${position.net_position}`}>
                            <td>{position.ticker}</td>
                            <td>{formatAccountSide(position)}</td>
                            <td>{position.contract_count.toFixed(2)}</td>
                            <td>{formatOptionalMoney(position.market_exposure)}</td>
                            <td>{formatOptionalPrice(position.current_exit_price)}</td>
                            <td>{formatOptionalMoney(position.current_value)}</td>
                            <td className={(position.realized_pnl ?? 0) >= 0 ? "pnl-positive" : "pnl-negative"}>
                              {formatOptionalSignedMoney(position.realized_pnl)}
                            </td>
                            <td>{position.updated_at ? formatTime(position.updated_at) : "--"}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              ) : null}
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

      {(snapshot?.errors.length ?? 0) > 0 ? (
        <section className="trading-card trading-alerts">
          <div className="trading-card-head">
            <h2>Monitor Alerts</h2>
            <span className="tabular">{snapshot?.errors.length}</span>
          </div>
          <div className="trading-alert-list">
            {snapshot?.errors.map((error) => (
              <p key={error} className="decision-send-note error">{error}</p>
            ))}
          </div>
        </section>
      ) : null}

      <section className="trading-card trading-live-quotes">
        <div className="trading-card-head">
          <h2>Live Kalshi Values</h2>
          <span className="micro-label">{snapshot ? formatTime(snapshot.observed_at) : "loading"}</span>
        </div>
        {snapshotQuery.error ? (
          <p className="decision-send-note error">
            Unable to load Kalshi values: {snapshotQuery.error instanceof Error ? snapshotQuery.error.message : "unknown error"}
          </p>
        ) : liveQuotes.length === 0 ? (
          <p className="trading-empty">No tracked Kalshi tickers yet.</p>
        ) : (
          <div className="trading-table-wrap">
            <table className="trading-table tabular">
              <thead>
                <tr>
                  <th>Ticker</th>
                  <th>Side</th>
                  <th>Entry</th>
                  <th>Exit</th>
                  <th>YES</th>
                  <th>NO</th>
                  <th>Spread</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {liveQuotes.map((quote) => (
                  <tr key={quote.ticker}>
                    <td>{quote.ticker}</td>
                    <td>{formatQuoteSide(quote)}</td>
                    <td>{formatOptionalPrice(quote.entry_price)}</td>
                    <td>{formatOptionalPrice(quote.exit_price)}</td>
                    <td>{formatBidAsk(quote.yes_bid, quote.yes_ask)}</td>
                    <td>{formatBidAsk(quote.no_bid, quote.no_ask)}</td>
                    <td>{formatOptionalPrice(quote.spread)}</td>
                    <td className={quote.error ? "pnl-negative" : ""}>{quote.error ? "error" : quote.status ?? "--"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <section className="trading-card trading-orders">
        <div className="trading-card-head">
          <h2>Resting Orders</h2>
          <span className="tabular">{restingOrders.length}</span>
        </div>
        {restingOrders.length === 0 ? (
          <p className="trading-empty">No resting Kalshi orders.</p>
        ) : (
          <div className="trading-table-wrap">
            <table className="trading-table tabular">
              <thead>
                <tr>
                  <th>Order</th>
                  <th>Ticker</th>
                  <th>Side</th>
                  <th>Remaining</th>
                  <th>Price</th>
                  <th>Status</th>
                  <th>Created</th>
                </tr>
              </thead>
              <tbody>
                {restingOrders.map((order) => (
                  <tr key={order.order_id}>
                    <td>{shortOrderId(order)}</td>
                    <td>{order.ticker ?? "--"}</td>
                    <td>{order.side ?? "--"}</td>
                    <td>{formatOptionalCount(order.remaining_count)}</td>
                    <td>{formatOptionalPrice(order.price)}</td>
                    <td>{order.status ?? "--"}</td>
                    <td>{order.created_at ? formatTime(order.created_at) : "--"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

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

function ReadinessPanel({
  readiness,
  loading,
  error,
}: {
  readiness: TradingReadiness | undefined;
  loading: boolean;
  error: unknown;
}) {
  const state = readiness?.state ?? "loading";
  return (
    <section className={`trading-card trading-readiness ${state}`}>
      <div className="trading-card-head">
        <h2>Automation Readiness</h2>
        <span className={`readiness-pill ${state}`}>{formatReadinessState(state)}</span>
      </div>
      {loading ? (
        <div className="slate-skeleton-card" />
      ) : error ? (
        <p className="decision-send-note error">
          Unable to load readiness: {error instanceof Error ? error.message : "unknown error"}
        </p>
      ) : readiness ? (
        <>
          <p className="readiness-summary">{readiness.summary}</p>
          <div className="readiness-meta tabular">
            <span>{readiness.ticker ?? "No ticker"}</span>
            <span>{readiness.game_date ?? "No game date"}</span>
            <span>{readiness.market_status ?? "No market status"}</span>
            <span>{readiness.executable_symbol_count} sync row(s)</span>
          </div>
          <div className="readiness-check-grid">
            {readiness.checks.map((check) => (
              <ReadinessCheckRow key={check.key} check={check} />
            ))}
          </div>
        </>
      ) : null}
    </section>
  );
}

function ReadinessCheckRow({ check }: { check: TradingReadinessCheck }) {
  return (
    <div className={`readiness-check ${check.status}`}>
      <span className="readiness-dot" />
      <div>
        <p>{check.label}</p>
        <span>{check.detail}</span>
      </div>
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

function formatPositionMarket(position: TradingLivePosition): string {
  return position.ticker ?? position.market_symbol;
}

function formatQuoteSide(quote: TradingQuote): string {
  if (quote.side && quote.line_value !== null) {
    return `${quote.side} ${quote.line_value.toFixed(1)}`;
  }
  return quote.side ?? "--";
}

function formatAccountSide(position: TradingExchangePosition): string {
  return `${position.side} (${position.net_position.toFixed(2)})`;
}

function formatSignedMoney(value: number): string {
  const signed = value >= 0 ? "+" : "";
  return `${signed}${value.toFixed(2)}`;
}

function formatOptionalSignedMoney(value: number | null): string {
  return value === null ? "--" : formatSignedMoney(value);
}

function formatOptionalMoney(value: number | null): string {
  return value === null ? "--" : value.toFixed(2);
}

function formatPrice(value: number): string {
  return value.toFixed(3);
}

function formatOptionalPrice(value: number | null): string {
  return value === null ? "--" : formatPrice(value);
}

function formatOptionalCount(value: number | null): string {
  return value === null ? "--" : value.toFixed(2);
}

function formatBidAsk(bid: number | null, ask: number | null): string {
  if (bid === null && ask === null) {
    return "--";
  }
  return `${formatOptionalPrice(bid)} / ${formatOptionalPrice(ask)}`;
}

function shortOrderId(order: TradingRestingOrder): string {
  const id = order.order_id || order.client_order_id || "";
  return id.length > 10 ? `${id.slice(0, 10)}...` : id || "--";
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

function formatReadinessState(state: string): string {
  if (state === "ready") {
    return "Ready";
  }
  if (state === "blocked") {
    return "Blocked";
  }
  return "Checking";
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

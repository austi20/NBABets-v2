import * as Dialog from "@radix-ui/react-dialog";
import { useMemo, useState } from "react";
import type { PropWithInsight } from "../../api/client";
import { DistChart } from "./DistChart";
import { EdgeBadge } from "./EdgeBadge";

type DecisionDrawerProps = {
  open: boolean;
  item: PropWithInsight | null;
  onOpenChange: (open: boolean) => void;
  onSendToTrading: (item: PropWithInsight) => Promise<void>;
  sendState: "idle" | "submitting" | "success" | "error";
  sendMessage: string | null;
};

type QuoteSortKey = "edge" | "ev" | "book";

export function DecisionDrawer({
  open,
  item,
  onOpenChange,
  onSendToTrading,
  sendState,
  sendMessage,
}: DecisionDrawerProps) {
  const [sortKey, setSortKey] = useState<QuoteSortKey>("edge");
  const [descending, setDescending] = useState(true);

  const sortedQuotes = useMemo(() => {
    if (!item) {
      return [];
    }
    const rows = item.opportunity.quotes.map((quote) => {
      const side = normalizedSide(item.opportunity.recommended_side);
      const offeredOdds = side === "over" ? quote.over_odds : quote.under_odds;
      const impliedProbability = americanToProbability(offeredOdds);
      const trueProbability = quote.no_vig_market_probability;
      const modelProbability = quote.hit_probability;
      const edge = modelProbability - trueProbability;
      const ev = expectedValue(modelProbability, offeredOdds);
      return {
        quote,
        impliedProbability,
        trueProbability,
        edge,
        ev,
      };
    });
    const sorted = [...rows].sort((left, right) => {
      if (sortKey === "book") {
        return left.quote.sportsbook_name.localeCompare(right.quote.sportsbook_name);
      }
      if (sortKey === "ev") {
        return left.ev - right.ev;
      }
      return left.edge - right.edge;
    });
    return descending ? sorted.reverse() : sorted;
  }, [descending, item, sortKey]);

  const canSubmit = Boolean(item) && sendState !== "submitting";

  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className="decision-overlay" />
        <Dialog.Content className="decision-drawer" onEscapeKeyDown={() => onOpenChange(false)}>
          {item ? (
            <>
              <Dialog.Title className="decision-title">
                {item.opportunity.player_name} - {marketLabel(item.opportunity.market_key)} {item.opportunity.consensus_line}
              </Dialog.Title>
              <p className="decision-subtitle">
                {item.opportunity.player_team_abbreviation ?? "Team --"} - {item.insight.best_quote.sportsbook_name}
              </p>

              <section className="decision-grid-two">
                <ProbabilityBar label="Model probability" value={item.opportunity.hit_probability} accent="positive" />
                <ProbabilityBar
                  label="Market probability"
                  value={item.insight.best_quote.no_vig_market_probability}
                  accent="caution"
                />
              </section>

              <section className="decision-meta-row">
                <EdgeBadge edge={item.insight.edge * 100} />
                <span className="tabular">True Odds {formatAmericanOdds(item.insight.fair_american_odds)}</span>
                <span className="tabular">{renderEvMath(item)}</span>
              </section>

              <section className="decision-section">
                <h3>Distribution</h3>
                <DistChart
                  projectedMean={item.opportunity.projected_mean}
                  projectedVariance={item.opportunity.projected_variance}
                  line={item.opportunity.consensus_line}
                />
              </section>

              <section className="decision-section">
                <div className="decision-table-head">
                  <h3>Quotes</h3>
                  <div className="decision-sorters">
                    <SortButton
                      active={sortKey === "edge"}
                      onClick={() => toggleSort("edge", sortKey, descending, setSortKey, setDescending)}
                    >
                      Edge
                    </SortButton>
                    <SortButton
                      active={sortKey === "ev"}
                      onClick={() => toggleSort("ev", sortKey, descending, setSortKey, setDescending)}
                    >
                      EV
                    </SortButton>
                    <SortButton
                      active={sortKey === "book"}
                      onClick={() => toggleSort("book", sortKey, descending, setSortKey, setDescending)}
                    >
                      Book
                    </SortButton>
                  </div>
                </div>
                <div className="decision-table-wrap">
                  <table className="decision-table tabular">
                    <thead>
                      <tr>
                        <th>Book</th>
                        <th>Over</th>
                        <th>Under</th>
                        <th>Implied</th>
                        <th>True Odds</th>
                        <th>Edge</th>
                        <th>EV</th>
                      </tr>
                    </thead>
                    <tbody>
                      {sortedQuotes.map((row) => (
                        <tr key={`${row.quote.sportsbook_key}:${row.quote.market_key}:${row.quote.line_value}`}>
                          <td>{row.quote.sportsbook_name}</td>
                          <td>{formatAmericanOdds(row.quote.over_odds)}</td>
                          <td>{formatAmericanOdds(row.quote.under_odds)}</td>
                          <td>{formatPercent(row.impliedProbability)}</td>
                          <td>{formatAmericanOdds(probabilityToAmerican(row.trueProbability))}</td>
                          <td>{formatSignedPercent(row.edge)}</td>
                          <td>{formatSignedPercent(row.ev)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </section>

              <section className="decision-section decision-lists">
                <div>
                  <h3>Reasons</h3>
                  {item.insight.reason_lines.length > 0 ? (
                    <ul>
                      {item.insight.reason_lines.map((reason) => (
                        <li key={reason}>{reason}</li>
                      ))}
                    </ul>
                  ) : (
                    <p>No major reasons attached.</p>
                  )}
                </div>
                <div>
                  <h3>Warnings</h3>
                  {item.insight.warnings.length > 0 ? (
                    <ul>
                      {item.insight.warnings.map((warning) => (
                        <li key={warning}>{warning}</li>
                      ))}
                    </ul>
                  ) : (
                    <p>No warning flags right now.</p>
                  )}
                </div>
              </section>

              <div className="decision-cta">
                <button type="button" className="decision-send-btn" disabled={!canSubmit} onClick={() => void onSendToTrading(item)}>
                  {sendState === "submitting" ? "Sending..." : "Send to Trading"}
                </button>
                {sendMessage ? (
                  <p className={`decision-send-note ${sendState === "error" ? "error" : sendState === "success" ? "success" : ""}`}>
                    {sendMessage}
                  </p>
                ) : null}
              </div>
            </>
          ) : (
            <Dialog.Title className="decision-title">No prop selected</Dialog.Title>
          )}
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

function ProbabilityBar({ label, value, accent }: { label: string; value: number; accent: "positive" | "caution" }) {
  const safe = Math.max(0, Math.min(1, value));
  return (
    <div className="probability-bar">
      <p className="micro-label">{label}</p>
      <div className="probability-track">
        <div className={`probability-fill ${accent}`} style={{ width: `${safe * 100}%` }} />
      </div>
      <span className="tabular">{(safe * 100).toFixed(1)}%</span>
    </div>
  );
}

function SortButton({
  active,
  onClick,
  children,
}: {
  active: boolean;
  onClick: () => void;
  children: string;
}) {
  return (
    <button type="button" className={`decision-sort-btn ${active ? "active" : ""}`} onClick={onClick}>
      {children}
    </button>
  );
}

function toggleSort(
  nextKey: QuoteSortKey,
  currentKey: QuoteSortKey,
  descending: boolean,
  setSortKey: (value: QuoteSortKey) => void,
  setDescending: (value: boolean) => void,
) {
  if (nextKey === currentKey) {
    setDescending(!descending);
    return;
  }
  setSortKey(nextKey);
  setDescending(true);
}

function renderEvMath(item: PropWithInsight): string {
  const modelProbability = item.opportunity.hit_probability;
  const odds = item.insight.recommended_odds;
  if (odds === null) {
    return "EV unavailable";
  }
  const decimalOdds = americanToDecimal(odds);
  const ev = modelProbability * decimalOdds - 1;
  return `(${modelProbability.toFixed(2)} x ${decimalOdds.toFixed(2)}) - 1 = ${formatSignedPercent(ev)}`;
}

function formatAmericanOdds(odds: number | null): string {
  if (odds === null || !Number.isFinite(odds)) {
    return "--";
  }
  return odds > 0 ? `+${Math.round(odds)}` : `${Math.round(odds)}`;
}

function formatPercent(value: number): string {
  return `${(Math.max(0, Math.min(1, value)) * 100).toFixed(1)}%`;
}

function formatSignedPercent(value: number): string {
  const signed = value >= 0 ? "+" : "";
  return `${signed}${(value * 100).toFixed(1)}%`;
}

function americanToProbability(odds: number | null): number {
  if (odds === null || !Number.isFinite(odds)) {
    return 0;
  }
  if (odds > 0) {
    return 100 / (odds + 100);
  }
  return Math.abs(odds) / (Math.abs(odds) + 100);
}

function americanToDecimal(odds: number): number {
  if (odds > 0) {
    return 1 + odds / 100;
  }
  return 1 + 100 / Math.abs(odds);
}

function probabilityToAmerican(probability: number): number | null {
  const p = Math.max(0.0001, Math.min(0.9999, probability));
  if (!Number.isFinite(p)) {
    return null;
  }
  if (p >= 0.5) {
    return -Math.round((100 * p) / (1 - p));
  }
  return Math.round((100 * (1 - p)) / p);
}

function expectedValue(modelProbability: number, offeredOdds: number | null): number {
  if (offeredOdds === null) {
    return 0;
  }
  return modelProbability * americanToDecimal(offeredOdds) - 1;
}

function marketLabel(marketKey: string): string {
  return marketKey
    .split("_")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function normalizedSide(value: string): "over" | "under" {
  return value.toLowerCase().startsWith("under") ? "under" : "over";
}

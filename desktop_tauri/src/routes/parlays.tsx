import * as Dialog from "@radix-ui/react-dialog";
import * as Tabs from "@radix-ui/react-tabs";
import { createRoute } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";
import { useMemo, useState } from "react";
import { api, type ParlayLeg, type ParlayWithInsight } from "../api/client";
import { ConfidenceBar } from "../components/primitives/ConfidenceBar";
import { EdgeBadge } from "../components/primitives/EdgeBadge";
import { FilterStrip, type FilterOption } from "../components/primitives/FilterStrip";
import { PlayerCard } from "../components/primitives/PlayerCard";
import { useStartupContext } from "../startup/StartupContext";
import { Route as rootRoute } from "./__root";

export const Route = createRoute({
  getParentRoute: () => rootRoute,
  path: "/parlays",
  component: ParlaysPage,
});

const PARLAY_SORT_OPTIONS = ["Best EV", "Best Edge", "Highest Joint %", "Highest Confidence", "Most Balanced"] as const;

type ParlaySortOption = (typeof PARLAY_SORT_OPTIONS)[number];
type ParlayTab = "same-game" | "multi-game";

function ParlaysPage() {
  const startup = useStartupContext();
  const [activeTab, setActiveTab] = useState<ParlayTab>("same-game");
  const [sameBook, setSameBook] = useState("All");
  const [sameGame, setSameGame] = useState("All");
  const [sameLegCount, setSameLegCount] = useState("2");
  const [sameSort, setSameSort] = useState<ParlaySortOption>("Best EV");
  const [multiBook, setMultiBook] = useState("All");
  const [multiSort, setMultiSort] = useState<ParlaySortOption>("Best EV");
  const [selectedParlay, setSelectedParlay] = useState<ParlayWithInsight | null>(null);

  const { data: boardAvailability } = useQuery({
    queryKey: ["board-availability"],
    queryFn: api.boardAvailability,
    enabled: Boolean(startup.snapshot?.completed),
    staleTime: 30_000,
    refetchInterval: 30_000,
  });

  const sameGameQuery = useQuery({
    queryKey: ["parlays", "sgp", boardAvailability?.board_date],
    queryFn: () => api.sameGameParlays(),
    enabled: Boolean(startup.snapshot?.completed),
    staleTime: 30_000,
    refetchInterval: 30_000,
  });

  const multiGameQuery = useQuery({
    queryKey: ["parlays", "multi", boardAvailability?.board_date],
    queryFn: () => api.multiGameParlays(),
    enabled: Boolean(startup.snapshot?.completed),
    staleTime: 30_000,
    refetchInterval: 30_000,
  });

  const sameSections = sameGameQuery.data?.sections ?? {};
  const multiSections = multiGameQuery.data?.sections ?? {};

  const sameBookOptions = useMemo(() => buildSameBookOptions(sameSections), [sameSections]);
  const multiBookOptions = useMemo(() => buildMultiBookOptions(multiSections), [multiSections]);
  const activeSameBook = sameBookOptions.some((option) => option.value === sameBook) ? sameBook : (sameBookOptions[0]?.value ?? "All");
  const activeMultiBook = multiBookOptions.some((option) => option.value === multiBook) ? multiBook : (multiBookOptions[0]?.value ?? "All");

  const sameBookSections = useMemo(() => pickSameBookSections(sameSections, activeSameBook), [activeSameBook, sameSections]);
  const sameLegOptions = useMemo(() => {
    const counts = Object.keys(sameBookSections)
      .map((value) => Number.parseInt(value, 10))
      .filter(Number.isFinite)
      .sort((left, right) => left - right);
    return counts.map((count) => ({ value: String(count), label: `${count} Legs` }));
  }, [sameBookSections]);
  const activeSameLegCount = sameLegOptions.some((option) => option.value === sameLegCount)
    ? sameLegCount
    : (sameLegOptions[0]?.value ?? "2");

  const gameOptions = useMemo(() => buildGameOptions(sameBookSections), [sameBookSections]);
  const activeSameGame = gameOptions.some((option) => option.value === sameGame) ? sameGame : "All";

  const sameItems = useMemo(() => {
    const byGame = sameBookSections[activeSameLegCount] ?? {};
    const rows =
      activeSameGame === "All"
        ? Object.values(byGame).flatMap((items) => items)
        : (byGame[activeSameGame] ?? []);
    return sortParlays(rows, sameSort);
  }, [activeSameGame, activeSameLegCount, sameBookSections, sameSort]);

  const multiItems = useMemo(() => {
    const byLeg = pickMultiBookSections(multiSections, activeMultiBook);
    const rows = Object.values(byLeg).flatMap((items) => items);
    return sortParlays(rows, multiSort);
  }, [activeMultiBook, multiSections, multiSort]);

  return (
    <div className="parlays-page">
      <div className="parlays-header">
        <h1>Parlays</h1>
        <p>Same-game and multi-game parlay builder with ranked tickets and per-leg breakdown.</p>
      </div>

      <Tabs.Root value={activeTab} onValueChange={(value) => setActiveTab(value as ParlayTab)} className="parlays-tabs-root">
        <Tabs.List className="parlays-tabs-list" aria-label="Parlay mode">
          <Tabs.Trigger className="parlays-tab-trigger" value="same-game">
            Same Game
          </Tabs.Trigger>
          <Tabs.Trigger className="parlays-tab-trigger" value="multi-game">
            Multi Game
          </Tabs.Trigger>
        </Tabs.List>

        <Tabs.Content value="same-game" className="parlays-tab-content">
          <div className="parlays-toolbar">
            <FilterStrip label="Book" options={sameBookOptions} value={activeSameBook} onValueChange={setSameBook} />
            <FilterStrip label="Game" options={gameOptions} value={activeSameGame} onValueChange={setSameGame} />
            <label className="sort-control">
              <span className="micro-label">Sort</span>
              <select value={sameSort} onChange={(event) => setSameSort(event.target.value as ParlaySortOption)}>
                {PARLAY_SORT_OPTIONS.map((option) => (
                  <option key={option} value={option}>
                    {option}
                  </option>
                ))}
              </select>
            </label>
          </div>

          {sameLegOptions.length > 0 ? (
            <Tabs.Root value={activeSameLegCount} onValueChange={setSameLegCount} className="parlays-subtabs-root">
              <Tabs.List className="parlays-subtabs-list" aria-label="Leg count">
                {sameLegOptions.map((option) => (
                  <Tabs.Trigger key={option.value} value={option.value} className="parlays-subtab-trigger">
                    {option.label}
                  </Tabs.Trigger>
                ))}
              </Tabs.List>
            </Tabs.Root>
          ) : null}

          <ParlayList
            loading={sameGameQuery.isLoading || !startup.snapshot?.completed}
            error={sameGameQuery.error}
            items={sameItems}
            emptyMessage="No same-game parlays match the current selectors."
            onSelect={setSelectedParlay}
          />
        </Tabs.Content>

        <Tabs.Content value="multi-game" className="parlays-tab-content">
          <div className="parlays-toolbar">
            <FilterStrip label="Book" options={multiBookOptions} value={activeMultiBook} onValueChange={setMultiBook} />
            <label className="sort-control">
              <span className="micro-label">Sort</span>
              <select value={multiSort} onChange={(event) => setMultiSort(event.target.value as ParlaySortOption)}>
                {PARLAY_SORT_OPTIONS.map((option) => (
                  <option key={option} value={option}>
                    {option}
                  </option>
                ))}
              </select>
            </label>
          </div>

          <ParlayList
            loading={multiGameQuery.isLoading || !startup.snapshot?.completed}
            error={multiGameQuery.error}
            items={multiItems}
            emptyMessage="No multi-game parlays are available for the current board."
            onSelect={setSelectedParlay}
          />
        </Tabs.Content>
      </Tabs.Root>

      <ParlayDecisionDrawer
        open={Boolean(selectedParlay)}
        item={selectedParlay}
        onOpenChange={(open) => {
          if (!open) {
            setSelectedParlay(null);
          }
        }}
      />
    </div>
  );
}

function ParlayList({
  loading,
  error,
  items,
  emptyMessage,
  onSelect,
}: {
  loading: boolean;
  error: unknown;
  items: ParlayWithInsight[];
  emptyMessage: string;
  onSelect: (item: ParlayWithInsight) => void;
}) {
  if (error) {
    return (
      <div className="dashboard-state error-state">
        Unable to load parlays: {error instanceof Error ? error.message : "unknown error"}
      </div>
    );
  }

  if (loading) {
    return (
      <div className="slate-skeleton-stack" aria-label="Loading parlays">
        {Array.from({ length: 4 }, (_, index) => (
          <div key={index} className="slate-skeleton-card" />
        ))}
      </div>
    );
  }

  if (items.length === 0) {
    return (
      <div className="dashboard-state">
        <h3>No parlays available</h3>
        <p>{emptyMessage}</p>
      </div>
    );
  }

  return (
    <div className="parlay-list">
      {items.map((item, index) => (
        <button key={parlayKey(item)} type="button" className="parlay-card" onClick={() => onSelect(item)}>
          <div className="parlay-card-head">
            <div>
              <p className="micro-label">
                Rank {index + 1} - {item.parlay.sportsbook_icon} {item.parlay.sportsbook_name}
              </p>
              <h3>{item.parlay.matchup}</h3>
            </div>
            <EdgeBadge edge={item.parlay.edge * 100} />
          </div>
          <p className="parlay-card-line tabular">
            {item.parlay.leg_count} legs - Joint {formatPercent(item.parlay.joint_probability)} - Odds{" "}
            {formatAmerican(item.parlay.combined_american_odds)} - EV {formatSignedPercent(item.parlay.expected_profit_per_unit)}
          </p>
          <ConfidenceBar value={item.insight.confidence_score} max={99} />
          <div className="parlay-card-foot">
            <span className="tabular">Weakest leg {formatPercent(item.parlay.weakest_leg_hit_probability)}</span>
            <span>{item.insight.fragility_label}</span>
          </div>
        </button>
      ))}
    </div>
  );
}

function ParlayDecisionDrawer({
  open,
  item,
  onOpenChange,
}: {
  open: boolean;
  item: ParlayWithInsight | null;
  onOpenChange: (open: boolean) => void;
}) {
  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className="decision-overlay" />
        <Dialog.Content className="decision-drawer" onEscapeKeyDown={() => onOpenChange(false)}>
          {item ? (
            <>
              <Dialog.Title className="decision-title">
                {item.parlay.sportsbook_icon} {item.parlay.sportsbook_name} - {item.parlay.matchup}
              </Dialog.Title>
              <p className="decision-subtitle">
                {item.parlay.leg_count} legs across {item.parlay.game_count} game{item.parlay.game_count === 1 ? "" : "s"}
              </p>

              <section className="decision-meta-row tabular">
                <span>Joint {formatPercent(item.parlay.joint_probability)}</span>
                <span>Odds {formatAmerican(item.parlay.combined_american_odds)}</span>
                <span>EV {formatSignedPercent(item.parlay.expected_profit_per_unit)}</span>
                <span>Edge {formatSignedPercent(item.parlay.edge)}</span>
              </section>

              <section className="decision-section">
                <h3>Ticket confidence</h3>
                <ConfidenceBar value={item.insight.confidence_score} max={99} />
                <p className="decision-subtitle">
                  {item.insight.confidence_tier} ({item.insight.confidence_score}/99) - {item.insight.fragility_label}
                </p>
              </section>

              <section className="decision-section parlay-legs-grid">
                <h3>Leg structure</h3>
                {item.parlay.legs.map((leg, index) => (
                  <div key={`${leg.player_name}:${leg.market_key}:${leg.line_value}:${index}`} className="parlay-leg-card">
                    <PlayerCard
                      playerName={leg.player_name}
                      team={leg.matchup}
                      position={leg.recommended_side}
                      marketLabel={marketLabel(leg.market_key)}
                      lineLabel={`${sideLabel(leg.recommended_side)} ${formatLine(leg.line_value)}`}
                      edge={legEdgePercent(leg)}
                      confidence={Math.max(0, Math.min(100, leg.likelihood_score))}
                      injuryStatus="Healthy"
                      fetchedAt={null}
                      avatarLabel={initials(leg.player_name)}
                      bookLabel={`Odds ${formatAmerican(leg.american_odds)}`}
                    />
                  </div>
                ))}
              </section>

              <section className="decision-section decision-lists">
                <div>
                  <h3>Why it ranks</h3>
                  {item.insight.reason_lines.length > 0 ? (
                    <ul>
                      {item.insight.reason_lines.map((reason) => (
                        <li key={reason}>{reason}</li>
                      ))}
                    </ul>
                  ) : (
                    <p>No reason lines available.</p>
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
            </>
          ) : (
            <Dialog.Title className="decision-title">No parlay selected</Dialog.Title>
          )}
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

function pickSameBookSections(
  sections: Record<string, Record<string, Record<string, ParlayWithInsight[]>>>,
  book: string,
): Record<string, Record<string, ParlayWithInsight[]>> {
  if (book !== "All") {
    return sections[book] ?? {};
  }
  const aggregate: Record<string, Record<string, ParlayWithInsight[]>> = {};
  for (const byLegCount of Object.values(sections)) {
    for (const [legCount, byGame] of Object.entries(byLegCount)) {
      aggregate[legCount] ??= {};
      for (const [gameId, items] of Object.entries(byGame)) {
        aggregate[legCount][gameId] ??= [];
        aggregate[legCount][gameId].push(...items);
      }
    }
  }
  return aggregate;
}

function pickMultiBookSections(
  sections: Record<string, Record<string, ParlayWithInsight[]>>,
  book: string,
): Record<string, ParlayWithInsight[]> {
  if (book !== "All") {
    return sections[book] ?? {};
  }
  const aggregate: Record<string, ParlayWithInsight[]> = {};
  for (const byLegCount of Object.values(sections)) {
    for (const [legCount, items] of Object.entries(byLegCount)) {
      aggregate[legCount] ??= [];
      aggregate[legCount].push(...items);
    }
  }
  return aggregate;
}

function buildSameBookOptions(sections: Record<string, Record<string, Record<string, ParlayWithInsight[]>>>): FilterOption[] {
  const options: FilterOption[] = [{ value: "All", label: "All" }];
  for (const [bookKey, payload] of Object.entries(sections)) {
    const label = firstSameParlay(payload)?.parlay.sportsbook_name ?? bookKey;
    options.push({ value: bookKey, label });
  }
  return options;
}

function buildMultiBookOptions(sections: Record<string, Record<string, ParlayWithInsight[]>>): FilterOption[] {
  const options: FilterOption[] = [{ value: "All", label: "All" }];
  for (const [bookKey, payload] of Object.entries(sections)) {
    const label = firstMultiParlay(payload)?.parlay.sportsbook_name ?? bookKey;
    options.push({ value: bookKey, label });
  }
  return options;
}

function firstSameParlay(payload: Record<string, Record<string, ParlayWithInsight[]>>) {
  for (const byGame of Object.values(payload)) {
    for (const items of Object.values(byGame)) {
      if (items.length > 0) {
        return items[0];
      }
    }
  }
  return null;
}

function firstMultiParlay(payload: Record<string, ParlayWithInsight[]>) {
  for (const items of Object.values(payload)) {
    if (items.length > 0) {
      return items[0];
    }
  }
  return null;
}

function buildGameOptions(sections: Record<string, Record<string, ParlayWithInsight[]>>): FilterOption[] {
  const gameLabels = new Map<string, string>();
  for (const byGame of Object.values(sections)) {
    for (const [gameId, items] of Object.entries(byGame)) {
      const label = items[0]?.parlay.game_labels[0] ?? items[0]?.parlay.matchup ?? `Game ${gameId}`;
      gameLabels.set(gameId, label);
    }
  }
  const sorted = Array.from(gameLabels.entries()).sort((left, right) => left[1].localeCompare(right[1]));
  return [{ value: "All", label: "All" }, ...sorted.map(([value, label]) => ({ value, label }))];
}

function sortParlays(items: ParlayWithInsight[], sort: ParlaySortOption): ParlayWithInsight[] {
  const rows = [...items];
  rows.sort((left, right) => {
    if (sort === "Best Edge") {
      return compareDesc(left.parlay.edge, right.parlay.edge, left.parlay.expected_profit_per_unit, right.parlay.expected_profit_per_unit);
    }
    if (sort === "Highest Joint %") {
      return compareDesc(
        left.parlay.joint_probability,
        right.parlay.joint_probability,
        left.parlay.edge,
        right.parlay.edge,
      );
    }
    if (sort === "Highest Confidence") {
      return compareDesc(
        left.insight.confidence_score,
        right.insight.confidence_score,
        left.parlay.edge,
        right.parlay.edge,
      );
    }
    if (sort === "Most Balanced") {
      return compareDesc(
        left.parlay.weakest_leg_hit_probability,
        right.parlay.weakest_leg_hit_probability,
        left.parlay.average_leg_hit_probability,
        right.parlay.average_leg_hit_probability,
      );
    }
    return compareDesc(
      left.parlay.expected_profit_per_unit,
      right.parlay.expected_profit_per_unit,
      left.parlay.edge,
      right.parlay.edge,
    );
  });
  return rows;
}

function compareDesc(primaryLeft: number, primaryRight: number, tieLeft: number, tieRight: number) {
  const primary = primaryRight - primaryLeft;
  if (primary !== 0) {
    return primary;
  }
  return tieRight - tieLeft;
}

function parlayKey(item: ParlayWithInsight): string {
  return [
    item.parlay.sportsbook_key,
    item.parlay.leg_count,
    item.parlay.game_ids.join("-"),
    item.parlay.rank,
    item.parlay.combined_american_odds,
  ].join(":");
}

function legEdgePercent(leg: ParlayLeg): number {
  const implied = americanToProbability(leg.american_odds);
  return (leg.hit_probability - implied) * 100;
}

function americanToProbability(odds: number): number {
  if (odds > 0) {
    return 100 / (odds + 100);
  }
  return Math.abs(odds) / (Math.abs(odds) + 100);
}

function formatAmerican(value: number): string {
  return value > 0 ? `+${Math.round(value)}` : `${Math.round(value)}`;
}

function formatPercent(value: number): string {
  return `${(Math.max(0, Math.min(1, value)) * 100).toFixed(2)}%`;
}

function formatSignedPercent(value: number): string {
  const signed = value >= 0 ? "+" : "";
  return `${signed}${(value * 100).toFixed(2)}%`;
}

function marketLabel(marketKey: string): string {
  return marketKey
    .split("_")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function sideLabel(side: string): string {
  return side.toLowerCase().startsWith("under") ? "U" : "O";
}

function formatLine(value: number): string {
  return Number.isInteger(value) ? value.toFixed(0) : value.toFixed(1);
}

function initials(playerName: string): string {
  const parts = playerName
    .split(" ")
    .map((token) => token.trim())
    .filter(Boolean);
  if (parts.length === 0) {
    return "P";
  }
  if (parts.length === 1) {
    return parts[0][0]?.toUpperCase() ?? "P";
  }
  return `${parts[0][0] ?? ""}${parts[1][0] ?? ""}`.toUpperCase();
}

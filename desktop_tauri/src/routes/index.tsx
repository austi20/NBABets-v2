import { createRoute } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";
import { useVirtualizer } from "@tanstack/react-virtual";
import { useEffect, useMemo, useRef, useState } from "react";
import { Route as rootRoute } from "./__root";
import { api, type PropWithInsight } from "../api/client";
import { DecisionDrawer } from "../components/primitives/DecisionDrawer";
import { FilterStrip, type FilterOption } from "../components/primitives/FilterStrip";
import { PlayerCard } from "../components/primitives/PlayerCard";
import { HeaderBar } from "../components/shell/HeaderBar";
import { MetricStrip } from "../components/shell/MetricStrip";
import { useStartupContext } from "../startup/StartupContext";

export const Route = createRoute({
  getParentRoute: () => rootRoute,
  path: "/",
  component: DashboardPage,
});

const CONFIDENCE_FILTERS: FilterOption[] = [
  { value: "All", label: "All" },
  { value: "Watch+", label: "Watch+" },
  { value: "Solid+", label: "Solid+" },
  { value: "Strong+", label: "Strong+" },
  { value: "Elite", label: "Elite" },
];

const MARKET_FILTERS: FilterOption[] = [
  { value: "All", label: "All" },
  { value: "points", label: "Points" },
  { value: "rebounds", label: "Rebounds" },
  { value: "assists", label: "Assists" },
  { value: "pra", label: "PRA" },
  { value: "threes", label: "Threes" },
  { value: "turnovers", label: "Turnovers" },
];

const SORT_OPTIONS = [
  "Best Edge",
  "Best EV",
  "Highest Hit Rate",
  "Most Consensus",
  "Freshest",
  "Player A-Z",
];

type DashboardFilters = {
  confidence: string;
  market: string;
  book: string;
  sort: string;
};

type SelectedOpportunity = {
  playerId: number;
  market: string;
  line: number;
};

type SlateRow =
  | {
      kind: "game";
      key: string;
      label: string;
      timeLabel: string | null;
      count: number;
    }
  | {
      kind: "prop";
      key: string;
      item: PropWithInsight;
    };

function DashboardPage() {
  const startup = useStartupContext();
  const [refreshing, setRefreshing] = useState(false);
  const [filters, setFilters] = useState<DashboardFilters>(() => readFiltersFromUrl());
  const [selectedKey, setSelectedKey] = useState<string | null>(null);
  const [selectedOpportunity, setSelectedOpportunity] = useState<SelectedOpportunity | null>(null);
  const [sendState, setSendState] = useState<"idle" | "submitting" | "success" | "error">("idle");
  const [sendMessage, setSendMessage] = useState<string | null>(null);
  const listParentRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    writeFiltersToUrl(filters);
  }, [filters]);

  const { data: health } = useQuery({
    queryKey: ["health"],
    queryFn: api.health,
    retry: 2,
    staleTime: 60_000,
  });

  const { data: boardAvailability } = useQuery({
    queryKey: ["board-availability"],
    queryFn: api.boardAvailability,
    enabled: Boolean(startup.snapshot?.completed),
    staleTime: 30_000,
    refetchInterval: 30_000,
  });

  const { data: boardSummary } = useQuery({
    queryKey: ["board-summary", boardAvailability?.board_date],
    queryFn: api.boardSummary,
    enabled: Boolean(startup.snapshot?.completed),
    staleTime: 30_000,
    refetchInterval: 30_000,
  });

  const {
    data: propsData,
    error: propsError,
    isFetching: propsFetching,
    isLoading: propsLoading,
  } = useQuery({
    queryKey: ["props", filters, boardAvailability?.board_date],
    queryFn: () => api.props({ ...filters, page: 1, page_size: 200 }),
    enabled: Boolean(startup.snapshot?.completed),
    staleTime: 30_000,
    refetchInterval: 30_000,
  });

  const { data: tradingPnl } = useQuery({
    queryKey: ["trading-pnl"],
    queryFn: api.tradingPnl,
    retry: false,
    staleTime: 30_000,
    refetchInterval: 30_000,
  });

  const { data: selectedDetail } = useQuery({
    queryKey: ["prop-detail", selectedOpportunity],
    queryFn: () =>
      selectedOpportunity
        ? api.propDetail({
            playerId: selectedOpportunity.playerId,
            market: selectedOpportunity.market,
            line: selectedOpportunity.line,
          })
        : Promise.resolve(null),
    enabled: Boolean(selectedOpportunity),
    staleTime: 30_000,
    retry: 1,
  });

  const items = propsData?.items ?? [];
  const featured = useMemo(() => pickFeatured(items), [items]);
  const slateRows = useMemo(() => buildSlateRows(items), [items]);
  const bookFilters = useMemo(() => buildBookFilters(items, filters.book), [items, filters.book]);
  const selectedFromList = useMemo(() => {
    if (!selectedKey) {
      return null;
    }
    return items.find((entry) => propKey(entry) === selectedKey) ?? null;
  }, [items, selectedKey]);
  const selectedItem = selectedDetail ?? selectedFromList;

  const rowVirtualizer = useVirtualizer({
    count: slateRows.length,
    getScrollElement: () => listParentRef.current,
    estimateSize: (index) => (slateRows[index]?.kind === "game" ? 42 : 164),
    overscan: 8,
  });

  const statusText = startup.snapshot?.failed
    ? "Startup failed - review startup timeline"
    : startup.snapshot?.completed
      ? propsFetching
        ? "Refreshing live prop board"
        : "Board data and models loaded"
      : "Loading board data and models";

  const boardDateText = boardSummary?.board_date
    ? `Board date: ${boardSummary.board_date}`
    : startup.snapshot?.board_date_message || "Board date: pending";

  const pnlText = tradingPnl
    ? `Daily P&L: ${tradingPnl.daily_realized_pnl >= 0 ? "+" : ""}${tradingPnl.daily_realized_pnl.toFixed(2)}`
    : "Daily P&L: --";

  const runRefresh = async () => {
    setRefreshing(true);
    try {
      await startup.refreshStartup();
    } finally {
      setRefreshing(false);
    }
  };

  const updateFilter = <Key extends keyof DashboardFilters>(key: Key, value: DashboardFilters[Key]) => {
    setFilters((current) => ({ ...current, [key]: value }));
    setSelectedKey(null);
    setSelectedOpportunity(null);
    setSendState("idle");
    setSendMessage(null);
  };

  const selectItem = (item: PropWithInsight) => {
    setSelectedKey(propKey(item));
    setSelectedOpportunity({
      playerId: item.opportunity.player_id,
      market: item.opportunity.market_key,
      line: item.opportunity.consensus_line,
    });
    setSendState("idle");
    setSendMessage(null);
  };

  const handleDrawerOpen = (open: boolean) => {
    if (!open) {
      setSelectedKey(null);
      setSelectedOpportunity(null);
      setSendState("idle");
      setSendMessage(null);
    }
  };

  const handleSendToTrading = async (item: PropWithInsight) => {
    setSendState("submitting");
    setSendMessage(null);
    try {
      const side = item.opportunity.recommended_side.toLowerCase().startsWith("under") ? "under" : "over";
      const response = await api.tradingIntent({
        game_id: item.opportunity.game_id,
        player_id: item.opportunity.player_id,
        market: item.opportunity.market_key,
        line: item.opportunity.consensus_line,
        side,
        sportsbook_key: item.insight.best_quote.sportsbook_key,
        stake: 1,
      });
      setSendState(response.accepted ? "success" : "error");
      setSendMessage(response.message);
    } catch (error) {
      setSendState("error");
      setSendMessage(error instanceof Error ? error.message : "Unable to send trading intent");
    }
  };

  const updated = boardSummary?.latest_quote_at
    ? formatRelativeAge(boardSummary.latest_quote_at)
    : "--";

  const totalLabel = propsData
    ? `${propsData.total.toLocaleString()} matching props`
    : "Props pending";

  return (
    <div className="dashboard-page">
      <HeaderBar
        boardDateText={boardDateText}
        statusText={statusText}
        dailyPnlText={pnlText}
        onRefreshBoard={runRefresh}
        refreshDisabled={refreshing}
      />

      <MetricStrip
        board={boardSummary?.board_date ?? "Pending"}
        games={boardSummary ? String(boardSummary.game_count) : "0"}
        props={boardSummary ? String(boardSummary.opportunity_count) : "0"}
        books={boardSummary ? String(boardSummary.sportsbook_count) : "0"}
        alt={boardSummary ? String(boardSummary.alt_line_count) : "0"}
        updated={updated}
      />

      <section className="slate-toolbar" aria-label="Slate filters">
        <div className="slate-filter-row">
          <FilterStrip
            label="Confidence"
            options={CONFIDENCE_FILTERS}
            value={filters.confidence}
            onValueChange={(value) => updateFilter("confidence", value)}
          />
          <FilterStrip
            label="Market"
            options={MARKET_FILTERS}
            value={filters.market}
            onValueChange={(value) => updateFilter("market", value)}
          />
          <FilterStrip
            label="Book"
            options={bookFilters}
            value={filters.book}
            onValueChange={(value) => updateFilter("book", value)}
          />
        </div>
        <label className="sort-control">
          <span className="micro-label">Sort</span>
          <select value={filters.sort} onChange={(event) => updateFilter("sort", event.target.value)}>
            {SORT_OPTIONS.map((option) => (
              <option key={option} value={option}>
                {option}
              </option>
            ))}
          </select>
        </label>
      </section>

      {featured ? (
        <section className="featured-slate" aria-label="Featured top edge">
          <div className="section-heading">
            <div>
              <p className="micro-label">Best available</p>
              <h2>Top edge on the board</h2>
            </div>
            <span className="tabular">{totalLabel}</span>
          </div>
          {renderPlayerCard(featured, {
            featured: true,
            selected: selectedKey === propKey(featured),
            onSelect: () => selectItem(featured),
          })}
        </section>
      ) : null}

      <section className="slate-list-section" aria-label="Prop slate">
        <div className="section-heading">
          <div>
            <p className="micro-label">Slate</p>
            <h2>Props by game context</h2>
          </div>
          <span className="tabular">{totalLabel}</span>
        </div>

        {propsError ? (
          <div className="dashboard-state error-state">
            Unable to load live props: {propsError instanceof Error ? propsError.message : "unknown error"}
          </div>
        ) : propsLoading || !startup.snapshot?.completed ? (
          <SkeletonSlate />
        ) : slateRows.length === 0 ? (
          <EmptySlate scheduledGames={boardAvailability?.scheduled_games ?? 0} boardDate={boardSummary?.board_date ?? null} />
        ) : (
          <div ref={listParentRef} className="virtual-list">
            <div className="virtual-spacer" style={{ height: rowVirtualizer.getTotalSize() }}>
              {rowVirtualizer.getVirtualItems().map((virtualRow) => {
                const row = slateRows[virtualRow.index];
                return (
                  <div
                    key={row.key}
                    className="virtual-row"
                    style={{
                      height: virtualRow.size,
                      transform: `translateY(${virtualRow.start}px)`,
                    }}
                  >
                    {row.kind === "game" ? (
                      <GameHeader row={row} />
                    ) : (
                      renderPlayerCard(row.item, {
                        selected: selectedKey === row.key,
                        onSelect: () => selectItem(row.item),
                      })
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        )}
      </section>

      <div className="dashboard-footnote">
        <span>API {health?.ok ? "connected" : "pending"}</span>
        {health?.version ? <span className="tabular">v{health.version}</span> : null}
        {startup.errorMessage ? <span className="error">Startup stream degraded</span> : null}
      </div>

      <DecisionDrawer
        open={Boolean(selectedKey)}
        item={selectedItem}
        onOpenChange={handleDrawerOpen}
        onSendToTrading={handleSendToTrading}
        sendState={sendState}
        sendMessage={sendMessage}
      />
    </div>
  );
}

function renderPlayerCard(
  item: PropWithInsight,
  options: { featured?: boolean; selected: boolean; onSelect: () => void },
) {
  const opportunity = item.opportunity;
  const insight = item.insight;
  return (
    <PlayerCard
      playerName={opportunity.player_name}
      team={opportunity.player_team_abbreviation ?? "Team --"}
      position={opportunity.player_position ?? "UTIL"}
      marketLabel={marketLabel(opportunity.market_key)}
      lineLabel={lineLabel(opportunity)}
      edge={insight.edge * 100}
      confidence={insight.confidence_score}
      injuryStatus={injuryStatus(insight.injury_label)}
      fetchedAt={insight.best_quote.timestamp}
      avatarLabel={opportunity.player_icon}
      bookLabel={insight.best_quote.sportsbook_name}
      variant={options.featured ? "featured" : "default"}
      selected={options.selected}
      onClick={options.onSelect}
      volatilityTier={opportunity.volatility_tier}
      volatilityCoefficient={opportunity.volatility_coefficient}
    />
  );
}

function GameHeader({ row }: { row: Extract<SlateRow, { kind: "game" }> }) {
  return (
    <div className="game-context-header">
      <div>
        <strong>{row.label}</strong>
        {row.timeLabel ? <span>{row.timeLabel}</span> : null}
      </div>
      <span className="tabular">{row.count} props</span>
    </div>
  );
}

function SkeletonSlate() {
  return (
    <div className="slate-skeleton-stack" aria-label="Loading props">
      {Array.from({ length: 5 }, (_, index) => (
        <div key={index} className="slate-skeleton-card" />
      ))}
    </div>
  );
}

function EmptySlate({ scheduledGames, boardDate }: { scheduledGames: number; boardDate: string | null }) {
  const title = scheduledGames === 0 ? "No games today" : "No props match these filters";
  const detail =
    scheduledGames === 0
      ? "Refresh will move to the next available board once the schedule feed reports one."
      : "Loosen the confidence, market, or book filters to bring more opportunities back into view.";
  return (
    <div className="dashboard-state">
      <h3>{title}</h3>
      <p>{boardDate ? `Board date ${boardDate}. ` : null}{detail}</p>
    </div>
  );
}

function buildSlateRows(items: PropWithInsight[]): SlateRow[] {
  const groups = new Map<string, PropWithInsight[]>();
  for (const item of items) {
    const gameKey = String(item.opportunity.game_id);
    const group = groups.get(gameKey) ?? [];
    group.push(item);
    groups.set(gameKey, group);
  }

  return Array.from(groups.entries()).flatMap(([gameId, entries]) => {
    const first = entries[0].opportunity;
    const header: SlateRow = {
      kind: "game",
      key: `game:${gameId}`,
      label: first.game_label ?? `Game ${gameId}`,
      timeLabel: formatGameTime(first.game_start_time),
      count: entries.length,
    };
    return [header, ...entries.map((item) => ({ kind: "prop" as const, key: propKey(item), item }))];
  });
}

function buildBookFilters(items: PropWithInsight[], selectedBook: string): FilterOption[] {
  const names = new Set<string>();
  for (const item of items) {
    names.add(item.insight.best_quote.sportsbook_name);
  }
  if (selectedBook !== "All") {
    names.add(selectedBook);
  }
  return [
    { value: "All", label: "All" },
    ...Array.from(names)
      .sort((left, right) => left.localeCompare(right))
      .map((name) => ({ value: name, label: name })),
  ];
}

function pickFeatured(items: PropWithInsight[]): PropWithInsight | null {
  if (items.length === 0) {
    return null;
  }
  return [...items].sort((left, right) => {
    const edgeDelta = right.insight.edge - left.insight.edge;
    if (edgeDelta !== 0) {
      return edgeDelta;
    }
    return right.insight.expected_profit_per_unit - left.insight.expected_profit_per_unit;
  })[0];
}

function propKey(item: PropWithInsight): string {
  const opportunity = item.opportunity;
  return [
    opportunity.game_id,
    opportunity.player_id,
    opportunity.market_key,
    opportunity.consensus_line,
    item.insight.best_quote.sportsbook_key,
  ].join(":");
}

function lineLabel(opportunity: PropWithInsight["opportunity"]): string {
  const side = opportunity.recommended_side.toUpperCase().startsWith("UNDER") ? "U" : "O";
  return `${side} ${formatLine(opportunity.consensus_line)}`;
}

function marketLabel(marketKey: string): string {
  return marketKey
    .split("_")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function injuryStatus(label: string): "Q" | "D" | "O" | "Healthy" {
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

function formatLine(value: number): string {
  return Number.isInteger(value) ? value.toFixed(0) : value.toFixed(1);
}

function formatGameTime(timestamp: string | null): string | null {
  if (!timestamp) {
    return null;
  }
  const parsed = new Date(timestamp);
  if (!Number.isFinite(parsed.getTime())) {
    return null;
  }
  return new Intl.DateTimeFormat("en-US", {
    hour: "numeric",
    minute: "2-digit",
    timeZone: "America/New_York",
    timeZoneName: "short",
  }).format(parsed);
}

function formatRelativeAge(timestamp: string): string {
  const parsed = new Date(timestamp);
  const deltaMs = Date.now() - parsed.getTime();
  if (!Number.isFinite(deltaMs) || deltaMs < 0) {
    return "--";
  }
  const minute = 60_000;
  const hour = 60 * minute;
  const day = 24 * hour;

  if (deltaMs < minute) {
    return "just now";
  }
  if (deltaMs < hour) {
    return `${Math.floor(deltaMs / minute)}m ago`;
  }
  if (deltaMs < day) {
    return `${Math.floor(deltaMs / hour)}h ago`;
  }
  return `${Math.floor(deltaMs / day)}d ago`;
}

function readFiltersFromUrl(): DashboardFilters {
  if (typeof window === "undefined") {
    return defaultFilters();
  }
  const params = new URLSearchParams(window.location.search);
  return {
    confidence: normalizeOption(params.get("confidence"), CONFIDENCE_FILTERS, "All"),
    market: normalizeOption(params.get("market"), MARKET_FILTERS, "All"),
    book: params.get("book") || "All",
    sort: SORT_OPTIONS.includes(params.get("sort") || "") ? params.get("sort") || "Best Edge" : "Best Edge",
  };
}

function writeFiltersToUrl(filters: DashboardFilters) {
  if (typeof window === "undefined") {
    return;
  }
  const params = new URLSearchParams(window.location.search);
  setParam(params, "confidence", filters.confidence, "All");
  setParam(params, "market", filters.market, "All");
  setParam(params, "book", filters.book, "All");
  setParam(params, "sort", filters.sort, "Best Edge");
  const next = params.toString();
  const nextUrl = next ? `${window.location.pathname}?${next}` : window.location.pathname;
  window.history.replaceState(null, "", nextUrl);
}

function defaultFilters(): DashboardFilters {
  return {
    confidence: "All",
    market: "All",
    book: "All",
    sort: "Best Edge",
  };
}

function normalizeOption(value: string | null, options: FilterOption[], fallback: string): string {
  if (!value) {
    return fallback;
  }
  return options.some((option) => option.value === value) ? value : fallback;
}

function setParam(params: URLSearchParams, key: string, value: string, fallback: string) {
  if (value === fallback) {
    params.delete(key);
  } else {
    params.set(key, value);
  }
}

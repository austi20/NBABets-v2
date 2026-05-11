# Trading Tab Redesign — Terminal Aesthetic + Live Data + Pick Curation

**Date:** 2026-05-11
**Status:** draft — awaiting user spec review
**Worktree:** `.claude/worktrees/stoic-proskuriakova-0f9c71`
**Author:** brainstorming session
**Implementation method:** subagent-driven-development (see §15)

---

## 1. Context & motivation

The current Trading tab is functional but reads like an AI-generated dashboard: many panels, vague labels, every refresh interval blinks a skeleton, only one prop ever ends up in the auto-bet loop, and there is no way to adjust the budget, exclude a prop, or see what the engine will actually do before it does it.

The user wants a tab they can sit in front of every game night that:

1. Looks and feels like a real trading terminal (Kalshi/Bloomberg dense-data style), not a marketing dashboard.
2. Shows the full list of available props with hit probability, edge, allocated stake, and estimated profit — all on one row each.
3. Lets the user toggle props in or out of the auto-bet loop with one click.
4. Lets the user adjust the daily budget inline; initial budget seeds from the Kalshi wallet balance on app startup.
5. Allocates the budget proportional to model probability, with a soft 35% per-prop ceiling.
6. Updates **live** — every Kalshi price tick reflows the edges/allocations/estimated profits without flashing skeletons or reloading the page.
7. Surfaces blockers clearly (per-prop tooltips on the red `⊘` bullet, plus a unified event log at the bottom).

This design is the umbrella for the redesign. It also lays the groundwork for porting the same terminal aesthetic to the rest of the app (homepage, insights, parlays, players, settings) in follow-up sub-projects.

---

## 2. Goals & non-goals

### Goals

- Replace the existing Trading tab (`desktop_tauri/src/routes/trading.tsx`) with a redesigned page that follows the locked-in visual identity (§4) and layout (§5).
- Add live updates via a single SSE stream (§7) backed by the existing `KalshiMarketService` / `MarketBook` infrastructure.
- Add pick curation: per-prop bullet toggles, persistent selections, bulk actions, threshold filters (§6).
- Add budget management: wallet-init on startup, inline-edit on toolbar, Limits modal for full configuration, soft 35% per-prop ceiling (§9).
- Add a sticky bet-slip sidebar to the right of the picks table (§10).
- Add a terminal-style event log strip at the bottom of the page (§5).
- Extract the allocation algorithm into a shared helper used by both the live snapshot builder and the loop runner (§11).
- Replace the existing `DecisionBrainPanel`, `ReadinessPanel`, `TradingLoopPanel` components — they merge into the new design.

### Non-goals

- Live injury feed widget — adjacent work, spec'd separately (§16).
- Porting the terminal aesthetic to other tabs — deferred to follow-up sub-projects (§17).
- Replacing the existing brain → trading wiring direction (still write-only per project memory).
- Changing the underlying brain logic, scoring, or candidate generation.
- Sportsbook odds collection cadence — leave the existing provider system as-is, only document the refresh interval (§7).

---

## 3. User decisions captured (from brainstorming)

| Decision | Choice |
|---|---|
| Visual direction | A — Dark data terminal (Kalshi/Bloomberg vibe) |
| Layout structure | A — Stacked sections, top-to-bottom |
| Prop selector pattern | Bulleted list with interactive bullets (●/○/⊘) |
| Prop row density | A — Single line per prop |
| Top of page | B — Two strips (4 KPI tiles + control bar below) |
| Sections kept below picks | All seven — auto-collapse rare ones, errors stream to event log |
| Budget control | D — Inline edit on toolbar + Limits modal |
| Picks list controls | 1 sort, 2 summary, 3 bulk, 4 filter pills, 6 thresholds, 7 expand (no search) |
| Default selection state | A — All hittable selected by default (opt-out) |
| Per-prop cap | Soft 35% of budget |
| Allocation recompute | Live, as user toggles bullets |
| Companion panel | Sticky bet-slip sidebar (~30% width) right of picks table |
| Live data approach | Single SSE stream; per-cell reactivity; no skeletons after first paint |
| Brain re-sync | Auto every 5 min in live mode (configurable) |
| Sportsbook odds | 10 min refresh cadence (configurable) |
| Live injury widget | Out of scope; flagged for follow-up |

---

## 4. Visual identity & design tokens

The terminal aesthetic uses a new set of CSS custom properties scoped to the trading page (`.trading-page` root). These tokens are added to `desktop_tauri/src/theme.css`. They do **not** replace the existing app-wide tokens; the design is opt-in per page during the rollout.

```css
:root {
  --trading-bg: #0a0d14;
  --trading-surface: #111827;
  --trading-surface-alt: #0f1421;
  --trading-border: #1f2937;
  --trading-border-soft: #374151;

  --trading-fg: #e5e7eb;
  --trading-fg-muted: #9ca3af;
  --trading-fg-subtle: #6b7280;

  --trading-accent-pnl: #2ecc71;     /* P&L tile, positive numbers, included bullets when teal feels too cool */
  --trading-accent-budget: #3b82f6;  /* Budget tile, edit affordances */
  --trading-accent-picks: #00d4aa;   /* Picks tile, included bullets, primary action button */
  --trading-accent-system: #f59e0b;  /* System status tile */
  --trading-accent-danger: #ef4444;  /* Kill switch, blockers, errors */

  --trading-font-mono: ui-monospace, "Cascadia Code", "JetBrains Mono", Menlo, monospace;
  --trading-font-sans: inherit; /* uses existing app sans */

  --trading-pad-sm: 8px;
  --trading-pad-md: 12px;
  --trading-pad-lg: 14px;

  --trading-pulse-positive: rgba(46, 204, 113, 0.25);
  --trading-pulse-negative: rgba(239, 68, 68, 0.25);
}
```

### Typography rules

- All numeric columns and IDs use `--trading-font-mono`.
- Section titles and prop names use `--trading-font-sans` at 13–14px, weight 700.
- Micro-labels (column headers, tile titles) use 8–9px, uppercase, letter-spacing 1.5px, color `--trading-fg-subtle`.
- Body data text is 10–11px, color `--trading-fg`.
- Tertiary detail (game context, timestamps) is 9px, color `--trading-fg-subtle`.

### Value-pulse animation

When any numeric field changes, it briefly flashes its background color (250ms ease-out fade). Direction-aware: green for increases, red for decreases. Implemented as a React hook `usePulseOnChange(value)` that toggles a CSS class.

---

## 5. Layout

Single-column, top-to-bottom stack. No fixed header, no left navigation (uses the existing app's `NavRail`). Page width inherits from the existing app shell (no max-width override).

```
┌──────────────────────────────────────────────────────────────────────────┐
│ KPI TILE STRIP — 4 tiles, equal width                                    │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐                  │
│  │ P&L      │  │ Budget   │  │ Picks    │  │ System   │                  │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘                  │
├──────────────────────────────────────────────────────────────────────────┤
│ CONTROL BAR — mode toggle · Limits gear · Start Auto-Bet · Kill Switch   │
├──────────────────────────────────────────────────────────────────────────┤
│ PICKS SECTION — two-column row (70% / 30%)                               │
│  ┌──────────────────────────────────┐  ┌────────────────────────────┐    │
│  │ Picks table                      │  │ Bet-slip sidebar (sticky)  │    │
│  │  ─ header summary line           │  │  ─ selected picks list     │    │
│  │  ─ filter pills                  │  │  ─ totals block            │    │
│  │  ─ thresholds (collapsible)      │  │                            │    │
│  │  ─ sortable column header        │  │                            │    │
│  │  ─ single-line rows (●/○/⊘)      │  │                            │    │
│  │  ─ click row to expand           │  │                            │    │
│  └──────────────────────────────────┘  └────────────────────────────┘    │
├──────────────────────────────────────────────────────────────────────────┤
│ OPEN POSITIONS — table, always visible                                   │
│ RECENT FILLS — feed, always visible                                      │
│ P&L TREND — sparkline chart, always visible                              │
├──────────────────────────────────────────────────────────────────────────┤
│ COLLAPSED SECTIONS — one-line headers, click to expand                   │
│  ▸ Resting Orders · auto-hidden when empty                               │
│  ▸ Live Kalshi Quotes · expand to view                                   │
│  ▸ System Diagnostics · expand to debug                                  │
├──────────────────────────────────────────────────────────────────────────┤
│ EVENT LOG STRIP — black background, ~100px, color-coded lines            │
│  [20:14:03] info  Filled CUNN PTS o22.5 · BUY YES @ 0.43                 │
│  [20:13:47] warn  Hardaway 3PT blocked: spread 0.12 > 0.08               │
└──────────────────────────────────────────────────────────────────────────┘
```

### KPI tiles

Each tile is a `--trading-surface` card with a 2px colored left border (one accent per tile). Contents:

- **Daily P&L** — large number, mini progress bar showing % of `daily_loss_cap` consumed, sub-line showing realized + unrealized split.
- **Budget** — `$used / $total` with inline-edit affordance ("edit" link in top-right), mini progress bar showing allocated %, sub-line showing free remaining.
- **Picks** — `N selected of M total`, sub-line showing estimated total profit + counts of excluded/blocked.
- **System** — Status pill (READY/BLOCKED), sub-line summarizing mode + gate count + WS status.

### Control bar

Single horizontal `--trading-surface` strip. Left side: mode toggle (`● Live` / `○ Watch`) — clicking either button immediately fires `tradingBrainSync` with that mode (this is the wire-up fix carried forward from earlier in the session). `⚙ Limits` button opens the limits modal. Right side: primary `▶ Start Auto-Bet (N picks · $X.XX)` button, `⏻ Kill Switch` button. Both right-side buttons show contextual labels.

### Below picks (always-visible sections)

- **Open Positions** — table, two sub-tables: "Tracked in this app (ledger)" and "Held on Kalshi (exchange)". Column treatment matches the picks table.
- **Recent Fills** — vertical feed of fill rows. New fills flash on arrival.
- **P&L Trend** — Recharts line chart, ~60–80px tall. Single line for cumulative P&L by fill.

### Below picks (auto-collapsed sections)

One-line headers with caret indicator. Click to expand. Items:

- **Resting Orders** — hidden entirely when count is 0; otherwise collapsed by default.
- **Live Kalshi Quotes** — collapsed by default; expand shows the bid/ask/spread table for every tracked ticker.
- **System Diagnostics** — collapsed by default; expand shows the full gate checklist + brain checks (replaces the existing `ReadinessPanel` content).

### Event log strip

Black background (`#000`), bordered with `--trading-border`. Lines are 10px monospace, color-coded:

- `--trading-fg-subtle` for `info`
- `--trading-accent-system` for `warn`
- `--trading-accent-danger` for `error`

Each line: `[HH:MM:SS] level  message`. Newest at the top. Bounded ring buffer (250 lines). Auto-scrolls when at the top; pauses auto-scroll when the user scrolls down.

---

## 6. Picks list behavior

### Default state

On load, every pick whose status is not `blocked` is `●` (selected). Blocked picks render as `⊘`. The user opts out by clicking bullets.

### Bullet semantics

| Glyph | Color | Meaning | Click behavior |
|---|---|---|---|
| `●` | `--trading-accent-picks` | Included — will be bet by the loop | Toggle to `○` |
| `○` | `--trading-fg-subtle` | Manually excluded | Toggle to `●` |
| `⊘` | `--trading-accent-danger` | Blocked by a gate (spread, market closed, unresolved symbol, etc.) | Not clickable; hover shows blocker reason tooltip |

### Sortable columns

Each column header (`Prop / Hit% / Edge / Alloc / Est. Profit / State`) is clickable. Default sort is the brain's rank score (highest first). Sort state is session-only (not persisted). Visual indicator: arrow next to the active sort column.

### Filter pills

Row of pills above the table: `All / Hittable / Excluded / Blocked`. Each pill shows the count. Clicking sets the filter; bullets still toggle correctly on the filtered subset.

### Bulk actions

Buttons in the section header (right side):

- **Select all hittable** — flips every `○` to `●`, skips `⊘`
- **Deselect all** — flips every `●` to `○`
- **Top 5** — keeps the 5 highest-edge bullets `●`, sets rest to `○`

### Threshold row (collapsible)

Click `▾ Thresholds` to reveal two number inputs:

- `min hit %` (0–100, integer)
- `min edge bps` (0–500, integer)

Any pick below either threshold is force-excluded (rendered as `○` with a small note). Thresholds persist globally to `data/trading_selections.json` under `thresholds`.

### Click-to-expand row

Clicking anywhere on a row (other than the bullet itself) expands a panel below it showing:

- Top features driving the model probability
- Projected distribution percentiles (p25 / p50 / p75)
- Live Kalshi quote breakdown (bid/ask/spread/timestamp)
- Full gate checklist with pass/fail per gate
- Manual "Place bet now" button (uses single-prop one-shot path — bypasses loop)

Only one row expanded at a time by default. Shift+click keeps multiple open.

### Header summary line

Below the section title, updates live as selections change:

```
12 picks available · 2 selected · $1.00 allocated · est. profit +$0.73
```

---

## 7. Live data architecture

### Single SSE stream

New endpoint `GET /api/trading/stream` emits server-sent events. Pattern matches the existing `app/server/routers/startup.py` SSE implementation. The stream publishes a `snapshot` event whenever:

1. The Kalshi `MarketBook` receives an update (debounced to ~1 Hz max via `asyncio.Event` + sleep window), OR
2. A periodic 2s tick fires (covers cases when WS is idle but other state changes), OR
3. A `trading_selections.json` write completes (immediate snapshot for responsive feel), OR
4. The brain auto-resync completes (new picks list available).

Snapshot payload (Pydantic model):

```python
class TradingLiveSnapshot(BaseModel):
    observed_at: datetime
    kpis: KpiTilesModel              # P&L, budget, picks, system
    control: ControlBarStateModel    # mode, loop state, can_start
    picks: list[PickRowModel]        # full picks list with live edge/alloc
    bet_slip: BetSlipModel           # selected picks + totals
    positions: list[LivePositionModel]
    fills: list[FillModel]           # last N
    quotes: list[TradingQuoteModel]
    resting_orders: list[RestingOrderModel]
    diagnostics: SystemDiagnosticsModel
    event_log: list[EventLogLineModel]  # delta-encoded, only new lines
    pnl_trend: list[PnlPointModel]
    errors: list[str]
```

The stream sends full snapshots, not deltas (simpler; payload is small enough at ~5–20 KB). Event log is delta-encoded by a monotonic cursor so the client doesn't accumulate duplicates.

### Fallback endpoint

`GET /api/trading/snapshot-live` returns the same `TradingLiveSnapshot` as a single shot. The frontend falls back to 2s polling on this endpoint if the SSE stream errors out.

### Brain auto-resync

A new `asyncio.Task` is created in the FastAPI `lifespan` alongside `KalshiMarketService`. Runs every `BRAIN_AUTO_RESYNC_SECONDS` (default 300) while the loop mode is `supervised-live`. Calls `sync_decision_brain(...)`. Errors get logged to the event-log ring buffer but don't crash the task. Manual "Refresh picks" button stays available and triggers an immediate resync.

### Sportsbook odds cadence

Document only — no code change in this design. The existing provider cache (`app/services/provider_cache.py`) already handles refresh. We surface the cadence as a new setting `SPORTSBOOK_REFRESH_SECONDS` (default 600) wired into the existing cache TTL. Brain auto-resync naturally picks up newer odds whenever it runs.

---

## 8. UI update mechanics

### State store

Replace `useQuery({ refetchInterval })` for trading data with a single SSE subscription that feeds a normalized store. Use either Zustand (already in the dependency list — verify with `npm ls zustand`; add if missing) or a hand-rolled `useReducer + Context`. Zustand is preferred for its selector subscriptions, which give automatic per-cell reactivity.

Store shape (normalized by entity ID):

```typescript
type TradingStore = {
  kpis: KpiTiles;
  controlState: ControlBarState;
  picks: Record<string, PickRow>;       // keyed by candidate_id
  pickOrder: string[];                  // sort order
  betSlip: BetSlip;
  positions: Record<string, LivePosition>;
  fills: Fill[];                        // bounded to 50
  quotes: Record<string, TradingQuote>; // keyed by ticker
  restingOrders: RestingOrder[];
  diagnostics: SystemDiagnostics;
  eventLog: EventLogLine[];             // bounded to 250
  pnlTrend: PnlPoint[];
  errors: string[];
  streamConnected: boolean;
  lastSnapshotAt: string | null;
};
```

### No skeletons after first paint

The store starts in a "loading" state that renders skeletons. After the first SSE snapshot lands, the loading state flips to `loaded` and never returns to `loading` — even if the stream disconnects.

When the stream errors out, the store sets `streamConnected = false` and a chip appears in the System tile. Numbers stay on screen at their last known values, slightly dimmed via opacity 0.85.

### Per-cell reactivity

Each numeric cell uses a Zustand selector like `useStore(s => s.picks[candidateId]?.edgeBps)`. Only cells whose selected slice changed re-render.

The pulse animation hook is also per-cell:

```typescript
function usePulseOnChange(value: number) {
  const prev = useRef(value);
  const [pulse, setPulse] = useState<"up" | "down" | null>(null);
  useEffect(() => {
    if (value > prev.current) setPulse("up");
    else if (value < prev.current) setPulse("down");
    prev.current = value;
    const t = setTimeout(() => setPulse(null), 250);
    return () => clearTimeout(t);
  }, [value]);
  return pulse; // null | "up" | "down"
}
```

### Disconnect chip

When `streamConnected` is `false`, the System tile renders a small `● disconnected · retrying` pill in its top-right corner. The SSE hook retries with exponential backoff (1s → 2s → 4s → 8s → 8s).

---

## 9. Budget & allocation logic

### Wallet-init on startup

During FastAPI `lifespan`, after `KalshiMarketService` starts and at least one successful WS frame has been seen (signaling Kalshi connectivity), the sidecar calls `KalshiClient.get_balance()`. If successful AND `config/trading_limits.json` either does not exist OR has `wallet_init_done_at` older than today's calendar date, the file is rewritten:

```json
{
  "max_open_notional": 10.11,
  "per_market_cap": 5.05,
  "daily_loss_cap": 2.00,
  "reject_cooldown_seconds": 300,
  "wallet_init_done_at": "2026-05-11T11:34:02Z"
}
```

Manual edits via the inline tile or Limits modal take precedence — the auto-init only runs once per day.

### Per-prop cap

`per_order_cap` is no longer a stored field. It's computed at read time as `max_open_notional * 0.35`. A `per_order_cap_override` field exists for power users who want to override the 35% rule from the Limits modal.

### Allocation algorithm

Lives in `app/trading/allocation.py` (new shared module, used by both the live snapshot builder and the loop runner):

```python
def allocate_proportional_with_soft_cap(
    selected_picks: list[Pick],
    budget: float,
    cap_fraction: float = 0.35,
    max_iterations: int = 3,
) -> dict[str, float]:
    """
    Returns {candidate_id: stake} for each pick in selected_picks.
    Raw allocation is proportional to pick.model_prob.
    Any single allocation > budget * cap_fraction is capped at that ceiling.
    Overflow gets redistributed to uncapped picks (also subject to the cap).
    Iterates up to max_iterations times to settle. Total may be < budget if
    all picks hit the cap (intentional safety property).
    """
```

Unit tests cover:

- Empty input → empty dict
- Single pick → `min(budget, budget * cap)`
- Two picks with equal probs → split, both below cap → 50/50
- Two picks with equal probs, both above cap → both capped, total < budget
- Three picks where one would be > cap → that one capped, other two get the overflow proportionally
- Convergence within `max_iterations`

### Inline budget edit

Clicking the "edit" link in the Budget KPI tile turns the value into a number input. Pressing Enter saves via `POST /api/trading/limits` with `{ max_open_notional: <value> }`. Escape cancels. Validation: must be `> 0` and `≤ wallet_balance * 2` (sanity ceiling). Save errors render as a toast at the top of the page.

### Limits modal

Triggered by `⚙ Limits` in the control bar. Form fields:

- `max_open_notional` (number, $) — saves to `config/trading_limits.json`
- `daily_loss_cap` (number, $)
- `reject_cooldown_seconds` (integer)
- `per_order_cap_override` (number, $; empty = use 35% default)
- "Refresh wallet balance" button — re-fetches Kalshi balance and offers to reset budget to that value

Save commits all fields atomically. The next snapshot streams reflect the new values.

---

## 10. Bet-slip sidebar

### Placement

Renders to the right of the picks table in a 70/30 grid row. Sticks to viewport top as the user scrolls the picks table.

### Contents

Header: `SELECTED PICKS · N` where N is the live count.

Each selected pick renders a compact card:

```
●  Cunningham PTS o22.5
   63% · +180bp
   $0.45  →  +$0.31
```

Then a totals block at the bottom:

```
Total stake     $1.00 of $2.52 cap
Est. profit     +$0.73
Unused budget   $1.52
```

### Behavior

- Cards animate in/out with a 200ms fade as bullets toggle.
- Total values get the pulse-on-change treatment.
- Empty state when 0 picks selected: `No picks selected · click bullets in the table to include`.

---

## 11. Backend changes — endpoints, services, files

### New endpoints (FastAPI)

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/trading/stream` | SSE — unified live trading snapshot |
| GET | `/api/trading/snapshot-live` | Single-shot of the same payload (SSE fallback) |
| POST | `/api/trading/picks/{candidate_id}/toggle` | Flip a single pick's selection |
| POST | `/api/trading/picks/bulk` | Bulk action: `{ action: "select_all_hittable" \| "deselect_all" \| "top_n", n?: 5 }` |
| POST | `/api/trading/thresholds` | Update `{ min_hit_pct, min_edge_bps }` |
| POST | `/api/trading/limits` | Update `{ max_open_notional?, daily_loss_cap?, reject_cooldown_seconds?, per_order_cap_override? }` |
| GET | `/api/trading/wallet` | Fetch fresh Kalshi balance |

### New services

`app/trading/allocation.py` — shared allocation helper (§9).

`app/trading/live_snapshot.py` — `TradingLiveSnapshotBuilder`. Pure function that takes `(decision_pack, market_book_snapshot, selections, limits, brain_state, ledger_state)` and returns a `TradingLiveSnapshot`. No I/O.

`app/trading/stream_publisher.py` — `TradingStreamPublisher`. Wraps SSE infrastructure. Subscribes to `MarketBook` updates. Maintains the event-log ring buffer. Yields snapshot events.

`app/trading/selections.py` — load/save/prune `data/trading_selections.json`. Thread-safe writes via `filelock.FileLock` (add to dependencies if not present; otherwise fall back to atomic `os.replace` of a temp file written in the same directory). Schema:

```json
{
  "thresholds": { "min_hit_pct": 0.55, "min_edge_bps": 50 },
  "selections": {
    "2026-05-11": {
      "<candidate_id>": true,
      "<candidate_id>": false
    }
  },
  "last_pruned_at": "2026-05-11T11:00:00Z"
}
```

Selections older than 7 days are pruned on save.

### Modified files

- `app/server/main.py` — register new routers, start `TradingStreamPublisher` and brain auto-resync task in `lifespan`.
- `app/server/routers/trading.py` — add new endpoints listed above.
- `app/server/schemas/trading.py` — add new Pydantic models (`TradingLiveSnapshot`, `KpiTilesModel`, `PickRowModel`, `BetSlipModel`, `EventLogLineModel`, etc.).
- `scripts/run_trading_loop.py` — filter loaded decisions through `trading_selections.json` before allocation; use `app/trading/allocation.py` for the per-prop cap math (already multi-prop from earlier session work).
- `app/config/settings.py` — add `BRAIN_AUTO_RESYNC_SECONDS`, `SPORTSBOOK_REFRESH_SECONDS`, `AUTO_INIT_BUDGET_FROM_WALLET`.

### Removed files / dead code

- `DecisionBrainPanel`, `ReadinessPanel`, `TradingLoopPanel` (React components in `trading.tsx`) — fully replaced.
- The old `useStartupSnapshot`-style polling for trading data — replaced by the new SSE subscription hook.

---

## 12. Frontend changes — component map

New components under `desktop_tauri/src/routes/trading/`:

```
trading/
  index.tsx                   — page entry, mounts the stream hook + composes sections
  store.ts                    — Zustand store + selectors
  hooks/
    useTradingStream.ts       — SSE subscription + reducer
    usePulseOnChange.ts       — value-flash animation hook
  components/
    KpiTileStrip.tsx
    KpiTile.tsx
    ControlBar.tsx
    LimitsModal.tsx
    PicksSection.tsx
    PicksTable.tsx
    PickRow.tsx
    PickRowExpansion.tsx
    FilterPills.tsx
    ThresholdsRow.tsx
    BulkActions.tsx
    BetSlipSidebar.tsx
    PositionsTable.tsx
    FillsFeed.tsx
    PnlTrendChart.tsx
    CollapsedSection.tsx
    EventLogStrip.tsx
  api/
    types.ts                  — TypeScript types matching backend Pydantic
    actions.ts                — toggle, bulk, thresholds, limits, wallet calls
```

The existing `desktop_tauri/src/routes/trading.tsx` becomes a thin re-export of `trading/index.tsx` (preserves the existing route registration).

### Styling

All new styles live in `desktop_tauri/src/styles/trading.css` (new file imported by `trading/index.tsx`). Existing `theme.css` only adds the new `--trading-*` tokens listed in §4. Avoid touching existing trading-related selectors in `theme.css` (they'll be removed only after the new components fully replace the old).

---

## 13. Persistent state & migrations

### `data/trading_selections.json`

Created on first toggle if missing. Schema in §11. No migration needed — empty file means "everything is selected by default".

### `config/trading_limits.json`

Existing file. New fields added:

- `wallet_init_done_at` (ISO datetime, optional) — set by wallet-init flow
- `per_order_cap_override` (number, optional)

Migration: on first read, if `wallet_init_done_at` is missing, treat as "never initialized" and run the wallet-init flow.

### Event log persistence

In-memory only for v1. Bounded ring buffer of 250 lines lives inside `TradingStreamPublisher`. Lost on sidecar restart — acceptable because the log is meant for "what happened just now", not historical audit. If we later want persistence, it lands as `data/event_log.jsonl` in a follow-up.

---

## 14. Testing strategy

### Unit tests (Python)

- `tests/unit/trading/test_allocation.py` — 6+ scenarios for `allocate_proportional_with_soft_cap`
- `tests/unit/trading/test_live_snapshot.py` — builder with mocked `MarketBook`, varied selections, varied gates
- `tests/unit/trading/test_selections.py` — load/save/prune cycles, concurrent write safety
- `tests/unit/trading/test_wallet_init.py` — wallet-init flow with mocked `KalshiClient.get_balance`; skips re-init when same-day timestamp

### Integration tests (Python)

- `tests/integration/server/test_trading_stream.py` — start sidecar, push mock `MarketBook` updates, assert SSE stream emits valid `TradingLiveSnapshot` events
- `tests/integration/server/test_trading_picks_toggle.py` — POST toggle, verify next snapshot reflects the change
- `tests/integration/server/test_trading_limits.py` — POST limits update, verify file write + next snapshot reflects values

### Frontend tests

- Component tests for `PickRow` (bullet state changes, sort interaction, expand)
- Component tests for `BetSlipSidebar` (renders selected picks, totals update, empty state)
- Hook test for `useTradingStream` (handles snapshot, handles error, reconnect logic)

### Manual / E2E

- Open the app, verify wallet-init shows correct balance in Budget tile
- Toggle 3 picks off, verify allocation re-flows live in the bet slip
- Click "Top 5", verify it picks the 5 highest-edge bullets
- Disconnect Kalshi WS (simulated), verify disconnect chip appears, numbers stay on screen
- Trigger an error (e.g. invalid limits POST), verify it appears in the event log strip

---

## 15. Subagent execution guidance

Implementation uses the **subagent-driven-development** skill (per user direction). This section captures the conventions for those subagent runs.

### Agent roster

| Role | Subagent type | Model | Purpose |
|---|---|---|---|
| Architect | `architect` | `claude-opus-4-7` | Cross-cutting design questions, allocation algorithm validation, SSE flow review |
| Planner | `planner` | `claude-sonnet-4-6` | Break each phase into step lists with file paths |
| Backend implementer | `general-purpose` (then `python-reviewer` reviews the diff) | `claude-sonnet-4-6` | New services, endpoints, schemas |
| Frontend implementer | `general-purpose` (then `typescript-reviewer` reviews the diff) | `claude-sonnet-4-6` | New React components, Zustand store, SSE hook |
| Test author | `tdd-guide` | `claude-sonnet-4-6` | Write unit + integration tests, enforce red-first |
| Reviewer (Python) | `python-reviewer` | `claude-sonnet-4-6` | Reviews every Python diff before commit |
| Reviewer (TS) | `typescript-reviewer` | `claude-sonnet-4-6` | Reviews every TS/TSX diff before commit |
| Security reviewer | `security-reviewer` | `claude-sonnet-4-6` | Wallet balance fetch, limits endpoint input validation, SSE auth |
| Build resolver | `python-reviewer` or `build-error-resolver` | `claude-haiku-4-5` | First-pass diagnosis of test/build failures (cheap, fast) |
| Final review | `code-reviewer` | `claude-opus-4-7` | End-of-phase holistic review before marking phase done |

Rationale: Sonnet 4.6 is the implementation workhorse. Haiku 4.5 handles first-pass build diagnoses (cheap and frequent). Opus 4.7 reserved for the architect role (rare, expensive, high-leverage) and final phase reviews.

### Gates each phase must pass

Every phase ends with a green check on all of:

1. `pytest` on the touched modules (narrow tests, not full suite)
2. `ruff check .` clean
3. `mypy app` clean
4. `npx tsc --noEmit` clean on the frontend
5. `npm run build --prefix desktop_tauri` succeeds
6. The `python-reviewer` and `typescript-reviewer` agents both report no CRITICAL or HIGH findings on the diff
7. For phases touching wallet, limits, or SSE: `security-reviewer` reports no CRITICAL findings
8. Manual smoke check via `npm run tauri:dev` — relevant interaction works without console errors

If any gate fails, the implementer subagent fixes the root cause (not bypasses) and the gate re-runs.

### Context to inject into every subagent prompt

Subagents start cold. The implementing parent must include this context in every spawn:

- **Project type:** Tauri 2 desktop app + React 18 + FastAPI 0.111 sidecar. Single user, local-only. Python 3.11. Node 24.
- **Data source:** BallDontLie GOAT API (key in `.env`). No cloud backend.
- **Kalshi WS already wired:** `app/trading/ws_consumer.py`, `app/trading/ws_service.py`, `app/trading/market_book.py`. WS service is started in FastAPI `lifespan` if `KALSHI_WS_ENABLED=true`.
- **Brain → trading is write-only currently:** `vault_bridge.py` writes notes. The loop reads decisions from `app/evaluation/prop_decision.py`. The new design does not change this direction.
- **Multi-prop loop fix already landed:** `scripts/run_trading_loop.py` already loads all executable rows and allocates proportionally — see the earlier session in this worktree for the diff.
- **Existing SSE pattern to copy:** `app/server/routers/startup.py` (`startup_stream`).
- **Existing settings layout:** `app/config/settings.py` uses Pydantic Settings; add new fields there.
- **CSS lives in:** `desktop_tauri/src/theme.css` (existing tokens) and the new `desktop_tauri/src/styles/trading.css` (new design).
- **Test layout:** `tests/unit/<area>/test_*.py` for unit, `tests/integration/<area>/test_*.py` for integration.
- **Allowed commands (no permission prompts):** `pytest`, `ruff check`, `mypy app`, `npx tsc --noEmit`, `npm run build --prefix desktop_tauri`.

### Token-consumption discipline

- Subagents must use `Grep` before `Read`. Never `Read` whole large files (`theme.css` 1842 lines, `trading.tsx` 800+ lines) without first grep-narrowing the section.
- Subagents must not re-read files they already read in the same turn.
- Self-contained prompts only — no "see the conversation above" references (subagents have no context).
- Cap parallel subagent spawns at 3 per phase. Sequential is fine when dependent.
- Prefer narrow validation (`pytest tests/unit/trading/test_allocation.py`) over full-suite runs (`pytest`) when a single module is in flight.

### Phase decomposition (high-level)

The writing-plans skill expands these into step lists. Each phase ends at a green-gate commit.

1. **Phase 1 — Allocation helper + selections store** (backend-only, no UI changes)
2. **Phase 2 — Live snapshot builder + SSE stream endpoint** (backend, mock data)
3. **Phase 3 — Wallet-init + Limits modal endpoints** (backend)
4. **Phase 4 — Brain auto-resync background task** (backend)
5. **Phase 5 — New CSS tokens + design system primitives** (frontend, no behavior)
6. **Phase 6 — Zustand store + `useTradingStream` hook** (frontend, no UI yet)
7. **Phase 7 — KPI tiles + Control bar** (frontend)
8. **Phase 8 — Picks table + bullet interactions + bet slip** (frontend)
9. **Phase 9 — Below-picks sections + event log strip** (frontend)
10. **Phase 10 — Limits modal + inline budget edit** (frontend)
11. **Phase 11 — Wire-up + integration test + replace old `trading.tsx`** (cutover)
12. **Phase 12 — E2E smoke + cleanup of dead code** (close-out)

---

## 16. Adjacent work (out of scope)

### Live injury feed widget

Belongs in the app shell or homepage, not the trading tab. Existing `app/services/insights.py` already pulls BallDontLie injury data; what's missing is:

- A new endpoint exposing the most recent N injury notes as a stream
- A small widget component (probably in `AppShell.tsx`) that subscribes to the stream and renders a horizontal ticker or a dropdown

Spec'd separately. Captured here so it doesn't get lost.

---

## 17. Future — extending the design system to other tabs

After the trading tab redesign lands and stabilizes, the same terminal aesthetic should be ported to:

| Tab | File | Effort |
|---|---|---|
| Homepage / board | `desktop_tauri/src/routes/index.tsx` | medium — most data-heavy |
| Insights | `desktop_tauri/src/routes/insights.tsx` | medium |
| Parlays | `desktop_tauri/src/routes/parlays.tsx` | medium |
| Players | `desktop_tauri/src/routes/players.tsx` | small |
| Settings | `desktop_tauri/src/routes/settings.tsx` | small |

Each becomes its own sub-project with its own spec. They all reuse the design tokens from §4 and (where applicable) the SSE stream + Zustand store patterns from §7–8.

The brain template `04 Workflow and Systems/Project Templates/terminal-design-system.md` (created as part of this work — see §18) captures the methodology so future redesigns can follow it without re-deriving.

---

## 18. Brain template (to be created)

A reference note saved to the brain at `E:/AI Brain/ClaudeBrain/04 Workflow and Systems/Project Templates/terminal-design-system.md`. Captures:

- Visual identity tokens (the same `--trading-*` table from §4)
- Layout primitives (KPI tile strip, control bar, bet-slip sidebar pattern)
- Live data pattern (SSE + Zustand + per-cell selectors + pulse-on-change)
- Allocation algorithm template
- Subagent execution conventions (model selection, gate list, context injection)
- Decision criteria for "is this tab a candidate for the terminal aesthetic?"

This note gets created alongside Phase 1 of implementation so the methodology is captured before drift starts.

---

## 19. Risks & open questions

1. **SSE through the existing `AppTokenMiddleware`.** The middleware protects `/api/trading/*`. The SSE stream must include the app token. Frontend `EventSource` doesn't support headers, so the token goes in the query string (`?token=...`). Mitigation: validate on connect, log token use; security-reviewer flagged item for Phase 2.
2. **Zustand may not be installed.** Verify in Phase 5; add via `npm install zustand --prefix desktop_tauri` if needed. If the team wants to avoid the dependency, fall back to a `useReducer + Context` store; the design accommodates both.
3. **`MarketBook` update rate.** If WS chatter is too fast, the 1 Hz debounce in `TradingStreamPublisher` is the safety valve. Configurable via `TRADING_STREAM_MAX_HZ` setting.
4. **`config/trading_limits.json` writes race.** Inline edit + Limits modal + wallet-init all write to the same file. Use `filelock` (existing dependency? verify) or a simple lock file pattern. Tested in `test_selections.py`.
5. **Selections during board rollover.** If the user is on the page at midnight, selections for the old `board_date` become irrelevant. New picks load with default opt-out. Existing UI doesn't need special handling — the snapshot just shows the new picks.

---

## 20. Acceptance criteria

This design is complete when implementation passes all of:

- [ ] All gates in §15 green on every phase
- [ ] Manual smoke checks in §14 pass without console errors
- [ ] The old `DecisionBrainPanel`, `ReadinessPanel`, `TradingLoopPanel` are deleted from the codebase
- [ ] Brain template note created at the path in §18
- [ ] No new `useQuery({ refetchInterval })` for trading data — all live data flows through `useTradingStream`
- [ ] Allocation algorithm has unit test coverage for all 6+ scenarios in §9
- [ ] Wallet-init flow works end-to-end with a real Kalshi balance call
- [ ] `npm run build --prefix desktop_tauri` succeeds with zero warnings on the new files

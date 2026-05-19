# Profit Machine — Active Trading Loop for Kalshi (v2)

## 1. Strategy, restated after operator clarification

**Portfolio mode**, not single-position rotation. Buy YES on every prop the model rates highly, subject to filters; manage each position independently with a soft bracket that *the model can override*.

### Entry rule

For every Kalshi-resolved candidate in the latest decision pack:

- `model_prob >= min_model_prob` (default 0.60)
- `yes_ask <= max_entry_price` ($0.80 default — leaves ≥25% upside to settlement)
- `(yes_ask - yes_bid) <= spread_max_cents` (6¢ default — don't pay through a wide spread)
- Realized-volatility filter (see §3.4) passes, **unless** `model_prob >= volatility_override` (default 0.80 — high-conviction picks bypass the volatility veto)
- Not currently held (no double-down)
- Sufficient open-notional headroom against `max_open_notional` and `per_market_cap`

Stake sized per candidate via existing `allocate_proportional_with_soft_cap` keyed on `model_prob`. Multiple positions held concurrently; cap is your existing `max_open_notional`, which already scales with wallet balance via `wallet_init.py`.

### Exit rule (per position, evaluated on every tick)

```
soft_tp_price  = entry_price * 1.50   # +50%
soft_sl_price  = entry_price * 0.50   # -50%
high_conviction = model_prob >= conviction_hold     # default 0.85

if minutes_to_settle <= settle_hold_minutes (15 by default):
    if on_pace_to_hit(position):                    # live stats vs line
        hold                                         # let it settle for full payout
    else:
        sell at bid                                  # take what's left
elif yes_bid >= soft_tp_price:
    if high_conviction: hold                         # ride to settlement
    else: sell at bid (take profit)
elif yes_bid <= soft_sl_price:
    if high_conviction: hold                         # model still believes; don't fold
    else: sell at bid (stop loss)
else:
    hold
```

Both TP and SL are **soft** — they trigger a *decision*, not an automatic exit. The model's confidence is the override. This matches your "let it ride if model is really confident" rule.

### Kill switch = liquidate

When the operator engages the kill switch, every open position is sold at bid immediately (one write per ticker). Confirmed via `set_kill_switch(killed=True, flatten=True)` — the existing `set_kill_switch` signature gets a `flatten` flag.

### Volatility veto

For each candidate ticker, the trader maintains a rolling window of the last 60 seconds of `yes_bid` updates (delivered by the WS). If `stdev(window) > volatility_max_cents` (default 5¢), the candidate is skipped **unless** `model_prob >= volatility_override` (default 0.80). This handles your "threes market swings hard" concern: the model has to be very confident before we'll enter a thrashing book.

Existing positions are **not** force-closed by volatility — only entries are gated.

### Live-stats edge (most valuable, also highest-risk to build)

A separate live-stats poller hits BallDontLie GOAT `/stats?player_ids[]=...&seasons[]=2026` every N seconds during games-in-progress and exposes a `LivePlayerStats` view to the trader. Two uses:

1. **`on_pace_to_hit(position)`** — at the 15-min settle decision, compares the player's current stat line against `line_value`. Pace is judged with a simple linear model: `projected = current + remaining_minutes * recent_rate_per_minute`. If `projected >= line_value` (when the position is YES-OVER), we hold; otherwise we sell.
2. **Pre-emptive entry signal** — when a player crosses an internal "on-track" threshold (say, 75% of line value with >30% of game remaining) and the Kalshi quote hasn't yet adjusted, the trader can submit a buy *before* the market reprices. This is the "pounce before market responds" mechanic you asked for. Implementing this well needs a measured baseline of BDL update latency vs Kalshi quote latency; I'll instrument that during Phase 1 dry-run.

BallDontLie GOAT documents ~600 req/min capacity. With 10 active games × 1 poll/15s = 40 req/min — comfortable. Existing `app/providers/stats/balldontlie.py` already has a rolling-window limiter at line 53.

## 2. What already exists, reused as-is

| Layer | File | Role |
|---|---|---|
| WS market service | `app/trading/ws_service.py` | Streams Kalshi quotes into MarketBook. Supports multiple tickers on one socket (within the 5-conn limit, we only need 1). |
| In-mem book | `app/trading/market_book.py` | Snapshot + async subscribe; perfect for the volatility window. |
| Kalshi REST + 429 | `app/providers/exchanges/kalshi_*` | Token-bucket-aware error class already there; `retry_after` is parsed. |
| SQL ledger | `app/trading/ledger.py` | Persists fills + positions + P&L. The trader recovers state from here on restart. |
| Risk gates | `app/trading/risk.py` | Per-order, per-market, daily-loss kill switch, cooldown. Reused without changes. |
| Decision brain | `app/trading/decision_brain.py` | Provides ranked candidates with verified Kalshi tickers and `model_prob`. |
| BallDontLie | `app/providers/stats/balldontlie.py` | Already authenticated; live `/stats` available. |

## 3. New components

### 3.1 `app/trading/active_trader.py`

Long-lived asyncio task. Owns:

- `MarketBook` subscription (existing).
- `LiveStatsPoller` async task (new — see §3.3).
- `PortfolioManager` instance (new — multi-position FSM, see §3.2).
- `OrderRouter` instance (new — rate-limit-aware Kalshi writes).

Main loop:

```python
async def run(self):
    book_sub = self._book.subscribe()
    stats_task = asyncio.create_task(self._stats.run())
    poll_task  = asyncio.create_task(self._poll_fallback())

    async for update in book_sub:
        if self._kill_switch():
            await self._portfolio.flatten_all()
            break
        await self._portfolio.on_quote(update)        # may emit sells
        await self._maybe_enter_new_positions()
```

### 3.2 `PortfolioManager`

Per-ticker FSM:

```
no position
    │  candidate selected
    ▼
BUY_PENDING ── filled (full) ─► HOLDING
    │              │
    │              └── partial: recompute brackets on filled qty,
    │                  cancel remainder after 10s
    ▼
no position ◄── cancelled / rejected

HOLDING ── on_quote: TP/SL fire AND not high_conviction
       │   on_quote: minutes_to_settle <= 15 AND NOT on_pace
       │   external: kill_switch.flatten()
       │
       ▼
SELL_PENDING ── filled ─► CLOSED ─► (eligible for re-entry after cooldown
                                     OR after a separate trading session)
       │
       └── partial: re-submit for remainder; if 3 attempts fail in 30s,
           leave to settle and log warning.
```

Holds a `dict[ticker -> PositionState]`. Independent brackets per position. The "no double-down" rule blocks re-entering the same ticker while either `HOLDING` or `SELL_PENDING`. After `CLOSED`, a per-session `recently_exited` set blocks for `cooldown_after_exit_seconds` (default 30s) so we don't churn on noise. Different-line variants on the same player count as different tickers and are allowed concurrently.

### 3.3 `LiveStatsPoller`

Async task. Owns:

- A `dict[player_id -> LivePlayerStats]` with `points, rebounds, assists, threes, turnovers, minutes_played, period, clock_seconds_remaining, last_updated`.
- Polls BDL `/stats?player_ids[]=...&seasons[]=...` for the union of player_ids across open positions and live candidates.
- Cadence: every 10s during games-in-progress; pause polling for that player when the game is final.

Exposes:

```python
def on_pace_to_hit(player_id, market_key, line_value, side="over") -> bool
def current_stat(player_id, market_key) -> float | None
def is_signaling_entry(player_id, market_key, line_value) -> bool
```

`is_signaling_entry` returns True when the live stat is on a trajectory to easily clear the line (≥75% of line value with ≥30% of game remaining, configurable). The trader uses this to short-circuit waiting for the next decision-pack resync.

### 3.4 Volatility window in `MarketBook`

Tiny addition: each `MarketEntry` keeps a `deque[tuple[ts, yes_bid]]` of the last 60s of updates (existing entry already has `updated_at`; just extend with the deque). Exposes `stdev_cents(window_seconds=60)`. No behavior change unless the trader queries it.

### 3.5 `OrderRouter`

Wraps `KalshiClient.place_order` and `cancel_order`. Single shared semaphore enforces ≤ 5 writes/sec (well under Kalshi's 100/sec Basic-tier limit). On `KalshiRateLimited`, sleeps `retry_after` and retries up to 3 times. On 5xx, exponential backoff (1s, 2s, 4s, jittered, cap 30s). All submissions emit structured events into the existing `TradingStreamPublisher` so the UI gets live status.

## 4. Strategy parameters (all in `config/trading_limits.json`)

```jsonc
{
  "active_trader": {
    "enabled": false,
    "tp_pct": 0.50,
    "sl_pct": 0.50,
    "conviction_hold": 0.85,            // model_prob at/above this => override TP/SL, hold to settle
    "max_entry_price": 0.80,
    "min_model_prob": 0.60,
    "min_edge_bps": 100,
    "spread_max_cents": 6,
    "volatility_max_cents": 5,          // rolling 60s stdev of yes_bid
    "volatility_override": 0.80,        // model_prob at/above this => ignore volatility veto
    "max_open_positions": 50,           // soft cap; real bound is max_open_notional
    "settle_hold_minutes": 15,
    "pace_threshold_pct": 0.75,         // current_stat >= line * 0.75 with >=30% game remaining => entry signal
    "pace_remaining_pct_min": 0.30,
    "cooldown_after_exit_seconds": 30,
    "live_stats_poll_seconds": 10,
    "order_write_rps_cap": 5            // self-imposed, well under Kalshi 100/sec
  }
}
```

## 5. Rate-limit budget (verified against Kalshi docs)

| Bucket | Basic tier limit | Our footprint |
|---|---|---|
| Read tokens | 200/sec | <1/sec (occasional balance/positions refresh) |
| Write tokens | 100/sec | Capped at 5/sec by `order_write_rps_cap` (~0.05/sec sustained in normal use) |
| WS connections | 5/user | 1 multiplexed |

BDL: 600 req/min cap, our worst case 40 req/min.

## 6. Edge cases

| Case | Handling |
|---|---|
| Partial fill on buy | Recompute TP/SL on actual avg fill price; remaining qty cancelled after 10s. |
| Partial fill on sell | Re-submit for remainder; 3 attempts in 30s; if still partial, leave to settle. |
| WS disconnect | Existing `KalshiMarketService` auto-reconnects. While disconnected: entries paused, fallback `/markets/{ticker}` poll every 5s for held positions to keep brackets monitored. |
| BDL outage | Skip live-pace decisions at the 15-min lockout (default to "sell to bid" if SL has already fired; otherwise hold). Pre-emptive entry signaling pauses. Volatility veto unaffected (uses Kalshi quotes only). |
| Position spans two sessions (game crosses session boundary) | Ledger recovery on restart re-populates `HOLDING` state, reads entry price from fill record, rebuilds brackets. |
| Market suspended (`status != active`) | Block new entry; flatten held position at last bid. |
| Game ends during HOLDING | Kalshi settles to $1 or $0 automatically; ledger reads settlement and books P&L. |
| Kalshi 429 on entry order | Backoff; if still 429 after 3 retries, skip this candidate for the current pass. |
| Kalshi 429 on sell order | Same backoff; sell is critical so if still 429 after 3 retries, escalate to operator (event log + UI red banner). |
| Two simultaneous candidates for last $ of notional | Higher `model_prob` wins. Tie-break: higher edge_bps. |

## 7. Implementation phases

**Phase 1 — Strategy core (dry-run, paper mode)**
- New `app/trading/active_trader.py`, `app/trading/portfolio_manager.py`.
- Volatility window addition in `MarketBook`.
- Strategy params in settings + JSON.
- Unit tests: bracket math, conviction override, multi-position FSM, cooldown.
- No real Kalshi orders yet.

**Phase 2 — Live-stats poller + pace decisions**
- `app/trading/live_stats_poller.py` against existing BDL provider.
- `on_pace_to_hit` and `is_signaling_entry` with unit tests using fixture stat traces.
- Integration: at 15-min lockout, route to live stats; at entry filter, optional pre-emptive signal.

**Phase 3 — `OrderRouter` + live wiring**
- Token-bucket write limiter, 429/5xx backoff.
- Kalshi sandbox integration tests if available; otherwise mocked.
- End-to-end paper run against last night's recorded WS replay.

**Phase 4 — UI surface**
- `/api/trading/active/status` endpoint with per-position bracket state + recent exits + pacing info.
- New "Active Trader" panel in Tauri with start/stop, position list, P&L, parameter editor.
- Confirmation modal before going live (operator types `LIVE` like existing pattern).

**Phase 5 — Hardening**
- Structured event log per entry/exit/trigger/skip.
- Daily summary into ledger (trades, win rate, gross/net P&L, avg slippage).
- Auto-pause on consecutive losses (e.g., 3 in a row → operator must re-engage).
- Replay tool: feed historical WS log + BDL stats into trader for backtest.

Each phase ships through the planner → implementer → spec review → quality review subagent flow we used for the perf branch.

## 8. Final operator confirmations needed before Phase 1

I have your four answers. One more from the operator clarifications:

- **Live-stats coverage** — BallDontLie GOAT is your primary stats feed and I'm planning to use it. Confirm? (If you have a faster source — e.g., a paid NBA Live feed — pre-emptive entry signal becomes much more valuable.)

Once you confirm the live-stats feed, I'll spawn Phase 1.

---

## Sources

- [Kalshi Rate Limits and Tiers](https://docs.kalshi.com/getting_started/rate_limits) — Basic 200 reads/sec, 100 writes/sec; token bucket.
- [Kalshi API Developer's Guide — Zuplo](https://zuplo.com/learning-center/kalshi-api) — corroborates Basic ~20 req/s practical limit.
- [Kalshi Order Book API — QuantVPS](https://www.quantvps.com/blog/kalshi-order-book-api-endpoints-explained) — 5 WS connections/user, multiplex one.

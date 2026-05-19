# Active Trader — Handoff for next session

## Where we are

- **Branch:** `feature/active-trader-phase-1` (off main)
- **Spec:** `docs/plans/specs/profit-machine-design.md` (v2, Opus-planned, operator-confirmed)
- **Latest commit on branch:** `92e4b22 docs: active trader design v2 (Opus-planned)`
- **Latest commit on main:** `6f9999e fix(trading): preflight no longer requires max_open_notional <= daily_loss_cap`

## What the user wants

Build a "profit machine" — Kalshi prediction-market trading loop with:

- **Portfolio mode**: many positions concurrent, each independent, sized by `max_open_notional` (already auto-scaled to wallet via `wallet_init.py`).
- **Symmetric soft ±50% brackets** on contract price. Soft = decision, not auto-exit.
- **Conviction override**: when `model_prob >= 0.85`, ignore TP/SL and ride to settlement.
- **15-min settle-window decision via live stats**: if BallDontLie shows the player on pace to hit the line → hold; otherwise sell at bid. No dollar-based settle rule.
- **Volatility veto on entry**: skip tickers with rolling-60s `stdev(yes_bid) > 5¢`, unless `model_prob >= 0.80`. Threes especially.
- **Pre-emptive entry signal**: when BDL live stats cross ~75% of line value with ≥30% of game remaining, buy *before* the Kalshi quote adjusts. ("Pounce before market responds.")
- **Kill switch = liquidate**: sell all open positions at bid in parallel.
- **Live-stats feed**: BallDontLie GOAT, confirmed. Already wired in `app/providers/stats/balldontlie.py` (600 req/min cap; we'll use ~40 req/min).

## Already done (merged to main earlier this session)

| Commit | Effect |
|---|---|
| `4f68b51` | Calibration fold parallelism (4-wide ThreadPool, threadpool_limits) |
| `0d93515` | ORJSONResponse, winloop, sim_batch=25k, parquet mmap |
| `06f3937` | SQLite WAL + cache pragmas (256MB cache, 8GB mmap) |
| `78c296e` | FeatureEngineer disk cache (blake2b key + parquet) |
| `e95330a` | Vectorized `_fill_with_population_priors` |
| `c0215c8` | Perf plan + MC vectorization spec (deferred to a future PR) |
| `e3abac7` | Relaxed injury/projection staleness gates (30→240 min, 60→180 min) |
| `f627b0a` | Preserve valid same-day decision pack on transient sync errors |
| `6f9999e` | Preflight no longer requires max_open_notional <= daily_loss_cap |

App is currently launchable; preflight passes; 14-16 live Kalshi candidates produced per resync; one Kalshi ticker confirmed retrievable: `KXNBAREB-26MAY19CLENYK-CLEEMOBLEY4-8`.

## Phase 1 — task list (TaskList IDs in current session)

These are queued. Recommend implementing them in this order via subagent-driven-development workflow:

1. **TaskList #6 — P1.1: Strategy params in settings + trading_limits**
   - Add `active_trader` section to `config/trading_limits.json` defaults.
   - Expose via Settings (or a dedicated dataclass loaded from the limits file).
   - Exact JSON shape: see `docs/plans/specs/profit-machine-design.md` §4.

2. **TaskList #7 — P1.2: Volatility window in MarketBook**
   - `app/trading/market_book.py`: each `MarketEntry` gets a deque of last 60s `(yes_bid, ts)` pairs.
   - New method: `MarketBook.stdev_cents(ticker, window_seconds=60) -> float | None`.
   - Tests: empty → None; constant → 0.0; jittery → approximate stdev.

3. **TaskList #8 — P1.3: PortfolioManager FSM (paper mode)**
   - New file: `app/trading/portfolio_manager.py`.
   - Per-ticker FSM: `BUY_PENDING → HOLDING → SELL_PENDING → CLOSED`.
   - Multi-position. Conviction override on TP/SL. Cooldown. No double-down on held ticker.
   - Injected `OrderRouter` protocol so tests can mock all Kalshi I/O.

4. **TaskList #4 (still open from earlier)** — already deleted as deferred (MC vectorization). Ignore.

5. **NEXT TO QUEUE — P1.4: ActiveTrader scaffold (paper mode, no live wiring)**
   - New file: `app/trading/active_trader.py`.
   - Wires `MarketBook` subscription + `PortfolioManager` + a mocked OrderRouter.
   - End-to-end test: feed a synthetic quote stream through the trader and assert correct entries/exits.

6. **NEXT TO QUEUE — P1.5: Unit-test coverage gate**
   - Final test pass over all P1 components; require ≥80% line coverage on the new modules.

After Phase 1 lands, the design doc lists Phase 2-5:
- **Phase 2**: `LiveStatsPoller` (BDL) + pace decisions
- **Phase 3**: `OrderRouter` (token-bucket, 429/5xx backoff) + live wiring
- **Phase 4**: UI surface (`/api/trading/active/status` endpoint + Tauri panel)
- **Phase 5**: Hardening (event log, daily summary, replay tool, auto-pause on losses)

## Strategy parameter defaults (from spec §4)

```jsonc
{
  "active_trader": {
    "enabled": false,
    "tp_pct": 0.50,
    "sl_pct": 0.50,
    "conviction_hold": 0.85,
    "max_entry_price": 0.80,
    "min_model_prob": 0.60,
    "min_edge_bps": 100,
    "spread_max_cents": 6,
    "volatility_max_cents": 5,
    "volatility_override": 0.80,
    "max_open_positions": 50,
    "settle_hold_minutes": 15,
    "pace_threshold_pct": 0.75,
    "pace_remaining_pct_min": 0.30,
    "cooldown_after_exit_seconds": 30,
    "live_stats_poll_seconds": 10,
    "order_write_rps_cap": 5
  }
}
```

## Kalshi rate-limit constraints (verified against current docs)

- Basic tier: **200 read tokens/sec**, **100 write tokens/sec**, **5 WebSocket connections/user**.
- Our self-imposed cap: 5 writes/sec via `OrderRouter` (Phase 3).
- BDL GOAT: 600 req/min — we use ~40.
- Existing 429 handling: `app/providers/exchanges/kalshi_errors.py:KalshiRateLimited` with `retry_after` parsing — reuse this.

## How to resume in a fresh session

```bash
cd "/e/Projects/NBABets v2"
git checkout feature/active-trader-phase-1
cat docs/plans/specs/profit-machine-design.md      # full design
cat docs/plans/HANDOFF-active-trader.md            # this file
```

Then either:
- **Use subagent-driven-development workflow** (preferred — same as the perf branch): dispatch one implementer subagent per task in TaskList #6 → #7 → #8 → P1.4 → P1.5, with code-reviewer between each.
- **Or run inline** if context allows.

Each task has a clear "what to build" in this doc + the spec. The spec has FSM diagrams and pseudo-code. The OrderRouter is mocked in Phase 1; nothing hits real Kalshi until Phase 3.

## Watch out for

- **Don't touch existing one-shot `scripts/run_trading_loop.py`** — leave it for now; it's still wired to the trading loop subprocess via `loop_controller.py`. We're building a parallel path. Once active_trader works end-to-end, that script can be retired.
- **Don't bypass the existing risk gates** in `app/trading/risk.py` — the new trader still respects per-order-cap, per-market-cap, and the SQL kill switch. The kill switch is consulted on every quote update.
- **PRA market uses the legacy code path** in `_parallel_market_probabilities` — has no effect on the trader (which only consumes the decision pack output), but worth knowing if you debug discrepancies between PRA props and others.
- **No Polars/numba/cython** dependencies. Pure numpy + asyncio.
- **Tauri sidecar is `--onefile` PyInstaller** — every code change requires `powershell -ExecutionPolicy Bypass -File scripts/build_sidecar.ps1` (~90 sec) + Tauri restart to pick up.

## Open questions parked (don't block Phase 1)

- Should the trader log to the existing `TradingStreamPublisher` event bus, or a new dedicated stream? (Default: existing one — the UI already consumes it.)
- Should we expose Phase 1 paper-mode runs to the UI immediately, or wait for Phase 4? (Default: wait; paper mode is for tests only.)
- Should `flatten_all` use market orders or aggressive limits at bid - 1¢? (Default: market orders — Kalshi handles them; faster execution under stress.)

# Active Trader — Handoff for Phase 3 and beyond

## Where we are

- **Branch:** `feature/active-trader-phase-2` (6 commits ahead of `feature/active-trader-phase-1`)
- **PR:** https://github.com/austi20/NBABets-v2/pull/2 (open — Phase 2, paper mode; stacked on Phase 1 PR #1, base `feature/active-trader-phase-1`)
- **Spec:** `docs/plans/specs/profit-machine-design.md` (v2, operator-confirmed) — Phase 3 is §7; the components it builds are §3.5 (OrderRouter), §5 (rate-limit budget), §6 (edge cases).
- **Prior handoffs:** `docs/plans/HANDOFF-active-trader.md` (Phase 1 origin), `docs/plans/HANDOFF-active-trader-phase-2.md` (Phase 2).

Phases 1 and 2 are complete and reviewed. Everything is still inert: `active_trader.enabled` defaults `false`, **no code can place a real Kalshi order** — order I/O only exists behind the mocked `OrderRouter` protocol, and the live-stats poller only *reads* BallDontLie. **Phase 3 is the FIRST phase that touches real Kalshi.**

## Phase 2 — what shipped (the seams Phase 3 wires into)

| File | What it gives Phase 3 |
|---|---|
| `app/trading/portfolio_manager.py` | The async `OrderRouter` Protocol (`place_buy(ticker, qty, limit_price_cents) -> OrderHandle`, `place_sell(...) -> OrderHandle`, `cancel(handle)`); `OrderHandle(order_id, ticker, side)`. The FSM consumes fills via `on_fill(handle, filled_qty, avg_price_cents)` and already handles partial buy/sell, cancel-remainder, and `flatten_all()`. **Key Phase 3 seam:** implement a real `OrderRouter` and feed it `on_fill`. PositionState now also carries `player_id`, `stat_type` (Phase 2). |
| `app/trading/live_stats_poller.py` | `LiveStatsPoller` (read-only BDL), `LivePlayerStats`, `parse_live_stats`, and pace methods `on_pace_to_hit` / `current_stat` / `is_signaling_entry`. Constructor seams: `player_ids_source: Callable[[], Iterable[int]]`, `fetch: Callable[[list[int]], Awaitable[dict]]`, `sleep`. **Phase 3 wiring:** real `fetch = lambda ids: (await provider.fetch_live_player_stats(ids, season)).payload`; real `player_ids_source` = union of `player_id` across open positions + live candidates. |
| `app/trading/active_trader.py` | `make_on_pace_callable(poller)` (inject into `PortfolioManager`), `MergingCandidateProvider(base, poller, preemptive_source)` (pre-emptive entry through the single gate chokepoint), `ActiveCandidate` (now carries `player_id`, `stat_type`). The loop already calls `kill_switch()` per update and `flatten_all()` on kill. **Phase 3 seam:** `_risk_ok` intentionally enforces only per-order / per-market / open-notional from `RiskLimits`; it deliberately does **not** enforce `daily_loss_cap` or `reject_cooldown_seconds` — see the comment in `_risk_ok`. Those belong to `ExposureRiskEngine`, wired in this phase. |
| `app/providers/stats/balldontlie.py` | `fetch_live_player_stats(player_ids, season) -> ProviderFetchResult` (read-only, rate-limited via the existing rolling-window limiter). |

Coverage at end of Phase 2: live_stats_poller 91%, active_trader 97%, portfolio_manager 89%. `ruff` + `mypy` clean on all new modules. Full `tests/unit/trading/` green except the two known pre-existing failures (below).

## Phase 2 commit map (on the branch)

| Commit | Effect |
|---|---|
| `3584b4f` / `9180f64` | P2.1 LivePlayerStats view + BDL live-stats fetch (+ cleanup) |
| `37dad78` | P2.2 LiveStatsPoller async task with finality pause |
| `19f2d9e` | P2.3 on_pace_to_hit + current_stat + is_signaling_entry |
| `2b7af8a` | P2.4 wire live-pace into FSM settle window |
| `45e40a1` | P2.5 merging candidate provider for pre-emptive entry |

## What already exists, reused as-is (the Kalshi infra Phase 3 wraps)

| File | Role for Phase 3 |
|---|---|
| `app/providers/exchanges/kalshi_client.py` | **`KalshiClient` is SYNC (httpx.Client).** Key methods: `create_order(*, ticker, side="bid"/"ask", count, price_dollars, client_order_id, time_in_force="fill_or_kill", ...)`; `create_exit_order(*, ticker, held_side="yes", count, exit_price_dollars, client_order_id, time_in_force="immediate_or_cancel", reduce_only=True)`; `cancel_order(order_id)`; `get_order`, `get_fills`, `get_positions`, `get_orderbook`, `get_balance`, `get_market`. Buying YES = `create_order(side="bid", ...)`; selling YES = `create_exit_order(held_side="yes", ...)`. |
| `app/providers/exchanges/kalshi_errors.py` | `KalshiRateLimited(status, body, retry_after)` (line 26; `retry_after` already parsed) and `classify_response(...)` which raises it on 429. **Reuse for the backoff loop — do not re-parse headers.** |
| `app/trading/kalshi_adapter.py` | The **legacy sync** adapter (`KalshiAdapter.place_order(intent)`) for the one-shot decision-pack path. It already implements the create_order → poll `get_order`/`get_fills` → emit `OrderEvent`/`Fill` loop and terminal-status handling (`_TERMINAL_STATUSES`). **Mine its polling/fill-mapping logic** for the new async router; do not call it directly (it's the `ExecutionIntent` path, not the FSM path). |
| `app/trading/risk.py` | `RiskLimits` (per_order_cap, per_market_cap, max_open_notional, daily_loss_cap, reject_cooldown_seconds) and `ExposureRiskEngine.evaluate(intent, ledger) -> (ok, reason)` which adds the daily-loss cap, reject cooldown, and in-memory kill (`set_killed`/`killed`). |
| `app/trading/loop.py` | `set_kill_switch(session_factory, *, killed, set_by="system")` writes the SQL `TradingKillSwitch` row (id=1). |
| `app/trading/ledger.py` | SQL ledger (fills + positions + P&L) for persistence and restart recovery. |
| `app/trading/stream_publisher.py` | `TradingStreamPublisher` — the UI already consumes it; emit structured order/fill/exit events here. |

## Phase 3 — task list (OrderRouter + live wiring)

Spec §3.5, §5, §6. Same subagent-driven flow as Phases 1-2 (implementer → spec review → code-quality review per task), **but note:** the subagent path was usage-capped during Phase 2, so several tasks ran inline. Either path is fine; keep one task per commit with tests + ruff + mypy green.

1. **P3.1 — `KalshiOrderRouter` implementing the async `OrderRouter` protocol**
   - New file `app/trading/kalshi_order_router.py`. Implements `place_buy`, `place_sell`, `cancel` returning `OrderHandle`.
   - **`KalshiClient` is synchronous; the FSM/loop is async.** Wrap each blocking call in `asyncio.to_thread(...)` (or an injected executor) so the event loop is never blocked.
   - `place_buy(ticker, qty, limit_price_cents)` → `create_order(ticker, side="bid", count=qty, price_dollars=cents/100, client_order_id=<uuid>, time_in_force="immediate_or_cancel")`. Use IOC (not fill_or_kill) so the FSM's partial-fill path is exercised; map the response → `OrderHandle(order_id, ticker, "buy")`.
   - `place_sell(ticker, qty, limit_price_cents)` → `create_exit_order(held_side="yes", count=qty, exit_price_dollars=cents/100, client_order_id=<uuid>)` (reduce_only) → `OrderHandle(..., "sell")`.
   - `cancel(handle)` → `cancel_order(handle.order_id)`.
   - **Fill delivery:** the FSM expects `PortfolioManager.on_fill(handle, filled_qty, avg_price_cents)` to be driven externally. Decide and implement the fill source — poll `get_fills(order_id=...)` after submit (mirror `KalshiAdapter`'s poll loop), or a WS fills subscription if available. Convert price dollars → cents for `avg_price_cents`. **(Open question #1 below.)**
   - Tests: mock `KalshiClient`; assert side/price/count mapping, OrderHandle wiring, cancel, and that fills round-trip into `on_fill`.

2. **P3.2 — Write rate limiter + retry/backoff**
   - Single shared `asyncio.Semaphore`/token bucket enforcing ≤ `config.order_write_rps_cap` (5) writes/sec across the whole router (well under Kalshi Basic 100/sec).
   - On `KalshiRateLimited`: sleep `exc.retry_after`, retry up to 3 times. On 5xx (`KalshiApiError` with 5xx status): exponential backoff 1s → 2s → 4s, jittered, cap 30s.
   - **Sell is critical:** if a sell still 429s after 3 retries, escalate (event log + UI red banner via `TradingStreamPublisher`). Entry orders that still 429 just skip the candidate this pass (§6).
   - Tests: a fake client that raises `KalshiRateLimited` then succeeds; assert it honored `retry_after` and retried; assert the rps cap serializes writes; assert sell-exhaustion escalates.

3. **P3.3 — Wire `ExposureRiskEngine` into the entry path**
   - `ActiveTrader._risk_ok` deliberately omits `daily_loss_cap` + `reject_cooldown_seconds`. Before placing a *real* order, consult `ExposureRiskEngine` (it adds daily-loss, cooldown, kill). `evaluate` takes `(ExecutionIntent, PortfolioLedger)` — bridge `ActiveCandidate` → that check, or add equivalent daily-loss/cooldown checks reading the ledger. **(Open question #2.)**
   - Tests: candidate rejected when daily loss cap hit; rejected during reject cooldown; rejected when killed.

4. **P3.4 — SQL kill switch wiring + `flatten` flag**
   - Provide the `kill_switch` callable to `ActiveTrader` as a reader of the SQL `TradingKillSwitch` row (consult per quote update; `ActiveTrader.run` already flattens on kill). Cache/throttle the read so it isn't a DB hit every tick if needed.
   - Add a `flatten` flag to `set_kill_switch` (spec "kill switch = liquidate": `set_kill_switch(killed=True, flatten=True)` should both set the row and trigger `flatten_all`). Keep backward compat for existing callers.
   - Tests: kill row True → loop flattens and exits; flatten flag path.

5. **P3.5 — Ledger persistence + restart recovery**
   - Persist entries/exits/fills/P&L through `ledger.py`. On startup, rebuild `HOLDING` `PositionState` from fill records (entry price → brackets via the FSM's bracket math) so a position spanning two sessions (game crosses a session boundary) survives a restart (§6).
   - Tests: write fills → restart → positions rehydrated with correct brackets.

6. **P3.6 — Production wiring of the Phase 2 components**
   - Construct the real `LiveStatsPoller` (`fetch` = BDL provider lambda, `player_ids_source` = union of open-position + candidate `player_id`s, `season` from settings), inject `make_on_pace_callable(poller)` into `PortfolioManager`, and wrap the decision-pack `CandidateProvider` in `MergingCandidateProvider` with a `preemptive_source` (the broader candidate universe carrying `player_id`/`stat_type`). Emit structured events via `TradingStreamPublisher`.
   - **Pre-emptive entry is highest-value/highest-risk (spec §1):** instrument BDL-update vs Kalshi-quote latency during a dry run *before* trusting `is_signaling_entry` to place real orders. Gate it behind a config flag if the measured latency edge isn't real.

7. **P3.7 — WS disconnect fallback (§6)**
   - While the WS is disconnected: pause new entries; poll `get_market`/`get_orderbook` every 5s for *held* positions so brackets keep being monitored. `KalshiMarketService` already auto-reconnects. Market suspended (`status != active`): block entry, flatten held at last bid.
   - Tests: simulate disconnect → entries paused, held positions still bracket-checked via fallback poll.

8. **P3.8 — Integration tests + coverage gate**
   - Kalshi sandbox integration if credentials/sandbox available; otherwise mocked transport (`KalshiClient` accepts an injected `httpx.BaseTransport`). End-to-end paper run against a recorded WS replay (the spec calls for "last night's recorded WS log").
   - ≥80% line coverage on all new Phase 3 modules; full `tests/unit/trading/` green except the two known pre-existing failures.

## Phases 4-5 (unchanged from spec §7)

- **Phase 4 — UI.** `/api/trading/active/status` endpoint (per-position bracket state + recent exits + pacing). Tauri "Active Trader" panel: start/stop, position list, P&L, param editor, `LIVE`-typed confirmation modal. **Rebuild the PyInstaller sidecar after Python changes** (`powershell -ExecutionPolicy Bypass -File scripts/build_sidecar.ps1`, ~90s) + Tauri restart.
- **Phase 5 — Hardening.** Structured event log per entry/exit/trigger/skip; daily summary into ledger (win rate, gross/net P&L, avg slippage); auto-pause on N consecutive losses; WS+BDL replay backtest tool.

## Watch out for

- **This is the first phase that places real Kalshi orders. Do sandbox/mocked first.** Keep `active_trader.enabled=false` until end-to-end paper replay passes.
- **`KalshiClient` is synchronous.** The router/loop are async — never call it directly on the event loop; use `asyncio.to_thread`.
- **Don't bypass `app/trading/risk.py`.** The SQL kill switch is consulted on every quote update once live; `ExposureRiskEngine` owns daily-loss + cooldown.
- **Don't touch `scripts/run_trading_loop.py` / `app/trading/loop.py`'s one-shot path.** The active trader is a parallel path; retire the legacy script only after live wiring works end-to-end.
- **No Polars/numba/cython.** Pure numpy + asyncio.
- **Buy = `create_order(side="bid")`; sell = `create_exit_order(held_side="yes", reduce_only=True)`.** Prices are dollars (0-1) in the client; the FSM speaks cents — convert at the router boundary.
- **Test runner:** no `.venv`; use the `python` on PATH (pytest/mypy/ruff/pytest-cov). `pip install -e .[dev]` is the setup. Async tests in `tests/unit/trading/` use `@pytest.mark.asyncio`.

## Known pre-existing failures (NOT from this work — already flagged as separate tasks)

- `tests/unit/trading/test_selections.py::test_save_then_load_roundtrip` — `SelectionStore` drops explicit `False` (de-selected) entries on save/reload. Real data bug in untouched `selections.py`.
- `tests/unit/trading/test_wallet_init.py::test_reinitializes_when_yesterday` — UTC-vs-local date flake; only fails after ~8pm US-Eastern.

## Open questions parked (decide before/inside Phase 3)

1. **Fill delivery mechanism (P3.1):** poll `get_fills` after each submit (simple, mirrors `KalshiAdapter`) vs. a WS fills/positions subscription (lower latency, more moving parts). Lean: start with polling, swap to WS in Phase 5 if latency matters.
2. **`ExposureRiskEngine` bridge (P3.3):** synthesize an `ExecutionIntent` from an `ActiveCandidate` to reuse `evaluate(intent, ledger)` as-is, vs. add daily-loss/cooldown checks directly in the active-trader path reading the ledger. Lean: synthesize an intent so there's one risk authority.
3. **Pre-emptive entry trust (P3.6):** keep `is_signaling_entry`-driven entries behind a config flag until the BDL-vs-Kalshi latency edge is measured in a dry run.

## How to resume in a fresh session

```bash
cd "/e/Projects/NBABets v2"
# Phase 3 branches off Phase 2:
git worktree add .claude/worktrees/active-trader-phase-3 -b feature/active-trader-phase-3 feature/active-trader-phase-2
cd .claude/worktrees/active-trader-phase-3
cat docs/plans/specs/profit-machine-design.md          # full design (§3.5, §5, §6 for Phase 3)
cat docs/plans/HANDOFF-active-trader-phase-3.md         # this file
python -m pytest tests/unit/trading/ -q                # baseline: 162 passed, 2 known failures
```

Then run the subagent-driven-development workflow over P3.1 → P3.8, one implementer per task with spec + code-quality review between each (same cadence as Phases 1-2).

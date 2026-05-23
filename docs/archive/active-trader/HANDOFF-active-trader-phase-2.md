# Active Trader — Handoff for Phase 2 and beyond

## Where we are

- **Branch:** `feature/active-trader-phase-1` (12 commits ahead of `main`)
- **PR:** https://github.com/austi20/NBABets-v2/pull/1 (open — Phase 1, paper mode)
- **Spec:** `docs/plans/specs/profit-machine-design.md` (v2, operator-confirmed) — Phases 2-5 are in §7.
- **Phase 1 handoff (origin of this work):** `docs/plans/HANDOFF-active-trader.md`

Phase 1 (strategy core, paper mode) is complete and reviewed. Everything is inert: `active_trader.enabled` defaults to `false`, and **no code in the four Phase 1 modules can place a real Kalshi order** — order I/O only exists behind a mocked `OrderRouter` protocol.

## Phase 1 — what shipped (the seams Phase 2 wires into)

| File | What it gives Phase 2 |
|---|---|
| `app/trading/live_limits.py` | `ActiveTraderConfig` + `load_active_trader_config(path)`. Pace knobs already present: `pace_threshold_pct` (0.75), `pace_remaining_pct_min` (0.30), `settle_hold_minutes` (15), `live_stats_poll_seconds` (10). |
| `app/trading/market_book.py` | Rolling-60s `stdev_cents(ticker, window_seconds=60) -> float | None`. Unrelated to Phase 2 but already consumed by the entry gates. |
| `app/trading/portfolio_manager.py` | The FSM. **Key Phase 2 seam:** constructor takes `on_pace_callable: Callable[[PositionState], bool] | None`. Phase 1 passes `None` (= "no live data, hold"). Phase 2 injects the real callable. `PositionState` already carries `line_value`, `market_close_at`, `ticker`, `model_prob` — everything `on_pace_to_hit` needs. |
| `app/trading/active_trader.py` | The loop. **Key Phase 2 seam:** `candidates_source: CandidateProvider` (`async def candidates() -> Iterable[ActiveCandidate]`). `ActiveCandidate` carries `ticker, model_prob, edge_bps, line_value, market_close_at, stake_cents`. Pre-emptive entry (Phase 2) feeds candidates through this same provider. |

Coverage at end of Phase 1: live_limits 92%, market_book 88%, portfolio_manager 87%, active_trader 96%. `ruff` + `mypy` clean on all four.

## Phase 1 commit map (on the branch)

| Commit | Effect |
|---|---|
| `639073d` / `cb14864` | P1.1 active_trader config + loader |
| `9d40e51` / `dae3e1d` | P1.2 rolling stdev window in MarketBook |
| `3d44dde` / `888a784` | P1.3 PortfolioManager FSM |
| `c079205` / `f0690e6` | P1.4 ActiveTrader scaffold + entry gates |
| `ce12c74` | P1.5 coverage gap tests |
| `a122fe4` | mypy typing fix in live_limits |

## Phase 2 — task list (LiveStatsPoller + pace decisions)

Spec §3.3 and §1 ("Live-stats edge"). Recommend the same subagent-driven flow used for Phase 1 (implementer → spec review → code-quality review per task).

1. **P2.1 — `LivePlayerStats` view + BDL live-stats fetch**
   - New file: `app/trading/live_stats_poller.py`.
   - Add a live-stats fetch to the BDL provider (or call `_get` directly): endpoint `GET /stats?player_ids[]=...&seasons[]=2026`. Existing provider: `app/providers/stats/balldontlie.py` — it already has the rolling-window limiter (`_acquire_bdl_rate_slot`, default ~540/60s) and `_get`. Existing fetch methods to mirror for style: `fetch_player_game_logs` (line 187), `fetch_rosters` (line 117).
   - `LivePlayerStats` dataclass: `points, rebounds, assists, threes, turnovers, minutes_played, period, clock_seconds_remaining, last_updated`.
   - Unit tests with fixture JSON (no live network). Verify parse + field mapping.

2. **P2.2 — `LiveStatsPoller` async task**
   - Owns `dict[player_id -> LivePlayerStats]`.
   - Polls the union of `player_id` across open positions + live candidates every `config.live_stats_poll_seconds` (10s). Pauses polling for a player when their game is final.
   - Budget check: 10 games × 1 poll/15s ≈ 40 req/min vs BDL 600 cap — comfortable.
   - Tests: poller updates the view from a fake fetch; stops polling finals; respects cadence with a fake clock.

3. **P2.3 — `on_pace_to_hit` + `current_stat` + `is_signaling_entry`**
   - Methods on the poller (spec §3.3):
     - `on_pace_to_hit(player_id, market_key, line_value, side="over") -> bool` — linear projection: `projected = current + remaining_minutes * recent_rate_per_minute`; `projected >= line_value` ⇒ True (for YES-OVER).
     - `current_stat(player_id, market_key) -> float | None`.
     - `is_signaling_entry(player_id, market_key, line_value) -> bool` — True when `current_stat >= line_value * pace_threshold_pct` (0.75) AND `>= pace_remaining_pct_min` (0.30) of game remaining.
   - Tests with fixture stat traces covering: on-pace, off-pace, exactly-at-threshold, end-of-game edge.
   - **Player↔market mapping is the hard part.** Need a `market_key` → `(player_id, stat_type)` resolver. Check `app/trading/symbol_resolver.py` and `app/trading/mapper.py` for existing player/market mapping before building new.

4. **P2.4 — Wire pace into the FSM (settle window)**
   - Build the `on_pace_callable: Callable[[PositionState], bool]` that adapts a `PositionState` → `poller.on_pace_to_hit(...)` and inject it into `PortfolioManager`. `PositionState` already has `line_value` and `market_close_at`; you'll need the `player_id`/stat_type from the resolver (P2.3).
   - BDL outage handling (spec §6): if stats unavailable at the 15-min lockout, default to "sell to bid if SL already fired, otherwise hold." The callable raising is already safe — `PortfolioManager` swallows `on_pace_callable` exceptions and holds.

5. **P2.5 — Pre-emptive entry signal**
   - Wire `is_signaling_entry` into the candidate flow so a player crossing the on-track threshold injects/raises a candidate before the Kalshi quote reprices ("pounce before market responds"). Cleanest seam: a `CandidateProvider` impl that merges decision-pack candidates with pre-emptive signals.
   - Spec flags this as highest-value/highest-risk; instrument BDL-update vs Kalshi-quote latency during a dry run before trusting it.

6. **P2.6 — Coverage gate** — ≥80% line coverage on all new Phase 2 modules; full `tests/unit/trading/` green (except the two known pre-existing failures below).

## Phases 3-5 (unchanged from spec §7)

- **Phase 3 — `OrderRouter` + live wiring.** Token-bucket write limiter (≤5 writes/sec via `order_write_rps_cap`), 429/5xx backoff. **Reuse `app/providers/exchanges/kalshi_errors.py:KalshiRateLimited`** (parses `retry_after`). This is the FIRST phase that touches real Kalshi. Wire `ExposureRiskEngine` here — it owns `daily_loss_cap` and `reject_cooldown_seconds`, which the ActiveTrader entry gates intentionally do **not** enforce (see comment in `active_trader.py:_risk_ok`). Sandbox/replay integration tests.
- **Phase 4 — UI.** `/api/trading/active/status` endpoint (per-position bracket state + recent exits + pacing). Tauri "Active Trader" panel with start/stop, position list, P&L, param editor. Confirmation modal: operator types `LIVE` (existing pattern). Rebuild sidecar after Python changes (see watch-outs).
- **Phase 5 — Hardening.** Structured event log per entry/exit/trigger/skip; daily summary into ledger; auto-pause on N consecutive losses; WS+BDL replay backtest tool.

## Watch out for

- **Nothing hits real Kalshi until Phase 3.** Phase 2 is still paper mode — the poller reads BDL (read-only), no writes.
- **Don't bypass existing risk gates** (`app/trading/risk.py`). The SQL kill switch is consulted on every quote update once live.
- **Don't touch `scripts/run_trading_loop.py`** — it's the legacy one-shot path wired via `loop_controller.py`. The active trader is a parallel path; retire the script only after live wiring works end-to-end.
- **No Polars/numba/cython.** Pure numpy + asyncio.
- **Tauri sidecar is `--onefile` PyInstaller** — Python changes need `powershell -ExecutionPolicy Bypass -File scripts/build_sidecar.ps1` (~90s) + Tauri restart before the desktop app sees them. (Only matters from Phase 4 on.)
- **Test runner:** there is no `.venv` dir; use the `python` on PATH (has pytest/mypy/ruff/pytest-cov). `pip install -e .[dev]` is the setup.

## Known pre-existing failures (NOT from this work — already flagged as separate tasks)

- `tests/unit/trading/test_selections.py::test_save_then_load_roundtrip` — `SelectionStore` drops explicit `False` (de-selected) entries on save/reload. Real data bug in untouched `selections.py`.
- `tests/unit/trading/test_wallet_init.py::test_reinitializes_when_yesterday` — UTC-vs-local date flake; only fails after ~8pm US-Eastern. (There may be an uncommitted local edit to this test file in the working tree from an earlier fix attempt — not part of this branch.)

## Open questions parked (decide before/inside Phase 2)

- **Pre-emptive entry candidate source:** merge into the existing decision-pack `CandidateProvider`, or a dedicated one? (Lean: a merging provider so the entry gates stay the single chokepoint.)
- **Player↔market resolver:** reuse `symbol_resolver.py`/`mapper.py` or add a small dedicated map? Confirm what those already expose before building.
- **Event-bus target (Phase 5):** existing `TradingStreamPublisher` (UI already consumes it) vs a new dedicated stream. (Default from Phase 1 handoff: existing one.)

## How to resume in a fresh session

```bash
cd "/e/Projects/NBABets v2"
git checkout feature/active-trader-phase-1
cat docs/plans/specs/profit-machine-design.md         # full design (§3.3, §1 for Phase 2)
cat docs/plans/HANDOFF-active-trader-phase-2.md        # this file
```

Then run the subagent-driven-development workflow over P2.1 → P2.6, one implementer per task with spec + code-quality review between each (same cadence as Phase 1).

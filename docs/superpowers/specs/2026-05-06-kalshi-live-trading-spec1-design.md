# Kalshi Live Trading — Spec 1 (Walking Skeleton)

**Date:** 2026-05-06
**Status:** Approved for planning
**Sequel:** Spec 2 (always-on scheduler, UI auto-toggle, catalog crawler, reconciliation)

---

## 1. Goal

Place one real Kalshi NBA contract order from a model-generated signal, see the
fill recorded to a persistent SQL ledger, and prove the kill switch halts
further orders — driven from the existing `loop.py` CLI invoked via a new
`scripts/run_trading_loop.py --live` entry point.

Stake is hard-clamped to **$0.25/order** and **$2.00 daily realized-loss cap**
during the shakedown phase. Limits are loaded from a config file so they can
be tuned without code changes.

## 2. Non-goals (deferred to Spec 2 or later)

- Always-on `TradingScheduler` running inside the FastAPI sidecar.
- Desktop UI auto-trading toggle and editable limit controls.
- Catalog crawler / automatic Kalshi NBA market discovery.
- Reconciliation poller (drift detection between SQL ledger and Kalshi state).
- Limit-order pricing strategies (Spec 1 ships market-style only).
- Polymarket adapter.
- Multi-account or multi-key support.

## 3. Step 0 — human prerequisites (not code)

These must be done by the operator before any of this code is useful.

1. Enable API access on the existing Kalshi account.
2. Generate an RSA keypair via the Kalshi dashboard. Save the private key to a
   local path outside the repo (e.g. `%USERPROFILE%/.kalshi/private_key.pem`).
3. Confirm state-eligibility for NBA event contracts.
4. Deposit funds; recommended ≥ $50 for headroom over the $2 daily cap.
5. Verify at least 5 NBA player-prop tickers exist on the planned run day so
   the symbol map can be populated.

## 4. Architecture overview

Three new modules; existing trading scaffolding (`TradingLoop`,
`ExposureRiskEngine`, protocols) is reused unchanged.

```
scripts/run_trading_loop.py --live
       |
       v
app/trading/loop.py (existing)  ---- ExchangeAdapter --->  KalshiAdapter (new)
       |                                                          |
       | PortfolioLedger                                           | uses
       v                                                           v
SqlPortfolioLedger (new, drop-in for InMemoryPortfolioLedger)   KalshiClient (new
       |                                                            httpx + RSA-PSS)
       v                                                            |
existing app/db SQLAlchemy engine                                   v
(new tables: trading_orders, trading_fills,                  api.elections.kalshi.com
 trading_positions, trading_kill_switch, trading_daily_pnl)
```

**Data flow for one live order:**

1. Operator runs
   `python scripts/run_trading_loop.py --live --decisions today.json`.
2. CLI loads `config/trading_limits.json`, instantiates `SqlPortfolioLedger`,
   `ExposureRiskEngine` with the live limits, and `KalshiAdapter` wrapping
   `KalshiClient`.
3. Existing `TradingLoop.run_decisions()` iterates: signal → mapper → intent →
   risk check → adapter.
4. `KalshiAdapter.place_order()`:
   - Resolves model `Signal` to a Kalshi ticker via the hand-curated map.
   - Submits a market-style order via `KalshiClient.create_order()` (taker,
     accept current best price up to slippage cap).
   - Polls `KalshiClient.get_order(order_id)` until terminal state or 5 s
     timeout.
   - Returns `OrderEvent`s + `Fill`s.
5. `SqlPortfolioLedger.record_fill()` writes to SQLite in a single transaction.
6. Before each order, the loop checks `trading_kill_switch`; if killed, halts.

## 5. Components

### 5.1 `KalshiClient` — `app/providers/exchanges/kalshi_client.py` (new)

Hand-rolled httpx client for Kalshi v2 API. ~150 LoC, no Kalshi-specific
third-party deps.

**Auth.** Each request carries:

- `KALSHI-ACCESS-KEY: <api_key_id>`
- `KALSHI-ACCESS-TIMESTAMP: <unix_ms>`
- `KALSHI-ACCESS-SIGNATURE: base64(sign_pss(timestamp + method + path))`

Signing helper uses `cryptography.hazmat.primitives.asymmetric.padding.PSS`
with SHA-256 and salt length equal to the digest length. The private key is
loaded once at client init from a path supplied via env var.

**Endpoints implemented (only what Spec 1 needs).**

| Method | Endpoint                                       | Purpose                                  |
|--------|------------------------------------------------|------------------------------------------|
| GET    | `/trade-api/v2/portfolio/balance`              | Sanity-check on startup                  |
| GET    | `/trade-api/v2/markets/{ticker}`               | Verify ticker resolves before placing    |
| POST   | `/trade-api/v2/portfolio/orders`               | Place order                              |
| GET    | `/trade-api/v2/portfolio/orders/{order_id}`    | Poll for fill                            |

**Error semantics.** All non-2xx responses raise typed exceptions:
`KalshiAuthError` (401/403), `KalshiMarketError` (404 on ticker),
`KalshiInsufficientFunds` (specific 4xx body), `KalshiRateLimited` (429,
surfaces `Retry-After`), `KalshiServerError` (5xx). The adapter handles each
distinctly. No swallowed errors.

**Configuration** (read from env, with `app/config/settings.py` wiring):

- `KALSHI_API_KEY_ID`
- `KALSHI_PRIVATE_KEY_PATH`
- `KALSHI_BASE_URL` (defaults to `https://api.elections.kalshi.com`; can point
  at the demo environment)

### 5.2 SQL ledger — `app/db/models/trading.py` and `app/trading/sql_ledger.py` (new)

**New SQLAlchemy models** extending the existing `Base`:

```
trading_orders
  intent_id (PK, str), kalshi_order_id (str, nullable), market_symbol,
  market_key, side, stake, status, message, created_at, updated_at

trading_fills
  fill_id (PK, str), intent_id (FK), market_symbol, market_key, side,
  stake, price, fee, realized_pnl, kalshi_trade_id (str), filled_at

trading_positions
  market_symbol (PK), market_key, side, open_stake, weighted_price_total,
  realized_pnl, updated_at

trading_kill_switch
  id (PK, always 1), killed (bool), set_at, set_by

trading_daily_pnl
  date (PK), realized_pnl
```

**`SqlPortfolioLedger`** implements the existing `PortfolioLedger` Protocol
exactly. Same method signatures as `InMemoryPortfolioLedger`, so `TradingLoop`
does not change. All writes happen in a single `with SessionLocal.begin()`
block per fill so a crash mid-write either commits the whole fill or none of
it.

**Idempotency.** `record_fill()` checks if `fill_id` already exists; if so,
no-op + log. Lets us safely retry on transient failures without
double-counting.

### 5.3 `KalshiAdapter` — `app/trading/kalshi_adapter.py` (new)

Implements `ExchangeAdapter` Protocol. ~120 LoC.

**`place_order(intent)` flow:**

1. Resolve to Kalshi ticker via `SymbolResolver.resolve(intent)`. The resolver
   reads structured fields from `intent.signal`:
   `(market_key, side, line_value, player_id from metadata, game_date)`. See
   §5.4 for how `game_date` is derived. If unresolved, return
   `OrderEvent(event_type="rejected", message="no kalshi ticker")` and empty
   fills.
2. Call `KalshiClient.get_market(ticker)` to confirm tradable and capture
   current best price (used for the slippage-cap calculation and contract-count
   math).
3. Translate `intent.stake` (USD) into Kalshi contract count:
   `count = floor(stake_dollars / contract_price_dollars)`. If count < 1,
   reject with `event_type="rejected", message="contract price exceeds stake cap"`.
4. Build a unique `client_order_id` from `intent.intent_id + retry_attempt` for
   Kalshi-side idempotency.
5. Call `KalshiClient.create_order(...)` with `type="market"`,
   `side="yes"|"no"` (mapped from OVER/UNDER), `count`, `client_order_id`.
6. Poll `get_order(order_id)` every 250 ms for up to 5 s. Terminal states emit
   `Fill`(s) and `OrderEvent`(s).
7. Any exception from the client emits
   `OrderEvent(event_type="error", status="failed", message=<typed>)`, no fill,
   and propagates so the loop can apply cooldown via the existing
   `ExposureRiskEngine` semantics.

### 5.4 `SymbolResolver` — `app/trading/symbol_resolver.py` (new, hand-curated for Spec 1)

Deliberately dumb. Loads a JSON file at `config/kalshi_symbols.json`
(gitignored, operator-maintained):

```json
[
  {
    "market_key": "points",
    "side": "over",
    "line_value": 25.5,
    "player_id": 237,
    "game_date": "2026-05-06",
    "kalshi_ticker": "KXNBASGPL-26MAY06LAL-LEBRON-OPTS25"
  }
]
```

JSON chosen over YAML to avoid adding a new top-level dependency.

Lookup is exact match on the five-tuple key. No fuzzy matching, no
auto-discovery — that is Spec 2's job. Operator hand-builds 5–10 entries
before each run.

**`game_date` derivation.** The existing `Signal` dataclass does not carry
`game_date`. The resolver derives it from
`signal.metadata.get("game_date") or signal.created_at.date().isoformat()`.
Spec 1 also adds `game_date` to the metadata dict produced by
`TradingLoop._decision_to_signal()` (one-line change) so callers can pass it
explicitly when the run date differs from the game date (rare but possible
for late-night runs spanning UTC midnight).

### 5.5 CLI wiring — `scripts/run_trading_loop.py` (new) and `app/trading/loop.py` (minor edit)

New top-level script (separate from `loop.py`'s `main()` so the existing paper
CLI is untouched):

```
python scripts/run_trading_loop.py --live --decisions today.json
```

Behavior:

- Requires `--live` AND env-var `KALSHI_LIVE_TRADING=1`. Belt-and-suspenders to
  prevent accidental live runs.
- Prints a confirmation banner: current Kalshi balance, daily-cap remaining,
  kill-switch state, and ticker count from the resolver. Waits for `y`
  keypress.
- Instantiates `SqlPortfolioLedger`, `ExposureRiskEngine` with limits loaded
  from `config/trading_limits.json`, and `KalshiAdapter`.
- Calls `TradingLoop.run_decisions(...)`. Prints summary.

`loop.py` change: `TradingLoop` checks the kill-switch table at the start of
each iteration (one extra DB read per signal) so an operator hitting the
existing `/api/trading/kill-switch` endpoint while a CLI run is mid-loop
actually halts it.

### 5.6 Adjustable limits via config file

New file: `config/trading_limits.json` (gitignored, operator-owned, with a
checked-in `config/trading_limits.example.json`).

```json
{
  "per_order_cap": 0.25,
  "per_market_cap": 0.50,
  "max_open_notional": 2.00,
  "daily_loss_cap": 2.00,
  "reject_cooldown_seconds": 300
}
```

- Loaded at CLI startup. If the file is missing, the live CLI refuses to start
  with a clear error message pointing at the example file.
- Constant `LIVE_RISK_LIMITS_FALLBACK` in code is used only by tests, never by
  the live CLI, so a missing config can never silently fall back to defaults.
- Existing `/api/trading/pnl` endpoint is extended to include the active
  limits as read-only fields (`active_limits: {...}`). This lets the desktop
  UI display the live values immediately, with no editing affordance — the
  edit UI is Spec 2.

## 6. Safety surface

### 6.1 Live risk limits (defaults shipped in `trading_limits.example.json`)

```python
RiskLimits(
    per_order_cap=0.25,        # $0.25 per order, hard
    per_market_cap=0.50,       # $0.50 cumulative per Kalshi ticker
    max_open_notional=2.00,    # $2.00 across all open positions
    daily_loss_cap=2.00,       # halt for the day at -$2.00 realized
    reject_cooldown_seconds=300,
)
```

These are loaded from config rather than hard-coded. The paper-mode defaults
in `app/trading/risk.py` are unchanged so paper and live paths cannot bleed
into each other.

### 6.2 Kill switch reach-through

- The existing `/api/trading/kill-switch` POST endpoint is extended to also
  write to the new `trading_kill_switch` SQL row (today it only flips an
  in-process flag on `ExposureRiskEngine`).
- `TradingLoop.run_signals()` reads the SQL row before each signal. One small
  DB hit; cost is negligible vs the network round-trip.
- The CLI installs a `SIGINT` handler that flips the kill-switch row before
  exit, so Ctrl-C halts cleanly even if the loop is mid-iteration.

### 6.3 Error handling matrix

| Failure                           | Adapter behavior                          | Loop behavior                                        |
|-----------------------------------|-------------------------------------------|------------------------------------------------------|
| `KalshiAuthError`                 | Raise to loop                             | Halt run immediately, exit non-zero                  |
| `KalshiInsufficientFunds`         | Emit `error` event                        | Halt run, surface clearly                            |
| `KalshiMarketError` (404 ticker)  | Emit `rejected` event, no fill            | Skip signal, continue                                |
| `KalshiRateLimited`               | Sleep `Retry-After`, retry once           | Continue if retry succeeds, else `error`             |
| `KalshiServerError` (5xx)         | Retry once with 1 s backoff, then `error` | Cooldown applies via `ExposureRiskEngine`            |
| `httpx.TimeoutError` mid-poll     | Emit `error`; do **not** assume filled    | Surface for manual reconciliation                    |
| Process crash mid `record_fill`   | Wrapped in transaction; all-or-nothing    | On restart, fill is either fully there or fully gone |
| Local SQL row exists, Kalshi has none | Manual reconciliation only in Spec 1  | Operator-runbook step; Spec 2 adds the poller        |

**No silent failures.** Every error path emits a `rejected` or `error`
`OrderEvent` that lands in `trading_orders`.

## 7. Testing strategy

**Unit (pytest, no network):**

- `test_kalshi_signing.py` — sign a known payload with a fixture RSA key,
  assert byte-exact signature against a hand-computed expected value.
- `test_kalshi_client.py` — mock httpx transport, assert each typed exception
  is raised on the right status/body.
- `test_sql_ledger.py` — `SqlPortfolioLedger` against an in-memory SQLite
  engine, mirror existing `InMemoryPortfolioLedger` test cases plus
  crash-recovery and idempotency tests.
- `test_kalshi_adapter.py` — mocked `KalshiClient`, assert intent → ticker →
  order-call mapping, count calculation, partial-fill emission, error-path
  event shape.
- `test_symbol_resolver.py` — exact-match hits, miss returns `None`, malformed
  YAML rejected at load.
- `test_trading_limits_config.py` — config loads, missing file refuses to
  start, malformed JSON rejected.

**Integration (pytest, gated by env var, demo env):**

- `test_kalshi_demo_smoke.py` — marked `@pytest.mark.integration`, runs only
  when `KALSHI_DEMO_KEY_ID` env is set. Hits Kalshi demo, places a $0.25 demo
  order, verifies fill, asserts ledger row matches. The single end-to-end test
  that proves the full wire.

**Manual acceptance** (in operator runbook, not automated):

1. Set demo creds, run
   `scripts/run_trading_loop.py --live --decisions sample.json` against demo,
   see fill in UI.
2. Mid-run, hit `/api/trading/kill-switch`, confirm next signal is rejected.
3. Flip env to live creds + production base URL, repeat with one signal on a
   real ticker.

## 8. New dependencies

Confirmed against `pyproject.toml`:

- `cryptography` — **must be added** (RSA-PSS signing).
- `httpx>=0.28.0` — already present.
- Symbol map uses JSON (stdlib), no YAML dep needed.

## 9. File-level diff summary

**New files:**

- `app/providers/exchanges/__init__.py`
- `app/providers/exchanges/kalshi_client.py`
- `app/db/models/trading.py`
- `app/trading/sql_ledger.py`
- `app/trading/kalshi_adapter.py`
- `app/trading/symbol_resolver.py`
- `app/trading/live_limits.py` (loads `config/trading_limits.json`)
- `scripts/run_trading_loop.py`
- `config/trading_limits.example.json`
- `config/kalshi_symbols.example.json`
- Tests under `tests/unit/trading/` and `tests/integration/trading/`

**Modified files:**

- `app/trading/loop.py` — add SQL kill-switch read at iteration start; add
  `game_date` to the metadata produced by `_decision_to_signal()`.
- `app/server/routers/trading.py` — extend kill-switch endpoint to write SQL
  row; extend `/pnl` endpoint to include active limits.
- `app/server/schemas/trading.py` — add `active_limits` field on
  `TradingPnlModel`.
- `app/config/settings.py` — add Kalshi env-var fields.
- `.gitignore` — add `config/trading_limits.json`,
  `config/kalshi_symbols.json`.
- `pyproject.toml` — pin any newly required deps.

**Untouched (deliberately):**

- `app/trading/protocols.py`, `app/trading/types.py`, `app/trading/risk.py`,
  `app/trading/ledger.py` (the in-memory one stays for paper / unit tests),
  `app/trading/paper_adapter.py`, `app/trading/mapper.py`,
  `app/trading/pricing.py`.

## 10. Acceptance criteria for Spec 1

Spec 1 is "done" when **all** of the following hold:

1. Unit tests pass (`pytest tests/unit/trading/`).
2. Integration smoke test passes against Kalshi demo
   (`pytest -m integration tests/integration/trading/`).
3. Operator can run
   `KALSHI_LIVE_TRADING=1 python scripts/run_trading_loop.py --live --decisions today.json`
   against the live Kalshi API and observe **one** real fill recorded in
   SQLite.
4. Hitting `POST /api/trading/kill-switch` mid-run causes the next iteration
   to halt.
5. Active limits are visible read-only in the desktop UI (via the existing
   `/api/trading/pnl` endpoint) and reflect what is in
   `config/trading_limits.json`.
6. No path in the code can place a live Kalshi order without **both** the
   `--live` CLI flag AND the `KALSHI_LIVE_TRADING=1` env var being set.

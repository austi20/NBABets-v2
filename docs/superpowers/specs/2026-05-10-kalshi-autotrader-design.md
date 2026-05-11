# Kalshi Autonomous Trader — Umbrella Design + Sub-project 1 Spec

**Date:** 2026-05-10
**Status:** approved (Q&A locked, awaiting spec review)
**Worktree:** `.claude/worktrees/stoic-proskuriakova-0f9c71`

This document covers two things:

1. **Umbrella design** for the full autonomous Kalshi trading system, including the locked-in product and architecture decisions.
2. **Sub-project 1 spec** (Kalshi WebSocket consumer + `MarketBook`), which is the first piece to build. Sub-projects 2–7 each get their own spec when their turn comes.

---

## 1. Context

The repo already has a complete Kalshi trading toolkit (`app/trading/` — 18 modules), an exchange adapter, an idempotent SQL ledger, a risk engine, a kill switch, and a guarded one-shot live execution script. What it lacks is the connective tissue that turns these pieces into a continuous, hands-off bot:

- The live runner (`scripts/run_trading_loop.py`) is intentionally one-shot ("Spec 1 cap: using only the first decision; remaining decisions ignored").
- `TradingLoop.run_signals()` iterates a static list once and returns. No scheduler.
- Decisions are sourced from a static JSON file rebuilt off-line.
- The frontend Trading tab uses HTTP polling (`refetchInterval: 3_000` for snapshot, 5s for fills/pnl/readiness).
- There is no WebSocket usage anywhere in the repo. Kalshi v2 *does* expose one at `wss://api.elections.kalshi.com/trade-api/ws/v2` with channels `ticker`, `trade`, `orderbook_delta`, `fill`, `market_positions`.
- The brain (`app/services/brain/`) writes Obsidian notes but no code reads them back into decisions.
- The local LLM (currently Qwen3-1.7b on `:8080`) is used for unrelated summaries; it is **not** wired into prop evaluation.

**Goal of the umbrella project:** turn the existing components into a fully automated Kalshi contract trader that the user only starts and stops, fed by live market data, gated by a local LLM, informed by the brain, and surfaced in the Trading tab via push (not polling).

## 2. Locked-in design decisions (umbrella)

These were resolved in the brainstorming Q&A and apply to every sub-project below.

| Decision | Choice | Reason |
|---|---|---|
| Loop start mode | **Explicit start button.** Loop initialized but idle on sidecar boot; runs until Stop or kill switch. | Keeps an obvious gate during the trust-building phase. Autostart can be added later as a setting. |
| LLM role in v1 | **Hard veto only.** Quant signal → LLM reviews context → returns approve/reject. Stake size unchanged. Structured logging of `{decision, confidence, reasoning}` per evaluation. | Gives clean attribution data: every trade is an A/B sample of LLM-on vs LLM-off outcomes. Graduate to confidence-weighted stakes (see §6.4) once data shows the score is meaningful. |
| LLM model | **Qwen 3.5 8B**, replacing the current Qwen3-1.7b reference. Model is already downloaded and running locally outside this repo. **It is not currently wired into this project's code.** Wiring is part of sub-project #4. | Better structured-output reliability and multi-fact reasoning; latency is acceptable because the gate is only invoked on candidates that already cleared the quant edge threshold. |
| Brain read pattern | **Read-at-decision-time + LRU cache** (5 min TTL). New `app/services/brain/vault_reader.py` parses `Market Profiles/<market_key>.md` and `Strategies/*.md`. | Simple semantics, no manual refresh, naturally invalidating cache. |
| Process architecture | **In-process FastAPI lifespan task with Tauri watchdog.** Trading service is an `asyncio` task started in `lifespan`. Tauri auto-restarts the sidecar on crash. | Single process matches the local-first design. Shares DB/ledger/risk-engine in memory. Watchdog gives crash isolation without IPC complexity. |
| Frontend push channel | **SSE.** `GET /api/trading/stream` emits events. Trading tab swaps polling for `EventSource`. UI commands keep using `POST` endpoints. | The Trading tab is fundamentally a display. SSE is dramatically simpler than WS, has built-in auto-reconnect, and is debuggable with `curl`. |

## 3. Umbrella architecture (target end-state)

```
                         ┌────────────────────────────────────────────────────┐
                         │                  FastAPI sidecar                   │
                         │                                                    │
  Kalshi WS ─────────────►  KalshiWebSocketConsumer ─────► MarketBook ─┐      │
  (ticker, orderbook,    │                                  (in-memory) │     │
   fill, positions)      │                                              │     │
                         │                                              ▼     │
                         │                                  Decision evaluator│
                         │                                  (quant signal)    │
  Vault (Obsidian) ──────►  vault_reader (LRU cache) ─────►       │           │
                         │                                              │     │
  Local LLM (Qwen 3.5 8B)│  llm_gate ────────────────────────────►      │     │
                         │                                              ▼     │
                         │                                       ExposureRiskEngine
                         │                                              │     │
                         │                                              ▼     │
                         │                                       KalshiAdapter
                         │                                              │     │
                         │                                              ▼     │
                         │                  SqlPortfolioLedger / TradingKillSwitch
                         │                                              │     │
                         │  ┌──────────────── EventBus ─────────────────┘     │
                         │  │                                                  │
                         │  ▼                                                  │
  Browser (Trading tab) ◄── SSE /api/trading/stream                            │
                         └────────────────────────────────────────────────────┘
                              ▲
                              │ POST /api/trading/loop/start | /loop/stop | /kill-switch
                              │
                         Tauri webview UI (start button, kill switch, live displays)
```

Existing modules that stay as-is and are reused: `KalshiClient` (REST for orders + balance), `KalshiAdapter`, `ExposureRiskEngine`, `SqlPortfolioLedger`, `TradingKillSwitch`, `signal_to_market_ref`, `kalshi_signing.sign_request`.

## 4. Sub-project sequence

Each sub-project gets its own brainstorm → spec → plan → implement → verify cycle. Numbered in build order.

| # | Sub-project | Delivers | Effort |
|---|---|---|---|
| 1 | **Kalshi WS consumer + MarketBook** | Live in-memory bid/ask/last/spread per tracked ticker. Auto-reconnect. No trading changes. | M |
| 2 | **SSE push channel** | `GET /api/trading/stream` emits events. Trading tab uses `EventSource`. UI live without bot trading. | M |
| 3 | **Continuous loop service** | Replaces one-shot script. Asyncio task in FastAPI lifespan. Start/Stop endpoints. Pure quant pipeline. Paper adapter first. | L |
| 4 | **LLM veto gate** | Wires Qwen 3.5 8B as veto-only step in the signal pipeline. Structured prompt → JSON. Logs everything. | M |
| 5 | **Brain read path** | `vault_reader.py` with LRU cache. Plugs into signal pipeline before LLM. | S |
| 6 | **Lifecycle + safety** | Tauri watchdog for sidecar restart. Start button in Trading tab. Live-mode env gate carried forward. Push-driven kill switch UI. | M |
| 7 | **Trading tab redesign** | Final UX pass: live markets/props/P&L panel, loop status banner, activity log. | M |

The bot starts running in **paper mode after #3**. Live Kalshi mode unlocks at #6 once safety is in.

---

## 5. Sub-project 1 spec — Kalshi WebSocket Consumer + `MarketBook`

### 5.1 Goal

Provide live, in-memory Kalshi market data to the rest of the system. After this is shipped:

- The sidecar can subscribe to Kalshi WS for our tracked tickers.
- Anywhere in the app code that needs a live quote can read from `MarketBook` instead of polling the REST market-data client.
- The Trading tab is unchanged in this sub-project; sub-project #2 wires SSE to push these updates to the frontend.

### 5.2 Non-goals

- No trading loop changes. The continuous loop is sub-project #3.
- No SSE channel. That is sub-project #2.
- No LLM, no brain reads. Those are sub-projects #4 and #5.
- No frontend changes.
- No new orders, no risk-engine changes, no schema changes.

### 5.3 Components

**`app/trading/ws_consumer.py` — `KalshiWebSocketConsumer`**

- Async class. Connects to `wss://api.elections.kalshi.com/trade-api/ws/v2` (prod) or `wss://demo-api.kalshi.co/trade-api/ws/v2` (demo) via setting.
- Authenticates via existing RSA-PSS signing — reuse `app/providers/exchanges/kalshi_signing.py`. Confirm during build whether Kalshi requires the WS handshake to be signed (the REST signer should work directly since the path/method/timestamp model is identical) or whether they accept signed first message instead. Spec assumes handshake-time signing; build will adjust if Kalshi specifies otherwise.
- Subscribes to channels `ticker` and `orderbook_delta` for a configurable list of tickers passed in at start.
- Emits structured `BookEvent`s to a callback (or `asyncio.Queue`).
- Auto-reconnect with exponential backoff: 1s, 2s, 4s, 8s, 16s, capped at `kalshi_ws_max_backoff_seconds`.
- Handles ping/pong keepalive at `kalshi_ws_ping_interval_seconds`.

**`app/trading/market_book.py` — `MarketBook`**

- In-memory async-safe store. One `MarketEntry` per ticker:
  ```python
  @dataclass(frozen=True)
  class MarketEntry:
      ticker: str
      yes_bid: float | None
      yes_ask: float | None
      no_bid: float | None
      no_ask: float | None
      last: float | None
      spread: float | None
      status: str            # "open" | "closed" | "settled" | "unknown"
      updated_at: datetime
  ```
- Public API:
  - `update(entry: MarketEntry) -> BookUpdate` — atomic replace; returns the diff.
  - `get(ticker: str) -> MarketEntry | None`
  - `snapshot() -> dict[str, MarketEntry]` — copy of current state.
  - `subscribe() -> AsyncIterator[BookUpdate]` — async generator yielding deltas. Backed by an `asyncio.Queue` per subscriber so a slow consumer cannot block fast ones (drop oldest with bounded queue if subscriber falls behind).
- Protected by an `asyncio.Lock` for the rare case of overlapping updates within the same task. No threading concerns (single asyncio loop).

**`app/trading/ws_service.py` — `KalshiMarketService`**

- Lifecycle wrapper that owns a `KalshiWebSocketConsumer` + a `MarketBook`.
- `async start() -> None` / `async stop() -> None` — used from FastAPI `lifespan`.
- Tracker list: read from `config/kalshi_symbols.json` (existing, executable rows only) at start. If empty, do not connect (idle service). `add_ticker()` / `remove_ticker()` available for sub-project #3.
- Health surface for `/api/trading/readiness`:
  - `is_connected() -> bool`
  - `last_message_at: datetime | None`
  - `reconnect_count: int`
  - `consecutive_auth_failures: int`

### 5.4 Data flow

```
Kalshi WS server
        │ json frames
        ▼
KalshiWebSocketConsumer
        │ parse + validate
        ▼
MarketBook.update()
        │
        ▼
BookUpdate fan-out (async generator)
        │
        ├──► (sub-project 2) SSE fan-out to frontend
        └──► (sub-project 3) decision loop reads snapshot per signal
```

In sub-project 1, the fan-out has zero subscribers — that is fine. The plumbing exists; later sub-projects connect to it.

### 5.5 Lifecycle integration

Add a `lifespan` async context manager to `app/server/main.py` (does not exist yet — current `create_app` only wires routers):

```python
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    market_service = KalshiMarketService(...)
    app.state.market_service = market_service
    if settings.kalshi_ws_enabled:
        await market_service.start()
    try:
        yield
    finally:
        await market_service.stop()
```

This is a generic lifespan; future sub-projects add their own startup tasks alongside the market service.

### 5.6 Settings additions (`app/config/settings.py`)

```python
kalshi_ws_enabled: bool = False                       # off by default
kalshi_ws_base_url: str = "wss://api.elections.kalshi.com/trade-api/ws/v2"
kalshi_ws_max_backoff_seconds: int = 30
kalshi_ws_ping_interval_seconds: int = 10
kalshi_ws_max_consecutive_auth_failures: int = 5
```

When `kalshi_ws_enabled=False`, behavior is identical to today — the `lifespan` does not call `start()`. This is the default to keep current tests and runs unchanged.

### 5.7 Error handling

- **Connection failures:** backoff + reconnect, never raise out of the service. Log + increment `reconnect_count`.
- **Auth failures (401/403 on handshake):** log loudly, increment `consecutive_auth_failures`, stop reconnecting after `kalshi_ws_max_consecutive_auth_failures` (default 5). Auth failure means key rotation or env misconfiguration; silent retry would burn time and rate budget.
- **Malformed frames:** log at warning, skip. Do not crash the consumer.
- **WS server-side close (1001, 1006):** treat as transient, reconnect.
- **Unknown channel/event types:** log at debug, skip.
- **Slow subscriber:** bounded queue per subscriber, drop oldest update when full. Log at info.

### 5.8 Testing

| Layer | Test |
|---|---|
| Unit — `MarketBook` | concurrent `update` + `get` consistency, snapshot is a copy not a reference, subscribe/unsubscribe lifecycle, bounded queue drops oldest under backpressure |
| Unit — frame parser | valid `ticker` frames, valid `orderbook_delta` frames, malformed JSON, missing required fields |
| Integration — fake WS server | subscribe → receive → book update cycle |
| Integration — reconnect | server closes mid-stream, consumer reconnects with backoff and resubscribes |
| Integration — auth failure | server returns 401, consumer stops after threshold and exposes `consecutive_auth_failures = N` |
| Integration — lifespan | `kalshi_ws_enabled=False` does not connect; `=True` starts service; shutdown closes cleanly |
| Smoke | manual run against Kalshi demo env (`wss://demo-api.kalshi.co/trade-api/ws/v2`) with a real ticker |

Use `httpx_ws` or the `websockets` library's test fixtures for the fake server. Tests must not hit the real Kalshi network.

### 5.9 Build order within sub-project 1

1. `MarketBook` data class + tests (no IO, pure logic).
2. Frame parser tests (parse Kalshi WS message shapes from fixtures).
3. `KalshiWebSocketConsumer` against fake WS server.
4. `KalshiMarketService` lifecycle wrapper.
5. FastAPI `lifespan` integration (off by default).
6. Manual smoke test against Kalshi demo env.

### 5.10 Acceptance criteria

- [ ] With `kalshi_ws_enabled=true` and at least one executable ticker in `config/kalshi_symbols.json`, sidecar starts a WS connection to Kalshi demo on boot.
- [ ] `MarketBook.snapshot()` returns live data within 5 seconds of sidecar start (assuming demo market is open).
- [ ] Force-disconnecting the WS server triggers auto-reconnect within `kalshi_ws_max_backoff_seconds`.
- [ ] All unit tests pass; integration tests pass against the fake WS server.
- [ ] No changes to existing trading endpoints, paper trading, or live trading guards.
- [ ] Sidecar shutdown cleanly closes the WS connection.
- [ ] Setting flag off restores current behavior exactly (no new connections, no new logs, all existing tests still pass).

### 5.11 Risks and mitigations

| Risk | Mitigation |
|---|---|
| Kalshi WS handshake auth differs from REST signing | Verify against Kalshi docs during build (`docs.kalshi.com/getting_started/quick_start_websockets`); spec leaves room to adjust. Smoke against demo before declaring done. |
| Demo env market data is sparse / different from prod | Add a small synthetic-frame test that bypasses the network; rely on demo only for smoke. |
| Slow consumer blocks fast updates | Bounded per-subscriber queue with drop-oldest; logged when triggered. |
| Reconnect storm against Kalshi if creds break | `kalshi_ws_max_consecutive_auth_failures` threshold halts retry; surfaces in `/readiness`. |

---

## 6. Appendix — frozen umbrella decisions (reference)

### 6.1 Out-of-scope for v1 (sub-projects 1–7)

- Multi-exchange support beyond Kalshi
- Backtesting against the new live loop (existing `app/evaluation/backtest.py` keeps working independently)
- Multi-account/multi-tenant
- Mobile app

### 6.2 Performance budget (informational)

- WS frame → MarketBook update: target < 5ms
- Quant signal evaluation per candidate: target < 100ms
- LLM veto call: budget 3–15s (only invoked on candidates above edge threshold; expected cadence is single-digit per day)
- SSE event push: target < 50ms after MarketBook update

### 6.3 Open questions deferred to later sub-projects

- (#3) Trigger cadence for the decision loop: per book update for tracked tickers, or every N seconds?
- (#3) Maximum concurrent open positions across all tickers (separate from the existing per-market and total-notional caps).
- (#4) Exact LLM prompt template and what context it receives (player news? line history? our quant edge?).
- (#5) Which vault notes are loaded by default for a given decision (market profile, strategy, latest correction history?).
- (#6) Sidecar watchdog: Tauri-side restart policy (max retries per minute, backoff, user notification on repeated failure).
- (#7) UI behavior when SSE disconnects (banner, manual reconnect, automatic).

### 6.4 Graduation criteria for LLM v1 → v2

After sub-project #4 ships and at least 50 LLM-evaluated decisions have outcomes (paper or live), evaluate:

- Does LLM rejection correlate with negative EV? (If yes, the veto is earning its keep.)
- Does LLM confidence score correlate with hit rate? (If yes, graduate to confidence-weighted stakes per Option C in the original Q&A.)
- Are rejections systematically wrong on certain prop types? (If yes, prompt-engineer or carve out exemptions.)

Graduation is a separate spec/sub-project, not part of #1–7.

# Kalshi Live Trading — Spec 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the existing trading scaffolding to a real Kalshi account so a CLI invocation can place one $0.25 NBA contract order, persist the fill in SQLite, and respect the kill switch — with limits loaded from a tunable config file.

**Architecture:** Drop-in implementations of the existing `ExchangeAdapter` and `PortfolioLedger` Protocols. New `KalshiClient` (httpx + RSA-PSS signing) wrapped by `KalshiAdapter`. New `SqlPortfolioLedger` reuses the existing SQLAlchemy engine. Hand-curated JSON symbol map. Live limits live in `config/trading_limits.json`. CLI requires both `--live` flag and `KALSHI_LIVE_TRADING=1` env var.

**Tech Stack:** Python 3.12, httpx, SQLAlchemy 2.x, cryptography (RSA-PSS), pytest. No new top-level deps beyond `cryptography`.

**Spec:** [docs/superpowers/specs/2026-05-06-kalshi-live-trading-spec1-design.md](../specs/2026-05-06-kalshi-live-trading-spec1-design.md)

---

## File map

**New files (production):**

- `app/providers/exchanges/__init__.py`
- `app/providers/exchanges/kalshi_client.py`
- `app/providers/exchanges/kalshi_signing.py`
- `app/providers/exchanges/kalshi_errors.py`
- `app/db/models/__init__.py` (if not present)
- `app/db/models/trading.py`
- `app/trading/sql_ledger.py`
- `app/trading/kalshi_adapter.py`
- `app/trading/symbol_resolver.py`
- `app/trading/live_limits.py`
- `scripts/run_trading_loop.py`
- `config/trading_limits.example.json`
- `config/kalshi_symbols.example.json`

**New files (tests):**

- `tests/unit/test_kalshi_signing.py`
- `tests/unit/test_kalshi_client.py`
- `tests/unit/test_kalshi_errors.py`
- `tests/unit/test_sql_portfolio_ledger.py`
- `tests/unit/test_kalshi_adapter.py`
- `tests/unit/test_symbol_resolver.py`
- `tests/unit/test_live_limits.py`
- `tests/unit/test_trading_loop_killswitch.py`
- `tests/integration/test_kalshi_demo_smoke.py`

**Modified files:**

- `pyproject.toml` — add `cryptography>=43.0.0` to `dependencies`.
- `app/config/settings.py` — add `kalshi_api_key_id`, `kalshi_private_key_path`, `kalshi_base_url`, `kalshi_live_trading` fields.
- `app/trading/loop.py` — DB kill-switch check at iteration start; populate `game_date` in `_decision_to_signal` metadata.
- `app/server/routers/trading.py` — kill-switch endpoint writes SQL row; `/pnl` returns `active_limits`.
- `app/server/schemas/trading.py` — `TradingPnlModel` gains `active_limits` field.
- `.gitignore` — add `config/trading_limits.json` and `config/kalshi_symbols.json`.

---

## Task 1: Add `cryptography` dep and create config example files

**Files:**
- Modify: `pyproject.toml`
- Create: `config/trading_limits.example.json`
- Create: `config/kalshi_symbols.example.json`
- Modify: `.gitignore`

- [ ] **Step 1: Add `cryptography` to `pyproject.toml`**

In `pyproject.toml`, find the `dependencies = [` block and add the line (alphabetically before `fastapi`):

```toml
  "cryptography>=43.0.0",
```

- [ ] **Step 2: Install the dep**

Run:

```
pip install -e .[dev]
```

Expected: `cryptography` installed without error. Verify:

```
python -c "from cryptography.hazmat.primitives.asymmetric.padding import PSS; print('ok')"
```

Expected output: `ok`

- [ ] **Step 3: Create `config/trading_limits.example.json`**

```json
{
  "per_order_cap": 0.25,
  "per_market_cap": 0.50,
  "max_open_notional": 2.00,
  "daily_loss_cap": 2.00,
  "reject_cooldown_seconds": 300
}
```

- [ ] **Step 4: Create `config/kalshi_symbols.example.json`**

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

- [ ] **Step 5: Update `.gitignore`**

Append to `.gitignore`:

```
config/trading_limits.json
config/kalshi_symbols.json
```

- [ ] **Step 6: Commit**

```
git add pyproject.toml config/trading_limits.example.json config/kalshi_symbols.example.json .gitignore
git commit -m "chore: add cryptography dep and Kalshi config examples"
```

---

## Task 2: Add Kalshi settings fields

**Files:**
- Modify: `app/config/settings.py`

- [ ] **Step 1: Add new settings fields**

In `app/config/settings.py`, after the existing Field declarations in the `Settings` class (around the existing balldontlie fields if they exist, otherwise after `enable_provider_cache`), add:

```python
    kalshi_api_key_id: str = Field(default="", alias="KALSHI_API_KEY_ID")
    kalshi_private_key_path: str = Field(default="", alias="KALSHI_PRIVATE_KEY_PATH")
    kalshi_base_url: str = Field(
        default="https://api.elections.kalshi.com",
        alias="KALSHI_BASE_URL",
    )
    kalshi_live_trading: bool = Field(default=False, alias="KALSHI_LIVE_TRADING")
    trading_limits_path: str = Field(
        default="config/trading_limits.json",
        alias="TRADING_LIMITS_PATH",
    )
    kalshi_symbols_path: str = Field(
        default="config/kalshi_symbols.json",
        alias="KALSHI_SYMBOLS_PATH",
    )
```

- [ ] **Step 2: Verify settings load**

Run:

```
python -c "from app.config.settings import get_settings; s = get_settings(); print(s.kalshi_base_url)"
```

Expected output: `https://api.elections.kalshi.com`

- [ ] **Step 3: Commit**

```
git add app/config/settings.py
git commit -m "feat(config): add Kalshi and trading-limits settings"
```

---

## Task 3: Live-limits loader

**Files:**
- Create: `app/trading/live_limits.py`
- Test: `tests/unit/test_live_limits.py`

- [ ] **Step 1: Write failing test**

Create `tests/unit/test_live_limits.py`:

```python
from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.trading.live_limits import LimitsConfigError, load_live_limits
from app.trading.risk import RiskLimits


def test_load_live_limits_success(tmp_path: Path) -> None:
    config_file = tmp_path / "limits.json"
    config_file.write_text(
        json.dumps(
            {
                "per_order_cap": 0.25,
                "per_market_cap": 0.50,
                "max_open_notional": 2.00,
                "daily_loss_cap": 2.00,
                "reject_cooldown_seconds": 300,
            }
        )
    )
    limits = load_live_limits(config_file)
    assert isinstance(limits, RiskLimits)
    assert limits.per_order_cap == 0.25
    assert limits.daily_loss_cap == 2.00


def test_load_live_limits_missing_file_raises(tmp_path: Path) -> None:
    missing = tmp_path / "nope.json"
    with pytest.raises(LimitsConfigError, match="not found"):
        load_live_limits(missing)


def test_load_live_limits_malformed_json_raises(tmp_path: Path) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text("{not json")
    with pytest.raises(LimitsConfigError, match="malformed"):
        load_live_limits(bad)


def test_load_live_limits_missing_field_raises(tmp_path: Path) -> None:
    bad = tmp_path / "incomplete.json"
    bad.write_text(json.dumps({"per_order_cap": 0.25}))
    with pytest.raises(LimitsConfigError, match="missing field"):
        load_live_limits(bad)


def test_load_live_limits_negative_value_raises(tmp_path: Path) -> None:
    bad = tmp_path / "neg.json"
    bad.write_text(
        json.dumps(
            {
                "per_order_cap": -1.0,
                "per_market_cap": 0.50,
                "max_open_notional": 2.00,
                "daily_loss_cap": 2.00,
                "reject_cooldown_seconds": 300,
            }
        )
    )
    with pytest.raises(LimitsConfigError, match="must be"):
        load_live_limits(bad)
```

- [ ] **Step 2: Run test, verify failure**

```
pytest tests/unit/test_live_limits.py -v
```

Expected: ImportError on `app.trading.live_limits`.

- [ ] **Step 3: Implement loader**

Create `app/trading/live_limits.py`:

```python
from __future__ import annotations

import json
from pathlib import Path

from app.trading.risk import RiskLimits

_REQUIRED_FIELDS: tuple[str, ...] = (
    "per_order_cap",
    "per_market_cap",
    "max_open_notional",
    "daily_loss_cap",
    "reject_cooldown_seconds",
)


class LimitsConfigError(RuntimeError):
    """Raised when the live trading limits config cannot be loaded."""


def load_live_limits(path: Path | str) -> RiskLimits:
    config_path = Path(path)
    if not config_path.is_file():
        raise LimitsConfigError(
            f"trading limits config not found at {config_path}; "
            "copy config/trading_limits.example.json to that path and edit."
        )
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise LimitsConfigError(f"malformed JSON in {config_path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise LimitsConfigError(f"limits config in {config_path} must be a JSON object")
    for field in _REQUIRED_FIELDS:
        if field not in payload:
            raise LimitsConfigError(f"limits config missing field: {field}")
    for field in _REQUIRED_FIELDS:
        value = payload[field]
        if not isinstance(value, int | float):
            raise LimitsConfigError(f"limits field {field} must be a number")
        if value < 0:
            raise LimitsConfigError(f"limits field {field} must be >= 0")
    return RiskLimits(
        per_order_cap=float(payload["per_order_cap"]),
        per_market_cap=float(payload["per_market_cap"]),
        max_open_notional=float(payload["max_open_notional"]),
        daily_loss_cap=float(payload["daily_loss_cap"]),
        reject_cooldown_seconds=int(payload["reject_cooldown_seconds"]),
    )
```

- [ ] **Step 4: Run test, verify pass**

```
pytest tests/unit/test_live_limits.py -v
```

Expected: 5 passed.

- [ ] **Step 5: Commit**

```
git add app/trading/live_limits.py tests/unit/test_live_limits.py
git commit -m "feat(trading): live-limits config loader with strict validation"
```

---

## Task 4: SQLAlchemy models for trading tables

**Files:**
- Create: `app/db/models/__init__.py` (if not present)
- Create: `app/db/models/trading.py`
- Test: `tests/unit/test_trading_db_models.py`

- [ ] **Step 1: Check if `app/db/models/` directory exists**

Run:

```
python -c "import app.db.models" 2>&1 | head -5
```

If `ModuleNotFoundError`: create `app/db/models/__init__.py` as an empty file.

- [ ] **Step 2: Write failing test**

Create `tests/unit/test_trading_db_models.py`:

```python
from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db.base import Base
from app.db.models.trading import (
    TradingDailyPnL,
    TradingFill,
    TradingKillSwitch,
    TradingOrder,
    TradingPosition,
)


def test_trading_models_create_tables() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine, tables=[
        TradingOrder.__table__,
        TradingFill.__table__,
        TradingPosition.__table__,
        TradingKillSwitch.__table__,
        TradingDailyPnL.__table__,
    ])
    with Session(engine, future=True) as session:
        order = TradingOrder(
            intent_id="i1",
            kalshi_order_id=None,
            market_symbol="kalshi:points:over:25.5:g0:p237",
            market_key="points",
            side="OVER",
            stake=0.25,
            status="pending",
            message="created",
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
        )
        session.add(order)
        session.commit()
        loaded = session.get(TradingOrder, "i1")
        assert loaded is not None
        assert loaded.market_key == "points"


def test_kill_switch_singleton_row() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine, tables=[TradingKillSwitch.__table__])
    with Session(engine, future=True) as session:
        switch = TradingKillSwitch(id=1, killed=False, set_at=datetime.now(UTC), set_by="test")
        session.add(switch)
        session.commit()
        loaded = session.get(TradingKillSwitch, 1)
        assert loaded is not None
        assert loaded.killed is False
```

- [ ] **Step 3: Run test, verify failure**

```
pytest tests/unit/test_trading_db_models.py -v
```

Expected: ImportError on `app.db.models.trading`.

- [ ] **Step 4: Implement models**

Create `app/db/models/trading.py`:

```python
from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, Date, DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class TradingOrder(Base):
    __tablename__ = "trading_orders"

    intent_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    kalshi_order_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    market_symbol: Mapped[str] = mapped_column(String(255), index=True)
    market_key: Mapped[str] = mapped_column(String(64))
    side: Mapped[str] = mapped_column(String(16))
    stake: Mapped[float] = mapped_column(Float)
    status: Mapped[str] = mapped_column(String(32))
    message: Mapped[str] = mapped_column(String(512), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class TradingFill(Base):
    __tablename__ = "trading_fills"

    fill_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    intent_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("trading_orders.intent_id"), index=True
    )
    market_symbol: Mapped[str] = mapped_column(String(255), index=True)
    market_key: Mapped[str] = mapped_column(String(64))
    side: Mapped[str] = mapped_column(String(16))
    stake: Mapped[float] = mapped_column(Float)
    price: Mapped[float] = mapped_column(Float)
    fee: Mapped[float] = mapped_column(Float, default=0.0)
    realized_pnl: Mapped[float] = mapped_column(Float, default=0.0)
    kalshi_trade_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    filled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class TradingPosition(Base):
    __tablename__ = "trading_positions"

    market_symbol: Mapped[str] = mapped_column(String(255), primary_key=True)
    market_key: Mapped[str] = mapped_column(String(64))
    side: Mapped[str] = mapped_column(String(16))
    open_stake: Mapped[float] = mapped_column(Float, default=0.0)
    weighted_price_total: Mapped[float] = mapped_column(Float, default=0.0)
    realized_pnl: Mapped[float] = mapped_column(Float, default=0.0)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class TradingKillSwitch(Base):
    __tablename__ = "trading_kill_switch"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    killed: Mapped[bool] = mapped_column(Boolean, default=False)
    set_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    set_by: Mapped[str] = mapped_column(String(64), default="")


class TradingDailyPnL(Base):
    __tablename__ = "trading_daily_pnl"

    date: Mapped[datetime] = mapped_column(Date, primary_key=True)
    realized_pnl: Mapped[float] = mapped_column(Float, default=0.0)
```

- [ ] **Step 5: Run test, verify pass**

```
pytest tests/unit/test_trading_db_models.py -v
```

Expected: 2 passed.

- [ ] **Step 6: Commit**

```
git add app/db/models/__init__.py app/db/models/trading.py tests/unit/test_trading_db_models.py
git commit -m "feat(db): SQLAlchemy models for live trading tables"
```

---

## Task 5: SqlPortfolioLedger

**Files:**
- Create: `app/trading/sql_ledger.py`
- Test: `tests/unit/test_sql_portfolio_ledger.py`

- [ ] **Step 1: Write failing test**

Create `tests/unit/test_sql_portfolio_ledger.py`:

```python
from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.base import Base
from app.db.models.trading import (
    TradingDailyPnL,
    TradingFill,
    TradingKillSwitch,
    TradingOrder,
    TradingPosition,
)
from app.trading.sql_ledger import SqlPortfolioLedger
from app.trading.types import Fill, MarketRef, OrderEvent


@pytest.fixture()
def session_factory():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(
        engine,
        tables=[
            TradingOrder.__table__,
            TradingFill.__table__,
            TradingPosition.__table__,
            TradingKillSwitch.__table__,
            TradingDailyPnL.__table__,
        ],
    )
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def _market(symbol: str = "kalshi:points:over:25.5:g0:p237") -> MarketRef:
    return MarketRef(
        exchange="kalshi",
        symbol=symbol,
        market_key="points",
        side="OVER",
        line_value=25.5,
    )


def test_record_fill_creates_position(session_factory) -> None:
    ledger = SqlPortfolioLedger(session_factory)
    fill = Fill(
        fill_id="f1",
        intent_id="i1",
        market=_market(),
        side="buy",
        stake=0.25,
        price=0.40,
        fee=0.0,
        timestamp=datetime.now(UTC),
    )
    ledger.record_fill(fill)
    positions = ledger.open_positions()
    assert len(positions) == 1
    assert positions[0].open_stake == pytest.approx(0.25)
    assert positions[0].avg_price == pytest.approx(0.40)


def test_record_fill_idempotent(session_factory) -> None:
    ledger = SqlPortfolioLedger(session_factory)
    fill = Fill(
        fill_id="f1",
        intent_id="i1",
        market=_market(),
        side="buy",
        stake=0.25,
        price=0.40,
        timestamp=datetime.now(UTC),
    )
    ledger.record_fill(fill)
    ledger.record_fill(fill)  # second call must be a no-op
    assert ledger.open_positions()[0].open_stake == pytest.approx(0.25)


def test_market_exposure_and_open_notional(session_factory) -> None:
    ledger = SqlPortfolioLedger(session_factory)
    ledger.record_fill(
        Fill(fill_id="f1", intent_id="i1", market=_market("a"), side="buy", stake=0.25, price=0.40, timestamp=datetime.now(UTC))
    )
    ledger.record_fill(
        Fill(fill_id="f2", intent_id="i2", market=_market("b"), side="buy", stake=0.50, price=0.50, timestamp=datetime.now(UTC))
    )
    assert ledger.market_exposure("a") == pytest.approx(0.25)
    assert ledger.open_notional() == pytest.approx(0.75)


def test_record_order_event_persists(session_factory) -> None:
    ledger = SqlPortfolioLedger(session_factory)
    event = OrderEvent(intent_id="i1", event_type="rejected", status="blocked", message="kill switch")
    ledger.record_order_event(event)
    # Re-instantiate to prove persistence
    ledger2 = SqlPortfolioLedger(session_factory)
    # No public read API for events in Spec 1, but the order row should exist with status=blocked
    with session_factory() as session:
        order = session.get(TradingOrder, "i1")
        assert order is not None
        assert order.status == "blocked"
        assert "kill switch" in order.message
    _ = ledger2  # silence unused warning


def test_recent_fills_returns_in_reverse_order(session_factory) -> None:
    ledger = SqlPortfolioLedger(session_factory)
    base = datetime.now(UTC)
    for idx in range(3):
        ledger.record_fill(
            Fill(
                fill_id=f"f{idx}",
                intent_id=f"i{idx}",
                market=_market("a"),
                side="buy",
                stake=0.25,
                price=0.40,
                timestamp=base.replace(microsecond=idx),
            )
        )
    fills = ledger.recent_fills(limit=2)
    assert [f.fill_id for f in fills] == ["f2", "f1"]


def test_daily_realized_pnl_sums_today(session_factory) -> None:
    ledger = SqlPortfolioLedger(session_factory)
    market = _market()
    now = datetime.now(UTC)
    ledger.record_fill(Fill(fill_id="b1", intent_id="i1", market=market, side="buy", stake=1.0, price=0.40, timestamp=now))
    ledger.record_fill(Fill(fill_id="s1", intent_id="i2", market=market, side="sell", stake=1.0, price=0.50, timestamp=now))
    # 1.0 stake * (0.50 - 0.40) = 0.10 realized
    assert ledger.daily_realized_pnl() == pytest.approx(0.10)
```

- [ ] **Step 2: Run test, verify failure**

```
pytest tests/unit/test_sql_portfolio_ledger.py -v
```

Expected: ImportError on `app.trading.sql_ledger`.

- [ ] **Step 3: Implement `SqlPortfolioLedger`**

Create `app/trading/sql_ledger.py`:

```python
from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models.trading import (
    TradingDailyPnL,
    TradingFill,
    TradingOrder,
    TradingPosition,
)
from app.trading.protocols import PortfolioLedger
from app.trading.types import Fill, MarketRef, OrderEvent, Position


class SqlPortfolioLedger(PortfolioLedger):
    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self._session_factory = session_factory

    def record_order_event(self, event: OrderEvent) -> None:
        with self._session_factory() as session:
            order = session.get(TradingOrder, event.intent_id)
            now = event.timestamp
            if order is None:
                order = TradingOrder(
                    intent_id=event.intent_id,
                    kalshi_order_id=None,
                    market_symbol="",
                    market_key="",
                    side="",
                    stake=0.0,
                    status=event.status,
                    message=event.message,
                    created_at=now,
                    updated_at=now,
                )
                session.add(order)
            else:
                order.status = event.status
                order.message = event.message
                order.updated_at = now
            session.commit()

    def record_fill(self, fill: Fill) -> None:
        with self._session_factory() as session:
            existing = session.get(TradingFill, fill.fill_id)
            if existing is not None:
                return  # idempotent
            self._upsert_position(session, fill)
            row = TradingFill(
                fill_id=fill.fill_id,
                intent_id=fill.intent_id,
                market_symbol=fill.market.symbol,
                market_key=fill.market.market_key,
                side=fill.side,
                stake=float(fill.stake),
                price=float(fill.price),
                fee=float(fill.fee),
                realized_pnl=float(fill.realized_pnl),
                kalshi_trade_id=None,
                filled_at=fill.timestamp,
            )
            session.add(row)
            session.commit()

    def _upsert_position(self, session: Session, fill: Fill) -> None:
        market: MarketRef = fill.market
        position = session.get(TradingPosition, market.symbol)
        fill_stake = float(fill.stake)
        computed_realized = 0.0
        if position is None:
            position = TradingPosition(
                market_symbol=market.symbol,
                market_key=market.market_key,
                side=market.side,
                open_stake=0.0,
                weighted_price_total=0.0,
                realized_pnl=0.0,
                updated_at=fill.timestamp,
            )
            session.add(position)
        if fill.side == "sell":
            if fill_stake > position.open_stake + 1e-9:
                raise ValueError(
                    f"cannot sell stake {fill_stake:.4f} with only {position.open_stake:.4f} open on {market.symbol}"
                )
            avg_price = (
                position.weighted_price_total / position.open_stake
                if position.open_stake > 0
                else 0.0
            )
            closing = min(fill_stake, position.open_stake)
            if closing > 0:
                position.open_stake -= closing
                position.weighted_price_total = avg_price * position.open_stake
                computed_realized = (float(fill.price) - avg_price) * closing
        else:
            position.open_stake += fill_stake
            position.weighted_price_total += float(fill.price) * fill_stake
        effective_realized = float(fill.realized_pnl) + computed_realized
        position.realized_pnl += effective_realized - float(fill.fee)
        position.updated_at = fill.timestamp
        # Update daily PnL row
        fill_day = fill.timestamp.astimezone(UTC).date()
        daily = session.get(TradingDailyPnL, fill_day)
        if daily is None:
            daily = TradingDailyPnL(date=fill_day, realized_pnl=0.0)
            session.add(daily)
        daily.realized_pnl += effective_realized - float(fill.fee)

    def open_positions(self) -> list[Position]:
        with self._session_factory() as session:
            stmt = select(TradingPosition).where(TradingPosition.open_stake > 0)
            rows = session.execute(stmt).scalars().all()
            results = [
                Position(
                    market_symbol=row.market_symbol,
                    market_key=row.market_key,
                    side=row.side,
                    open_stake=round(row.open_stake, 4),
                    avg_price=round(
                        row.weighted_price_total / row.open_stake if row.open_stake > 0 else 0.0,
                        6,
                    ),
                    realized_pnl=round(row.realized_pnl, 4),
                    updated_at=row.updated_at,
                )
                for row in rows
            ]
            results.sort(key=lambda r: r.updated_at, reverse=True)
            return results

    def recent_fills(self, limit: int = 20) -> list[Fill]:
        bounded = max(0, int(limit))
        if bounded == 0:
            return []
        with self._session_factory() as session:
            stmt = (
                select(TradingFill)
                .order_by(TradingFill.filled_at.desc())
                .limit(bounded)
            )
            rows = session.execute(stmt).scalars().all()
            return [
                Fill(
                    fill_id=row.fill_id,
                    intent_id=row.intent_id,
                    market=MarketRef(
                        exchange="kalshi",
                        symbol=row.market_symbol,
                        market_key=row.market_key,
                        side=row.side,
                        line_value=0.0,
                    ),
                    side=row.side,
                    stake=row.stake,
                    price=row.price,
                    fee=row.fee,
                    realized_pnl=row.realized_pnl,
                    timestamp=row.filled_at,
                )
                for row in rows
            ]

    def market_exposure(self, market_symbol: str) -> float:
        with self._session_factory() as session:
            row = session.get(TradingPosition, market_symbol)
            return float(row.open_stake) if row else 0.0

    def open_notional(self) -> float:
        with self._session_factory() as session:
            stmt = select(TradingPosition.open_stake).where(TradingPosition.open_stake > 0)
            return float(sum(session.execute(stmt).scalars().all()))

    def daily_realized_pnl(self) -> float:
        with self._session_factory() as session:
            today = datetime.now(UTC).date()
            row = session.get(TradingDailyPnL, today)
            return float(row.realized_pnl) if row else 0.0
```

- [ ] **Step 4: Run test, verify pass**

```
pytest tests/unit/test_sql_portfolio_ledger.py -v
```

Expected: 6 passed.

- [ ] **Step 5: Commit**

```
git add app/trading/sql_ledger.py tests/unit/test_sql_portfolio_ledger.py
git commit -m "feat(trading): SqlPortfolioLedger with idempotent fills"
```

---

## Task 6: Kalshi typed errors

**Files:**
- Create: `app/providers/exchanges/__init__.py`
- Create: `app/providers/exchanges/kalshi_errors.py`
- Test: `tests/unit/test_kalshi_errors.py`

- [ ] **Step 1: Create package marker**

Create empty `app/providers/exchanges/__init__.py`.

- [ ] **Step 2: Write failing test**

Create `tests/unit/test_kalshi_errors.py`:

```python
from __future__ import annotations

import pytest

from app.providers.exchanges.kalshi_errors import (
    KalshiApiError,
    KalshiAuthError,
    KalshiInsufficientFunds,
    KalshiMarketError,
    KalshiRateLimited,
    KalshiServerError,
    classify_response,
)


def test_classify_401_returns_auth_error() -> None:
    with pytest.raises(KalshiAuthError):
        classify_response(401, b"{}", {})


def test_classify_404_returns_market_error() -> None:
    with pytest.raises(KalshiMarketError):
        classify_response(404, b"{}", {})


def test_classify_insufficient_funds_body() -> None:
    body = b'{"error":{"code":"insufficient_funds","message":"need more"}}'
    with pytest.raises(KalshiInsufficientFunds):
        classify_response(400, body, {})


def test_classify_429_surfaces_retry_after() -> None:
    with pytest.raises(KalshiRateLimited) as exc_info:
        classify_response(429, b"{}", {"retry-after": "7"})
    assert exc_info.value.retry_after == 7


def test_classify_500_returns_server_error() -> None:
    with pytest.raises(KalshiServerError):
        classify_response(500, b"{}", {})


def test_classify_2xx_no_raise() -> None:
    classify_response(200, b"{}", {})


def test_classify_unknown_4xx_returns_generic() -> None:
    with pytest.raises(KalshiApiError):
        classify_response(418, b"{}", {})
```

- [ ] **Step 3: Run test, verify failure**

```
pytest tests/unit/test_kalshi_errors.py -v
```

Expected: ImportError.

- [ ] **Step 4: Implement errors**

Create `app/providers/exchanges/kalshi_errors.py`:

```python
from __future__ import annotations

import json
from typing import Mapping


class KalshiApiError(RuntimeError):
    def __init__(self, status: int, body: bytes, message: str = "") -> None:
        self.status = status
        self.body = body
        super().__init__(message or f"Kalshi API error (status={status})")


class KalshiAuthError(KalshiApiError):
    pass


class KalshiMarketError(KalshiApiError):
    pass


class KalshiInsufficientFunds(KalshiApiError):
    pass


class KalshiRateLimited(KalshiApiError):
    def __init__(self, status: int, body: bytes, retry_after: int) -> None:
        super().__init__(status, body, message=f"Kalshi rate limited; retry after {retry_after}s")
        self.retry_after = retry_after


class KalshiServerError(KalshiApiError):
    pass


def _looks_like_insufficient_funds(body: bytes) -> bool:
    try:
        payload = json.loads(body or b"{}")
    except json.JSONDecodeError:
        return False
    error = payload.get("error") if isinstance(payload, dict) else None
    if not isinstance(error, dict):
        return False
    code = str(error.get("code", "")).lower()
    return "insufficient" in code or "funds" in code


def _parse_retry_after(headers: Mapping[str, str]) -> int:
    raw = headers.get("retry-after") or headers.get("Retry-After") or "1"
    try:
        return max(1, int(raw))
    except ValueError:
        return 1


def classify_response(status: int, body: bytes, headers: Mapping[str, str]) -> None:
    if 200 <= status < 300:
        return
    if status in (401, 403):
        raise KalshiAuthError(status, body)
    if status == 404:
        raise KalshiMarketError(status, body)
    if status == 429:
        raise KalshiRateLimited(status, body, _parse_retry_after(headers))
    if status >= 500:
        raise KalshiServerError(status, body)
    if 400 <= status < 500 and _looks_like_insufficient_funds(body):
        raise KalshiInsufficientFunds(status, body)
    raise KalshiApiError(status, body)
```

- [ ] **Step 5: Run test, verify pass**

```
pytest tests/unit/test_kalshi_errors.py -v
```

Expected: 7 passed.

- [ ] **Step 6: Commit**

```
git add app/providers/exchanges/__init__.py app/providers/exchanges/kalshi_errors.py tests/unit/test_kalshi_errors.py
git commit -m "feat(kalshi): typed exception hierarchy for API errors"
```

---

## Task 7: Kalshi RSA-PSS signing helper

**Files:**
- Create: `app/providers/exchanges/kalshi_signing.py`
- Test: `tests/unit/test_kalshi_signing.py`

- [ ] **Step 1: Write failing test**

Create `tests/unit/test_kalshi_signing.py`:

```python
from __future__ import annotations

import base64
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from app.providers.exchanges.kalshi_signing import sign_request


@pytest.fixture()
def private_key_pem(tmp_path: Path) -> Path:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    path = tmp_path / "key.pem"
    path.write_bytes(pem)
    return path


def test_sign_request_returns_base64_pss_signature_verifiable(private_key_pem: Path) -> None:
    timestamp_ms = "1714953600000"
    method = "GET"
    path = "/trade-api/v2/portfolio/balance"
    sig = sign_request(private_key_pem, timestamp_ms, method, path)
    raw = base64.b64decode(sig)
    pubkey = serialization.load_pem_private_key(private_key_pem.read_bytes(), password=None).public_key()
    message = (timestamp_ms + method + path).encode("utf-8")
    pubkey.verify(
        raw,
        message,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=hashes.SHA256.digest_size),
        hashes.SHA256(),
    )  # raises if invalid


def test_sign_request_caches_key_load(private_key_pem: Path) -> None:
    sig1 = sign_request(private_key_pem, "1", "GET", "/x")
    sig2 = sign_request(private_key_pem, "2", "GET", "/x")
    assert sig1 != sig2  # different timestamps yield different signatures
```

- [ ] **Step 2: Run test, verify failure**

```
pytest tests/unit/test_kalshi_signing.py -v
```

Expected: ImportError.

- [ ] **Step 3: Implement signing helper**

Create `app/providers/exchanges/kalshi_signing.py`:

```python
from __future__ import annotations

import base64
from functools import lru_cache
from pathlib import Path

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey


@lru_cache(maxsize=4)
def _load_private_key(path_str: str) -> RSAPrivateKey:
    pem = Path(path_str).read_bytes()
    key = serialization.load_pem_private_key(pem, password=None)
    if not isinstance(key, RSAPrivateKey):
        raise ValueError(f"Kalshi private key at {path_str} is not an RSA key")
    return key


def sign_request(
    private_key_path: Path | str,
    timestamp_ms: str,
    method: str,
    path: str,
) -> str:
    key = _load_private_key(str(private_key_path))
    message = (timestamp_ms + method.upper() + path).encode("utf-8")
    signature = key.sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=hashes.SHA256.digest_size,
        ),
        hashes.SHA256(),
    )
    return base64.b64encode(signature).decode("ascii")
```

- [ ] **Step 4: Run test, verify pass**

```
pytest tests/unit/test_kalshi_signing.py -v
```

Expected: 2 passed.

- [ ] **Step 5: Commit**

```
git add app/providers/exchanges/kalshi_signing.py tests/unit/test_kalshi_signing.py
git commit -m "feat(kalshi): RSA-PSS request signing helper"
```

---

## Task 8: KalshiClient HTTP wrapper

**Files:**
- Create: `app/providers/exchanges/kalshi_client.py`
- Test: `tests/unit/test_kalshi_client.py`

- [ ] **Step 1: Write failing test**

Create `tests/unit/test_kalshi_client.py`:

```python
from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from app.providers.exchanges.kalshi_client import KalshiClient
from app.providers.exchanges.kalshi_errors import KalshiAuthError, KalshiMarketError


@pytest.fixture()
def private_key_pem(tmp_path: Path) -> Path:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    path = tmp_path / "key.pem"
    path.write_bytes(pem)
    return path


def _client_with(private_key_pem: Path, transport: httpx.MockTransport) -> KalshiClient:
    return KalshiClient(
        api_key_id="test-key",
        private_key_path=private_key_pem,
        base_url="https://api.example",
        transport=transport,
    )


def test_get_balance_includes_signing_headers(private_key_pem: Path) -> None:
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["headers"] = dict(request.headers)
        return httpx.Response(200, json={"balance": 5000})

    client = _client_with(private_key_pem, httpx.MockTransport(handler))
    result = client.get_balance()
    assert result["balance"] == 5000
    assert captured["headers"]["kalshi-access-key"] == "test-key"
    assert "kalshi-access-timestamp" in captured["headers"]
    assert "kalshi-access-signature" in captured["headers"]


def test_get_market_404_raises_market_error(private_key_pem: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"error": {"code": "market_not_found"}})

    client = _client_with(private_key_pem, httpx.MockTransport(handler))
    with pytest.raises(KalshiMarketError):
        client.get_market("FAKE-TICKER")


def test_create_order_sends_payload(private_key_pem: Path) -> None:
    captured_body: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured_body["json"] = json.loads(request.content)
        return httpx.Response(
            200, json={"order": {"order_id": "ord123", "status": "executed"}}
        )

    client = _client_with(private_key_pem, httpx.MockTransport(handler))
    result = client.create_order(
        ticker="X-TICKER",
        side="yes",
        count=1,
        order_type="market",
        client_order_id="intent-1",
    )
    assert result["order"]["order_id"] == "ord123"
    assert captured_body["json"]["ticker"] == "X-TICKER"
    assert captured_body["json"]["count"] == 1
    assert captured_body["json"]["client_order_id"] == "intent-1"


def test_get_balance_401_raises_auth(private_key_pem: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"error": "bad sig"})

    client = _client_with(private_key_pem, httpx.MockTransport(handler))
    with pytest.raises(KalshiAuthError):
        client.get_balance()
```

- [ ] **Step 2: Run test, verify failure**

```
pytest tests/unit/test_kalshi_client.py -v
```

Expected: ImportError.

- [ ] **Step 3: Implement KalshiClient**

Create `app/providers/exchanges/kalshi_client.py`:

```python
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import httpx

from app.providers.exchanges.kalshi_errors import classify_response
from app.providers.exchanges.kalshi_signing import sign_request


class KalshiClient:
    def __init__(
        self,
        *,
        api_key_id: str,
        private_key_path: Path | str,
        base_url: str = "https://api.elections.kalshi.com",
        timeout_seconds: float = 10.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        if not api_key_id:
            raise ValueError("KalshiClient requires a non-empty api_key_id")
        self._api_key_id = api_key_id
        self._private_key_path = Path(private_key_path)
        self._base_url = base_url.rstrip("/")
        self._client = httpx.Client(
            base_url=self._base_url,
            timeout=timeout_seconds,
            transport=transport,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "KalshiClient":
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.close()

    def _signed_headers(self, method: str, path: str) -> dict[str, str]:
        ts = str(int(time.time() * 1000))
        signature = sign_request(self._private_key_path, ts, method, path)
        return {
            "KALSHI-ACCESS-KEY": self._api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "accept": "application/json",
        }

    def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        headers = self._signed_headers(method, path)
        if json_body is not None:
            headers["content-type"] = "application/json"
        response = self._client.request(
            method, path, headers=headers, content=json.dumps(json_body).encode("utf-8") if json_body else None
        )
        classify_response(response.status_code, response.content, dict(response.headers))
        if not response.content:
            return {}
        return response.json()

    # --- public API surface (Spec 1 minimum) ---

    def get_balance(self) -> dict[str, Any]:
        return self._request("GET", "/trade-api/v2/portfolio/balance")

    def get_market(self, ticker: str) -> dict[str, Any]:
        return self._request("GET", f"/trade-api/v2/markets/{ticker}")

    def create_order(
        self,
        *,
        ticker: str,
        side: str,
        count: int,
        order_type: str,
        client_order_id: str,
        max_price_cents: int | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "ticker": ticker,
            "side": side,
            "count": int(count),
            "type": order_type,
            "client_order_id": client_order_id,
            "action": "buy",
        }
        if max_price_cents is not None:
            body["yes_price"] = int(max_price_cents)
        return self._request("POST", "/trade-api/v2/portfolio/orders", json_body=body)

    def get_order(self, order_id: str) -> dict[str, Any]:
        return self._request("GET", f"/trade-api/v2/portfolio/orders/{order_id}")
```

- [ ] **Step 4: Run test, verify pass**

```
pytest tests/unit/test_kalshi_client.py -v
```

Expected: 4 passed.

- [ ] **Step 5: Commit**

```
git add app/providers/exchanges/kalshi_client.py tests/unit/test_kalshi_client.py
git commit -m "feat(kalshi): HTTP client with signed requests and 4 endpoints"
```

---

## Task 9: SymbolResolver

**Files:**
- Create: `app/trading/symbol_resolver.py`
- Test: `tests/unit/test_symbol_resolver.py`

- [ ] **Step 1: Write failing test**

Create `tests/unit/test_symbol_resolver.py`:

```python
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from app.trading.symbol_resolver import (
    SymbolResolver,
    SymbolResolverConfigError,
    load_symbol_resolver,
)
from app.trading.types import (
    ExecutionIntent,
    MarketRef,
    Signal,
)


def _signal(player_id: int, market_key: str, side: str, line: float, game_date: str | None) -> Signal:
    metadata: dict = {"player_id": player_id, "game_id": 1}
    if game_date is not None:
        metadata["game_date"] = game_date
    return Signal(
        signal_id="s1",
        created_at=datetime.now(UTC),
        market_key=market_key,
        side=side,
        confidence="high",
        edge=0.05,
        model_probability=0.55,
        line_value=line,
        metadata=metadata,
    )


def _intent(signal: Signal) -> ExecutionIntent:
    return ExecutionIntent(
        intent_id="i1",
        signal=signal,
        market=MarketRef(
            exchange="kalshi",
            symbol="kalshi:x",
            market_key=signal.market_key,
            side=signal.side,
            line_value=signal.line_value,
        ),
        side="buy",
        stake=0.25,
    )


def test_resolver_exact_match(tmp_path: Path) -> None:
    config = tmp_path / "syms.json"
    config.write_text(
        json.dumps(
            [
                {
                    "market_key": "points",
                    "side": "over",
                    "line_value": 25.5,
                    "player_id": 237,
                    "game_date": "2026-05-06",
                    "kalshi_ticker": "KX-LEBRON-OPTS25",
                }
            ]
        )
    )
    resolver = load_symbol_resolver(config)
    intent = _intent(_signal(237, "points", "OVER", 25.5, "2026-05-06"))
    assert resolver.resolve(intent) == "KX-LEBRON-OPTS25"


def test_resolver_miss_returns_none(tmp_path: Path) -> None:
    config = tmp_path / "syms.json"
    config.write_text(json.dumps([]))
    resolver = load_symbol_resolver(config)
    intent = _intent(_signal(999, "points", "OVER", 25.5, "2026-05-06"))
    assert resolver.resolve(intent) is None


def test_resolver_uses_signal_created_date_when_metadata_absent(tmp_path: Path) -> None:
    today = datetime.now(UTC).date().isoformat()
    config = tmp_path / "syms.json"
    config.write_text(
        json.dumps(
            [
                {
                    "market_key": "points",
                    "side": "over",
                    "line_value": 25.5,
                    "player_id": 237,
                    "game_date": today,
                    "kalshi_ticker": "KX-LEBRON-OPTS25",
                }
            ]
        )
    )
    resolver = load_symbol_resolver(config)
    intent = _intent(_signal(237, "points", "OVER", 25.5, game_date=None))
    assert resolver.resolve(intent) == "KX-LEBRON-OPTS25"


def test_resolver_malformed_json_raises(tmp_path: Path) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text("{not json")
    with pytest.raises(SymbolResolverConfigError, match="malformed"):
        load_symbol_resolver(bad)


def test_resolver_missing_field_raises(tmp_path: Path) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps([{"market_key": "points"}]))
    with pytest.raises(SymbolResolverConfigError, match="missing"):
        load_symbol_resolver(bad)


def test_resolver_count_property(tmp_path: Path) -> None:
    config = tmp_path / "syms.json"
    config.write_text(json.dumps([
        {"market_key": "points", "side": "over", "line_value": 25.5,
         "player_id": 237, "game_date": "2026-05-06", "kalshi_ticker": "T1"},
        {"market_key": "points", "side": "over", "line_value": 27.5,
         "player_id": 237, "game_date": "2026-05-06", "kalshi_ticker": "T2"},
    ]))
    resolver = load_symbol_resolver(config)
    assert resolver.ticker_count == 2


def test_direct_constructor_accepts_inline_entries() -> None:
    resolver = SymbolResolver(entries=[
        {"market_key": "points", "side": "over", "line_value": 25.5,
         "player_id": 237, "game_date": "2026-05-06", "kalshi_ticker": "T1"},
    ])
    assert resolver.ticker_count == 1
```

- [ ] **Step 2: Run test, verify failure**

```
pytest tests/unit/test_symbol_resolver.py -v
```

Expected: ImportError.

- [ ] **Step 3: Implement resolver**

Create `app/trading/symbol_resolver.py`:

```python
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.trading.types import ExecutionIntent

_REQUIRED = ("market_key", "side", "line_value", "player_id", "game_date", "kalshi_ticker")


class SymbolResolverConfigError(RuntimeError):
    """Raised when the symbol map JSON cannot be loaded."""


def _key(market_key: str, side: str, line_value: float, player_id: int, game_date: str) -> tuple:
    return (market_key.lower(), side.lower(), round(float(line_value), 2), int(player_id), game_date)


class SymbolResolver:
    def __init__(self, entries: list[dict[str, Any]]) -> None:
        self._table: dict[tuple, str] = {}
        for entry in entries:
            for field in _REQUIRED:
                if field not in entry:
                    raise SymbolResolverConfigError(f"symbol entry missing field: {field}")
            key = _key(
                entry["market_key"],
                entry["side"],
                entry["line_value"],
                entry["player_id"],
                str(entry["game_date"]),
            )
            self._table[key] = str(entry["kalshi_ticker"])

    @property
    def ticker_count(self) -> int:
        return len(self._table)

    def resolve(self, intent: ExecutionIntent) -> str | None:
        signal = intent.signal
        player_id = signal.metadata.get("player_id")
        if player_id is None:
            return None
        game_date_raw = signal.metadata.get("game_date") or signal.created_at.date().isoformat()
        try:
            key = _key(
                signal.market_key,
                signal.side,
                signal.line_value,
                int(player_id),
                str(game_date_raw),
            )
        except (TypeError, ValueError):
            return None
        return self._table.get(key)


def load_symbol_resolver(path: Path | str) -> SymbolResolver:
    config_path = Path(path)
    if not config_path.is_file():
        raise SymbolResolverConfigError(
            f"symbol map not found at {config_path}; "
            "copy config/kalshi_symbols.example.json to that path."
        )
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SymbolResolverConfigError(f"malformed JSON in {config_path}: {exc}") from exc
    if not isinstance(payload, list):
        raise SymbolResolverConfigError(f"symbol map in {config_path} must be a JSON array")
    return SymbolResolver(entries=payload)
```

- [ ] **Step 4: Run test, verify pass**

```
pytest tests/unit/test_symbol_resolver.py -v
```

Expected: 7 passed.

- [ ] **Step 5: Commit**

```
git add app/trading/symbol_resolver.py tests/unit/test_symbol_resolver.py
git commit -m "feat(trading): SymbolResolver for hand-curated Kalshi ticker map"
```

---

## Task 10: KalshiAdapter

**Files:**
- Create: `app/trading/kalshi_adapter.py`
- Test: `tests/unit/test_kalshi_adapter.py`

- [ ] **Step 1: Write failing test**

Create `tests/unit/test_kalshi_adapter.py`:

```python
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from app.trading.kalshi_adapter import KalshiAdapter
from app.trading.symbol_resolver import SymbolResolver
from app.trading.types import ExecutionIntent, MarketRef, Signal


class _FakeClient:
    def __init__(
        self,
        *,
        market: dict[str, Any] | None = None,
        order: dict[str, Any] | None = None,
        order_polls: list[dict[str, Any]] | None = None,
    ) -> None:
        self._market = market or {"market": {"yes_ask": 40, "yes_bid": 38, "status": "open"}}
        self._order = order or {"order": {"order_id": "ord1", "status": "executed"}}
        self._polls = list(order_polls or [{"order": {"order_id": "ord1", "status": "executed", "fills": [
            {"trade_id": "t1", "count": 1, "yes_price": 40, "fee": 1}
        ]}}])
        self.create_calls: list[dict[str, Any]] = []
        self.poll_calls: list[str] = []

    def get_market(self, ticker: str) -> dict[str, Any]:
        return self._market

    def create_order(self, **kwargs: Any) -> dict[str, Any]:
        self.create_calls.append(kwargs)
        return self._order

    def get_order(self, order_id: str) -> dict[str, Any]:
        self.poll_calls.append(order_id)
        if self._polls:
            return self._polls.pop(0)
        return {"order": {"order_id": order_id, "status": "executed", "fills": []}}


def _resolver_with(ticker: str) -> SymbolResolver:
    return SymbolResolver(entries=[{
        "market_key": "points", "side": "over", "line_value": 25.5,
        "player_id": 237, "game_date": "2026-05-06", "kalshi_ticker": ticker,
    }])


def _intent(stake: float = 0.50, player_id: int = 237) -> ExecutionIntent:
    signal = Signal(
        signal_id="s1",
        created_at=datetime(2026, 5, 6, tzinfo=UTC),
        market_key="points",
        side="OVER",
        confidence="high",
        edge=0.05,
        model_probability=0.55,
        line_value=25.5,
        metadata={"player_id": player_id, "game_id": 1, "game_date": "2026-05-06"},
    )
    return ExecutionIntent(
        intent_id="intent-1",
        signal=signal,
        market=MarketRef(exchange="kalshi", symbol="kalshi:x", market_key="points", side="OVER", line_value=25.5),
        side="buy",
        stake=stake,
    )


def test_place_order_resolves_ticker_and_fires_create() -> None:
    client = _FakeClient()
    adapter = KalshiAdapter(client=client, resolver=_resolver_with("KX-T1"))
    events, fills = adapter.place_order(_intent(stake=0.50))
    assert client.create_calls[0]["ticker"] == "KX-T1"
    assert client.create_calls[0]["count"] == 1  # 0.50 / 0.40 = 1 contract
    assert client.create_calls[0]["client_order_id"].startswith("intent-1")
    assert any(e.event_type == "filled" for e in events)
    assert len(fills) == 1
    assert fills[0].price == pytest.approx(0.40)


def test_place_order_unresolved_ticker_emits_rejected_no_call() -> None:
    client = _FakeClient()
    adapter = KalshiAdapter(client=client, resolver=SymbolResolver(entries=[]))
    events, fills = adapter.place_order(_intent())
    assert client.create_calls == []
    assert any(e.event_type == "rejected" for e in events)
    assert fills == []


def test_place_order_count_zero_when_stake_below_contract_price_emits_rejected() -> None:
    # contract is 40 cents, stake is 25 cents -> count = 0
    client = _FakeClient()
    adapter = KalshiAdapter(client=client, resolver=_resolver_with("KX-T1"))
    events, fills = adapter.place_order(_intent(stake=0.25))
    assert client.create_calls == []
    assert any(e.event_type == "rejected" and "exceeds" in e.message for e in events)
    assert fills == []


def test_place_order_handles_no_fills_in_response() -> None:
    client = _FakeClient(order_polls=[{"order": {"order_id": "ord1", "status": "canceled", "fills": []}}])
    adapter = KalshiAdapter(client=client, resolver=_resolver_with("KX-T1"), poll_interval_seconds=0.0, poll_timeout_seconds=0.1)
    events, fills = adapter.place_order(_intent(stake=0.50))
    assert fills == []
    assert any(e.status == "canceled" or e.event_type == "filled" for e in events) or any(e.event_type == "error" for e in events)
```

- [ ] **Step 2: Run test, verify failure**

```
pytest tests/unit/test_kalshi_adapter.py -v
```

Expected: ImportError.

- [ ] **Step 3: Implement KalshiAdapter**

Create `app/trading/kalshi_adapter.py`:

```python
from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any, Protocol

from app.providers.exchanges.kalshi_errors import KalshiApiError
from app.trading.protocols import ExchangeAdapter
from app.trading.symbol_resolver import SymbolResolver
from app.trading.types import ExecutionIntent, Fill, MarketRef, OrderEvent

_TERMINAL_STATUSES = {"executed", "filled", "canceled", "cancelled", "rejected"}


class _KalshiClientLike(Protocol):
    def get_market(self, ticker: str) -> dict[str, Any]: ...
    def create_order(self, **kwargs: Any) -> dict[str, Any]: ...
    def get_order(self, order_id: str) -> dict[str, Any]: ...


class KalshiAdapter(ExchangeAdapter):
    def __init__(
        self,
        *,
        client: _KalshiClientLike,
        resolver: SymbolResolver,
        poll_interval_seconds: float = 0.25,
        poll_timeout_seconds: float = 5.0,
    ) -> None:
        self._client = client
        self._resolver = resolver
        self._poll_interval = float(poll_interval_seconds)
        self._poll_timeout = float(poll_timeout_seconds)

    def place_order(self, intent: ExecutionIntent) -> tuple[list[OrderEvent], list[Fill]]:
        ticker = self._resolver.resolve(intent)
        if ticker is None:
            return (
                [OrderEvent(intent_id=intent.intent_id, event_type="rejected",
                            status="blocked", message="no kalshi ticker for signal")],
                [],
            )
        try:
            market = self._client.get_market(ticker)
        except KalshiApiError as exc:
            return (
                [OrderEvent(intent_id=intent.intent_id, event_type="error",
                            status="failed", message=f"market lookup failed: {exc}")],
                [],
            )
        ask_cents = self._extract_yes_ask_cents(market)
        if ask_cents is None or ask_cents <= 0:
            return (
                [OrderEvent(intent_id=intent.intent_id, event_type="rejected",
                            status="blocked", message="market has no tradable ask")],
                [],
            )
        contract_price_dollars = ask_cents / 100.0
        count = int(intent.stake // contract_price_dollars)
        if count < 1:
            return (
                [OrderEvent(intent_id=intent.intent_id, event_type="rejected",
                            status="blocked",
                            message=f"contract price {contract_price_dollars:.2f} exceeds stake cap {intent.stake:.2f}")],
                [],
            )
        side_yesno = "yes" if intent.signal.side.upper() == "OVER" else "no"
        client_order_id = f"{intent.intent_id}-1"
        try:
            order_response = self._client.create_order(
                ticker=ticker,
                side=side_yesno,
                count=count,
                order_type="market",
                client_order_id=client_order_id,
            )
        except KalshiApiError as exc:
            return (
                [OrderEvent(intent_id=intent.intent_id, event_type="error",
                            status="failed", message=f"create_order failed: {exc}")],
                [],
            )
        order_id = order_response.get("order", {}).get("order_id")
        events: list[OrderEvent] = [
            OrderEvent(intent_id=intent.intent_id, event_type="accepted", status="ok",
                       message=f"kalshi order {order_id}"),
        ]
        terminal = self._poll_until_terminal(order_id) if order_id else None
        if terminal is None:
            events.append(OrderEvent(intent_id=intent.intent_id, event_type="error",
                                      status="failed", message="poll timeout, fill unknown"))
            return events, []
        status = str(terminal.get("order", {}).get("status", "")).lower()
        fills = self._extract_fills(intent, ticker, terminal)
        events.append(OrderEvent(intent_id=intent.intent_id, event_type="filled" if fills else status,
                                  status="ok" if fills else "info",
                                  message=f"status={status} fills={len(fills)}"))
        return events, fills

    def _poll_until_terminal(self, order_id: str) -> dict[str, Any] | None:
        deadline = time.monotonic() + self._poll_timeout
        last: dict[str, Any] | None = None
        while time.monotonic() <= deadline:
            try:
                last = self._client.get_order(order_id)
            except KalshiApiError:
                return None
            status = str(last.get("order", {}).get("status", "")).lower()
            if status in _TERMINAL_STATUSES:
                return last
            if self._poll_interval > 0:
                time.sleep(self._poll_interval)
        return last

    def _extract_yes_ask_cents(self, market_payload: dict[str, Any]) -> int | None:
        market = market_payload.get("market") if isinstance(market_payload, dict) else None
        if not isinstance(market, dict):
            return None
        ask = market.get("yes_ask")
        try:
            return int(ask) if ask is not None else None
        except (TypeError, ValueError):
            return None

    def _extract_fills(
        self,
        intent: ExecutionIntent,
        ticker: str,
        order_payload: dict[str, Any],
    ) -> list[Fill]:
        order = order_payload.get("order", {})
        raw_fills = order.get("fills") or []
        if not isinstance(raw_fills, list):
            return []
        market_ref = MarketRef(
            exchange="kalshi",
            symbol=f"kalshi:{ticker}",
            market_key=intent.signal.market_key,
            side=intent.signal.side.upper(),
            line_value=float(intent.signal.line_value),
        )
        results: list[Fill] = []
        for idx, raw in enumerate(raw_fills, start=1):
            try:
                count = int(raw.get("count", 0))
                price_cents = int(raw.get("yes_price", 0))
                fee_cents = int(raw.get("fee", 0))
                trade_id = str(raw.get("trade_id", f"{intent.intent_id}-trade-{idx}"))
            except (TypeError, ValueError):
                continue
            if count <= 0:
                continue
            stake_dollars = count * price_cents / 100.0
            results.append(
                Fill(
                    fill_id=f"{intent.intent_id}-{trade_id}",
                    intent_id=intent.intent_id,
                    market=market_ref,
                    side=intent.side,
                    stake=round(stake_dollars, 4),
                    price=round(price_cents / 100.0, 4),
                    fee=round(fee_cents / 100.0, 4),
                    realized_pnl=0.0,
                    timestamp=datetime.now(UTC),
                )
            )
        return results
```

- [ ] **Step 4: Run test, verify pass**

```
pytest tests/unit/test_kalshi_adapter.py -v
```

Expected: 4 passed.

- [ ] **Step 5: Commit**

```
git add app/trading/kalshi_adapter.py tests/unit/test_kalshi_adapter.py
git commit -m "feat(trading): KalshiAdapter wrapping client with poll-to-terminal"
```

---

## Task 11: TradingLoop kill-switch DB hook + game_date metadata

**Files:**
- Modify: `app/trading/loop.py`
- Test: `tests/unit/test_trading_loop_killswitch.py`

- [ ] **Step 1: Write failing test**

Create `tests/unit/test_trading_loop_killswitch.py`:

```python
from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.base import Base
from app.db.models.trading import (
    TradingDailyPnL,
    TradingFill,
    TradingKillSwitch,
    TradingOrder,
    TradingPosition,
)
from app.evaluation.prop_decision import PropDecision
from app.trading.ledger import InMemoryPortfolioLedger
from app.trading.loop import TradingLoop, set_kill_switch
from app.trading.paper_adapter import FakePaperAdapter
from app.trading.risk import ExposureRiskEngine


@pytest.fixture()
def session_factory():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine, tables=[
        TradingOrder.__table__, TradingFill.__table__, TradingPosition.__table__,
        TradingKillSwitch.__table__, TradingDailyPnL.__table__,
    ])
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def _decision(market_key: str = "points") -> PropDecision:
    return PropDecision(
        model_prob=0.6, market_prob=0.5, no_vig_market_prob=0.5,
        ev=0.05, recommendation="OVER", confidence="high", driver="test",
        market_key=market_key, line_value=25.5, over_odds=-110, under_odds=-110,
    )


def test_loop_halts_when_kill_switch_set_in_db(session_factory) -> None:
    set_kill_switch(session_factory, killed=True, set_by="test")
    loop = TradingLoop(
        risk_engine=ExposureRiskEngine(),
        ledger=InMemoryPortfolioLedger(),
        adapter=FakePaperAdapter(),
        session_factory=session_factory,
    )
    result = loop.run_decisions([_decision(), _decision("rebounds")], stake=1.0)
    assert result.accepted == 0
    assert result.rejected == 2


def test_loop_runs_normally_when_kill_switch_off(session_factory) -> None:
    set_kill_switch(session_factory, killed=False, set_by="test")
    loop = TradingLoop(
        risk_engine=ExposureRiskEngine(),
        ledger=InMemoryPortfolioLedger(),
        adapter=FakePaperAdapter(),
        session_factory=session_factory,
    )
    result = loop.run_decisions([_decision()], stake=1.0)
    assert result.accepted == 1


def test_decision_to_signal_includes_game_date_metadata() -> None:
    loop = TradingLoop(
        risk_engine=ExposureRiskEngine(),
        ledger=InMemoryPortfolioLedger(),
        adapter=FakePaperAdapter(),
    )
    sig = loop._decision_to_signal(_decision())  # type: ignore[attr-defined]
    assert "game_date" in sig.metadata
    today = datetime.now(UTC).date().isoformat()
    assert sig.metadata["game_date"] == today
```

- [ ] **Step 2: Run test, verify failure**

```
pytest tests/unit/test_trading_loop_killswitch.py -v
```

Expected: ImportError on `set_kill_switch` or signature mismatch on `TradingLoop.__init__`.

- [ ] **Step 3: Modify `TradingLoop` and add helper**

In `app/trading/loop.py`, add these imports (note: `Callable` is already imported, do not re-add):

```python
from sqlalchemy.orm import Session

from app.db.models.trading import TradingKillSwitch
```

Update the `TradingLoop.__init__` signature:

```python
    def __init__(
        self,
        *,
        risk_engine: RiskEngine,
        ledger: PortfolioLedger,
        adapter: ExchangeAdapter,
        market_mapper: Callable[[Signal, str], MarketRef] = signal_to_market_ref,
        session_factory: Callable[[], Session] | None = None,
    ) -> None:
        self._risk = risk_engine
        self._ledger = ledger
        self._adapter = adapter
        self._market_mapper = market_mapper
        self._sequence = 0
        self._session_factory = session_factory
```

Add a private kill-switch check method on `TradingLoop`:

```python
    def _kill_switch_active(self) -> bool:
        if self._session_factory is None:
            return False
        with self._session_factory() as session:
            row = session.get(TradingKillSwitch, 1)
            return bool(row and row.killed)
```

In `run_signals`, immediately after `for signal in signals:`, add:

```python
            if self._kill_switch_active():
                rejected += 1
                event = OrderEvent(
                    intent_id=f"{signal.signal_id}-intent",
                    event_type="rejected",
                    status="blocked",
                    message="kill switch active",
                    timestamp=datetime.now(UTC),
                )
                self._ledger.record_order_event(event)
                event_count += 1
                continue
```

In `_decision_to_signal`, change the metadata dict literal to include `game_date`:

```python
            metadata={
                "signal_id": signal_id,
                "game_id": 0,
                "player_id": synthetic_player_id,
                "game_date": datetime.now(UTC).date().isoformat(),
                "market_prob": float(
                    decision.market_prob
                    if decision.market_prob > 0
                    else american_to_prob(decision.over_odds if decision.recommendation.upper() == "OVER" else decision.under_odds)
                ),
                "no_vig_market_prob": float(
                    decision.no_vig_market_prob
                    if decision.no_vig_market_prob > 0
                    else no_vig_over_probability(decision.over_odds, decision.under_odds)
                ),
                "driver": decision.driver,
            },
```

Add the `set_kill_switch` helper function at module level (above `_load_decisions`):

```python
def set_kill_switch(
    session_factory: Callable[[], Session],
    *,
    killed: bool,
    set_by: str = "system",
) -> None:
    with session_factory() as session:
        row = session.get(TradingKillSwitch, 1)
        now = datetime.now(UTC)
        if row is None:
            row = TradingKillSwitch(id=1, killed=killed, set_at=now, set_by=set_by)
            session.add(row)
        else:
            row.killed = killed
            row.set_at = now
            row.set_by = set_by
        session.commit()
```

- [ ] **Step 4: Run test, verify pass**

```
pytest tests/unit/test_trading_loop_killswitch.py -v
```

Expected: 3 passed.

- [ ] **Step 5: Run pre-existing trading tests to confirm no regression**

```
pytest tests/unit/test_trading_ledger.py tests/unit/test_trading_mapper.py tests/unit/test_trading_pricing.py tests/unit/test_trading_types.py tests/integration/test_paper_loop.py -v
```

Expected: all pass.

- [ ] **Step 6: Commit**

```
git add app/trading/loop.py tests/unit/test_trading_loop_killswitch.py
git commit -m "feat(trading): SQL kill-switch hook in TradingLoop and game_date metadata"
```

---

## Task 12: Wire kill-switch endpoint to SQL and add active_limits to /pnl

**Files:**
- Modify: `app/server/routers/trading.py`
- Modify: `app/server/schemas/trading.py`
- Test: extend `tests/integration/test_server_local_agent_trading.py` or add a new focused test

- [ ] **Step 1: Inspect existing trading test to learn the pattern**

```
pytest tests/integration/test_server_local_agent_trading.py -v --collect-only
```

Note the fixture/setup style. The new tests below mirror it.

- [ ] **Step 2: Write failing test**

Create `tests/integration/test_trading_endpoints_v2.py`:

```python
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.base import Base
from app.db.models.trading import (
    TradingDailyPnL, TradingFill, TradingKillSwitch, TradingOrder, TradingPosition,
)
from app.server.main import create_app
from app.trading.ledger import InMemoryPortfolioLedger
from app.trading.risk import ExposureRiskEngine, RiskLimits


@pytest.fixture()
def app_with_session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine, tables=[
        TradingOrder.__table__, TradingFill.__table__, TradingPosition.__table__,
        TradingKillSwitch.__table__, TradingDailyPnL.__table__,
    ])
    factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    app = create_app()
    app.state.trading_ledger = InMemoryPortfolioLedger()
    app.state.trading_risk = ExposureRiskEngine(RiskLimits(per_order_cap=0.25))
    app.state.trading_session_factory = factory
    return app, factory


def test_kill_switch_writes_sql_row(app_with_session: tuple[Any, Any]) -> None:
    app, factory = app_with_session
    client = TestClient(app)
    res = client.post("/api/trading/kill-switch")
    assert res.status_code == 200
    with factory() as session:
        row = session.get(TradingKillSwitch, 1)
        assert row is not None
        assert row.killed is True


def test_pnl_returns_active_limits(app_with_session: tuple[Any, Any]) -> None:
    app, _factory = app_with_session
    client = TestClient(app)
    res = client.get("/api/trading/pnl")
    assert res.status_code == 200
    body = res.json()
    assert "active_limits" in body
    assert body["active_limits"]["per_order_cap"] == pytest.approx(0.25)
```

- [ ] **Step 3: Run test, verify failure**

```
pytest tests/integration/test_trading_endpoints_v2.py -v
```

Expected: failures around missing `active_limits` and SQL row not written.

- [ ] **Step 4: Update schema**

In `app/server/schemas/trading.py`, replace the `TradingPnlModel` with:

```python
class ActiveLimitsModel(BaseModel):
    per_order_cap: float
    per_market_cap: float
    max_open_notional: float
    daily_loss_cap: float
    reject_cooldown_seconds: int


class TradingPnlModel(BaseModel):
    daily_realized_pnl: float
    kill_switch_active: bool
    active_limits: ActiveLimitsModel | None = None
```

- [ ] **Step 5: Update router**

In `app/server/routers/trading.py`, update imports:

```python
from datetime import UTC, datetime

from app.db.models.trading import TradingKillSwitch
from app.server.schemas.trading import (
    ActiveLimitsModel,
    FillModel,
    PositionModel,
    TradingIntentRequestModel,
    TradingIntentResponseModel,
    TradingPnlModel,
)
```

Replace `trading_pnl` and `trading_kill_switch` with:

```python
def _active_limits(risk_engine) -> ActiveLimitsModel | None:
    limits = getattr(risk_engine, "limits", None)
    if limits is None:
        return None
    return ActiveLimitsModel(
        per_order_cap=limits.per_order_cap,
        per_market_cap=limits.per_market_cap,
        max_open_notional=limits.max_open_notional,
        daily_loss_cap=limits.daily_loss_cap,
        reject_cooldown_seconds=limits.reject_cooldown_seconds,
    )


@router.get("/pnl", response_model=TradingPnlModel)
def trading_pnl(request: Request) -> TradingPnlModel:
    ledger = request.app.state.trading_ledger
    risk_engine = request.app.state.trading_risk
    return TradingPnlModel(
        daily_realized_pnl=ledger.daily_realized_pnl(),
        kill_switch_active=risk_engine.killed,
        active_limits=_active_limits(risk_engine),
    )


@router.post("/kill-switch", response_model=TradingPnlModel)
def trading_kill_switch(request: Request) -> TradingPnlModel:
    risk_engine = request.app.state.trading_risk
    risk_engine.set_killed(True)
    factory = getattr(request.app.state, "trading_session_factory", None)
    if factory is not None:
        with factory() as session:
            row = session.get(TradingKillSwitch, 1)
            now = datetime.now(UTC)
            if row is None:
                row = TradingKillSwitch(id=1, killed=True, set_at=now, set_by="api")
                session.add(row)
            else:
                row.killed = True
                row.set_at = now
                row.set_by = "api"
            session.commit()
    ledger = request.app.state.trading_ledger
    return TradingPnlModel(
        daily_realized_pnl=ledger.daily_realized_pnl(),
        kill_switch_active=risk_engine.killed,
        active_limits=_active_limits(risk_engine),
    )
```

- [ ] **Step 6: Run test, verify pass**

```
pytest tests/integration/test_trading_endpoints_v2.py tests/integration/test_server_local_agent_trading.py -v
```

Expected: all pass. If `test_server_local_agent_trading.py` regresses because it uses `request.app.state.trading_session_factory` not being set, that's fine — the new code makes it optional via `getattr(..., None)`.

- [ ] **Step 7: Commit**

```
git add app/server/routers/trading.py app/server/schemas/trading.py tests/integration/test_trading_endpoints_v2.py
git commit -m "feat(api): kill-switch writes SQL row, /pnl exposes active_limits"
```

---

## Task 13: Live trading CLI

**Files:**
- Create: `scripts/run_trading_loop.py`

This task has no dedicated automated test; the integration smoke test in Task 14 exercises the wiring end-to-end. Manual acceptance is in §10 of the spec.

- [ ] **Step 1: Implement CLI**

Create `scripts/run_trading_loop.py`:

```python
"""Live trading loop entry point for Spec 1.

Requires BOTH `--live` flag AND env var `KALSHI_LIVE_TRADING=1` to fire real orders.
Without both, the script prints what it would do and exits non-zero.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
from pathlib import Path

from app.config.settings import get_settings
from app.db.base import Base
from app.db.models.trading import (
    TradingDailyPnL, TradingFill, TradingKillSwitch, TradingOrder, TradingPosition,
)
from app.db.session import SessionLocal, configure_engine, get_engine
from app.providers.exchanges.kalshi_client import KalshiClient
from app.trading.kalshi_adapter import KalshiAdapter
from app.trading.live_limits import load_live_limits
from app.trading.loop import TradingLoop, _load_decisions, set_kill_switch
from app.trading.risk import ExposureRiskEngine
from app.trading.sql_ledger import SqlPortfolioLedger
from app.trading.symbol_resolver import load_symbol_resolver


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Live Kalshi trading loop (Spec 1).")
    parser.add_argument("--live", action="store_true", required=True, help="REQUIRED — confirms live intent.")
    parser.add_argument("--decisions", required=True, help="Path to decisions JSON file.")
    parser.add_argument("--yes", action="store_true", help="Skip the confirmation prompt (DANGEROUS).")
    return parser.parse_args()


def _ensure_tables() -> None:
    engine = get_engine()
    Base.metadata.create_all(engine, tables=[
        TradingOrder.__table__, TradingFill.__table__, TradingPosition.__table__,
        TradingKillSwitch.__table__, TradingDailyPnL.__table__,
    ])


def main() -> int:
    args = _parse_args()
    settings = get_settings()

    if not settings.kalshi_live_trading:
        print("ABORT: KALSHI_LIVE_TRADING env var must be set to 1 for live trading.", file=sys.stderr)
        return 2
    if not args.live:
        print("ABORT: --live flag required.", file=sys.stderr)
        return 2
    if not settings.kalshi_api_key_id or not settings.kalshi_private_key_path:
        print("ABORT: KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH env vars required.", file=sys.stderr)
        return 2

    configure_engine()
    _ensure_tables()
    factory = SessionLocal

    limits = load_live_limits(settings.trading_limits_path)
    resolver = load_symbol_resolver(settings.kalshi_symbols_path)

    client = KalshiClient(
        api_key_id=settings.kalshi_api_key_id,
        private_key_path=settings.kalshi_private_key_path,
        base_url=settings.kalshi_base_url,
    )
    try:
        balance = client.get_balance()
    except Exception as exc:  # noqa: BLE001 — surface any startup failure
        print(f"ABORT: Kalshi balance check failed: {exc}", file=sys.stderr)
        client.close()
        return 3

    ledger = SqlPortfolioLedger(factory)
    risk = ExposureRiskEngine(limits)
    adapter = KalshiAdapter(client=client, resolver=resolver)

    print("=== KALSHI LIVE TRADING ===")
    print(f"  base url:      {settings.kalshi_base_url}")
    print(f"  balance:       {balance}")
    print(f"  daily realized:{ledger.daily_realized_pnl():+.2f}")
    print(f"  ticker count:  {resolver.ticker_count}")
    print(f"  per-order cap: ${limits.per_order_cap:.2f}")
    print(f"  daily loss cap:${limits.daily_loss_cap:.2f}")
    print(f"  decisions file:{args.decisions}")
    print("============================")

    if not args.yes:
        try:
            answer = input("Type 'y' to proceed: ").strip().lower()
        except EOFError:
            answer = ""
        if answer != "y":
            print("Aborted by operator.")
            client.close()
            return 1

    def _on_sigint(_signum: int, _frame: object) -> None:
        print("\nSIGINT received — engaging kill switch.", file=sys.stderr)
        set_kill_switch(factory, killed=True, set_by="sigint")

    signal.signal(signal.SIGINT, _on_sigint)

    decisions = _load_decisions(Path(args.decisions))
    loop = TradingLoop(
        risk_engine=risk,
        ledger=ledger,
        adapter=adapter,
        session_factory=factory,
    )
    result = loop.run_decisions(decisions, exchange="kalshi", stake=limits.per_order_cap)
    print(
        f"live-loop accepted={result.accepted} rejected={result.rejected} "
        f"fills={result.fills} events={result.events} "
        f"open_positions={len(ledger.open_positions())}"
    )
    client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Smoke-check the CLI refuses without env**

Run (without env var set):

```
python scripts/run_trading_loop.py --live --decisions config/kalshi_symbols.example.json
```

Expected: exits with code 2 and message about `KALSHI_LIVE_TRADING`. (Note: passing the example symbols file as decisions is intentionally wrong — the script should refuse before it ever tries to load it.)

- [ ] **Step 3: Smoke-check the CLI refuses with env but no creds**

PowerShell:

```
$env:KALSHI_LIVE_TRADING="1"; python scripts/run_trading_loop.py --live --decisions config/kalshi_symbols.example.json
```

Expected: exits with code 2 and message about missing API key / key path. Then unset:

```
Remove-Item Env:KALSHI_LIVE_TRADING
```

- [ ] **Step 4: Commit**

```
git add scripts/run_trading_loop.py
git commit -m "feat(cli): scripts/run_trading_loop.py for live Kalshi runs"
```

---

## Task 14: Kalshi demo integration smoke test

**Files:**
- Create: `tests/integration/test_kalshi_demo_smoke.py`

- [ ] **Step 1: Write the gated integration test**

Create `tests/integration/test_kalshi_demo_smoke.py`:

```python
"""End-to-end smoke test against Kalshi demo environment.

Requires the following env vars:
  KALSHI_DEMO_API_KEY_ID
  KALSHI_DEMO_PRIVATE_KEY_PATH
  KALSHI_DEMO_TICKER         (a known-tradable demo ticker)
  KALSHI_DEMO_PLAYER_ID      (used by symbol map; can be any int)
  KALSHI_DEMO_GAME_DATE      (YYYY-MM-DD)

If any of these are unset, the test is skipped.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.base import Base
from app.db.models.trading import (
    TradingDailyPnL, TradingFill, TradingKillSwitch, TradingOrder, TradingPosition,
)
from app.providers.exchanges.kalshi_client import KalshiClient
from app.trading.kalshi_adapter import KalshiAdapter
from app.trading.loop import TradingLoop
from app.trading.risk import ExposureRiskEngine, RiskLimits
from app.trading.sql_ledger import SqlPortfolioLedger
from app.trading.symbol_resolver import SymbolResolver
from app.trading.types import Signal

REQUIRED_ENV = (
    "KALSHI_DEMO_API_KEY_ID",
    "KALSHI_DEMO_PRIVATE_KEY_PATH",
    "KALSHI_DEMO_TICKER",
    "KALSHI_DEMO_PLAYER_ID",
    "KALSHI_DEMO_GAME_DATE",
)


@pytest.mark.integration
def test_kalshi_demo_one_dollar_order_end_to_end(tmp_path: Path) -> None:
    missing = [key for key in REQUIRED_ENV if not os.environ.get(key)]
    if missing:
        pytest.skip(f"Kalshi demo env vars missing: {missing}")

    engine = create_engine(f"sqlite:///{tmp_path / 'demo.sqlite'}", future=True)
    Base.metadata.create_all(engine, tables=[
        TradingOrder.__table__, TradingFill.__table__, TradingPosition.__table__,
        TradingKillSwitch.__table__, TradingDailyPnL.__table__,
    ])
    factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

    resolver = SymbolResolver(entries=[{
        "market_key": "points",
        "side": "over",
        "line_value": 25.5,
        "player_id": int(os.environ["KALSHI_DEMO_PLAYER_ID"]),
        "game_date": os.environ["KALSHI_DEMO_GAME_DATE"],
        "kalshi_ticker": os.environ["KALSHI_DEMO_TICKER"],
    }])

    client = KalshiClient(
        api_key_id=os.environ["KALSHI_DEMO_API_KEY_ID"],
        private_key_path=os.environ["KALSHI_DEMO_PRIVATE_KEY_PATH"],
        base_url="https://demo-api.kalshi.co",
    )

    ledger = SqlPortfolioLedger(factory)
    adapter = KalshiAdapter(client=client, resolver=resolver)

    signal = Signal(
        signal_id="demo-1",
        created_at=datetime.now(UTC),
        market_key="points",
        side="OVER",
        confidence="high",
        edge=0.05,
        model_probability=0.55,
        line_value=25.5,
        metadata={
            "player_id": int(os.environ["KALSHI_DEMO_PLAYER_ID"]),
            "game_id": 1,
            "game_date": os.environ["KALSHI_DEMO_GAME_DATE"],
        },
    )

    loop = TradingLoop(
        risk_engine=ExposureRiskEngine(RiskLimits(
            per_order_cap=1.0,
            per_market_cap=1.0,
            max_open_notional=1.0,
            daily_loss_cap=10.0,
        )),
        ledger=ledger,
        adapter=adapter,
        session_factory=factory,
    )

    try:
        result = loop.run_signals([signal], exchange="kalshi", stake=1.0)
    finally:
        client.close()

    assert result.events >= 1
    if result.fills > 0:
        with factory() as session:
            from sqlalchemy import select
            row_count = session.execute(select(TradingFill)).scalars().all()
            assert len(row_count) == result.fills
```

- [ ] **Step 2: Verify the test is collected and skipped without env**

```
pytest tests/integration/test_kalshi_demo_smoke.py -v
```

Expected: 1 skipped (with the missing-env-vars message).

- [ ] **Step 3: Commit**

```
git add tests/integration/test_kalshi_demo_smoke.py
git commit -m "test(integration): gated Kalshi demo smoke test"
```

---

## Task 15: Run full test suite and lint

**Files:** none (verification only)

- [ ] **Step 1: Run the full test suite**

```
pytest
```

Expected: every test passes; the demo smoke test is skipped without env.

- [ ] **Step 2: Run the linter**

```
ruff check .
```

Expected: clean.

- [ ] **Step 3: Run the type checker**

```
mypy app
```

Expected: clean. If errors point at intentional Protocol design (e.g. structural typing edges), narrow the offending types or add `# type: ignore[<rule>]` with a one-line justification.

- [ ] **Step 4: If anything fails, fix and re-run before marking complete.**

- [ ] **Step 5: Final commit (only if any lint/type fixes were needed)**

```
git add -u
git commit -m "chore: lint and type-check fixes for live trading wire"
```

---

## Acceptance recap (mirrors spec §10)

After all tasks complete, the following must hold:

1. `pytest tests/unit/` passes.
2. `pytest -m integration tests/integration/test_kalshi_demo_smoke.py` passes when demo env vars are set.
3. Operator can run
   `KALSHI_LIVE_TRADING=1 python scripts/run_trading_loop.py --live --decisions today.json`
   against the live Kalshi API and observe **one** real fill recorded in SQLite.
4. `POST /api/trading/kill-switch` mid-run halts the next iteration.
5. `GET /api/trading/pnl` returns `active_limits` matching `config/trading_limits.json`.
6. No code path can place a live order without **both** the `--live` flag AND `KALSHI_LIVE_TRADING=1`.

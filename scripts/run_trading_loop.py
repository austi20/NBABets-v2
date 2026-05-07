"""Guarded live Kalshi trading entry point for Spec 1."""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
from pathlib import Path
from typing import Any, cast

from sqlalchemy import Table

from app.config.settings import get_settings
from app.db.base import Base
from app.db.models.trading import (
    TradingDailyPnL,
    TradingFill,
    TradingKillSwitch,
    TradingOrder,
    TradingPosition,
)
from app.db.session import SessionLocal, configure_engine, get_engine
from app.evaluation.prop_decision import PropDecision
from app.providers.exchanges.kalshi_client import KalshiClient
from app.trading.kalshi_adapter import KalshiAdapter
from app.trading.live_limits import load_live_limits
from app.trading.loop import TradingLoop, set_kill_switch
from app.trading.risk import ExposureRiskEngine
from app.trading.sql_ledger import SqlPortfolioLedger
from app.trading.symbol_resolver import load_symbol_resolver

_REQUIRED_LIVE_DECISION_FIELDS = ("market_key", "recommendation", "line_value", "player_id", "game_date")
_LIVE_GATE_FIELDS = (
    "symbol_resolved",
    "fresh_market_snapshot",
    "spread_within_limit",
    "one_order_cap_ok",
    "price_within_limit",
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one guarded live Kalshi trading cycle.")
    parser.add_argument("--live", action="store_true", help="Required confirmation flag for live execution.")
    parser.add_argument("--decisions", required=True, help="Path to a JSON list of PropDecision-like objects.")
    parser.add_argument("--yes", action="store_true", help="Skip the final operator confirmation prompt.")
    return parser.parse_args()


def _ensure_tables() -> None:
    tables = [
        cast(Table, TradingOrder.__table__),
        cast(Table, TradingFill.__table__),
        cast(Table, TradingPosition.__table__),
        cast(Table, TradingKillSwitch.__table__),
        cast(Table, TradingDailyPnL.__table__),
    ]
    Base.metadata.create_all(
        get_engine(),
        tables=tables,
    )


def _confirm(args: argparse.Namespace) -> bool:
    if args.yes:
        return True
    try:
        return input("Type 'LIVE' to place at most one Kalshi order: ").strip() == "LIVE"
    except EOFError:
        return False


def _decision_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict) and isinstance(payload.get("decisions"), list):
        rows = payload["decisions"]
    else:
        raise ValueError("decisions file must contain a non-empty JSON list or a decisions array")
    if not rows:
        raise ValueError("decisions file must contain a non-empty JSON list")
    if not all(isinstance(row, dict) for row in rows):
        raise ValueError("live decisions must be JSON objects")
    return rows


def _live_side(raw: object) -> str | None:
    value = str(raw).strip().lower()
    if value in {"over", "buy_yes", "yes"}:
        return "OVER"
    if value in {"under", "buy_no", "no"}:
        return "UNDER"
    if value in {"observe", "observe_only", "watch"}:
        return None
    raise ValueError("first live decision recommendation must be OVER, UNDER, buy_yes, or buy_no")


def _require_rich_live_gates(row: dict[str, Any]) -> None:
    if "execution" not in row and "gates" not in row and "kalshi" not in row:
        return
    mode = str(row.get("mode", "")).strip().lower()
    if mode != "live":
        raise ValueError("first live decision is not in live mode")
    execution = row.get("execution")
    if not isinstance(execution, dict) or execution.get("allow_live_submit") is not True:
        raise ValueError("first live decision is not live-submit enabled")
    gates = row.get("gates")
    if not isinstance(gates, dict):
        raise ValueError("first live decision missing execution gates")
    failed = [field for field in _LIVE_GATE_FIELDS if gates.get(field) is not True]
    if failed:
        raise ValueError(f"first live decision failed gate(s): {', '.join(failed)}")
    kalshi = row.get("kalshi")
    if not isinstance(kalshi, dict) or not kalshi.get("ticker"):
        raise ValueError("first live decision missing resolved Kalshi ticker")


def _confidence_label(value: object) -> str:
    if isinstance(value, int | float):
        return "high" if float(value) >= 0.60 else "watch"
    text = str(value or "watch").strip().lower()
    return text or "watch"


def _decision_from_row(row: dict[str, Any], side: str) -> PropDecision:
    kalshi = row.get("kalshi") if isinstance(row.get("kalshi"), dict) else {}
    edge_bps = row.get("edge_bps")
    metadata: dict[str, Any] = {}
    if isinstance(kalshi, dict):
        for key in ("max_price_dollars", "post_only", "time_in_force"):
            if kalshi.get(key) is not None:
                metadata[key] = kalshi[key]
        if kalshi.get("ticker") is not None:
            metadata["kalshi_ticker"] = kalshi["ticker"]
        if kalshi.get("target_id") is not None:
            metadata["kalshi_target_id"] = kalshi["target_id"]
    return PropDecision(
        model_prob=float(row.get("model_prob", row.get("model_probability", row.get("confidence", 0.5))) or 0.5),
        market_prob=float(row.get("market_prob", row.get("kalshi_market_prob", 0.5)) or 0.5),
        no_vig_market_prob=float(row.get("no_vig_market_prob", row.get("market_prob", 0.5)) or 0.5),
        ev=float(row.get("ev", (float(edge_bps) / 10000.0 if edge_bps is not None else 0.0)) or 0.0),
        recommendation=side,
        confidence=_confidence_label(row.get("confidence")),
        driver=str(row.get("driver", row.get("source_model", "kalshi_decision_file"))),
        market_key=str(row["market_key"]),
        line_value=float(row["line_value"]),
        over_odds=(int(row["over_odds"]) if row.get("over_odds") is not None else None),
        under_odds=(int(row["under_odds"]) if row.get("under_odds") is not None else None),
        player_id=row.get("player_id"),
        game_id=row.get("game_id"),
        game_date=str(row["game_date"]),
        metadata=metadata,
    )


def _load_live_decisions(path: Path) -> tuple[list[PropDecision], int]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ValueError(f"decisions file could not be read: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"decisions file is malformed JSON: {exc}") from exc
    rows = _decision_rows(payload)
    first = rows[0]
    _require_rich_live_gates(first)
    missing = [field for field in _REQUIRED_LIVE_DECISION_FIELDS if first.get(field) in (None, "")]
    if missing:
        raise ValueError(f"first live decision missing required field(s): {', '.join(missing)}")
    side = _live_side(first["recommendation"])
    if side is None:
        raise ValueError("first live decision is observe-only")
    decisions = [_decision_from_row(first, side)]
    return decisions, len(rows)


def main() -> int:
    args = _parse_args()
    settings = get_settings()
    client: KalshiClient | None = None

    try:
        if not args.live:
            print("ABORT: --live flag is required.", file=sys.stderr)
            return 2
        if os.environ.get("KALSHI_LIVE_TRADING") != "1":
            print("ABORT: KALSHI_LIVE_TRADING must be exactly '1'.", file=sys.stderr)
            return 2
        if not settings.kalshi_api_key_id or not settings.kalshi_private_key_path:
            print("ABORT: KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH are required.", file=sys.stderr)
            return 2
        private_key_path = Path(settings.kalshi_private_key_path)
        if not private_key_path.exists():
            print(f"ABORT: KALSHI_PRIVATE_KEY_PATH does not exist: {private_key_path}", file=sys.stderr)
            return 2

        try:
            decisions, decision_count = _load_live_decisions(Path(args.decisions))
        except ValueError as exc:
            print(f"ABORT: {exc}", file=sys.stderr)
            return 2
        if decision_count > 1:
            print("Spec 1 cap: using only the first decision; remaining decisions ignored.", file=sys.stderr)

        configure_engine()
        _ensure_tables()
        limits = load_live_limits(settings.trading_limits_path)
        resolver = load_symbol_resolver(settings.kalshi_symbols_path)

        client = KalshiClient(
            api_key_id=settings.kalshi_api_key_id,
            private_key_path=private_key_path,
            base_url=settings.kalshi_base_url,
        )
        balance = client.get_balance()

        ledger = SqlPortfolioLedger(SessionLocal)
        risk = ExposureRiskEngine(limits)
        adapter = KalshiAdapter(client=client, resolver=resolver)

        print("=== KALSHI LIVE TRADING ===")
        print(f"base_url={settings.kalshi_base_url}")
        print(f"balance={balance}")
        print(f"ticker_count={resolver.ticker_count}")
        print(f"per_order_cap={limits.per_order_cap:.2f}")
        print(f"daily_realized_pnl={ledger.daily_realized_pnl():+.2f}")
        print("============================")

        if not _confirm(args):
            print("Aborted by operator.")
            return 1

        def _on_sigint(_signum: int, _frame: object) -> None:
            print("\nSIGINT received; engaging SQL kill switch.", file=sys.stderr)
            set_kill_switch(SessionLocal, killed=True, set_by="sigint")

        signal.signal(signal.SIGINT, _on_sigint)

        loop = TradingLoop(
            risk_engine=risk,
            ledger=ledger,
            adapter=adapter,
            session_factory=SessionLocal,
        )
        result = loop.run_decisions(decisions, exchange="kalshi", stake=limits.per_order_cap)
        print(
            f"live-loop accepted={result.accepted} rejected={result.rejected} "
            f"fills={result.fills} events={result.events} "
            f"open_positions={len(ledger.open_positions())}"
        )
        if result.fills < 1:
            print("ABORT: live cycle completed without a persisted fill.", file=sys.stderr)
            return 4
        return 0
    finally:
        if client is not None:
            try:
                client.close()
            except Exception as exc:  # noqa: BLE001
                print(f"WARN: failed to close Kalshi client: {exc}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())

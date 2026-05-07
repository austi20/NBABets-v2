"""Guarded live Kalshi trading entry point for Spec 1."""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
from pathlib import Path
from typing import cast

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
from app.trading.loop import TradingLoop, _load_decisions, set_kill_switch
from app.trading.risk import ExposureRiskEngine
from app.trading.sql_ledger import SqlPortfolioLedger
from app.trading.symbol_resolver import load_symbol_resolver

_REQUIRED_LIVE_DECISION_FIELDS = ("market_key", "recommendation", "line_value", "player_id", "game_date")


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


def _load_live_decisions(path: Path) -> tuple[list[PropDecision], int]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ValueError(f"decisions file could not be read: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"decisions file is malformed JSON: {exc}") from exc
    if not isinstance(payload, list) or not payload:
        raise ValueError("decisions file must contain a non-empty JSON list")
    first = payload[0]
    if not isinstance(first, dict):
        raise ValueError("first live decision must be a JSON object")
    missing = [field for field in _REQUIRED_LIVE_DECISION_FIELDS if first.get(field) in (None, "")]
    if missing:
        raise ValueError(f"first live decision missing required field(s): {', '.join(missing)}")
    side = str(first["recommendation"]).strip().upper()
    if side not in {"OVER", "UNDER"}:
        raise ValueError("first live decision recommendation must be OVER or UNDER")
    decisions = _load_decisions(path)
    if not decisions:
        raise ValueError("decisions file did not contain any executable decisions")
    return decisions[:1], len(payload)


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

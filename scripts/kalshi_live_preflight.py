"""Read-only preflight for a supervised live Kalshi trading run.

This script does not place, cancel, or modify orders. It validates the current
decision pack, limits, executable price, and optional authenticated account
visibility before the operator enables live trading.
"""

from __future__ import annotations

import math
import sys
from datetime import date
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config.settings import get_settings  # noqa: E402
from app.providers.exchanges.kalshi_client import KalshiClient  # noqa: E402
from app.trading.ledger import InMemoryPortfolioLedger  # noqa: E402
from app.trading.live_limits import load_live_limits  # noqa: E402
from app.trading.monitoring import KalshiPublicMarketDataClient, MonitoredSymbol, build_monitor_snapshot  # noqa: E402
from app.trading.symbol_resolver import SymbolResolverConfigError, load_symbol_resolver  # noqa: E402
from scripts.run_trading_loop import _load_live_decisions  # noqa: E402


def _metadata_float(metadata: dict[str, Any], key: str) -> float | None:
    value = metadata.get(key)
    if value in (None, ""):
        return None
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _check(label: str, ok: bool, detail: str, failures: list[str]) -> None:
    status = "PASS" if ok else "FAIL"
    print(f"{status}: {label}: {detail}")
    if not ok:
        failures.append(f"{label}: {detail}")


def main() -> int:
    settings = get_settings()
    failures: list[str] = []

    decisions_path = Path(settings.kalshi_decisions_path)
    print("=== KALSHI LIVE PREFLIGHT (READ ONLY) ===")
    print(f"date={date.today().isoformat()} decisions={decisions_path}")
    print(f"base_url={settings.kalshi_base_url}")
    print(f"live_env={'enabled' if settings.kalshi_live_trading else 'disabled'}")

    try:
        limits = load_live_limits(settings.trading_limits_path)
    except Exception as exc:  # noqa: BLE001 - surfaced as preflight failure
        print(f"FAIL: limits: {exc}")
        return 2

    _check("per-order cap", limits.per_order_cap <= 1.0, f"{limits.per_order_cap:.2f} <= 1.00", failures)
    _check("daily loss cap", limits.daily_loss_cap <= 10.0, f"{limits.daily_loss_cap:.2f} <= 10.00", failures)
    _check(
        "open notional cap",
        limits.max_open_notional <= limits.daily_loss_cap,
        f"{limits.max_open_notional:.2f} <= daily loss cap {limits.daily_loss_cap:.2f}",
        failures,
    )

    try:
        decisions, decision_count = _load_live_decisions(decisions_path)
    except Exception as exc:  # noqa: BLE001 - surfaced as preflight failure
        print(f"FAIL: live decision pack: {exc}")
        return 2
    if decision_count > 1:
        print(f"WARN: live runner will use only the first of {decision_count} decisions.")
    decision = decisions[0]
    metadata = decision.metadata
    ticker = metadata.get("kalshi_ticker")
    _check("verified ticker", bool(ticker and metadata.get("kalshi_ticker_verified") is True), str(ticker), failures)
    max_contracts = _metadata_float(metadata, "max_contracts")
    _check("max contracts", max_contracts is not None and max_contracts <= 1.0, str(max_contracts), failures)

    try:
        resolver = load_symbol_resolver(settings.kalshi_symbols_path)
        print(f"PASS: symbol map: loaded {resolver.ticker_count} executable ticker(s)")
    except SymbolResolverConfigError as exc:
        if ticker and metadata.get("kalshi_ticker_verified") is True:
            print(f"WARN: symbol map not live-clean; decision-pack verified ticker will be used: {exc}")
        else:
            failures.append(f"symbol map: {exc}")
            print(f"FAIL: symbol map: {exc}")

    if ticker:
        symbol = MonitoredSymbol(
            ticker=str(ticker),
            market_key=decision.market_key,
            side=decision.recommendation.upper(),
            line_value=float(decision.line_value),
            player_id=str(decision.player_id) if decision.player_id is not None else None,
            game_date=str(decision.game_date),
        )
        with KalshiPublicMarketDataClient(base_url=settings.kalshi_market_data_base_url) as market_client:
            snapshot = build_monitor_snapshot(
                ledger=InMemoryPortfolioLedger(),
                limits=limits,
                kill_switch_active=False,
                monitored_symbols=[symbol],
                market_client=market_client,
            )
        quote = snapshot.quotes[0] if snapshot.quotes else None
        entry_price = quote.entry_price if quote is not None else None
        max_price = _metadata_float(metadata, "max_price_dollars")
        _check(
            "entry price available",
            entry_price is not None and entry_price > 0,
            f"{entry_price}",
            failures,
        )
        if entry_price is not None and entry_price > 0:
            count = math.floor(limits.per_order_cap / entry_price)
            if max_contracts is not None:
                count = min(count, int(max_contracts))
            _check("computed order count", count == 1, f"{count} contract(s)", failures)
            if max_price is not None:
                _check(
                    "max price gate",
                    entry_price <= max_price,
                    f"{entry_price:.4f} <= {max_price:.4f}",
                    failures,
                )
            print(
                "QUOTE: "
                f"ticker={ticker} side={decision.recommendation.upper()} "
                f"entry={entry_price:.4f} exit={quote.exit_price if quote else None} "
                f"yes={quote.yes_bid if quote else None}/{quote.yes_ask if quote else None} "
                f"no={quote.no_bid if quote else None}/{quote.no_ask if quote else None}"
            )

    private_key_path = Path(settings.kalshi_private_key_path) if settings.kalshi_private_key_path else None
    if settings.kalshi_api_key_id and private_key_path is not None and private_key_path.exists():
        with KalshiClient(
            api_key_id=settings.kalshi_api_key_id,
            private_key_path=private_key_path,
            base_url=settings.kalshi_base_url,
        ) as client:
            balance = client.get_balance()
            positions = client.get_positions(limit=100)
            resting = client.get_orders(status="resting", limit=100)
        position_count = len(positions.get("market_positions") or positions.get("positions") or [])
        resting_count = len(resting.get("orders") or [])
        print(f"PASS: account read: balance={balance} positions={position_count} resting_orders={resting_count}")
    else:
        print("WARN: account read skipped; KALSHI_API_KEY_ID/private key path not fully configured.")

    if failures:
        print("=== PREFLIGHT FAILED ===")
        for failure in failures:
            print(f"- {failure}")
        return 2
    print("=== PREFLIGHT PASSED ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

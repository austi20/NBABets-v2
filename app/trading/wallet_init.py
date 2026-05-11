# app/trading/wallet_init.py
from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Protocol

_log = logging.getLogger("nba.trading.wallet_init")


class _BalanceClient(Protocol):
    def get_balance(self) -> Any: ...


def _extract_balance(raw: Any) -> float:
    """Extract a dollar-denominated float from a Kalshi balance response.

    The Kalshi /portfolio/balance endpoint returns a dict with a ``balance``
    key in cents (integer).  Accept either a plain numeric value (for mocks
    and future API changes) or a dict with a ``balance`` key.
    """
    if isinstance(raw, dict):
        cents = raw.get("balance", 0)
        return float(cents) / 100.0
    return float(raw)


def init_budget_from_wallet(
    *,
    client: _BalanceClient,
    path: Path,
    today: date | None = None,
) -> None:
    """Seed config/trading_limits.json from the Kalshi wallet balance.

    Runs at most once per calendar day. Manual edits made via the UI between
    runs are preserved -- we only overwrite when wallet_init_done_at is older
    than the current calendar day.
    """
    today_date = today or date.today()
    existing = _read_existing(path)
    if _already_initialized_today(existing, today_date):
        return
    try:
        raw = client.get_balance()
        balance = _extract_balance(raw)
    except Exception as exc:  # noqa: BLE001
        _log.warning("wallet-init: balance fetch failed: %s", exc)
        return
    if balance <= 0:
        _log.info("wallet-init: balance is %.2f, skipping init", balance)
        return
    payload: dict[str, Any] = {
        "max_open_notional": round(balance, 2),
        "per_market_cap": balance / 2,
        "per_order_cap": round(balance / 10, 2),
        "daily_loss_cap": round(balance / 5, 2),
        "reject_cooldown_seconds": int(existing.get("reject_cooldown_seconds", 300)),
        "wallet_init_done_at": datetime.now(UTC).isoformat(),
    }
    for preserved in ("per_order_cap_override",):
        if preserved in existing:
            payload[preserved] = existing[preserved]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    _log.info("wallet-init: max_open_notional=%.2f", payload["max_open_notional"])


def _read_existing(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _already_initialized_today(existing: dict[str, Any], today_date: date) -> bool:
    stamp = existing.get("wallet_init_done_at")
    if not isinstance(stamp, str):
        return False
    try:
        return datetime.fromisoformat(stamp).date() == today_date
    except ValueError:
        return False

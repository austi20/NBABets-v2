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
        value = payload[field]
        if isinstance(value, bool) or not isinstance(value, int | float):
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

from __future__ import annotations

import json
import math
from dataclasses import dataclass, fields
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


def _reject_non_finite(value: str) -> None:
    raise LimitsConfigError(f"limits config contains non-finite value: {value}")


def load_live_limits(path: Path | str) -> RiskLimits:
    config_path = Path(path)
    if not config_path.is_file():
        raise LimitsConfigError(
            f"trading limits config not found at {config_path}; "
            "copy config/trading_limits.example.json to that path and edit."
        )
    try:
        payload = json.loads(
            config_path.read_text(encoding="utf-8"),
            parse_constant=_reject_non_finite,
        )
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
        if not math.isfinite(float(value)):
            raise LimitsConfigError(f"limits field {field} must be finite")
        if value < 0:
            raise LimitsConfigError(f"limits field {field} must be >= 0")
    cooldown = payload["reject_cooldown_seconds"]
    if not float(cooldown).is_integer():
        raise LimitsConfigError("limits field reject_cooldown_seconds must be a whole number")
    return RiskLimits(
        per_order_cap=float(payload["per_order_cap"]),
        per_market_cap=float(payload["per_market_cap"]),
        max_open_notional=float(payload["max_open_notional"]),
        daily_loss_cap=float(payload["daily_loss_cap"]),
        reject_cooldown_seconds=int(cooldown),
    )


@dataclass(frozen=True)
class ActiveTraderConfig:
    """Strategy parameters for the active-trader loop (Phase 1)."""

    enabled: bool
    tp_pct: float
    sl_pct: float
    conviction_hold: float
    max_entry_price: float
    min_model_prob: float
    min_edge_bps: int
    spread_max_cents: int
    volatility_max_cents: int
    volatility_override: float
    max_open_positions: int
    settle_hold_minutes: int
    pace_threshold_pct: float
    pace_remaining_pct_min: float
    cooldown_after_exit_seconds: int
    live_stats_poll_seconds: int
    order_write_rps_cap: int


ACTIVE_TRADER_DEFAULTS: ActiveTraderConfig = ActiveTraderConfig(
    enabled=False,
    tp_pct=0.50,
    sl_pct=0.50,
    conviction_hold=0.85,
    max_entry_price=0.80,
    min_model_prob=0.60,
    min_edge_bps=100,
    spread_max_cents=6,
    volatility_max_cents=5,
    volatility_override=0.80,
    max_open_positions=50,
    settle_hold_minutes=15,
    pace_threshold_pct=0.75,
    pace_remaining_pct_min=0.30,
    cooldown_after_exit_seconds=30,
    live_stats_poll_seconds=10,
    order_write_rps_cap=5,
)


_ACTIVE_TRADER_INT_FIELDS: frozenset[str] = frozenset(
    {
        "min_edge_bps",
        "spread_max_cents",
        "volatility_max_cents",
        "max_open_positions",
        "settle_hold_minutes",
        "cooldown_after_exit_seconds",
        "live_stats_poll_seconds",
        "order_write_rps_cap",
    }
)


_ACTIVE_TRADER_BOOL_FIELDS: frozenset[str] = frozenset({"enabled"})


def _validate_active_trader_bool(name: str, value: object) -> bool:
    if not isinstance(value, bool):
        raise LimitsConfigError(f"active_trader field {name} must be a boolean")
    return value


def _validate_active_trader_number(name: str, value: object, *, integer: bool) -> int | float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise LimitsConfigError(f"active_trader field {name} must be a number")
    if not math.isfinite(float(value)):
        raise LimitsConfigError(f"active_trader field {name} must be finite")
    if value < 0:
        raise LimitsConfigError(f"active_trader field {name} must be >= 0")
    if integer:
        if not float(value).is_integer():
            raise LimitsConfigError(f"active_trader field {name} must be a whole number")
        return int(value)
    return float(value)


def load_active_trader_config(path: Path | str) -> ActiveTraderConfig:
    """Load the active_trader block from the trading limits config.

    Missing file or missing/empty active_trader key -> defaults. Unknown keys
    inside the block are ignored for forward-compat.
    """
    config_path = Path(path)
    if not config_path.is_file():
        return ACTIVE_TRADER_DEFAULTS
    try:
        payload = json.loads(
            config_path.read_text(encoding="utf-8"),
            parse_constant=_reject_non_finite,
        )
    except json.JSONDecodeError as exc:
        raise LimitsConfigError(f"malformed JSON in {config_path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise LimitsConfigError(f"limits config in {config_path} must be a JSON object")
    block = payload.get("active_trader")
    if block is None or block == {}:
        return ACTIVE_TRADER_DEFAULTS
    if not isinstance(block, dict):
        raise LimitsConfigError("active_trader block must be a JSON object")

    kwargs: dict[str, object] = {}
    for field in fields(ActiveTraderConfig):
        name = field.name
        if name not in block:
            # Fall back to default for any missing key; forward-compat.
            kwargs[name] = getattr(ACTIVE_TRADER_DEFAULTS, name)
            continue
        raw = block[name]
        if name in _ACTIVE_TRADER_BOOL_FIELDS:
            kwargs[name] = _validate_active_trader_bool(name, raw)
        else:
            integer = name in _ACTIVE_TRADER_INT_FIELDS
            kwargs[name] = _validate_active_trader_number(name, raw, integer=integer)
    return ActiveTraderConfig(**kwargs)

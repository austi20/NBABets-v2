from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.trading.live_limits import (
    ACTIVE_TRADER_DEFAULTS,
    ActiveTraderConfig,
    LimitsConfigError,
    load_active_trader_config,
)

VALID_BLOCK: dict[str, object] = {
    "enabled": True,
    "tp_pct": 0.45,
    "sl_pct": 0.55,
    "conviction_hold": 0.9,
    "max_entry_price": 0.75,
    "min_model_prob": 0.62,
    "min_edge_bps": 120,
    "spread_max_cents": 4,
    "volatility_max_cents": 3,
    "volatility_override": 0.7,
    "max_open_positions": 25,
    "settle_hold_minutes": 10,
    "pace_threshold_pct": 0.8,
    "pace_remaining_pct_min": 0.25,
    "cooldown_after_exit_seconds": 45,
    "live_stats_poll_seconds": 5,
    "order_write_rps_cap": 3,
}


def _write(tmp_path: Path, payload: dict[str, object]) -> Path:
    path = tmp_path / "trading_limits.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_loads_valid_active_trader_block(tmp_path: Path) -> None:
    path = _write(tmp_path, {"active_trader": VALID_BLOCK})
    cfg = load_active_trader_config(path)
    assert isinstance(cfg, ActiveTraderConfig)
    assert cfg.enabled is True
    assert cfg.tp_pct == 0.45
    assert cfg.sl_pct == 0.55
    assert cfg.conviction_hold == 0.9
    assert cfg.max_entry_price == 0.75
    assert cfg.min_model_prob == 0.62
    assert cfg.min_edge_bps == 120
    assert cfg.spread_max_cents == 4
    assert cfg.volatility_max_cents == 3
    assert cfg.volatility_override == 0.7
    assert cfg.max_open_positions == 25
    assert cfg.settle_hold_minutes == 10
    assert cfg.pace_threshold_pct == 0.8
    assert cfg.pace_remaining_pct_min == 0.25
    assert cfg.cooldown_after_exit_seconds == 45
    assert cfg.live_stats_poll_seconds == 5
    assert cfg.order_write_rps_cap == 3


def test_missing_active_trader_key_returns_defaults(tmp_path: Path) -> None:
    path = _write(tmp_path, {"per_order_cap": 0.25})
    assert load_active_trader_config(path) == ACTIVE_TRADER_DEFAULTS


def test_empty_active_trader_returns_defaults(tmp_path: Path) -> None:
    path = _write(tmp_path, {"active_trader": {}})
    assert load_active_trader_config(path) == ACTIVE_TRADER_DEFAULTS


def test_missing_file_returns_defaults(tmp_path: Path) -> None:
    path = tmp_path / "does_not_exist.json"
    assert load_active_trader_config(path) == ACTIVE_TRADER_DEFAULTS


def test_unknown_keys_are_ignored(tmp_path: Path) -> None:
    block = dict(VALID_BLOCK)
    block["future_param"] = 99
    block["another_one"] = "string"
    path = _write(tmp_path, {"active_trader": block})
    cfg = load_active_trader_config(path)
    assert cfg.enabled is True
    assert cfg.tp_pct == 0.45


def test_negative_tp_pct_rejected(tmp_path: Path) -> None:
    block = dict(VALID_BLOCK)
    block["tp_pct"] = -0.1
    path = _write(tmp_path, {"active_trader": block})
    with pytest.raises(LimitsConfigError, match="tp_pct"):
        load_active_trader_config(path)


def test_enabled_as_number_rejected(tmp_path: Path) -> None:
    block = dict(VALID_BLOCK)
    block["enabled"] = 1
    path = _write(tmp_path, {"active_trader": block})
    with pytest.raises(LimitsConfigError, match="enabled"):
        load_active_trader_config(path)


def test_non_finite_value_rejected(tmp_path: Path) -> None:
    # json.loads with parse_constant rejects Infinity/NaN literals before our
    # validators run; build the payload string by hand to exercise that path.
    path = tmp_path / "trading_limits.json"
    path.write_text(
        '{"active_trader": {"tp_pct": Infinity, "enabled": false}}',
        encoding="utf-8",
    )
    with pytest.raises(LimitsConfigError):
        load_active_trader_config(path)


def test_wrong_type_string_rejected(tmp_path: Path) -> None:
    block = dict(VALID_BLOCK)
    block["min_edge_bps"] = "100"
    path = _write(tmp_path, {"active_trader": block})
    with pytest.raises(LimitsConfigError, match="min_edge_bps"):
        load_active_trader_config(path)


def test_integer_field_rejects_non_integer_float(tmp_path: Path) -> None:
    block = dict(VALID_BLOCK)
    block["max_open_positions"] = 5.5
    path = _write(tmp_path, {"active_trader": block})
    with pytest.raises(LimitsConfigError, match="max_open_positions"):
        load_active_trader_config(path)


def test_integer_field_accepts_whole_float(tmp_path: Path) -> None:
    block = dict(VALID_BLOCK)
    block["max_open_positions"] = 30.0
    path = _write(tmp_path, {"active_trader": block})
    cfg = load_active_trader_config(path)
    assert cfg.max_open_positions == 30
    assert isinstance(cfg.max_open_positions, int)


def test_active_trader_block_not_a_dict_rejected(tmp_path: Path) -> None:
    path = _write(tmp_path, {"active_trader": [1, 2, 3]})
    with pytest.raises(LimitsConfigError, match="active_trader block"):
        load_active_trader_config(path)

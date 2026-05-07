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

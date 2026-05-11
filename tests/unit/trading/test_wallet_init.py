# tests/unit/trading/test_wallet_init.py
from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from app.trading.wallet_init import init_budget_from_wallet


@pytest.fixture()
def limits_path(tmp_path: Path) -> Path:
    return tmp_path / "trading_limits.json"


def test_creates_file_when_missing(limits_path: Path) -> None:
    client = MagicMock()
    client.get_balance.return_value = 10.11
    init_budget_from_wallet(client=client, path=limits_path, today=date(2026, 5, 11))
    data = json.loads(limits_path.read_text())
    assert data["max_open_notional"] == 10.11
    assert data["per_market_cap"] == 10.11 / 2
    assert data["daily_loss_cap"] == pytest.approx(10.11 / 5, abs=0.01)
    assert data["wallet_init_done_at"].startswith("2026-05-11")


def test_skips_when_same_day_init_already_done(limits_path: Path) -> None:
    now = datetime.now(UTC).isoformat()
    limits_path.write_text(
        json.dumps(
            {
                "max_open_notional": 5.00,
                "per_market_cap": 2.50,
                "daily_loss_cap": 1.00,
                "reject_cooldown_seconds": 300,
                "wallet_init_done_at": now,
            }
        )
    )
    client = MagicMock()
    client.get_balance.return_value = 999.99
    init_budget_from_wallet(client=client, path=limits_path, today=date.fromisoformat(now[:10]))
    data = json.loads(limits_path.read_text())
    assert data["max_open_notional"] == 5.00
    client.get_balance.assert_not_called()


def test_reinitializes_when_yesterday(limits_path: Path) -> None:
    yesterday = (datetime.now(UTC) - timedelta(days=1)).isoformat()
    limits_path.write_text(
        json.dumps(
            {
                "max_open_notional": 5.00,
                "per_market_cap": 2.50,
                "daily_loss_cap": 1.00,
                "reject_cooldown_seconds": 300,
                "wallet_init_done_at": yesterday,
            }
        )
    )
    client = MagicMock()
    client.get_balance.return_value = 20.00
    init_budget_from_wallet(client=client, path=limits_path)
    data = json.loads(limits_path.read_text())
    assert data["max_open_notional"] == 20.00


def test_swallows_kalshi_errors(limits_path: Path) -> None:
    client = MagicMock()
    client.get_balance.side_effect = RuntimeError("kalshi down")
    init_budget_from_wallet(client=client, path=limits_path)
    assert not limits_path.is_file()

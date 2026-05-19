"""Verify that sync_decision_brain preserves a valid same-day decisions file
when a transient error fires inside the sync (e.g. Kalshi 429).
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest

from app.config.settings import Settings
from app.trading.decision_brain import (
    _existing_decisions_match_board_date,
    sync_decision_brain,
)


def _write_synced_pack(path: Path, board_date: date, ticker: str = "KX-TEST-1") -> None:
    payload = {
        "version": 1,
        "state": "synced",
        "board_date": board_date.isoformat(),
        "decisions": [
            {
                "decision_id": "test-1",
                "mode": "live",
                "market_key": "rebounds",
                "line_value": 7.5,
                "game_date": board_date.isoformat(),
                "kalshi": {"ticker": ticker},
            }
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_existing_decisions_match_board_date_true(tmp_path: Path) -> None:
    path = tmp_path / "decisions.json"
    _write_synced_pack(path, date(2026, 5, 19))
    settings = _settings_with_path(path)
    assert _existing_decisions_match_board_date(settings, date(2026, 5, 19)) is True


def test_existing_decisions_match_board_date_wrong_date(tmp_path: Path) -> None:
    path = tmp_path / "decisions.json"
    _write_synced_pack(path, date(2026, 5, 18))
    settings = _settings_with_path(path)
    assert _existing_decisions_match_board_date(settings, date(2026, 5, 19)) is False


def test_existing_decisions_match_board_date_blocked_state(tmp_path: Path) -> None:
    path = tmp_path / "decisions.json"
    path.write_text(json.dumps({"state": "blocked", "decisions": []}), encoding="utf-8")
    settings = _settings_with_path(path)
    assert _existing_decisions_match_board_date(settings, date(2026, 5, 19)) is False


def test_existing_decisions_match_board_date_missing_file(tmp_path: Path) -> None:
    settings = _settings_with_path(tmp_path / "missing.json")
    assert _existing_decisions_match_board_date(settings, date(2026, 5, 19)) is False


def test_sync_preserves_existing_pack_on_transient_error(tmp_path: Path) -> None:
    """A non-deterministic error mid-sync must NOT blank a valid same-day pack."""
    path = tmp_path / "decisions.json"
    target_date = date(2026, 5, 19)
    _write_synced_pack(path, target_date, ticker="KX-PRESERVED")
    settings = _settings_with_path(path)

    # Force the inner candidate-gathering step to blow up with a non-deterministic
    # error (mimicking a Kalshi 429). The except branch should detect the existing
    # valid same-day pack and skip write_blocked_decision_pack.
    with patch(
        "app.trading.decision_brain.load_policy",
        side_effect=RuntimeError("Client error '429 Too Many Requests'"),
    ):
        result = sync_decision_brain(
            settings=settings,
            board_entry=None,
            board_date=target_date,
            mode="supervised-live",
            resolve_markets=False,
            build_pack=True,
        )

    on_disk = json.loads(path.read_text(encoding="utf-8"))
    assert on_disk["state"] == "synced", "existing pack was blanked despite being valid"
    assert on_disk["decisions"], "decisions array was wiped"
    assert on_disk["decisions"][0]["kalshi"]["ticker"] == "KX-PRESERVED"
    # The sync result should now report synced (preserved) so loop_controller proceeds.
    assert result.state == "synced"


def test_sync_writes_blocked_pack_when_no_valid_existing(tmp_path: Path) -> None:
    """If there's no same-day pack on disk, a transient error still writes blocked."""
    path = tmp_path / "decisions.json"
    settings = _settings_with_path(path)

    with patch(
        "app.trading.decision_brain.load_policy",
        side_effect=RuntimeError("Client error '429'"),
    ):
        result = sync_decision_brain(
            settings=settings,
            board_entry=None,
            board_date=date(2026, 5, 19),
            mode="supervised-live",
            resolve_markets=False,
            build_pack=True,
        )

    on_disk = json.loads(path.read_text(encoding="utf-8"))
    assert on_disk["state"] == "blocked"
    assert on_disk["decisions"] == []
    assert result.state == "failed"


def _settings_with_path(path: Path) -> Settings:
    """Build a Settings stub good enough for the decision_brain helpers under test."""
    from unittest.mock import MagicMock

    settings = MagicMock(spec=[])
    settings.kalshi_decisions_path = str(path)
    settings.kalshi_decision_brain_root = None
    settings.brain_vault_root = str(path.parent)
    settings.snapshot_dir = str(path.parent)
    settings.database_url = "sqlite:///:memory:"
    settings.kalshi_resolution_targets_path = str(path.parent / "targets.json")
    settings.kalshi_symbols_path = str(path.parent / "symbols.json")
    return settings  # type: ignore[return-value]


# Ruff: unused import suppress.
_ = pytest

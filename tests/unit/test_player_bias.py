"""Per-player bias offset loader + lookup tests."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from app.config.settings import get_settings
from app.services import player_bias


@pytest.fixture(autouse=True)
def _reset_caches():
    player_bias.reset_caches()
    get_settings.cache_clear()  # type: ignore[attr-defined]
    yield
    player_bias.reset_caches()
    get_settings.cache_clear()  # type: ignore[attr-defined]


def test_returns_none_when_no_db_player_id() -> None:
    assert player_bias.get_player_bias_offset(None) is None


def test_returns_none_when_disabled(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("PLAYER_BIAS_ENABLED", "false")
    assert player_bias.get_player_bias_offset(42) is None


def test_returns_none_when_file_missing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(player_bias, "_default_offsets_path", lambda: tmp_path / "absent.json")
    assert player_bias.get_player_bias_offset(42) is None


def test_returns_offset_when_player_present(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fixture = tmp_path / "offsets.json"
    fixture.write_text(json.dumps({
        "schema_version": 1,
        "offsets": {
            "BDL-123": {"name": "Tester", "n": 50, "offset": 0.123},
        },
    }), encoding="utf-8")
    monkeypatch.setattr(player_bias, "_default_offsets_path", lambda: fixture)
    with patch.object(player_bias, "_provider_id_for_db", return_value="BDL-123"):
        assert player_bias.get_player_bias_offset(99) == pytest.approx(0.123)


def test_returns_none_when_provider_id_not_in_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fixture = tmp_path / "offsets.json"
    fixture.write_text(json.dumps({
        "schema_version": 1,
        "offsets": {
            "BDL-123": {"name": "Tester", "n": 50, "offset": 0.123},
        },
    }), encoding="utf-8")
    monkeypatch.setattr(player_bias, "_default_offsets_path", lambda: fixture)
    with patch.object(player_bias, "_provider_id_for_db", return_value="BDL-999"):
        assert player_bias.get_player_bias_offset(99) is None


def test_returns_none_when_player_unknown_to_db(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fixture = tmp_path / "offsets.json"
    fixture.write_text(json.dumps({"schema_version": 1, "offsets": {}}), encoding="utf-8")
    monkeypatch.setattr(player_bias, "_default_offsets_path", lambda: fixture)
    with patch.object(player_bias, "_provider_id_for_db", return_value=None):
        assert player_bias.get_player_bias_offset(99) is None

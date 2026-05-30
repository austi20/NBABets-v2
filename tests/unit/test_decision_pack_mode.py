from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import app.server.routers.trading as trading


def _settings(path: Path) -> SimpleNamespace:
    return SimpleNamespace(kalshi_decisions_path=str(path))


def test_missing_pack_is_observe(tmp_path: Path) -> None:
    trading._DECISION_PACK_MODE_CACHE.clear()
    assert trading._decision_pack_mode(_settings(tmp_path / "none.json")) == "observe"


def test_live_mode_detected_when_not_first_row(tmp_path: Path) -> None:
    # The live row is NOT first; the old rows[0]-only check would miss it.
    trading._DECISION_PACK_MODE_CACHE.clear()
    pack = tmp_path / "decisions.json"
    pack.write_text(
        json.dumps([{"mode": "observe"}, {"mode": "live"}]),
        encoding="utf-8",
    )
    assert trading._decision_pack_mode(_settings(pack)) == "supervised-live"


def test_all_observe_rows_is_observe(tmp_path: Path) -> None:
    trading._DECISION_PACK_MODE_CACHE.clear()
    pack = tmp_path / "decisions.json"
    pack.write_text(json.dumps([{"mode": "observe"}, {"mode": "observe"}]), encoding="utf-8")
    assert trading._decision_pack_mode(_settings(pack)) == "observe"


def test_mode_cached_by_mtime(tmp_path: Path, monkeypatch) -> None:
    trading._DECISION_PACK_MODE_CACHE.clear()
    pack = tmp_path / "decisions.json"
    pack.write_text(json.dumps([{"mode": "live"}]), encoding="utf-8")
    settings = _settings(pack)
    assert trading._decision_pack_mode(settings) == "supervised-live"

    # Once cached for this mtime, the file is not re-read. Replace the reader
    # with a sentinel that would fail if called, and confirm the cached value.
    def _boom(_path: Path) -> str:  # pragma: no cover - must not be called
        raise AssertionError("decision pack re-read despite unchanged mtime")

    monkeypatch.setattr(trading, "_read_decision_pack_mode", _boom)
    assert trading._decision_pack_mode(settings) == "supervised-live"

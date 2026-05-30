"""grade_historical_props must not crash on empty input."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import scripts.grade_historical_props as mod  # noqa: E402


def test_main_handles_empty_cache_without_crashing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cache_path = tmp_path / "historical_props.json"
    cache_path.write_text(
        json.dumps({"games": [], "props_by_game": {}, "box_scores_by_date": {}}),
        encoding="utf-8",
    )
    monkeypatch.setattr(mod, "CACHE_PATH", cache_path)
    monkeypatch.setattr(mod, "_REPO_ROOT", tmp_path)

    # Must return cleanly (no IndexError from rows[0]) when there are no graded rows.
    assert mod.main() is None

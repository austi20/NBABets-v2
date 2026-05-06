"""AG-TECH-003: provider rotation JSON persistence via settings path."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from app.providers.rotation import ProviderRotationState

_TEMP_ROOT = Path(__file__).resolve().parents[2] / "temp"


def test_provider_rotation_state_persists_cursor() -> None:
    _TEMP_ROOT.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=_TEMP_ROOT) as d:
        path = Path(d) / "rot.json"
        r1 = ProviderRotationState(path)
        assert r1.order("odds", ["a", "b", "c"]) == ["a", "b", "c"]
        assert r1.order("odds", ["a", "b", "c"]) == ["b", "c", "a"]
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert payload["odds"] == 2

        r2 = ProviderRotationState(path)
        assert r2.order("odds", ["a", "b", "c"]) == ["c", "a", "b"]

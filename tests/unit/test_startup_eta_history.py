"""AG-TECH-002: persisted rolling averages for startup step ETAs."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from app.services.startup_eta_history import load_step_estimates, record_step_duration

_TEMP_ROOT = Path(__file__).resolve().parents[2] / "temp"


def test_load_step_estimates_uses_rolling_mean() -> None:
    _TEMP_ROOT.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=_TEMP_ROOT) as d:
        path = Path(d) / "eta.json"
        path.write_text(json.dumps({"discover_db": [2.0, 4.0]}), encoding="utf-8")
        out = load_step_estimates(path, {"discover_db": 99.0, "other": 1.0})
        assert out["discover_db"] == 3.0
        assert out["other"] == 1.0


def test_record_step_duration_truncates_history() -> None:
    _TEMP_ROOT.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=_TEMP_ROOT) as d:
        path = Path(d) / "eta.json"
        for i in range(20):
            record_step_duration(path, "train_model", float(10 + i))
        store = json.loads(path.read_text(encoding="utf-8"))
        assert len(store["train_model"]) == 15
        assert store["train_model"][-1] == 29.0


def test_record_step_duration_ignores_extremes() -> None:
    _TEMP_ROOT.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=_TEMP_ROOT) as d:
        path = Path(d) / "eta.json"
        record_step_duration(path, "x", 0.001)
        record_step_duration(path, "x", 999999.0)
        assert not path.exists() or json.loads(path.read_text(encoding="utf-8")).get("x") in (None, [])

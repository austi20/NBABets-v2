"""diagnostic_run must fail LOUD: real errors propagate, not swallowed."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import scripts.diagnostic_run as mod  # noqa: E402


class _FakePolicy:
    policy_version = "test"
    allow_live_submit = False
    allowed_market_keys: set[str] = set()
    blocked_market_keys: set[str] = set()
    min_model_prob = 0.0
    min_confidence = 0.0
    min_edge_bps = 0.0


def test_stage4_propagates_malformed_targets_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    targets_path = tmp_path / "kalshi_resolution_targets.json"
    targets_path.write_text("{ this is : not valid json ", encoding="utf-8")

    import app.trading.decision_brain as brain

    monkeypatch.setattr(brain, "load_policy", lambda settings: _FakePolicy())
    monkeypatch.setattr(brain, "_stale_context_blockers", lambda settings, policy: [])

    settings = SimpleNamespace(kalshi_resolution_targets_path=str(targets_path))

    # A corrupt targets file is a real failure and must NOT be silently swallowed.
    with pytest.raises(json.JSONDecodeError):
        mod.stage4_brain_candidates(settings)


def test_stage4_reads_valid_targets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    targets_path = tmp_path / "kalshi_resolution_targets.json"
    targets_path.write_text(
        json.dumps({"generated_at": "now", "targets": [{"market_key": "nba.points", "recommendation": "buy_yes"}]}),
        encoding="utf-8",
    )

    import app.trading.decision_brain as brain

    monkeypatch.setattr(brain, "load_policy", lambda settings: _FakePolicy())
    monkeypatch.setattr(brain, "_stale_context_blockers", lambda settings, policy: [])

    settings = SimpleNamespace(kalshi_resolution_targets_path=str(targets_path))
    result = mod.stage4_brain_candidates(settings)
    assert len(result) == 1

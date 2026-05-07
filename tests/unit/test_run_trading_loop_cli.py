from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.run_trading_loop import _load_live_decisions


def test_load_live_decisions_rejects_symbol_map_payload(tmp_path: Path) -> None:
    payload = [
        {
            "market_key": "points",
            "side": "over",
            "line_value": 25.5,
            "player_id": 237,
            "game_date": "2026-05-06",
            "kalshi_ticker": "KXNBASGPL-26MAY06LAL-LEBRON-OPTS25",
        }
    ]
    path = tmp_path / "symbols-not-decisions.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="recommendation"):
        _load_live_decisions(path)


def test_load_live_decisions_rejects_missing_file(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="could not be read"):
        _load_live_decisions(tmp_path / "missing.json")


def test_load_live_decisions_caps_to_first_decision(tmp_path: Path) -> None:
    base = {
        "model_prob": 0.58,
        "market_prob": 0.51,
        "no_vig_market_prob": 0.50,
        "ev": 0.05,
        "recommendation": "OVER",
        "confidence": "high",
        "driver": "test",
        "market_key": "points",
        "line_value": 25.5,
        "over_odds": -110,
        "under_odds": -110,
        "player_id": 237,
        "game_id": 1001,
        "game_date": "2026-05-07",
    }
    path = tmp_path / "decisions.json"
    path.write_text(json.dumps([base, {**base, "player_id": 999}]), encoding="utf-8")

    decisions, decision_count = _load_live_decisions(path)

    assert decision_count == 2
    assert len(decisions) == 1
    assert decisions[0].player_id == 237

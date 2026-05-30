"""Tests for resolve_kalshi_targets match_quality taxonomy + base_target_id."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.resolve_kalshi_targets import _market_snapshot  # noqa: E402


def _market(**over: object) -> dict:
    base = {
        "ticker": "KXNBAPTS-26MAY30LAL-LBJ",
        "event_ticker": "KXNBAPTS-26MAY30LAL",
        "title": "LeBron James points",
        "status": "open",
        "floor_strike": 24.5,
    }
    base.update(over)
    return base


def _target(**over: object) -> dict:
    base = {
        "target_id": "tgt-1",
        "market_key": "nba.points",
        "game_date": "2026-05-30",
        "player_id": 100,
        "line_value": 24.5,
        "recommendation": "buy_yes",
        "match_rules": {
            "player_name_contains_any": ["lebron"],
            "stat_contains_any": ["points"],
        },
    }
    base.update(over)
    return base


def test_enumerated_threshold_quality_and_base_target_id() -> None:
    target = _target(match_rules={"enumerate_thresholds": True, "player_name_contains_any": ["lebron"]})
    market = _market()
    snap = _market_snapshot(target, market, "player,threshold")
    assert snap["match_quality"] == "enumerated_threshold"
    assert snap["base_target_id"] == "tgt-1"
    assert snap["target_id"] == "tgt-1::KXNBAPTS-26MAY30LAL-LBJ"


def test_exact_line_quality_and_base_target_id_present() -> None:
    target = _target()
    market = _market()
    snap = _market_snapshot(target, market, "player,stat,line")
    assert snap["match_quality"] == "exact"
    # base_target_id must always be present and equal target_id for non-enumerated rows
    assert snap["base_target_id"] == "tgt-1"
    assert snap["target_id"] == "tgt-1"


def test_text_only_match_quality_is_not_raw_csv() -> None:
    target = _target()
    market = _market()
    snap = _market_snapshot(target, market, "player,stat")
    assert snap["match_quality"] == "text_only"
    assert snap["base_target_id"] == "tgt-1"


def test_base_target_id_survives_none_stripping() -> None:
    # A non-enumerated row with several None-valued optional fields must still
    # carry base_target_id (regression: None-filtering used to drop it).
    target = _target(projected_mean=None, projected_variance=None, stat_projection_id=None)
    market = _market()
    snap = _market_snapshot(target, market, "player,stat,line")
    assert "base_target_id" in snap
    assert snap["base_target_id"] == snap["target_id"]

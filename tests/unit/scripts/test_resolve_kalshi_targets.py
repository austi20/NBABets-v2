"""Tests for resolve_kalshi_targets match_quality taxonomy + base_target_id."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.resolve_kalshi_targets import _market_snapshot, resolve_targets  # noqa: E402


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


def _enum_target(**over: object) -> dict:
    base = _target(
        match_rules={
            "enumerate_thresholds": True,
            "player_name_contains_any": ["lebron"],
            "stat_contains_any": ["rebounds"],
        }
    )
    base.pop("line_value", None)
    base.update(over)
    return base


def test_x_plus_market_applies_minus_half_shift() -> None:
    target = _enum_target()
    market = _market(
        ticker="KXNBAREB-26MAY30LAL-LBJ-25",
        title="LeBron James 25+ rebounds",
        floor_strike=25.0,
    )
    out = resolve_targets([target], [market], min_score=70)
    assert not out["unresolved"]
    assert len(out["symbols"]) == 1
    assert out["symbols"][0]["line_value"] == 24.5


def test_range_style_market_is_ambiguous_threshold() -> None:
    target = _enum_target()
    market = _market(
        ticker="KXNBAREB-26MAY30LAL-LBJ-B2024",
        title="LeBron James rebounds",
        subtitle="Between 20 and 24 rebounds",
        rules_primary="Resolves YES if rebounds are between 20 and 24 inclusive.",
        floor_strike=20.0,
    )
    out = resolve_targets([target], [market], min_score=70)
    assert not out["symbols"]
    reasons = {u["reason"] for u in out["unresolved"]}
    assert "ambiguous_threshold_semantics" in reasons

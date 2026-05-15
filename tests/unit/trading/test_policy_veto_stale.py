# tests/unit/trading/test_policy_veto_stale.py
from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

from app.trading.decision_brain import (
    DecisionBrainPolicy,
    _stale_context_blockers,
    load_policy,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_policy(**overrides) -> DecisionBrainPolicy:
    defaults = dict(
        policy_version="v1",
        policy_hash="abc123",
        allow_live_submit=False,
        allowed_market_keys=set(),
        blocked_market_keys=set(),
        min_edge_bps=0,
        min_model_prob=0.5,
        min_confidence=0.5,
        max_price_dollars_default=100.0,
        max_spread_dollars=5.0,
        max_contracts=10.0,
        post_only=True,
        time_in_force="good_till_canceled",
        same_day_only=True,
        ranking_weight_edge_bps=0.45,
        ranking_weight_ev=0.20,
        ranking_weight_liquidity=0.15,
        ranking_weight_calibration=0.10,
        ranking_weight_freshness=0.10,
        require_injury_refresh_minutes=30,
        require_projection_refresh_minutes=60,
    )
    defaults.update(overrides)
    return DecisionBrainPolicy(**defaults)


def _make_db(tmp_path: Path, *, injury_ts, pred_ts) -> str:
    db_path = tmp_path / "test.db"
    con = sqlite3.connect(str(db_path))
    try:
        con.execute("CREATE TABLE injury_reports (report_timestamp TEXT)")
        con.execute("CREATE TABLE predictions (predicted_at TEXT)")
        if injury_ts is not None:
            con.execute("INSERT INTO injury_reports VALUES (?)", (injury_ts,))
        if pred_ts is not None:
            con.execute("INSERT INTO predictions VALUES (?)", (pred_ts,))
        con.commit()
    finally:
        con.close()
    return str(db_path)


def _settings(db_path: str):
    s = MagicMock()
    s.database_url = f"sqlite:///{db_path}"
    return s


# ---------------------------------------------------------------------------
# DecisionBrainPolicy default fields
# ---------------------------------------------------------------------------

def test_policy_default_injury_minutes() -> None:
    assert _make_policy().require_injury_refresh_minutes == 30


def test_policy_default_projection_minutes() -> None:
    assert _make_policy().require_projection_refresh_minutes == 60


# ---------------------------------------------------------------------------
# _stale_context_blockers
# ---------------------------------------------------------------------------

def test_fresh_data_no_blockers(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    db = _make_db(tmp_path, injury_ts=(now - timedelta(minutes=10)).isoformat(), pred_ts=(now - timedelta(minutes=20)).isoformat())
    assert _stale_context_blockers(_settings(db), _make_policy()) == []


def test_stale_injury_returns_blocker(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    db = _make_db(tmp_path, injury_ts=(now - timedelta(minutes=90)).isoformat(), pred_ts=(now - timedelta(minutes=5)).isoformat())
    blockers = _stale_context_blockers(_settings(db), _make_policy())
    assert "stale_injury_context" in blockers
    assert "stale_projection_context" not in blockers


def test_stale_projection_returns_blocker(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    db = _make_db(tmp_path, injury_ts=(now - timedelta(minutes=5)).isoformat(), pred_ts=(now - timedelta(minutes=120)).isoformat())
    blockers = _stale_context_blockers(_settings(db), _make_policy())
    assert "stale_projection_context" in blockers
    assert "stale_injury_context" not in blockers


def test_both_stale_returns_both_blockers(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    db = _make_db(tmp_path, injury_ts=(now - timedelta(minutes=60)).isoformat(), pred_ts=(now - timedelta(minutes=120)).isoformat())
    blockers = _stale_context_blockers(_settings(db), _make_policy())
    assert "stale_injury_context" in blockers
    assert "stale_projection_context" in blockers


def test_null_rows_return_both_blockers(tmp_path: Path) -> None:
    db = _make_db(tmp_path, injury_ts=None, pred_ts=None)
    blockers = _stale_context_blockers(_settings(db), _make_policy())
    assert "stale_injury_context" in blockers
    assert "stale_projection_context" in blockers


def test_bad_db_path_silent_failure() -> None:
    s = MagicMock()
    s.database_url = "sqlite:////nonexistent/path/db.sqlite"
    blockers = _stale_context_blockers(s, _make_policy())
    assert isinstance(blockers, list)


# ---------------------------------------------------------------------------
# load_policy: new fields extracted from frontmatter
# ---------------------------------------------------------------------------

_BASE_FIELDS = {
    "brain_type": "policy_core",
    "policy_version": "v-test",
    "allow_live_submit": False,
    "allowed_market_keys": [],
    "blocked_market_keys": [],
    "min_edge_bps": 50,
    "min_model_prob": 0.55,
    "min_confidence": 0.6,
    "max_price_dollars_default": 200.0,
    "max_spread_dollars": 3.0,
    "max_contracts": 5.0,
}


def test_load_policy_extracts_refresh_minutes() -> None:
    fields = {**_BASE_FIELDS, "require_injury_refresh_minutes": 45, "require_projection_refresh_minutes": 90}
    with patch("app.trading.decision_brain.decision_brain_root"),          patch("pathlib.Path.is_file", return_value=True),          patch("app.trading.decision_brain.parse_frontmatter_file", return_value=(fields, "raw")):
        policy = load_policy(MagicMock())
    assert policy.require_injury_refresh_minutes == 45
    assert policy.require_projection_refresh_minutes == 90


def test_load_policy_refresh_minutes_default_when_absent() -> None:
    fields = {**_BASE_FIELDS}  # no refresh keys
    with patch("app.trading.decision_brain.decision_brain_root"),          patch("pathlib.Path.is_file", return_value=True),          patch("app.trading.decision_brain.parse_frontmatter_file", return_value=(fields, "raw")):
        policy = load_policy(MagicMock())
    assert policy.require_injury_refresh_minutes == 30
    assert policy.require_projection_refresh_minutes == 60

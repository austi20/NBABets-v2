"""Tests for game_already_played blocker in _candidate_policy_blockers."""
from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import patch

import pytest

from app.trading.decision_brain import DecisionBrainCandidate, DecisionBrainPolicy, _candidate_policy_blockers


def _make_policy(**overrides) -> DecisionBrainPolicy:
    defaults = dict(
        policy_version="v1",
        policy_hash="abc",
        allow_live_submit=False,
        allowed_market_keys={"points"},
        blocked_market_keys=set(),
        min_edge_bps=0,
        min_model_prob=0.0,
        min_confidence=0.0,
        max_price_dollars_default=100.0,
        max_spread_dollars=5.0,
        max_contracts=1.0,
        post_only=True,
        time_in_force="fill_or_kill",
        same_day_only=True,
        ranking_weight_edge_bps=1.0,
        ranking_weight_ev=1.0,
        ranking_weight_liquidity=1.0,
        ranking_weight_calibration=1.0,
        ranking_weight_freshness=1.0,
        require_injury_refresh_minutes=30,
        require_projection_refresh_minutes=60,
    )
    defaults.update(overrides)
    return DecisionBrainPolicy(**defaults)


def _make_candidate(game_date: date, **overrides) -> DecisionBrainCandidate:
    today = date.today()
    defaults = dict(
        stable_id="test-id",
        source="test",
        board_date=game_date,
        candidate_status="selected_observe_only",
        market_key="points",
        player_id="1",
        player_name="Test Player",
        game_id="g1",
        game_date=game_date,
        line_value=20.5,
        recommendation="buy_yes",
        outcome_side="yes",
        book_side="yes",
        model_prob=0.65,
        market_prob=0.55,
        no_vig_market_prob=0.52,
        edge_bps=50,
        ev=0.05,
        confidence=0.70,
        contracts=1.0,
        max_price_dollars=100.0,
        post_only=True,
        time_in_force="fill_or_kill",
        title_contains_all=[],
        player_name_contains_any=[],
        stat_contains_any=[],
        acceptable_line_values=[],
        event_or_page_hint=None,
        exclude_multivariate=True,
        driver="test",
    )
    defaults.update(overrides)
    return DecisionBrainCandidate(**defaults)


def test_past_game_blocked():
    """game_date before board_date means the game was already played."""
    board = date(2026, 5, 14)
    game = date(2026, 5, 13)
    candidate = _make_candidate(game_date=game)
    policy = _make_policy()
    blockers = _candidate_policy_blockers(candidate, policy=policy, board_date=board)
    assert "game_already_played" in blockers


def test_same_day_game_not_blocked_by_staleness():
    """game_date == board_date is current-day - no staleness block."""
    today = date(2026, 5, 14)
    candidate = _make_candidate(game_date=today)
    policy = _make_policy(same_day_only=True)
    blockers = _candidate_policy_blockers(candidate, policy=policy, board_date=today)
    assert "game_already_played" not in blockers


def test_future_game_not_blocked_by_staleness():
    """game_date after board_date is fine (advance scheduling)."""
    board = date(2026, 5, 14)
    tomorrow = date(2026, 5, 15)
    candidate = _make_candidate(game_date=tomorrow)
    policy = _make_policy(same_day_only=False)
    blockers = _candidate_policy_blockers(candidate, policy=policy, board_date=board)
    assert "game_already_played" not in blockers


def test_game_already_played_takes_priority_over_same_day_only():
    """Past-game blocker fires even when same_day_only is False - elif prevents both."""
    board = date(2026, 5, 14)
    game = date(2026, 5, 13)
    candidate = _make_candidate(game_date=game)
    policy = _make_policy(same_day_only=False)
    blockers = _candidate_policy_blockers(candidate, policy=policy, board_date=board)
    assert "game_already_played" in blockers
    assert "not_same_day" not in blockers

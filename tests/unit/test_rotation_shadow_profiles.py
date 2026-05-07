"""
Tests for _build_shadow_absence_profiles with historical_frame support.

RC2: Absent players who have no box scores in the eval frame were silently
dropped even when their stats existed in the training frame. The fix adds
an optional historical_frame parameter so eval-time calls can pass training
data as the baseline source.
"""
from __future__ import annotations

import pandas as pd
import pytest

from app.training.pipeline import _build_shadow_absence_profiles


def _make_player_rows(
    player_id: int,
    team_id: int,
    game_ids: list[int],
    dates: list[str],
    minutes: float = 25.0,
) -> pd.DataFrame:
    """Build minimal frame rows for one player across multiple games."""
    rows = []
    for gid, date in zip(game_ids, dates, strict=True):
        rows.append(
            {
                "game_id": gid,
                "player_id": player_id,
                "player_team_id": team_id,
                "game_date": date,
                "start_time": date + "T19:00:00",
                "player_name": f"Player{player_id}",
                "minutes": minutes,
                "usage_rate_blended": 0.22,
                "usage_rate_avg_10": 0.22,
                "usage_rate": 0.22,
                "assists_per_minute_avg_10": 0.08,
                "assists_per_minute": 0.08,
                "rebounds_per_minute_avg_10": 0.12,
                "rebounds_per_minute": 0.12,
                "estimated_three_point_attempts_per_minute_avg_10": 0.05,
                "estimated_three_point_attempts_per_minute": 0.05,
                "starter_flag": 1.0,
                "usage_proxy": 12.0,
                "position": "G",
                "season": 2024,
            }
        )
    return pd.DataFrame(rows)


def _make_absence(
    player_id: int,
    team_id: int,
    game_id: int,
    game_date: str,
    play_probability: float = 0.0,
) -> pd.DataFrame:
    """Build a single absence record."""
    return pd.DataFrame(
        [
            {
                "game_id": game_id,
                "team_id": team_id,
                "player_id": player_id,
                "player_name": f"Player{player_id}",
                "position": "G",
                "report_timestamp": game_date + "T17:00:00",
                "status": "Out",
                "source": "injury_report",
                "rotation_shock_confidence": 0.9,
                "play_probability": play_probability,
            }
        ]
    )


# ── constants ────────────────────────────────────────────────────────────────

TEAM_ID = 10
EVAL_GAME_ID = 200  # the game being evaluated
EVAL_DATE = "2024-11-15"
ABSENT_PLAYER = 999  # this player appears only in training, not in eval

# Training games: 10 games BEFORE the eval game
TRAIN_GAME_IDS = list(range(100, 110))
TRAIN_DATES = [f"2024-11-0{i}" for i in range(1, 10)] + ["2024-11-10"]

# A second player who DOES appear in the eval frame
ACTIVE_PLAYER = 1
EVAL_PLAYER_ROW = _make_player_rows(
    ACTIVE_PLAYER, TEAM_ID, [EVAL_GAME_ID], [EVAL_DATE]
)

HISTORICAL_ROWS = _make_player_rows(
    ABSENT_PLAYER, TEAM_ID, TRAIN_GAME_IDS, TRAIN_DATES
)

ABSENCE = _make_absence(ABSENT_PLAYER, TEAM_ID, EVAL_GAME_ID, EVAL_DATE)


def test_absent_player_dropped_without_historical_frame() -> None:
    """Without historical_frame, absent player not in eval frame is silently dropped."""
    profiles = _build_shadow_absence_profiles(EVAL_PLAYER_ROW, ABSENCE)
    assert profiles.empty, (
        f"Expected empty profiles when absent player has no eval-frame history, "
        f"got {len(profiles)} rows"
    )


def test_absent_player_included_with_historical_frame() -> None:
    """With historical_frame, absent player gets a profile from prior-game stats."""
    profiles = _build_shadow_absence_profiles(
        EVAL_PLAYER_ROW, ABSENCE, historical_frame=HISTORICAL_ROWS
    )
    assert not profiles.empty, "Expected one profile for the absent player"
    assert len(profiles) == 1
    row = profiles.iloc[0]
    assert int(row["player_id"]) == ABSENT_PLAYER
    assert int(row["game_id"]) == EVAL_GAME_ID
    assert row["baseline_minutes"] == pytest.approx(25.0, abs=0.5)

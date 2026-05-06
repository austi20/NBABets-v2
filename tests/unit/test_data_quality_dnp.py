"""Tests for DataQualityAgent DNP contamination, extreme prediction, and divergence checks."""

from __future__ import annotations

from datetime import UTC, date, datetime

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.db.base import Base
from app.models import all as _models  # noqa: F401 — register ORM models
from app.services.agents.contracts import AgentTask
from app.services.agents.data_quality import DataQualityAgent


def _make_session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def _seed_team(session) -> None:
    session.execute(
        text(
            "INSERT INTO teams (team_id, abbreviation, name, is_active) "
            "VALUES (1, 'TMA', 'Team A', 1)"
        )
    )


def _seed_player(session) -> None:
    session.execute(
        text(
            "INSERT INTO players (player_id, full_name, normalized_name, team_id, meta) "
            "VALUES (1, 'Test Player', 'test player', 1, '{}')"
        )
    )


def _seed_game(session, *, game_date: date | None = None, status: str = "completed") -> None:
    effective_date = game_date or date.today()
    now = datetime.now(UTC)
    session.execute(
        text(
            "INSERT INTO games (game_id, game_date, start_time, home_team_id, away_team_id, status, meta) "
            "VALUES (1, :gd, :st, 1, 1, :status, '{}')"
        ),
        {"gd": effective_date.isoformat(), "st": now.isoformat(), "status": status},
    )


def _seed_zero_minute_game(session, *, game_date: date | None = None) -> None:
    _seed_team(session)
    _seed_game(session, game_date=game_date, status="completed")
    _seed_player(session)
    session.execute(
        text(
            "INSERT INTO player_game_logs "
            "(player_id, game_id, team_id, opponent_team_id, minutes, points, rebounds, assists, "
            "threes, turnovers, steals, blocks, field_goal_attempts, field_goals_made, "
            "free_throw_attempts, free_throws_made, offensive_rebounds, defensive_rebounds, "
            "plus_minus, fouls, starter_flag, overtime_flag, meta) "
            "VALUES (1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, '{}')"
        )
    )
    session.commit()


def _seed_sportsbook(session) -> None:
    session.execute(
        text(
            "INSERT INTO sportsbooks (sportsbook_id, key, display_name, is_active) "
            "VALUES (1, 'test_book', 'Test Book', 1)"
        )
    )


def _seed_extreme_prediction(session, *, over_prob: float = 0.999) -> None:
    now = datetime.now(UTC)
    _seed_team(session)
    _seed_game(session, status="scheduled")
    _seed_player(session)
    _seed_sportsbook(session)
    session.execute(
        text(
            "INSERT INTO prop_markets (market_id, key, stat_type, display_name, distribution_family, is_active) "
            "VALUES (1, 'points', 'points', 'Points', 'negative_binomial', 1)"
        )
    )
    session.execute(
        text(
            "INSERT INTO line_snapshots (snapshot_id, timestamp, game_id, player_id, market_id, sportsbook_id, "
            "line_value, over_odds, under_odds, meta) "
            "VALUES (1, :now, 1, 1, 1, 1, 28.5, -110, -110, '{}')"
        ),
        {"now": now.isoformat()},
    )
    session.execute(
        text(
            "INSERT INTO model_runs (model_run_id, model_version, feature_version, started_at, completed_at, metrics) "
            "VALUES (1, 'test', 'test', :now, :now, '{}')"
        ),
        {"now": now.isoformat()},
    )
    session.execute(
        text(
            "INSERT INTO predictions (prediction_id, model_run_id, game_id, player_id, market_id, "
            "line_snapshot_id, predicted_at, projected_mean, projected_variance, projected_median, "
            "over_probability, under_probability, confidence_interval_low, confidence_interval_high, "
            "calibration_adjusted_probability, feature_attribution_summary) "
            "VALUES (1, 1, 1, 1, 1, 1, :now, 11.7, 50.0, 12.0, :op, :up, 5.0, 20.0, :op, '{}')"
        ),
        {"now": now.isoformat(), "op": over_prob, "up": 1.0 - over_prob},
    )
    session.commit()


def _make_task() -> AgentTask:
    return AgentTask(task_id="test-dq", role="data_quality", task_type="check", input_payload={}, dry_run=True)


def test_detects_zero_minute_games() -> None:
    session = _make_session()
    _seed_zero_minute_game(session)
    result = DataQualityAgent(session).handle(_make_task())
    action_types = [a.action_type for a in result.actions]
    assert "dnp_contamination_warning" in action_types
    assert result.details["zero_minute_games"] > 0


def test_detects_extreme_predictions() -> None:
    session = _make_session()
    _seed_extreme_prediction(session, over_prob=0.999)
    result = DataQualityAgent(session).handle(_make_task())
    action_types = [a.action_type for a in result.actions]
    assert "extreme_probability_detected" in action_types
    assert result.details["extreme_predictions"] > 0


def test_detects_projection_line_divergence() -> None:
    session = _make_session()
    # projected_mean=11.7, line_value=28.5 → divergence = 59% > 40%
    _seed_extreme_prediction(session, over_prob=0.50)
    result = DataQualityAgent(session).handle(_make_task())
    action_types = [a.action_type for a in result.actions]
    assert "projection_line_divergence" in action_types
    assert result.details["projection_line_divergences"] > 0


def test_clean_when_no_issues() -> None:
    session = _make_session()
    result = DataQualityAgent(session).handle(_make_task())
    assert result.status == "recommendation"
    assert result.details["zero_minute_games"] == 0
    assert result.details["extreme_predictions"] == 0
    assert result.details["projection_line_divergences"] == 0

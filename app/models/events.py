from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import JSON, Boolean, DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class PlayerGameLog(Base):
    __tablename__ = "player_game_logs"
    __table_args__ = (UniqueConstraint("player_id", "game_id", name="uq_player_game_logs_player_game"),)

    player_game_log_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    player_id: Mapped[int] = mapped_column(ForeignKey("players.player_id"), index=True)
    game_id: Mapped[int] = mapped_column(ForeignKey("games.game_id"), index=True)
    team_id: Mapped[int | None] = mapped_column(ForeignKey("teams.team_id"), nullable=True)
    opponent_team_id: Mapped[int | None] = mapped_column(ForeignKey("teams.team_id"), nullable=True)
    minutes: Mapped[float] = mapped_column(Float)
    points: Mapped[int] = mapped_column(Integer)
    rebounds: Mapped[int] = mapped_column(Integer)
    assists: Mapped[int] = mapped_column(Integer)
    threes: Mapped[int] = mapped_column(Integer)
    steals: Mapped[int] = mapped_column(Integer, default=0)
    blocks: Mapped[int] = mapped_column(Integer, default=0)
    turnovers: Mapped[int] = mapped_column(Integer, default=0)
    fouls: Mapped[int] = mapped_column(Integer, default=0)
    field_goal_attempts: Mapped[int] = mapped_column(Integer, default=0)
    field_goals_made: Mapped[int] = mapped_column(Integer, default=0)
    free_throw_attempts: Mapped[int] = mapped_column(Integer, default=0)
    free_throws_made: Mapped[int] = mapped_column(Integer, default=0)
    offensive_rebounds: Mapped[int] = mapped_column(Integer, default=0)
    defensive_rebounds: Mapped[int] = mapped_column(Integer, default=0)
    plus_minus: Mapped[float | None] = mapped_column(Float)
    starter_flag: Mapped[bool] = mapped_column(Boolean, default=False)
    overtime_flag: Mapped[bool] = mapped_column(Boolean, default=False)
    meta: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class TeamGameLog(Base):
    __tablename__ = "team_game_logs"
    __table_args__ = (UniqueConstraint("team_id", "game_id", name="uq_team_game_logs_team_game"),)

    team_game_log_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    team_id: Mapped[int] = mapped_column(ForeignKey("teams.team_id"), index=True)
    game_id: Mapped[int] = mapped_column(ForeignKey("games.game_id"), index=True)
    pace: Mapped[float | None] = mapped_column(Float)
    offensive_rating: Mapped[float | None] = mapped_column(Float)
    defensive_rating: Mapped[float | None] = mapped_column(Float)
    rebounds_allowed: Mapped[float | None] = mapped_column(Float)
    assists_allowed: Mapped[float | None] = mapped_column(Float)
    threes_allowed: Mapped[float | None] = mapped_column(Float)
    turnovers_forced: Mapped[float | None] = mapped_column(Float)
    meta: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class PlayerBoxScore(Base):
    __tablename__ = "player_box_scores"
    __table_args__ = (UniqueConstraint("player_id", "game_id", name="uq_player_box_scores_player_game"),)

    player_box_score_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    player_id: Mapped[int] = mapped_column(ForeignKey("players.player_id"), index=True)
    game_id: Mapped[int] = mapped_column(ForeignKey("games.game_id"), index=True)
    stat_line: Mapped[dict[str, Any]] = mapped_column(JSON)


class TeamBoxScore(Base):
    __tablename__ = "team_box_scores"
    __table_args__ = (UniqueConstraint("team_id", "game_id", name="uq_team_box_scores_team_game"),)

    team_box_score_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    team_id: Mapped[int] = mapped_column(ForeignKey("teams.team_id"), index=True)
    game_id: Mapped[int] = mapped_column(ForeignKey("games.game_id"), index=True)
    stat_line: Mapped[dict[str, Any]] = mapped_column(JSON)


class InjuryReport(Base):
    __tablename__ = "injury_reports"

    injury_report_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    player_id: Mapped[int] = mapped_column(ForeignKey("players.player_id"), index=True)
    team_id: Mapped[int] = mapped_column(ForeignKey("teams.team_id"), index=True)
    report_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    game_id: Mapped[int | None] = mapped_column(ForeignKey("games.game_id"), nullable=True, index=True)
    status: Mapped[str] = mapped_column(String(32), index=True)
    designation: Mapped[str | None] = mapped_column(String(64))
    body_part: Mapped[str | None] = mapped_column(String(64))
    notes: Mapped[str | None] = mapped_column(Text)
    expected_availability_flag: Mapped[bool | None] = mapped_column(Boolean)
    source_payload_id: Mapped[int | None] = mapped_column(ForeignKey("raw_payloads.payload_id"), nullable=True)


class ProjectedLineup(Base):
    __tablename__ = "projected_lineups"

    projected_lineup_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    game_id: Mapped[int] = mapped_column(ForeignKey("games.game_id"), index=True)
    team_id: Mapped[int] = mapped_column(ForeignKey("teams.team_id"), index=True)
    player_id: Mapped[int] = mapped_column(ForeignKey("players.player_id"), index=True)
    lineup_status: Mapped[str] = mapped_column(String(32), index=True)
    expected_minutes: Mapped[float | None] = mapped_column(Float)
    projected_starter: Mapped[bool] = mapped_column(Boolean, default=False)
    report_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class LineSnapshot(Base):
    __tablename__ = "line_snapshots"
    __table_args__ = (
        UniqueConstraint(
            "timestamp",
            "game_id",
            "sportsbook_id",
            "player_id",
            "market_id",
            "line_value",
            name="uq_line_snapshots_dedupe",
        ),
    )

    snapshot_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    game_id: Mapped[int] = mapped_column(ForeignKey("games.game_id"), index=True)
    sportsbook_id: Mapped[int] = mapped_column(ForeignKey("sportsbooks.sportsbook_id"), index=True)
    player_id: Mapped[int] = mapped_column(ForeignKey("players.player_id"), index=True)
    market_id: Mapped[int] = mapped_column(ForeignKey("prop_markets.market_id"), index=True)
    line_value: Mapped[float] = mapped_column(Float)
    over_odds: Mapped[int | None] = mapped_column(Integer)
    under_odds: Mapped[int | None] = mapped_column(Integer)
    event_status: Mapped[str | None] = mapped_column(String(32))
    source_payload_id: Mapped[int | None] = mapped_column(ForeignKey("raw_payloads.payload_id"), nullable=True)
    meta: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class ClosingLine(Base):
    __tablename__ = "closing_lines"
    __table_args__ = (
        UniqueConstraint("game_id", "sportsbook_id", "player_id", "market_id", name="uq_closing_lines_market"),
    )

    closing_line_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    game_id: Mapped[int] = mapped_column(ForeignKey("games.game_id"), index=True)
    sportsbook_id: Mapped[int] = mapped_column(ForeignKey("sportsbooks.sportsbook_id"), index=True)
    player_id: Mapped[int] = mapped_column(ForeignKey("players.player_id"), index=True)
    market_id: Mapped[int] = mapped_column(ForeignKey("prop_markets.market_id"), index=True)
    line_value: Mapped[float] = mapped_column(Float)
    over_odds: Mapped[int | None] = mapped_column(Integer)
    under_odds: Mapped[int | None] = mapped_column(Integer)
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class ModelRun(Base):
    __tablename__ = "model_runs"

    model_run_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    model_version: Mapped[str] = mapped_column(String(64), index=True)
    feature_version: Mapped[str] = mapped_column(String(64), index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    training_window_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    training_window_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    notes: Mapped[str | None] = mapped_column(Text)
    metrics: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class Prediction(Base):
    __tablename__ = "predictions"

    prediction_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    model_run_id: Mapped[int] = mapped_column(ForeignKey("model_runs.model_run_id"), index=True)
    game_id: Mapped[int] = mapped_column(ForeignKey("games.game_id"), index=True)
    player_id: Mapped[int] = mapped_column(ForeignKey("players.player_id"), index=True)
    market_id: Mapped[int] = mapped_column(ForeignKey("prop_markets.market_id"), index=True)
    line_snapshot_id: Mapped[int | None] = mapped_column(ForeignKey("line_snapshots.snapshot_id"), nullable=True)
    predicted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    projected_mean: Mapped[float] = mapped_column(Float)
    projected_variance: Mapped[float] = mapped_column(Float)
    projected_median: Mapped[float] = mapped_column(Float)
    over_probability: Mapped[float] = mapped_column(Float)
    under_probability: Mapped[float] = mapped_column(Float)
    confidence_interval_low: Mapped[float] = mapped_column(Float)
    confidence_interval_high: Mapped[float] = mapped_column(Float)
    calibration_adjusted_probability: Mapped[float] = mapped_column(Float)
    feature_attribution_summary: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class BacktestResult(Base):
    __tablename__ = "backtest_results"

    backtest_result_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    model_run_id: Mapped[int | None] = mapped_column(ForeignKey("model_runs.model_run_id"), nullable=True)
    market_id: Mapped[int | None] = mapped_column(ForeignKey("prop_markets.market_id"), nullable=True)
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    metrics: Mapped[dict[str, Any]] = mapped_column(JSON)
    artifact_path: Mapped[str | None] = mapped_column(String(256))


class AIProviderEvent(Base):
    __tablename__ = "ai_provider_events"

    event_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    provider_name: Mapped[str] = mapped_column(String(64), index=True)
    model_name: Mapped[str | None] = mapped_column(String(128))
    event_type: Mapped[str] = mapped_column(String(32), index=True)
    status: Mapped[str] = mapped_column(String(16), index=True)
    latency_ms: Mapped[int | None] = mapped_column(Integer)
    detail: Mapped[str | None] = mapped_column(Text)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class AgentRunEvent(Base):
    __tablename__ = "agent_run_events"

    run_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_id: Mapped[str] = mapped_column(String(64), index=True)
    agent_role: Mapped[str] = mapped_column(String(64), index=True)
    event_type: Mapped[str] = mapped_column(String(64), index=True)
    status: Mapped[str] = mapped_column(String(32), index=True)
    latency_ms: Mapped[int | None] = mapped_column(Integer)
    confidence: Mapped[float | None] = mapped_column(Float)
    action_summary: Mapped[str | None] = mapped_column(Text)
    error_category: Mapped[str | None] = mapped_column(String(64), index=True)
    detail: Mapped[str | None] = mapped_column(Text)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


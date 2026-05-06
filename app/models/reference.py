from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import JSON, Boolean, Date, DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class Season(Base):
    __tablename__ = "seasons"

    season_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    code: Mapped[str] = mapped_column(String(16), unique=True, index=True)
    start_date: Mapped[date] = mapped_column(Date)
    end_date: Mapped[date] = mapped_column(Date)


class Team(Base):
    __tablename__ = "teams"

    team_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    provider_team_id: Mapped[str | None] = mapped_column(String(64), index=True)
    abbreviation: Mapped[str] = mapped_column(String(8), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(128), index=True)
    city: Mapped[str | None] = mapped_column(String(128))
    conference: Mapped[str | None] = mapped_column(String(32))
    division: Mapped[str | None] = mapped_column(String(32))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    players: Mapped[list[Player]] = relationship(back_populates="team")


class Player(Base):
    __tablename__ = "players"
    __table_args__ = (UniqueConstraint("full_name", "team_id", name="uq_players_name_team"),)

    player_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    provider_player_id: Mapped[str | None] = mapped_column(String(64), index=True)
    full_name: Mapped[str] = mapped_column(String(128), index=True)
    normalized_name: Mapped[str] = mapped_column(String(128), index=True)
    team_id: Mapped[int | None] = mapped_column(ForeignKey("teams.team_id"), nullable=True)
    position: Mapped[str | None] = mapped_column(String(16))
    status: Mapped[str | None] = mapped_column(String(32))
    meta: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)

    team: Mapped[Team | None] = relationship(back_populates="players")


class Sportsbook(Base):
    __tablename__ = "sportsbooks"

    sportsbook_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    key: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    display_name: Mapped[str] = mapped_column(String(128))
    region: Mapped[str | None] = mapped_column(String(16))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)


class PropMarket(Base):
    __tablename__ = "prop_markets"

    market_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    key: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    stat_type: Mapped[str] = mapped_column(String(64), index=True)
    display_name: Mapped[str] = mapped_column(String(128))
    distribution_family: Mapped[str] = mapped_column(String(32), default="negative_binomial")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)


class Game(Base):
    __tablename__ = "games"

    game_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    provider_game_id: Mapped[str | None] = mapped_column(String(64), unique=True, index=True)
    season_id: Mapped[int | None] = mapped_column(ForeignKey("seasons.season_id"), nullable=True)
    game_date: Mapped[date] = mapped_column(Date, index=True)
    start_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    home_team_id: Mapped[int] = mapped_column(ForeignKey("teams.team_id"), index=True)
    away_team_id: Mapped[int] = mapped_column(ForeignKey("teams.team_id"), index=True)
    spread: Mapped[float | None] = mapped_column(Float)
    total: Mapped[float | None] = mapped_column(Float)
    status: Mapped[str] = mapped_column(String(32), default="scheduled", index=True)
    final_home_score: Mapped[int | None] = mapped_column(Integer)
    final_away_score: Mapped[int | None] = mapped_column(Integer)
    meta: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class RawPayload(Base):
    __tablename__ = "raw_payloads"

    payload_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    provider_type: Mapped[str] = mapped_column(String(32), index=True)
    provider_name: Mapped[str] = mapped_column(String(64), index=True)
    endpoint: Mapped[str] = mapped_column(String(256))
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    content_hash: Mapped[str] = mapped_column(String(64), index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)


class GamePlayerAvailability(Base):
    """Official NBA pre-game inactive list, refreshed on every startup.

    A row is written for every player who appears on the official inactive list
    for a game (is_active=False).  The absence of a row for a player means
    either the list has not been submitted yet or the player is confirmed active.
    The fetched_at column records when the data was last pulled so downstream
    code can distinguish "list not submitted" from "player is active".
    """

    __tablename__ = "game_player_availability"
    __table_args__ = (
        UniqueConstraint("game_id", "provider_player_id", name="uq_game_player_availability"),
    )

    availability_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    game_id: Mapped[int] = mapped_column(ForeignKey("games.game_id"), index=True)
    player_id: Mapped[int | None] = mapped_column(ForeignKey("players.player_id"), nullable=True, index=True)
    provider_player_id: Mapped[str] = mapped_column(String(64), index=True)
    player_name: Mapped[str | None] = mapped_column(String(128))
    team_abbreviation: Mapped[str | None] = mapped_column(String(8))
    is_active: Mapped[bool] = mapped_column(Boolean, default=False)
    reason: Mapped[str | None] = mapped_column(String(64))
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class ManualEntityOverride(Base):
    __tablename__ = "manual_entity_overrides"
    __table_args__ = (UniqueConstraint("provider", "provider_entity_id", name="uq_manual_override_provider_id"),)

    override_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    provider: Mapped[str] = mapped_column(String(64), index=True)
    provider_entity_id: Mapped[str] = mapped_column(String(64), index=True)
    provider_name: Mapped[str] = mapped_column(String(128))
    canonical_player_id: Mapped[int | None] = mapped_column(ForeignKey("players.player_id"), nullable=True)
    canonical_team_id: Mapped[int | None] = mapped_column(ForeignKey("teams.team_id"), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text)


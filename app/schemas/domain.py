from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class TeamPayload(BaseModel):
    provider_team_id: str
    abbreviation: str
    name: str
    city: str | None = None


class PlayerPayload(BaseModel):
    provider_player_id: str
    full_name: str
    team_abbreviation: str | None = None
    position: str | None = None
    status: str | None = None


class GamePayload(BaseModel):
    provider_game_id: str
    game_date: date
    start_time: datetime
    home_team_abbreviation: str
    away_team_abbreviation: str
    season_code: str | None = None
    spread: float | None = None
    total: float | None = None
    status: str = "scheduled"
    meta: dict[str, Any] = Field(default_factory=dict)


class PlayerGameLogPayload(BaseModel):
    provider_game_id: str
    provider_player_id: str
    team_abbreviation: str
    opponent_abbreviation: str | None = None
    minutes: float
    points: int
    rebounds: int
    assists: int
    threes: int
    steals: int = 0
    blocks: int = 0
    turnovers: int = 0
    fouls: int = 0
    field_goal_attempts: int = 0
    field_goals_made: int = 0
    free_throw_attempts: int = 0
    free_throws_made: int = 0
    offensive_rebounds: int = 0
    defensive_rebounds: int = 0
    plus_minus: float | None = None
    starter_flag: bool = False
    overtime_flag: bool = False
    meta: dict[str, Any] = Field(default_factory=dict)


class PlayerAvailabilityPayload(BaseModel):
    """Official pre-game inactive list entry from the NBA.

    The league requires teams to submit their inactive list roughly 90 minutes
    before tip-off.  Only players who are *officially ruled out* appear here;
    absence from this list means a player is expected to play.  The list may be
    empty before submission — callers should treat that as "not yet available"
    rather than "everyone is active".
    """

    provider_game_id: str
    provider_player_id: str
    player_name: str | None = None
    team_abbreviation: str | None = None
    is_active: bool = False  # False = on official inactive list
    reason: str | None = None  # jersey_num or designation when available
    fetched_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class InjuryPayload(BaseModel):
    provider_player_id: str
    team_abbreviation: str
    report_timestamp: datetime
    status: str
    designation: str | None = None
    body_part: str | None = None
    notes: str | None = None
    expected_availability_flag: bool | None = None
    provider_game_id: str | None = None


class LineOutcomePayload(BaseModel):
    side: Literal["over", "under"]
    odds: int | None = None


class LineSnapshotPayload(BaseModel):
    timestamp: datetime
    provider_game_id: str
    sportsbook_key: str
    provider_player_id: str
    market_key: str
    line_value: float
    over: LineOutcomePayload
    under: LineOutcomePayload
    event_status: str | None = None
    meta: dict[str, Any] = Field(default_factory=dict)


class ProviderFetchResult(BaseModel):
    endpoint: str
    fetched_at: datetime
    payload: dict[str, Any] | list[Any]


class PropPrediction(BaseModel):
    player_id: int
    player_name: str
    game_id: int
    market_key: str
    sportsbook_line: float
    projected_mean: float
    projected_variance: float
    projected_median: float
    over_probability: float
    under_probability: float
    calibrated_over_probability: float
    percentile_10: float
    percentile_50: float
    percentile_90: float
    confidence_interval_low: float
    confidence_interval_high: float
    top_features: list[str]
    model_version: str
    feature_version: str
    data_freshness: dict[str, datetime]
    data_sufficiency_tier: str = "A"
    data_confidence_score: float = 1.0

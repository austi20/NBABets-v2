from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel

from app.schemas.domain import PropPrediction


class PlayerSearchResult(BaseModel):
    player_id: int
    full_name: str
    team_abbreviation: str | None = None
    position: str | None = None
    status: str | None = None


class UpcomingGameResult(BaseModel):
    game_id: int
    game_date: date
    start_time: datetime
    home_team: str
    away_team: str
    spread: float | None = None
    total: float | None = None


class UpcomingPropResult(BaseModel):
    game_id: int
    player_id: int
    player_name: str
    market_key: str
    sportsbook: str
    line_value: float
    over_odds: int | None = None
    under_odds: int | None = None
    timestamp: datetime


class TrainResponse(BaseModel):
    model_run_id: int
    metrics: dict[str, float]


class BacktestResponse(BaseModel):
    summary: list[dict[str, object]]
    artifacts: dict[str, str]


class CalibrationMetricsResponse(BaseModel):
    market_key: str
    avg_calibrated_probability: float
    avg_raw_probability: float


PredictionResponse = PropPrediction


from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict

from app.services.query import BoardAvailability
from app.services.startup import StartupSnapshot, StartupStep


class StartupStepModel(BaseModel):
    key: str
    label: str
    weight: int
    estimated_seconds: float
    status: str
    message: str
    progress_fraction: float
    started_at: float | None
    ended_at: float | None

    @classmethod
    def from_dataclass(cls, step: StartupStep) -> StartupStepModel:
        return cls(
            key=step.key,
            label=step.label,
            weight=step.weight,
            estimated_seconds=step.estimated_seconds,
            status=step.status,
            message=step.message,
            progress_fraction=step.progress_fraction,
            started_at=step.started_at,
            ended_at=step.ended_at,
        )


class StartupSnapshotModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    progress_percent: float
    eta_seconds: float | None
    current_step: str
    current_detail: str
    database_message: str
    board_date_message: str
    started_at: datetime
    completed: bool
    failed: bool
    error_message: str | None
    steps: list[StartupStepModel]
    metrics: dict[str, Any]
    log_lines: list[str]

    @classmethod
    def from_dataclass(cls, snapshot: StartupSnapshot) -> StartupSnapshotModel:
        return cls(
            progress_percent=snapshot.progress_percent,
            eta_seconds=snapshot.eta_seconds,
            current_step=snapshot.current_step,
            current_detail=snapshot.current_detail,
            database_message=snapshot.database_message,
            board_date_message=snapshot.board_date_message,
            started_at=snapshot.started_at,
            completed=snapshot.completed,
            failed=snapshot.failed,
            error_message=snapshot.error_message,
            steps=[StartupStepModel.from_dataclass(step) for step in snapshot.steps],
            metrics=snapshot.metrics,
            log_lines=snapshot.log_lines,
        )


class BoardAvailabilityModel(BaseModel):
    board_date: date
    scheduled_games: int
    live_games: int
    final_games: int
    has_pregame_options: bool

    @classmethod
    def from_dataclass(cls, availability: BoardAvailability) -> BoardAvailabilityModel:
        return cls(
            board_date=availability.board_date,
            scheduled_games=availability.scheduled_games,
            live_games=availability.live_games,
            final_games=availability.final_games,
            has_pregame_options=availability.has_pregame_options,
        )


from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel

from app.services.insights import BoardSummary
from app.services.query import BoardAvailability


class BoardAvailabilityModel(BaseModel):
    board_date: date
    scheduled_games: int
    live_games: int
    final_games: int
    has_pregame_options: bool

    @classmethod
    def from_dataclass(cls, value: BoardAvailability) -> BoardAvailabilityModel:
        return cls(
            board_date=value.board_date,
            scheduled_games=value.scheduled_games,
            live_games=value.live_games,
            final_games=value.final_games,
            has_pregame_options=value.has_pregame_options,
        )


class BoardSummaryModel(BaseModel):
    board_date: date | None
    game_count: int
    opportunity_count: int
    sportsbook_count: int
    quote_count: int
    live_quote_count: int
    alt_line_count: int
    same_game_parlay_count: int
    multi_game_parlay_count: int
    latest_quote_at: datetime | None
    latest_prediction_at: datetime | None

    @classmethod
    def from_dataclass(cls, value: BoardSummary) -> BoardSummaryModel:
        return cls(
            board_date=value.board_date,
            game_count=value.game_count,
            opportunity_count=value.opportunity_count,
            sportsbook_count=value.sportsbook_count,
            quote_count=value.quote_count,
            live_quote_count=value.live_quote_count,
            alt_line_count=value.alt_line_count,
            same_game_parlay_count=value.same_game_parlay_count,
            multi_game_parlay_count=value.multi_game_parlay_count,
            latest_quote_at=value.latest_quote_at,
            latest_prediction_at=value.latest_prediction_at,
        )


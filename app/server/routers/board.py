from __future__ import annotations

from fastapi import APIRouter, Request

from app.server.board_access import board_cache_entry_or_503
from app.server.schemas.board import BoardAvailabilityModel, BoardSummaryModel

router = APIRouter(prefix="/api/board", tags=["board"])


@router.get("/availability", response_model=BoardAvailabilityModel)
def board_availability(request: Request) -> BoardAvailabilityModel:
    """Return whether the current board has data available for display."""
    entry = board_cache_entry_or_503(request)
    return BoardAvailabilityModel.from_dataclass(entry.board_availability)


@router.get("/summary", response_model=BoardSummaryModel)
def board_summary(request: Request) -> BoardSummaryModel:
    """Return the aggregate summary of the current prop board."""
    entry = board_cache_entry_or_503(request)
    return BoardSummaryModel.from_dataclass(entry.board_summary)


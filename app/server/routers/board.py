from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from app.server.schemas.board import BoardAvailabilityModel, BoardSummaryModel

router = APIRouter(prefix="/api/board", tags=["board"])


def _cached_entry_or_503(request: Request):
    cache = request.app.state.board_cache
    get_cached = getattr(cache, "get_cached", None)
    if callable(get_cached):
        entry = get_cached()
    else:  # compatibility for tests injecting lightweight doubles
        entry = cache.get_or_build()
    if entry is None:
        raise HTTPException(status_code=503, detail="Board cache is not ready. Run startup first.")
    return entry


@router.get("/availability", response_model=BoardAvailabilityModel)
def board_availability(request: Request) -> BoardAvailabilityModel:
    entry = _cached_entry_or_503(request)
    return BoardAvailabilityModel.from_dataclass(entry.board_availability)


@router.get("/summary", response_model=BoardSummaryModel)
def board_summary(request: Request) -> BoardSummaryModel:
    entry = _cached_entry_or_503(request)
    return BoardSummaryModel.from_dataclass(entry.board_summary)


from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request

from app.server.board_access import board_cache_entry_or_503
from app.server.schemas.insights import InjuryStatusBadgeModel, ProviderStatusModel

router = APIRouter(prefix="/api/insights", tags=["insights"])

MAX_PLAYER_IDS = 500


@router.get("/providers", response_model=list[ProviderStatusModel])
def provider_insights(request: Request) -> list[ProviderStatusModel]:
    """Return per-provider data freshness/health badges for the board."""
    entry = board_cache_entry_or_503(request)
    return [ProviderStatusModel.from_dataclass(status) for status in entry.provider_statuses]


@router.get("/injuries", response_model=dict[int, InjuryStatusBadgeModel])
def injury_insights(
    request: Request,
    player_ids: str | None = Query(default=None),
) -> dict[int, InjuryStatusBadgeModel]:
    """Return injury-status badges, optionally filtered to the given player ids."""
    requested_ids: set[int] | None = None
    if player_ids is not None and player_ids.strip() != "":
        parts = [part.strip() for part in player_ids.split(",") if part.strip()]
        if len(parts) > MAX_PLAYER_IDS:
            raise HTTPException(status_code=422, detail="too many player_ids")
        try:
            requested_ids = {int(part) for part in parts}
        except ValueError as exc:
            raise HTTPException(status_code=422, detail="invalid player_ids") from exc

    entry = board_cache_entry_or_503(request)
    if requested_ids is None:
        source = entry.injury_status_by_player_id
    else:
        source = {player_id: value for player_id, value in entry.injury_status_by_player_id.items() if player_id in requested_ids}
    return {player_id: InjuryStatusBadgeModel.from_dataclass(value) for player_id, value in source.items()}


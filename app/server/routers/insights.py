from __future__ import annotations

from fastapi import APIRouter, Query, Request

from app.server.board_access import board_cache_entry_or_503
from app.server.schemas.insights import InjuryStatusBadgeModel, ProviderStatusModel

router = APIRouter(prefix="/api/insights", tags=["insights"])


@router.get("/providers", response_model=list[ProviderStatusModel])
def provider_insights(request: Request) -> list[ProviderStatusModel]:
    entry = board_cache_entry_or_503(request)
    return [ProviderStatusModel.from_dataclass(status) for status in entry.provider_statuses]


@router.get("/injuries", response_model=dict[int, InjuryStatusBadgeModel])
def injury_insights(
    request: Request,
    player_ids: str | None = Query(default=None),
) -> dict[int, InjuryStatusBadgeModel]:
    entry = board_cache_entry_or_503(request)
    if player_ids is None or player_ids.strip() == "":
        source = entry.injury_status_by_player_id
    else:
        requested_ids = {
            int(part.strip())
            for part in player_ids.split(",")
            if part.strip()
        }
        source = {player_id: value for player_id, value in entry.injury_status_by_player_id.items() if player_id in requested_ids}
    return {player_id: InjuryStatusBadgeModel.from_dataclass(value) for player_id, value in source.items()}


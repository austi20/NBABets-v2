from __future__ import annotations

from fastapi import APIRouter, Query, Request

from app.server.board_access import board_cache_entry_or_503
from app.server.schemas.parlays import (
    MultiGameParlaysResponseModel,
    ParlayInsightModel,
    ParlayRecommendationModel,
    ParlayWithInsightModel,
    SameGameParlaysResponseModel,
)

router = APIRouter(prefix="/api/parlays", tags=["parlays"])


def _book_matches(entry_key: str, selected_book: str) -> bool:
    if selected_book == "":
        return True
    return entry_key.lower() == selected_book.lower()


@router.get("/sgp", response_model=SameGameParlaysResponseModel)
def same_game_parlays(
    request: Request,
    game_id: int | None = Query(default=None),
    book: str = Query(default=""),
) -> SameGameParlaysResponseModel:
    entry = board_cache_entry_or_503(request)
    sections: dict[str, dict[str, dict[str, list[ParlayWithInsightModel]]]] = {}
    for sportsbook_key, by_leg_count in entry.same_game_sections_by_book.items():
        if not _book_matches(sportsbook_key, book):
            continue
        leg_payload: dict[str, dict[str, list[ParlayWithInsightModel]]] = {}
        for leg_count, by_game in by_leg_count.items():
            game_payload: dict[str, list[ParlayWithInsightModel]] = {}
            for resolved_game_id, parlays in by_game.items():
                if game_id is not None and resolved_game_id != game_id:
                    continue
                items: list[ParlayWithInsightModel] = []
                for index, parlay in enumerate(parlays):
                    insight_key = (sportsbook_key, leg_count, resolved_game_id, index)
                    insight = entry.parlay_insights[insight_key]
                    items.append(
                        ParlayWithInsightModel(
                            parlay=ParlayRecommendationModel.from_dataclass(parlay),
                            insight=ParlayInsightModel.from_dataclass(insight),
                        )
                    )
                if items:
                    game_payload[str(resolved_game_id)] = items
            if game_payload:
                leg_payload[str(leg_count)] = game_payload
        if leg_payload:
            sections[sportsbook_key] = leg_payload
    return SameGameParlaysResponseModel(sections=sections)


@router.get("/multi", response_model=MultiGameParlaysResponseModel)
def multi_game_parlays(
    request: Request,
    book: str = Query(default=""),
) -> MultiGameParlaysResponseModel:
    entry = board_cache_entry_or_503(request)
    sections: dict[str, dict[str, list[ParlayWithInsightModel]]] = {}
    for sportsbook_key, by_leg_count in entry.multi_game_sections_by_book.items():
        if not _book_matches(sportsbook_key, book):
            continue
        leg_payload: dict[str, list[ParlayWithInsightModel]] = {}
        for leg_count, parlays in by_leg_count.items():
            items: list[ParlayWithInsightModel] = []
            for index, parlay in enumerate(parlays):
                insight_key = (sportsbook_key, leg_count, -1, index)
                insight = entry.parlay_insights[insight_key]
                items.append(
                    ParlayWithInsightModel(
                        parlay=ParlayRecommendationModel.from_dataclass(parlay),
                        insight=ParlayInsightModel.from_dataclass(insight),
                    )
                )
            if items:
                leg_payload[str(leg_count)] = items
        if leg_payload:
            sections[sportsbook_key] = leg_payload
    return MultiGameParlaysResponseModel(sections=sections)


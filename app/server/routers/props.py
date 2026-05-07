from __future__ import annotations

from typing import Any, cast

from fastapi import APIRouter, HTTPException, Query, Request

from app.server.schemas.props import (
    PropInsightModel,
    PropListResponseModel,
    PropOpportunityModel,
    PropWithInsightModel,
)
from app.server.services.board_cache import BoardCacheEntry
from app.services import rotation_audit
from app.services.prop_analysis import PropOpportunity

router = APIRouter(prefix="/api/props", tags=["props"])

_CONFIDENCE_ORDER = {"Fragile": 0, "Watch": 1, "Solid": 2, "Strong": 3, "Elite": 4}
_CONFIDENCE_FILTER_MIN = {"Watch+": 1, "Solid+": 2, "Strong+": 3, "Elite": 4}


def _cached_entry_or_503(request: Request) -> BoardCacheEntry:
    cache = request.app.state.board_cache
    get_cached = getattr(cache, "get_cached", None)
    if callable(get_cached):
        entry = get_cached()
    else:  # compatibility for tests injecting lightweight doubles
        entry = cache.get_or_build()
    if entry is None:
        raise HTTPException(status_code=503, detail="Board cache is not ready. Run startup first.")
    return cast(BoardCacheEntry, entry)


def _passes_confidence_filter(tier: str, selected_filter: str) -> bool:
    if selected_filter == "All":
        return True
    required = _CONFIDENCE_FILTER_MIN.get(selected_filter, 0)
    return _CONFIDENCE_ORDER.get(tier, 0) >= required


def _opportunity_key(opportunity: PropOpportunity) -> tuple[int, int, str, float]:
    return (opportunity.game_id, opportunity.player_id, opportunity.market_key, float(opportunity.consensus_line))


def _best_book_name(entry: BoardCacheEntry, opportunity: PropOpportunity) -> str:
    key = _opportunity_key(opportunity)
    insight = entry.opportunity_insights[key]
    return insight.best_quote.sportsbook_name


def _sort_opportunities(entry, opportunities: list[PropOpportunity], sort_choice: str) -> list[PropOpportunity]:
    if sort_choice == "Best EV":
        return sorted(
            opportunities,
            key=lambda row: (
                entry.opportunity_insights[_opportunity_key(row)].expected_profit_per_unit,
                entry.opportunity_insights[_opportunity_key(row)].edge,
                row.hit_probability,
            ),
            reverse=True,
        )
    if sort_choice == "Highest Hit Rate":
        return sorted(
            opportunities,
            key=lambda row: (
                row.hit_probability,
                entry.opportunity_insights[_opportunity_key(row)].edge,
                row.projected_mean,
            ),
            reverse=True,
        )
    if sort_choice == "Most Consensus":
        return sorted(
            opportunities,
            key=lambda row: (
                entry.opportunity_insights[_opportunity_key(row)].market_width,
                -len(row.quotes),
                -entry.opportunity_insights[_opportunity_key(row)].edge,
            ),
        )
    if sort_choice == "Freshest":
        return sorted(
            opportunities,
            key=lambda row: entry.opportunity_insights[_opportunity_key(row)].best_quote.timestamp,
            reverse=True,
        )
    if sort_choice == "Player A-Z":
        return sorted(
            opportunities,
            key=lambda row: (
                row.player_name,
                row.market_key,
                -entry.opportunity_insights[_opportunity_key(row)].edge,
            ),
        )
    return sorted(
        opportunities,
        key=lambda row: (
            entry.opportunity_insights[_opportunity_key(row)].edge,
            entry.opportunity_insights[_opportunity_key(row)].expected_profit_per_unit,
            row.hit_probability,
        ),
        reverse=True,
    )


@router.get("", response_model=PropListResponseModel)
def list_props(
    request: Request,
    confidence: str = Query(default="All"),
    market: str = Query(default="All"),
    sort: str = Query(default="Best Edge"),
    book: str = Query(default="All"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
) -> PropListResponseModel:
    entry = _cached_entry_or_503(request)

    filtered: list[PropOpportunity] = []
    for opportunity in entry.opportunities:
        insight = entry.opportunity_insights[_opportunity_key(opportunity)]
        if market != "All" and opportunity.market_key.lower() != market.lower():
            continue
        if book != "All" and insight.best_quote.sportsbook_name.lower() != book.lower():
            continue
        if not _passes_confidence_filter(insight.confidence_tier, confidence):
            continue
        filtered.append(opportunity)

    ordered = _sort_opportunities(entry, filtered, sort)
    total = len(ordered)
    start = (page - 1) * page_size
    end = start + page_size
    selected = ordered[start:end]

    items = [
        PropWithInsightModel(
            opportunity=PropOpportunityModel.from_dataclass(opportunity),
            insight=PropInsightModel.from_dataclass(entry.opportunity_insights[_opportunity_key(opportunity)]),
        )
        for opportunity in selected
    ]
    return PropListResponseModel(items=items, total=total, page=page, page_size=page_size)


@router.get("/{player_id}/{market}/{line}", response_model=PropWithInsightModel)
def prop_detail(
    request: Request,
    player_id: int,
    market: str,
    line: float,
) -> PropWithInsightModel:
    entry = _cached_entry_or_503(request)
    for opportunity in entry.opportunities:
        if opportunity.player_id != player_id:
            continue
        if opportunity.market_key.lower() != market.lower():
            continue
        if abs(float(opportunity.consensus_line) - line) > 1e-6:
            continue
        key = _opportunity_key(opportunity)
        return PropWithInsightModel(
            opportunity=PropOpportunityModel.from_dataclass(opportunity),
            insight=PropInsightModel.from_dataclass(entry.opportunity_insights[key]),
        )
    raise HTTPException(status_code=404, detail="Prop opportunity not found")


@router.get("/rotation-audit/{game_id}", response_model=dict[str, Any])
def rotation_audit_detail(game_id: int) -> dict[str, Any]:
    return rotation_audit.get_redistribution(game_id)


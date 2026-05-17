from __future__ import annotations

from datetime import date as _date_t
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

from app.config.settings import get_settings
from app.db.session import session_scope
from app.server.board_access import board_cache_entry_or_503
from app.server.schemas.props import (
    PropInsightModel,
    PropListResponseModel,
    PropOpportunityModel,
    PropWithInsightModel,
)
from app.server.services.board_cache import BoardCacheEntry
from app.services import rotation_audit
from app.services.prop_analysis import PropOpportunity
from app.services.volatility import build_feature_snapshot, compute_volatility

router = APIRouter(prefix="/api/props", tags=["props"])

_CONFIDENCE_ORDER = {"Fragile": 0, "Watch": 1, "Solid": 2, "Strong": 3, "Elite": 4}
_CONFIDENCE_FILTER_MIN = {"Watch+": 1, "Solid+": 2, "Strong+": 3, "Elite": 4}


def _passes_confidence_filter(tier: str, selected_filter: str) -> bool:
    if selected_filter == "All":
        return True
    required = _CONFIDENCE_FILTER_MIN.get(selected_filter, 0)
    return _CONFIDENCE_ORDER.get(tier, 0) >= required


def _passes_backtest_tuning_filters(
    market_key: str,
    hit_probability: float,
    *,
    disabled_markets: frozenset[str],
    max_edge: float,
) -> bool:
    """Apply 2026-05-16 backtest-driven filters.

    Returns False if the opportunity should be dropped (disabled market, or
    model edge exceeds the configured ceiling where calibration breaks down).
    """
    if market_key.lower() in disabled_markets:
        return False
    if max_edge > 0 and abs(float(hit_probability) - 0.5) > max_edge:
        return False
    return True


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
    entry = board_cache_entry_or_503(request)
    settings = get_settings()
    disabled_markets = settings.disabled_markets
    max_edge = settings.max_surfaceable_edge

    filtered: list[PropOpportunity] = []
    for opportunity in entry.opportunities:
        insight = entry.opportunity_insights[_opportunity_key(opportunity)]
        if not _passes_backtest_tuning_filters(
            opportunity.market_key,
            insight.best_quote.hit_probability,
            disabled_markets=disabled_markets,
            max_edge=max_edge,
        ):
            continue
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


@router.get("/predictions/{prediction_id}/volatility", response_model=dict[str, Any])
def prediction_volatility(prediction_id: int) -> dict[str, Any]:
    """Return the per-prediction volatility breakdown for diagnostics."""
    from app.models.all import Prediction

    with session_scope() as session:
        prediction = session.get(Prediction, prediction_id)
        if prediction is None:
            raise HTTPException(status_code=404, detail="prediction not found")

        market_key = "points"
        if prediction.market_id is not None:
            from app.models.all import PropMarket

            market = session.get(PropMarket, prediction.market_id)
            if market is not None:
                market_key = market.key

        as_of_date: _date_t = prediction.predicted_at.date()
        score = compute_volatility(
            raw_probability=prediction.over_probability,
            features=build_feature_snapshot(
                session=session,
                player_id=prediction.player_id,
                market_key=market_key,
                as_of_date=as_of_date,
                predicted_minutes_std=None,
            ),
        )

        return {
            "prediction_id": prediction_id,
            "coefficient": score.coefficient,
            "tier": score.tier,
            "adjusted_probability": score.adjusted_probability,
            "confidence_multiplier": score.confidence_multiplier,
            "reason": score.reason,
            "contributors": [
                {
                    "name": c.name,
                    "raw_value": c.raw_value,
                    "weight": c.weight,
                    "contribution": c.contribution,
                }
                for c in score.contributors
            ],
        }


@router.get("/{player_id}/{market}/{line}", response_model=PropWithInsightModel)
def prop_detail(
    request: Request,
    player_id: int,
    market: str,
    line: float,
) -> PropWithInsightModel:
    entry = board_cache_entry_or_503(request)
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

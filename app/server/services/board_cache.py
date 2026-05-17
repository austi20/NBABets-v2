from __future__ import annotations

import threading
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime
from typing import cast

from app.config.settings import get_settings
from app.db.session import session_scope
from app.services.insights import (
    BoardSummary,
    InjuryStatusBadge,
    LocalAgentStatus,
    ParlayInsight,
    PropInsight,
    ProviderStatus,
    build_board_summary,
    build_parlay_insight,
    build_prop_insight,
    load_injury_statuses,
    load_local_agent_status,
    load_provider_statuses,
)
from app.services.parlays import MultiGameParlayService, ParlayRecommendation, SameGameParlayService
from app.services.player_bias import effective_over_bias_offset
from app.services.prop_analysis import PropAnalysisService, PropOpportunity
from app.services.query import BoardAvailability, QueryService
from app.services.volatility import build_feature_snapshot, compute_volatility


@dataclass(frozen=True)
class BoardCacheEntry:
    board_date: date
    board_availability: BoardAvailability
    board_summary: BoardSummary
    opportunities: list[PropOpportunity]
    opportunity_insights: dict[tuple[int, int, str, float], PropInsight]
    same_game_sections_by_book: dict[str, dict[int, dict[int, list[ParlayRecommendation]]]]
    multi_game_sections_by_book: dict[str, dict[int, list[ParlayRecommendation]]]
    parlay_insights: dict[tuple[str, int, int, int], ParlayInsight]
    provider_statuses: list[ProviderStatus]
    injury_status_by_player_id: dict[int, InjuryStatusBadge]
    local_agent_status: LocalAgentStatus
    generated_at: datetime = field(default_factory=lambda: datetime.now(UTC))


class BoardCache:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._entries: dict[date, BoardCacheEntry] = {}

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()

    def get_cached(self, target_date: date | None = None) -> BoardCacheEntry | None:
        with self._lock:
            if target_date is not None:
                return self._entries.get(target_date)
            if self._entries:
                latest_key = max(self._entries.keys())
                return self._entries[latest_key]
        return None

    def populate(self, target_date: date | None = None) -> BoardCacheEntry:
        entry = self._build_entry(target_date)
        with self._lock:
            self._entries[entry.board_date] = entry
        return entry

    def get_or_build(self, target_date: date | None = None) -> BoardCacheEntry:
        entry = self.get_cached(target_date)
        if entry is not None:
            return entry
        return self.populate(target_date)

    def _build_entry(self, target_date: date | None) -> BoardCacheEntry:
        same_game_sections_by_book: dict[str, dict[int, dict[int, list[ParlayRecommendation]]]]
        multi_game_sections_by_book: dict[str, dict[int, list[ParlayRecommendation]]]
        with session_scope() as session:
            query_service = QueryService(session)
            board_date = target_date or query_service.resolve_board_date(date.today()) or date.today()
            board_availability = query_service.board_availability(board_date)
            opportunities = PropAnalysisService(session).all_opportunities(board_date)
            same_game_sections_by_book = cast(
                dict[str, dict[int, dict[int, list[ParlayRecommendation]]]],
                SameGameParlayService(session).suggest_by_sportsbook_leg_count_and_game(
                    board_date,
                    min_legs=2,
                    max_legs=20,
                    top_per_game=5,
                ),
            )
            multi_game_sections_by_book = cast(
                dict[str, dict[int, list[ParlayRecommendation]]],
                MultiGameParlayService(session).suggest_by_sportsbook_and_leg_count(
                    board_date,
                    min_legs=2,
                    max_legs=20,
                    top_per_leg_count=10,
                ),
            )
            provider_statuses = load_provider_statuses(session)
            local_agent_status = load_local_agent_status(session)
            injury_status_by_player_id = load_injury_statuses(
                session,
                {opportunity.player_id for opportunity in opportunities},
            )
            board_summary = build_board_summary(
                session,
                board_date,
                opportunities,
                same_game_sections_by_book,
                multi_game_sections_by_book,
            )

        opportunity_insights: dict[tuple[int, int, str, float], PropInsight] = {}
        if get_settings().volatility_tier_enabled:
            with session_scope() as volatility_session:
                for idx, opportunity in enumerate(opportunities):
                    # Apply the same side-bias offset used by _quote_recommendation
                    # BEFORE volatility shrinkage, so adjusted_over_probability is
                    # composed end-to-end (per-player -> per-market -> global, then
                    # volatility-shrunk). Downstream consumers (decision_brain,
                    # parlays) can trust adjusted_over_probability without
                    # re-applying the bias themselves.
                    bias_offset = effective_over_bias_offset(
                        opportunity.player_id, opportunity.market_key
                    )
                    bias_corrected_over = max(
                        0.001,
                        min(0.999, opportunity.calibrated_over_probability - bias_offset),
                    )
                    volatility = compute_volatility(
                        raw_probability=bias_corrected_over,
                        features=build_feature_snapshot(
                            session=volatility_session,
                            player_id=opportunity.player_id,
                            market_key=opportunity.market_key,
                            as_of_date=board_date,
                            predicted_minutes_std=None,
                        ),
                    )
                    enriched = replace(
                        opportunity,
                        volatility_coefficient=volatility.coefficient,
                        volatility_tier=volatility.tier,
                        adjusted_over_probability=volatility.adjusted_probability,
                    )
                    opportunities[idx] = enriched
                    key = (
                        enriched.game_id,
                        enriched.player_id,
                        enriched.market_key,
                        float(enriched.consensus_line),
                    )
                    opportunity_insights[key] = build_prop_insight(
                        enriched,
                        injury_status_by_player_id.get(enriched.player_id),
                        volatility=volatility,
                    )
        else:
            for opportunity in opportunities:
                key = (
                    opportunity.game_id,
                    opportunity.player_id,
                    opportunity.market_key,
                    float(opportunity.consensus_line),
                )
                opportunity_insights[key] = build_prop_insight(
                    opportunity,
                    injury_status_by_player_id.get(opportunity.player_id),
                )

        parlay_insights: dict[tuple[str, int, int, int], ParlayInsight] = {}
        for sportsbook_key, by_leg_count_same_game in same_game_sections_by_book.items():
            for leg_count, by_game in by_leg_count_same_game.items():
                for game_id, parlays in by_game.items():
                    for index, parlay in enumerate(parlays):
                        parlay_insights[(sportsbook_key, leg_count, game_id, index)] = build_parlay_insight(parlay)
        for sportsbook_key, by_leg_count_multi_game in multi_game_sections_by_book.items():
            for leg_count, parlays in by_leg_count_multi_game.items():
                for index, parlay in enumerate(parlays):
                    parlay_insights[(sportsbook_key, leg_count, -1, index)] = build_parlay_insight(parlay)

        return BoardCacheEntry(
            board_date=board_date,
            board_availability=board_availability,
            board_summary=board_summary,
            opportunities=opportunities,
            opportunity_insights=opportunity_insights,
            same_game_sections_by_book=same_game_sections_by_book,
            multi_game_sections_by_book=multi_game_sections_by_book,
            parlay_insights=parlay_insights,
            provider_statuses=provider_statuses,
            injury_status_by_player_id=injury_status_by_player_id,
            local_agent_status=local_agent_status,
        )


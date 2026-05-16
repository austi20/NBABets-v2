from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date
from math import ceil, prod
from statistics import NormalDist, mean

import numpy as np
from sqlalchemy import select
from sqlalchemy.orm import Session, aliased

from app.models.all import Game, Team
from app.services.board_date import matches_board_date
from app.services.live_games import sync_live_games_from_nba_api
from app.services.prop_analysis import PropAnalysisService, PropOpportunity, SportsbookQuote


@dataclass(frozen=True)
class ParlayLeg:
    game_id: int
    matchup: str
    player_name: str
    market_key: str
    recommended_side: str
    line_value: float
    american_odds: int
    hit_probability: float
    likelihood_score: int
    is_live_quote: bool
    verification_status: str
    odds_source_provider: str


@dataclass(frozen=True)
class ParlayRecommendation:
    rank: int
    game_id: int
    matchup: str
    sportsbook_key: str
    sportsbook_name: str
    sportsbook_icon: str
    leg_count: int
    game_count: int
    game_ids: tuple[int, ...]
    game_labels: tuple[str, ...]
    joint_probability: float
    combined_decimal_odds: float
    combined_american_odds: int
    expected_profit_per_unit: float
    implied_probability: float
    edge: float
    all_legs_live: bool
    verification_status: str
    odds_source_provider: str
    correlation_penalty: float
    average_leg_hit_probability: float
    weakest_leg_hit_probability: float
    legs: list[ParlayLeg]


SameGameParlay = ParlayRecommendation


class _BaseParlayService:
    def __init__(self, session: Session) -> None:
        self._session = session

    def suggest(
        self,
        target_date: date | None = None,
        *,
        min_legs: int = 3,
        max_legs: int = 20,
        top_per_leg_count: int = 10,
        candidate_limit_per_book: int = 40,
        minimum_distinct_games: int = 2,
    ) -> list[ParlayRecommendation]:
        sections = self.suggest_by_leg_count(
            target_date,
            min_legs=min_legs,
            max_legs=max_legs,
            top_per_leg_count=top_per_leg_count,
            candidate_limit_per_book=candidate_limit_per_book,
            minimum_distinct_games=minimum_distinct_games,
        )
        flattened: list[ParlayRecommendation] = []
        rank = 1
        for leg_count in sorted(sections):
            for parlay in sections[leg_count]:
                flattened.append(
                    ParlayRecommendation(
                        rank=rank,
                        game_id=parlay.game_id,
                        matchup=parlay.matchup,
                        sportsbook_key=parlay.sportsbook_key,
                        sportsbook_name=parlay.sportsbook_name,
                        sportsbook_icon=parlay.sportsbook_icon,
                        leg_count=parlay.leg_count,
                        game_count=parlay.game_count,
                        game_ids=parlay.game_ids,
                        game_labels=parlay.game_labels,
                        joint_probability=parlay.joint_probability,
                        combined_decimal_odds=parlay.combined_decimal_odds,
                        combined_american_odds=parlay.combined_american_odds,
                        expected_profit_per_unit=parlay.expected_profit_per_unit,
                        implied_probability=parlay.implied_probability,
                        edge=parlay.edge,
                        all_legs_live=parlay.all_legs_live,
                        verification_status=parlay.verification_status,
                        odds_source_provider=parlay.odds_source_provider,
                        correlation_penalty=parlay.correlation_penalty,
                        average_leg_hit_probability=parlay.average_leg_hit_probability,
                        weakest_leg_hit_probability=parlay.weakest_leg_hit_probability,
                        legs=parlay.legs,
                    )
                )
                rank += 1
        return flattened

    def suggest_by_leg_count(
        self,
        target_date: date | None = None,
        *,
        min_legs: int = 3,
        max_legs: int = 20,
        top_per_leg_count: int = 10,
        candidate_limit_per_book: int = 40,
        minimum_distinct_games: int = 2,
    ) -> dict[int, list[ParlayRecommendation]]:
        target_date = target_date or date.today()
        candidates_by_group, game_labels = self._candidate_groups(target_date)
        sections: dict[int, list[ParlayRecommendation]] = {}
        for leg_count in range(min_legs, max_legs + 1):
            parlays = self._candidate_parlays_for_leg_count(
                candidates_by_group=candidates_by_group,
                game_labels=game_labels,
                leg_count=leg_count,
                top_per_leg_count=top_per_leg_count,
                candidate_limit_per_book=candidate_limit_per_book,
                minimum_distinct_games=minimum_distinct_games,
            )
            ranked = self._rank_parlays(parlays, top_per_leg_count)
            if ranked:
                sections[leg_count] = ranked
        return sections

    def suggest_by_sportsbook_and_leg_count(
        self,
        target_date: date | None = None,
        *,
        min_legs: int = 3,
        max_legs: int = 20,
        top_per_leg_count: int = 10,
        candidate_limit_per_book: int = 40,
        minimum_distinct_games: int = 2,
    ) -> dict[str, dict[int, list[ParlayRecommendation]]]:
        target_date = target_date or date.today()
        candidates_by_group, game_labels = self._candidate_groups(target_date)
        sections: dict[str, dict[int, list[ParlayRecommendation]]] = {}
        for leg_count in range(min_legs, max_legs + 1):
            parlays = self._candidate_parlays_for_leg_count(
                candidates_by_group=candidates_by_group,
                game_labels=game_labels,
                leg_count=leg_count,
                top_per_leg_count=top_per_leg_count,
                candidate_limit_per_book=candidate_limit_per_book,
                minimum_distinct_games=minimum_distinct_games,
            )
            grouped: dict[str, list[ParlayRecommendation]] = {}
            for parlay in parlays:
                grouped.setdefault(parlay.sportsbook_key, []).append(parlay)
            for sportsbook_key, sportsbook_parlays in grouped.items():
                ranked = self._rank_parlays(sportsbook_parlays, top_per_leg_count)
                if ranked:
                    sections.setdefault(sportsbook_key, {})[leg_count] = ranked
        return dict(
            sorted(
                sections.items(),
                key=lambda item: self._sportsbook_sort_key(item[1], item[0]),
            )
        )

    def _candidate_groups(
        self,
        target_date: date,
    ) -> tuple[dict[tuple[object, ...], list[_CandidateLeg]], dict[int, str]]:
        raise NotImplementedError

    def _candidate_parlays_for_leg_count(
        self,
        *,
        candidates_by_group: dict[tuple[object, ...], list[_CandidateLeg]],
        game_labels: dict[int, str],
        leg_count: int,
        top_per_leg_count: int,
        candidate_limit_per_book: int,
        minimum_distinct_games: int,
    ) -> list[ParlayRecommendation]:
        raise NotImplementedError

    def _candidate_inventory(
        self,
        target_date: date,
    ) -> tuple[list[_CandidateLeg], dict[int, str]]:
        opportunities = PropAnalysisService(self._session).all_opportunities(target_date)
        game_labels = self._game_labels(target_date)
        candidates: list[_CandidateLeg] = []
        for opportunity in opportunities:
            for quote in opportunity.quotes:
                if not self._is_fully_verified_quote(quote):
                    continue
                american_odds = quote.over_odds if quote.recommended_side == "OVER" else quote.under_odds
                if american_odds is None:
                    continue
                candidate = _CandidateLeg(
                    opportunity=opportunity,
                    quote=quote,
                    american_odds=int(american_odds),
                )
                if not self._is_candidate_viable(candidate):
                    continue
                candidates.append(candidate)
        return candidates, game_labels

    def _rank_parlays(
        self,
        parlays: list[ParlayRecommendation],
        limit: int,
    ) -> list[ParlayRecommendation]:
        ordered = sorted(
            parlays,
            key=lambda row: (
                -row.expected_profit_per_unit,
                -row.edge,
                -len({leg.market_key for leg in row.legs}),
                -row.weakest_leg_hit_probability,
                row.sportsbook_name,
                row.matchup,
            ),
        )
        ranked: list[ParlayRecommendation] = []
        seen_signatures: set[tuple[str, ...]] = set()
        for row in ordered:
            signature = (
                row.sportsbook_key,
                *sorted(
                    f"{leg.game_id}|{leg.player_name}|{leg.market_key}|{leg.recommended_side}|{leg.line_value}"
                    for leg in row.legs
                ),
            )
            if signature in seen_signatures:
                continue
            seen_signatures.add(signature)
            ranked.append(
                ParlayRecommendation(
                    rank=len(ranked) + 1,
                    game_id=row.game_id,
                    matchup=row.matchup,
                    sportsbook_key=row.sportsbook_key,
                    sportsbook_name=row.sportsbook_name,
                    sportsbook_icon=row.sportsbook_icon,
                    leg_count=row.leg_count,
                    game_count=row.game_count,
                    game_ids=row.game_ids,
                    game_labels=row.game_labels,
                    joint_probability=row.joint_probability,
                    combined_decimal_odds=row.combined_decimal_odds,
                    combined_american_odds=row.combined_american_odds,
                    expected_profit_per_unit=row.expected_profit_per_unit,
                    implied_probability=row.implied_probability,
                    edge=row.edge,
                    all_legs_live=row.all_legs_live,
                    verification_status=row.verification_status,
                    odds_source_provider=row.odds_source_provider,
                    correlation_penalty=row.correlation_penalty,
                    average_leg_hit_probability=row.average_leg_hit_probability,
                    weakest_leg_hit_probability=row.weakest_leg_hit_probability,
                    legs=row.legs,
                )
            )
            if len(ranked) >= limit:
                break
        return ranked

    def _sportsbook_sort_key(
        self,
        sections: dict[int, list[ParlayRecommendation]],
        sportsbook_key: str,
    ) -> tuple[str, str]:
        for leg_count in sorted(sections):
            parlays = sections[leg_count]
            if parlays:
                return parlays[0].sportsbook_name, sportsbook_key
        return sportsbook_key, sportsbook_key

    def _prepare_candidates(
        self,
        candidates: list[_CandidateLeg],
        candidate_limit_per_book: int,
    ) -> list[_CandidateLeg]:
        candidate_orders = self._candidate_orders(candidates, candidate_limit_per_book)
        return candidate_orders[0] if candidate_orders else []

    def _candidate_orders(
        self,
        candidates: list[_CandidateLeg],
        candidate_limit_per_book: int,
    ) -> list[list[_CandidateLeg]]:
        deduped: dict[tuple[int, int, str, float], _CandidateLeg] = {}
        for candidate in candidates:
            key = (
                candidate.opportunity.game_id,
                candidate.opportunity.player_id,
                candidate.opportunity.market_key,
                float(candidate.quote.line_value),
            )
            existing = deduped.get(key)
            if existing is None or self._single_leg_expected_profit(candidate) > self._single_leg_expected_profit(existing):
                deduped[key] = candidate
        deduped_values = list(deduped.values())
        if not deduped_values:
            return []

        orders: list[list[_CandidateLeg]] = []
        seen_signatures: set[tuple[tuple[int, int, str, float], ...]] = set()
        sort_strategies = [
            lambda candidate: (
                _effective_hit_probability(candidate),
                self._single_leg_expected_profit(candidate),
                self._single_leg_edge(candidate),
                candidate.opportunity.projected_mean,
            ),
            lambda candidate: (
                self._single_leg_expected_profit(candidate),
                self._single_leg_edge(candidate),
                _effective_hit_probability(candidate),
                candidate.opportunity.projected_mean,
            ),
            lambda candidate: (
                self._single_leg_edge(candidate),
                self._single_leg_expected_profit(candidate),
                _effective_hit_probability(candidate),
                candidate.opportunity.projected_mean,
            ),
        ]
        for sort_key in sort_strategies:
            ordered = sorted(deduped_values, key=sort_key, reverse=True)[:candidate_limit_per_book]
            signature = tuple(self._candidate_key(candidate) for candidate in ordered)
            if not signature or signature in seen_signatures:
                continue
            seen_signatures.add(signature)
            orders.append(ordered)
        return orders

    def _compose_candidate_combo(
        self,
        candidates: list[_CandidateLeg],
        leg_count: int,
        *,
        maximize_games: bool,
        minimum_distinct_games: int,
        max_market_repeats: int = 2,
    ) -> list[_CandidateLeg]:
        combo: list[_CandidateLeg] = []
        selected_keys: set[tuple[int, int, str, float]] = set()
        market_counts: Counter[str] = Counter()
        game_counts: Counter[int] = Counter()

        def add_candidate(candidate: _CandidateLeg) -> None:
            combo.append(candidate)
            selected_keys.add(self._candidate_key(candidate))
            market_counts[candidate.opportunity.market_key] += 1
            game_counts[candidate.opportunity.game_id] += 1

        def scan(predicate) -> None:
            for candidate in candidates:
                if len(combo) >= leg_count:
                    break
                if self._candidate_key(candidate) in selected_keys:
                    continue
                if predicate(candidate, market_counts, game_counts):
                    add_candidate(candidate)

        distinct_market_count = max(1, len({candidate.opportunity.market_key for candidate in candidates}))
        soft_repeat_cap = max(max_market_repeats, ceil(leg_count / distinct_market_count))
        phases = [
            lambda candidate, markets, games: markets[candidate.opportunity.market_key] == 0
            and (not maximize_games or games[candidate.opportunity.game_id] == 0),
            lambda candidate, markets, games: maximize_games and games[candidate.opportunity.game_id] == 0,
            lambda candidate, markets, games: markets[candidate.opportunity.market_key] == 0,
            lambda candidate, markets, games: markets[candidate.opportunity.market_key] < soft_repeat_cap
            and (not maximize_games or games[candidate.opportunity.game_id] == 0),
            lambda candidate, markets, games: markets[candidate.opportunity.market_key] < soft_repeat_cap,
            lambda candidate, markets, games: maximize_games and games[candidate.opportunity.game_id] == 0,
            lambda candidate, markets, games: True,
        ]
        for phase in phases:
            scan(phase)
            if len(combo) >= leg_count:
                break

        if len(combo) < leg_count:
            return combo
        if len({candidate.opportunity.game_id for candidate in combo}) < minimum_distinct_games:
            return []
        return combo

    def _candidate_key(self, candidate: _CandidateLeg) -> tuple[int, int, str, float]:
        return (
            candidate.opportunity.game_id,
            candidate.opportunity.player_id,
            candidate.opportunity.market_key,
            float(candidate.quote.line_value),
        )

    def _build_parlay(
        self,
        *,
        combo: list[_CandidateLeg],
        rank: int,
        sportsbook_key: str,
        sportsbook_name: str,
        sportsbook_icon: str,
        game_labels: dict[int, str],
    ) -> ParlayRecommendation:
        legs = [
            ParlayLeg(
                game_id=candidate.opportunity.game_id,
                matchup=game_labels.get(candidate.opportunity.game_id, f"Game {candidate.opportunity.game_id}"),
                player_name=candidate.opportunity.player_name,
                market_key=candidate.opportunity.market_key,
                recommended_side=candidate.quote.recommended_side,
                line_value=candidate.quote.line_value,
                american_odds=candidate.american_odds,
                hit_probability=_effective_hit_probability(candidate),
                likelihood_score=min(int(_effective_hit_probability(candidate) * 100), 99),
                is_live_quote=candidate.quote.is_live_quote,
                verification_status=candidate.quote.verification_status,
                odds_source_provider=candidate.quote.odds_source_provider,
            )
            for candidate in combo
        ]
        leg_count = len(legs)
        game_ids = tuple(dict.fromkeys(leg.game_id for leg in legs))
        labels = tuple(dict.fromkeys(leg.matchup for leg in legs))
        independent_probability = prod(leg.hit_probability for leg in legs)
        joint_probability = min(self._joint_probability(combo), 0.995)
        correlation_penalty = joint_probability / max(independent_probability, 1e-6)
        raw_decimal_odds = prod(self._american_to_decimal(leg.american_odds) for leg in legs)
        decimal_odds = self._priced_parlay_decimal_odds(
            combo=combo,
            raw_decimal_odds=raw_decimal_odds,
            correlation_penalty=correlation_penalty,
        )
        implied_probability = 1.0 / decimal_odds
        expected_profit = joint_probability * decimal_odds - 1.0
        all_legs_live = all(leg.is_live_quote for leg in legs)
        verification_statuses = {leg.verification_status for leg in legs if leg.verification_status}
        verification_status = verification_statuses.pop() if len(verification_statuses) == 1 else "mixed"
        odds_source_provider = legs[0].odds_source_provider if legs else "unknown"
        matchup = labels[0] if len(labels) == 1 else f"{len(labels)} games"
        return ParlayRecommendation(
            rank=rank,
            game_id=game_ids[0],
            matchup=matchup,
            sportsbook_key=sportsbook_key,
            sportsbook_name=sportsbook_name,
            sportsbook_icon=sportsbook_icon,
            leg_count=leg_count,
            game_count=len(game_ids),
            game_ids=game_ids,
            game_labels=labels,
            joint_probability=joint_probability,
            combined_decimal_odds=decimal_odds,
            combined_american_odds=self._decimal_to_american(decimal_odds),
            expected_profit_per_unit=expected_profit,
            implied_probability=implied_probability,
            edge=joint_probability - implied_probability,
            all_legs_live=all_legs_live,
            verification_status=verification_status,
            odds_source_provider=odds_source_provider,
            correlation_penalty=correlation_penalty,
            average_leg_hit_probability=mean(leg.hit_probability for leg in legs),
            weakest_leg_hit_probability=min(leg.hit_probability for leg in legs),
            legs=legs,
        )

    def _priced_parlay_decimal_odds(
        self,
        *,
        combo: list[_CandidateLeg],
        raw_decimal_odds: float,
        correlation_penalty: float,
    ) -> float:
        payout_component = max(raw_decimal_odds - 1.0, 0.0)
        if payout_component <= 0.0:
            return max(raw_decimal_odds, 1.01)
        leg_count = len(combo)
        game_count = len({candidate.opportunity.game_id for candidate in combo})
        player_counts = Counter(candidate.opportunity.player_id for candidate in combo)
        market_counts = Counter(candidate.opportunity.market_key for candidate in combo)
        implied_probabilities = [self._american_to_probability(candidate.american_odds) for candidate in combo]
        average_implied_probability = mean(implied_probabilities)
        tail_longshot_count = sum(probability < 0.18 for probability in implied_probabilities)
        extreme_longshot_count = sum(probability < 0.10 for probability in implied_probabilities)
        same_player_repeats = sum(max(count - 1, 0) for count in player_counts.values())
        repeated_market_legs = sum(max(count - 1, 0) for count in market_counts.values())
        if game_count == 1:
            pricing_multiplier = 0.34 ** max(leg_count - 1, 0)
            pricing_multiplier /= max(correlation_penalty, 1.0) ** 1.15
            pricing_multiplier *= 0.78 ** same_player_repeats
            pricing_multiplier *= 0.86 ** repeated_market_legs
            pricing_multiplier *= float(np.clip((average_implied_probability / 0.30) ** 0.40, 0.42, 1.0))
            pricing_multiplier *= 0.82 ** tail_longshot_count
            pricing_multiplier *= 0.72 ** extreme_longshot_count
        else:
            pricing_multiplier = 0.97 ** max(leg_count - 1, 0)
            pricing_multiplier *= 0.94 ** same_player_repeats
            pricing_multiplier *= float(np.clip((average_implied_probability / 0.28) ** 0.18, 0.72, 1.0))
            pricing_multiplier *= 0.90 ** tail_longshot_count
        pricing_multiplier = float(np.clip(pricing_multiplier, 0.015, 1.0))
        return 1.0 + payout_component * pricing_multiplier

    def _correlation_penalty(self, combo: list[_CandidateLeg]) -> float:
        return 1.0

    def _joint_probability(self, combo: list[_CandidateLeg]) -> float:
        independent_probability = prod(candidate.quote.hit_probability for candidate in combo)
        if len(combo) <= 1:
            return independent_probability
        correlation_matrix = self._dependency_matrix(combo)
        thresholds = np.asarray(
            [NormalDist().inv_cdf(np.clip(candidate.quote.hit_probability, 1e-4, 1.0 - 1e-4)) for candidate in combo],
            dtype=float,
        )
        seed = int(
            sum(
                (candidate.opportunity.game_id * 37)
                + (candidate.opportunity.player_id * 13)
                + (index + 1) * 97
                for index, candidate in enumerate(combo)
            )
        )
        rng = np.random.default_rng(seed)
        samples = rng.multivariate_normal(np.zeros(len(combo), dtype=float), correlation_matrix, size=12000)
        hits = (samples <= thresholds).all(axis=1)
        simulated_probability = float(hits.mean())
        if len({candidate.opportunity.game_id for candidate in combo}) == len(combo):
            return float(np.clip(0.5 * simulated_probability + 0.5 * independent_probability, 1e-6, 0.995))
        return float(np.clip(simulated_probability, 1e-6, 0.995))

    def _dependency_matrix(self, combo: list[_CandidateLeg]) -> np.ndarray:
        size = len(combo)
        matrix = np.eye(size, dtype=float)
        for left_index in range(size):
            for right_index in range(left_index + 1, size):
                correlation = self._pairwise_correlation(combo[left_index], combo[right_index])
                matrix[left_index, right_index] = correlation
                matrix[right_index, left_index] = correlation
        return _nearest_positive_definite(matrix)

    def _pairwise_correlation(self, left: _CandidateLeg, right: _CandidateLeg) -> float:
        same_game = left.opportunity.game_id == right.opportunity.game_id
        same_player = left.opportunity.player_id == right.opportunity.player_id
        same_team_direction = left.quote.recommended_side == right.quote.recommended_side
        same_market = left.opportunity.market_key == right.opportunity.market_key
        market_pair = tuple(sorted((left.opportunity.market_key, right.opportunity.market_key)))
        correlation = 0.0
        if same_game:
            correlation += 0.04 if same_team_direction else 0.02
        if same_player:
            correlation += 0.18
        if same_market and same_game:
            correlation += 0.05
        if market_pair in {("assists", "points"), ("points", "pra"), ("assists", "pra"), ("pra", "rebounds")}:
            correlation += 0.10 if same_team_direction else -0.03
        if market_pair in {("points", "turnovers"), ("assists", "turnovers")}:
            correlation += 0.06 if same_team_direction else -0.04
        if market_pair == ("rebounds", "threes") and same_game:
            correlation += 0.03 if same_team_direction else 0.0
        if market_pair == ("assists", "threes") and same_game:
            correlation += 0.05 if same_team_direction else -0.02
        if same_player and "pra" in market_pair:
            correlation += 0.08
        if same_player and not same_team_direction:
            correlation -= 0.08
        return float(np.clip(correlation, -0.20, 0.45))

    def _game_labels(self, target_date: date) -> dict[int, str]:
        sync_live_games_from_nba_api(self._session, target_date)
        home_team = aliased(Team)
        away_team = aliased(Team)
        rows = self._session.execute(
            select(Game.game_id, Game.game_date, Game.start_time, home_team.abbreviation, away_team.abbreviation)
            .join(home_team, Game.home_team_id == home_team.team_id)
            .join(away_team, Game.away_team_id == away_team.team_id)
            .where(Game.status == "scheduled")
        ).all()
        return {
            game_id: f"{away} @ {home}"
            for game_id, game_date, start_time, home, away in rows
            if matches_board_date(game_date, start_time, target_date)
        }

    def _single_leg_expected_profit(self, candidate: _CandidateLeg) -> float:
        return candidate.quote.hit_probability * self._american_to_decimal(candidate.american_odds) - 1.0

    def _single_leg_edge(self, candidate: _CandidateLeg) -> float:
        implied_probability = 1.0 / self._american_to_decimal(candidate.american_odds)
        return candidate.quote.hit_probability - implied_probability

    def _is_candidate_viable(self, candidate: _CandidateLeg) -> bool:
        hit_probability = candidate.quote.hit_probability
        edge = self._single_leg_edge(candidate)
        expected_profit = self._single_leg_expected_profit(candidate)
        if hit_probability < 0.24:
            return False
        if edge <= 0.0 and expected_profit <= 0.0:
            return False
        if candidate.quote.is_alternate_line:
            if hit_probability < 0.28:
                return False
            if candidate.american_odds >= 400 and hit_probability < 0.31:
                return False
            if candidate.american_odds >= 800 and hit_probability < 0.35:
                return False
        if candidate.american_odds >= 500 and expected_profit < 0.06:
            return False
        if hit_probability >= 0.60:
            return edge > 0.01
        if hit_probability >= 0.52:
            return edge > 0.02 and expected_profit > 0.02
        return expected_profit > 0.05 and edge > 0.03

    def _american_to_probability(self, american_odds: int) -> float:
        if american_odds > 0:
            return 100.0 / (american_odds + 100.0)
        return abs(american_odds) / (abs(american_odds) + 100.0)

    def _american_to_decimal(self, american_odds: int) -> float:
        if american_odds > 0:
            return 1.0 + american_odds / 100.0
        return 1.0 + 100.0 / abs(american_odds)

    def _decimal_to_american(self, decimal_odds: float) -> int:
        if decimal_odds <= 2.0:
            return int(round(-100.0 / max(decimal_odds - 1.0, 1e-6)))
        return int(round((decimal_odds - 1.0) * 100.0))

    def _is_fully_verified_quote(self, quote: SportsbookQuote) -> bool:
        return quote.is_live_quote and quote.verification_status.lower() == "provider_live"


class SameGameParlayService(_BaseParlayService):
    def suggest_by_sportsbook_leg_count_and_game(
        self,
        target_date: date | None = None,
        *,
        min_legs: int = 3,
        max_legs: int = 20,
        top_per_game: int = 5,
        candidate_limit_per_book: int = 40,
    ) -> dict[str, dict[int, dict[int, list[ParlayRecommendation]]]]:
        target_date = target_date or date.today()
        candidates_by_group, game_labels = self._candidate_groups(target_date)
        sections: dict[str, dict[int, dict[int, list[ParlayRecommendation]]]] = {}
        for leg_count in range(min_legs, max_legs + 1):
            parlays = self._candidate_parlays_for_leg_count(
                candidates_by_group=candidates_by_group,
                game_labels=game_labels,
                leg_count=leg_count,
                top_per_leg_count=top_per_game,
                candidate_limit_per_book=candidate_limit_per_book,
                minimum_distinct_games=1,
            )
            grouped: dict[str, dict[int, list[ParlayRecommendation]]] = {}
            for parlay in parlays:
                grouped.setdefault(parlay.sportsbook_key, {}).setdefault(parlay.game_id, []).append(parlay)
            for sportsbook_key, game_sections in grouped.items():
                ranked_games: dict[int, list[ParlayRecommendation]] = {}
                for game_id, game_parlays in sorted(
                    game_sections.items(),
                    key=lambda item: (
                        -max(row.expected_profit_per_unit for row in item[1]),
                        game_labels.get(item[0], f"Game {item[0]}"),
                    ),
                ):
                    ranked = self._rank_parlays(game_parlays, top_per_game)
                    if ranked:
                        ranked_games[game_id] = ranked
                if ranked_games:
                    sections.setdefault(sportsbook_key, {})[leg_count] = ranked_games
        return dict(
            sorted(
                sections.items(),
                key=lambda item: self._sportsbook_game_sort_key(item[1], item[0]),
            )
        )

    def _candidate_groups(
        self,
        target_date: date,
    ) -> tuple[dict[tuple[object, ...], list[_CandidateLeg]], dict[int, str]]:
        candidates, game_labels = self._candidate_inventory(target_date)
        grouped: dict[tuple[object, ...], list[_CandidateLeg]] = {}
        for candidate in candidates:
            grouped.setdefault((candidate.opportunity.game_id, candidate.quote.sportsbook_key), []).append(candidate)
        return grouped, game_labels

    def _candidate_parlays_for_leg_count(
        self,
        *,
        candidates_by_group: dict[tuple[object, ...], list[_CandidateLeg]],
        game_labels: dict[int, str],
        leg_count: int,
        top_per_leg_count: int,
        candidate_limit_per_book: int,
        minimum_distinct_games: int,
    ) -> list[ParlayRecommendation]:
        candidates_for_leg_count: list[ParlayRecommendation] = []
        combo_signatures: set[tuple[tuple[int, int, str, float], ...]] = set()
        effective_candidate_limit = max(candidate_limit_per_book, top_per_leg_count * 6)
        for (game_id, sportsbook_key), candidates in candidates_by_group.items():
            candidate_orders = self._candidate_orders(candidates, effective_candidate_limit)
            if not candidate_orders:
                continue
            sportsbook_name = candidate_orders[0][0].quote.sportsbook_name
            sportsbook_icon = candidate_orders[0][0].quote.icon
            for ordered_candidates in candidate_orders:
                if len(ordered_candidates) < leg_count:
                    continue
                for offset in range(min(top_per_leg_count * 3, len(ordered_candidates) - leg_count + 1)):
                    combo = self._compose_candidate_combo(
                        ordered_candidates[offset:],
                        leg_count,
                        maximize_games=False,
                        minimum_distinct_games=1,
                    )
                    if len(combo) < leg_count:
                        continue
                    combo_signature = tuple(sorted(self._candidate_key(candidate) for candidate in combo))
                    if combo_signature in combo_signatures:
                        continue
                    combo_signatures.add(combo_signature)
                    parlay = self._build_parlay(
                        combo=combo,
                        rank=0,
                        sportsbook_key=str(sportsbook_key),
                        sportsbook_name=sportsbook_name,
                        sportsbook_icon=sportsbook_icon,
                        game_labels=game_labels,
                    )
                    if parlay.game_count != 1:
                        continue
                    if parlay.game_id != int(game_id):
                        continue
                    if parlay.expected_profit_per_unit <= 0:
                        continue
                    candidates_for_leg_count.append(parlay)
        return candidates_for_leg_count

    def _sportsbook_game_sort_key(
        self,
        sections: dict[int, dict[int, list[ParlayRecommendation]]],
        sportsbook_key: str,
    ) -> tuple[str, str]:
        for leg_count in sorted(sections):
            game_sections = sections[leg_count]
            for parlays in game_sections.values():
                if parlays:
                    return parlays[0].sportsbook_name, sportsbook_key
        return sportsbook_key, sportsbook_key


class MultiGameParlayService(_BaseParlayService):
    def _rank_parlays(
        self,
        parlays: list[ParlayRecommendation],
        limit: int,
    ) -> list[ParlayRecommendation]:
        ordered = sorted(
            parlays,
            key=lambda row: (
                -row.game_count,
                -row.expected_profit_per_unit,
                -row.edge,
                -len({leg.market_key for leg in row.legs}),
                -row.weakest_leg_hit_probability,
                row.sportsbook_name,
                row.matchup,
            ),
        )
        ranked: list[ParlayRecommendation] = []
        seen_signatures: set[tuple[str, ...]] = set()
        for row in ordered:
            signature = (
                row.sportsbook_key,
                *sorted(
                    f"{leg.game_id}|{leg.player_name}|{leg.market_key}|{leg.recommended_side}|{leg.line_value}"
                    for leg in row.legs
                ),
            )
            if signature in seen_signatures:
                continue
            seen_signatures.add(signature)
            ranked.append(
                ParlayRecommendation(
                    rank=len(ranked) + 1,
                    game_id=row.game_id,
                    matchup=row.matchup,
                    sportsbook_key=row.sportsbook_key,
                    sportsbook_name=row.sportsbook_name,
                    sportsbook_icon=row.sportsbook_icon,
                    leg_count=row.leg_count,
                    game_count=row.game_count,
                    game_ids=row.game_ids,
                    game_labels=row.game_labels,
                    joint_probability=row.joint_probability,
                    combined_decimal_odds=row.combined_decimal_odds,
                    combined_american_odds=row.combined_american_odds,
                    expected_profit_per_unit=row.expected_profit_per_unit,
                    implied_probability=row.implied_probability,
                    edge=row.edge,
                    all_legs_live=row.all_legs_live,
                    verification_status=row.verification_status,
                    odds_source_provider=row.odds_source_provider,
                    correlation_penalty=row.correlation_penalty,
                    average_leg_hit_probability=row.average_leg_hit_probability,
                    weakest_leg_hit_probability=row.weakest_leg_hit_probability,
                    legs=row.legs,
                )
            )
            if len(ranked) >= limit:
                break
        return ranked

    def _candidate_groups(
        self,
        target_date: date,
    ) -> tuple[dict[tuple[object, ...], list[_CandidateLeg]], dict[int, str]]:
        candidates, game_labels = self._candidate_inventory(target_date)
        grouped: dict[tuple[object, ...], list[_CandidateLeg]] = {}
        for candidate in candidates:
            grouped.setdefault((candidate.quote.sportsbook_key,), []).append(candidate)
        return grouped, game_labels

    def _candidate_parlays_for_leg_count(
        self,
        *,
        candidates_by_group: dict[tuple[object, ...], list[_CandidateLeg]],
        game_labels: dict[int, str],
        leg_count: int,
        top_per_leg_count: int,
        candidate_limit_per_book: int,
        minimum_distinct_games: int,
    ) -> list[ParlayRecommendation]:
        candidates_for_leg_count: list[ParlayRecommendation] = []
        combo_signatures: set[tuple[tuple[int, int, str, float], ...]] = set()
        effective_candidate_limit = max(candidate_limit_per_book, top_per_leg_count * 6)
        for (sportsbook_key,), candidates in candidates_by_group.items():
            candidate_orders = self._candidate_orders(candidates, effective_candidate_limit)
            if not candidate_orders:
                continue
            sportsbook_name = candidate_orders[0][0].quote.sportsbook_name
            sportsbook_icon = candidate_orders[0][0].quote.icon
            for ordered_candidates in candidate_orders:
                if len(ordered_candidates) < leg_count:
                    continue
                for offset in range(min(top_per_leg_count * 4, len(ordered_candidates) - leg_count + 1)):
                    combo = self._compose_candidate_combo(
                        ordered_candidates[offset:],
                        leg_count,
                        maximize_games=True,
                        minimum_distinct_games=minimum_distinct_games,
                    )
                    if len(combo) < leg_count:
                        continue
                    combo_signature = tuple(sorted(self._candidate_key(candidate) for candidate in combo))
                    if combo_signature in combo_signatures:
                        continue
                    combo_signatures.add(combo_signature)
                    parlay = self._build_parlay(
                        combo=combo,
                        rank=0,
                        sportsbook_key=str(sportsbook_key),
                        sportsbook_name=sportsbook_name,
                        sportsbook_icon=sportsbook_icon,
                        game_labels=game_labels,
                    )
                    if parlay.game_count < minimum_distinct_games:
                        continue
                    if parlay.expected_profit_per_unit <= 0:
                        continue
                    candidates_for_leg_count.append(parlay)
        return candidates_for_leg_count


@dataclass(frozen=True)
class _CandidateLeg:
    opportunity: PropOpportunity
    quote: SportsbookQuote
    american_odds: int


def _effective_hit_probability(candidate: _CandidateLeg) -> float:
    """Use the volatility-adjusted over probability when the opportunity carries one."""
    adjusted = getattr(candidate.opportunity, "adjusted_over_probability", None)
    if adjusted is not None:
        return float(adjusted)
    return float(candidate.quote.hit_probability)


def _nearest_positive_definite(matrix: np.ndarray) -> np.ndarray:
    symmetric = (matrix + matrix.T) / 2.0
    eigenvalues, eigenvectors = np.linalg.eigh(symmetric)
    clipped = np.clip(eigenvalues, 1e-6, None)
    positive = eigenvectors @ np.diag(clipped) @ eigenvectors.T
    diagonal = np.sqrt(np.clip(np.diag(positive), 1e-6, None))
    normalized = positive / np.outer(diagonal, diagonal)
    np.fill_diagonal(normalized, 1.0)
    return normalized

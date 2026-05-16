from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from statistics import median
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, aliased

from app.core.icons import player_icon, sportsbook_icon
from app.evaluation.prop_pricer import price_prop
from app.models.all import Game, LineSnapshot, Player, Prediction, PropMarket, Sportsbook, Team
from app.services.board_date import to_local_board_date
from app.services.live_games import sync_live_games_from_nba_api


@dataclass(frozen=True)
class SportsbookQuote:
    game_id: int
    sportsbook_key: str
    sportsbook_name: str
    icon: str
    market_key: str
    line_value: float
    over_odds: int | None
    under_odds: int | None
    timestamp: str
    is_live_quote: bool
    verification_status: str
    odds_source_provider: str
    over_probability: float
    under_probability: float
    push_probability: float
    calibrated_over_probability: float
    calibrated_under_probability: float
    recommended_side: str
    hit_probability: float
    no_vig_market_probability: float = 0.5
    source_market_key: str = ""
    is_alternate_line: bool = False


@dataclass(frozen=True)
class PropOpportunity:
    rank: int
    game_id: int
    player_id: int
    player_name: str
    player_icon: str
    market_key: str
    consensus_line: float
    projected_mean: float
    recommended_side: str
    hit_probability: float
    likelihood_score: int
    calibrated_over_probability: float
    sportsbooks_summary: str
    top_features: list[str]
    quotes: list[SportsbookQuote]
    projected_variance: float | None = None
    confidence_interval_low: float | None = None
    confidence_interval_high: float | None = None
    predicted_at: str | None = None
    data_sufficiency_tier: str = "A"
    data_confidence_score: float = 1.0
    player_team_abbreviation: str | None = None
    player_position: str | None = None
    game_label: str | None = None
    game_start_time: str | None = None
    percentile_25: float = 0.0
    percentile_75: float = 0.0
    dnp_risk: float = 0.0
    boom_probability: float = 0.0
    bust_probability: float = 0.0
    availability_branches: int = 1
    volatility_coefficient: float = 0.0
    volatility_tier: str = "low"
    adjusted_over_probability: float | None = None


class PropAnalysisService:
    def __init__(self, session: Session) -> None:
        self._session = session

    def top_opportunities(self, limit: int | None = None, target_date: date | None = None) -> list[PropOpportunity]:
        target_date = target_date or date.today()
        opportunities = self.all_opportunities(target_date)
        ranked = []
        for index, row in enumerate(opportunities if limit is None else opportunities[:limit], start=1):
            ranked.append(
                PropOpportunity(
                    rank=index,
                    game_id=row.game_id,
                    player_id=row.player_id,
                    player_name=row.player_name,
                    player_icon=row.player_icon,
                    market_key=row.market_key,
                    consensus_line=row.consensus_line,
                    projected_mean=row.projected_mean,
                    recommended_side=row.recommended_side,
                    hit_probability=row.hit_probability,
                    likelihood_score=row.likelihood_score,
                    calibrated_over_probability=row.calibrated_over_probability,
                    sportsbooks_summary=row.sportsbooks_summary,
                    top_features=row.top_features,
                    quotes=row.quotes,
                    projected_variance=row.projected_variance,
                    confidence_interval_low=row.confidence_interval_low,
                    confidence_interval_high=row.confidence_interval_high,
                    predicted_at=row.predicted_at,
                    data_sufficiency_tier=row.data_sufficiency_tier,
                    data_confidence_score=row.data_confidence_score,
                    player_team_abbreviation=row.player_team_abbreviation,
                    player_position=row.player_position,
                    game_label=row.game_label,
                    game_start_time=row.game_start_time,
                    percentile_25=row.percentile_25,
                    percentile_75=row.percentile_75,
                    dnp_risk=row.dnp_risk,
                    boom_probability=row.boom_probability,
                    bust_probability=row.bust_probability,
                    availability_branches=row.availability_branches,
                )
            )
        return ranked

    def all_opportunities(self, target_date: date | None = None) -> list[PropOpportunity]:
        target_date = target_date or date.today()
        sync_live_games_from_nba_api(self._session, target_date)
        grouped_predictions = self._latest_prediction_quotes(target_date)
        opportunities: list[PropOpportunity] = []

        for grouped_rows in grouped_predictions.values():
            quotes = [self._prediction_row_to_quote(row) for row in grouped_rows]
            if not quotes:
                continue
            best_row, best_quote = max(
                zip(grouped_rows, quotes, strict=False),
                key=lambda item: (
                    item[1].hit_probability,
                    item[0].projected_mean,
                    -abs(item[1].line_value),
                ),
            )
            line_values = [quote.line_value for quote in quotes]
            hit_probability = best_quote.hit_probability
            likelihood_score = min(int(hit_probability * 100), 99)
            sportsbooks_summary = " | ".join(
                [
                    (
                        f"{quote.icon} {quote.sportsbook_name} "
                        f"{quote.line_value:.1f} {quote.recommended_side} {quote.hit_probability * 100:.1f}% "
                        f"O:{quote.over_odds or '-'} U:{quote.under_odds or '-'}"
                    )
                    for quote in quotes
                ]
            )
            opportunities.append(
                PropOpportunity(
                    rank=0,
                    game_id=best_quote.game_id,
                    player_id=best_row.player_id,
                    player_name=best_row.player_name,
                    player_icon=player_icon(best_row.player_name),
                    market_key=best_row.market_key,
                    consensus_line=float(median(line_values)),
                    projected_mean=best_row.projected_mean,
                    recommended_side=best_quote.recommended_side,
                    hit_probability=hit_probability,
                    likelihood_score=likelihood_score,
                    calibrated_over_probability=best_quote.calibrated_over_probability,
                    sportsbooks_summary=sportsbooks_summary,
                    top_features=best_row.top_features,
                    quotes=sorted(quotes, key=lambda quote: (quote.hit_probability, quote.sportsbook_name), reverse=True),
                    projected_variance=best_row.projected_variance,
                    confidence_interval_low=best_row.confidence_interval_low,
                    confidence_interval_high=best_row.confidence_interval_high,
                    predicted_at=best_row.predicted_at,
                    data_sufficiency_tier=best_row.data_sufficiency_tier,
                    data_confidence_score=best_row.data_confidence_score,
                    player_team_abbreviation=best_row.player_team_abbreviation,
                    player_position=best_row.player_position,
                    game_label=best_row.game_label,
                    game_start_time=best_row.game_start_time,
                    percentile_25=best_row.percentile_25,
                    percentile_75=best_row.percentile_75,
                    dnp_risk=best_row.dnp_risk,
                    boom_probability=best_row.boom_probability,
                    bust_probability=best_row.bust_probability,
                    availability_branches=best_row.availability_branches,
                )
            )

        opportunities.sort(key=lambda item: (item.likelihood_score, item.hit_probability, item.projected_mean), reverse=True)
        return opportunities

    def quotes_for_player(self, player_id: int, target_date: date | None = None) -> list[SportsbookQuote]:
        target_date = target_date or date.today()
        sync_live_games_from_nba_api(self._session, target_date)
        quotes = self._latest_prediction_quotes(target_date)
        player_quotes = []
        for (_, _, _, row_player_id, _, _), entries in quotes.items():
            if row_player_id != player_id:
                continue
            player_quotes.extend(self._prediction_row_to_quote(entry) for entry in entries)
        return sorted(player_quotes, key=lambda quote: (quote.sportsbook_name, quote.timestamp))

    def _latest_prediction_quotes(
        self,
        target_date: date | None,
    ) -> dict[tuple[date, int, int, int, str, float], list[_QuotePredictionRow]]:
        home_team = aliased(Team)
        away_team = aliased(Team)
        player_team = aliased(Team)
        query = (
            select(
                Prediction,
                Player.full_name,
                Player.position,
                player_team.abbreviation,
                PropMarket.key,
                Game.game_date,
                Game.start_time,
                home_team.abbreviation,
                away_team.abbreviation,
                Game.home_team_id,
                Game.away_team_id,
                LineSnapshot.line_value,
                LineSnapshot.over_odds,
                LineSnapshot.under_odds,
                LineSnapshot.timestamp,
                LineSnapshot.meta,
                Sportsbook.key,
                Sportsbook.display_name,
            )
            .join(Player, Prediction.player_id == Player.player_id)
            .join(player_team, Player.team_id == player_team.team_id, isouter=True)
            .join(PropMarket, Prediction.market_id == PropMarket.market_id)
            .join(Game, Prediction.game_id == Game.game_id)
            .join(home_team, Game.home_team_id == home_team.team_id)
            .join(away_team, Game.away_team_id == away_team.team_id)
            .join(LineSnapshot, Prediction.line_snapshot_id == LineSnapshot.snapshot_id)
            .join(Sportsbook, LineSnapshot.sportsbook_id == Sportsbook.sportsbook_id)
            .where(Game.status == "scheduled")
            .order_by(Prediction.predicted_at.desc(), LineSnapshot.timestamp.desc())
        )
        rows = self._session.execute(query).all()
        latest: dict[tuple[date, int, int, int, str, str, float], _QuotePredictionRow] = {}
        for (
            prediction,
            player_name,
            player_position,
            player_team_abbreviation,
            market_key,
            game_date,
            start_time,
            home_abbreviation,
            away_abbreviation,
            home_team_id,
            away_team_id,
            line_value,
            over_odds,
            under_odds,
            timestamp,
            snapshot_meta,
            sportsbook_key,
            sportsbook_name,
        ) in rows:
            board_date = to_local_board_date(game_date, start_time)
            if target_date is not None and board_date != target_date:
                continue
            snapshot_payload = _coerce_snapshot_meta(snapshot_meta)
            if not _is_prediction_snapshot_usable(snapshot_payload, over_odds, under_odds):
                continue
            push_probability = _push_probability(prediction.over_probability, prediction.under_probability)
            calibrated_under = _calibrated_under_probability(
                prediction.calibration_adjusted_probability,
                push_probability,
            )
            recommendation = _quote_recommendation(
                over_odds=over_odds,
                under_odds=under_odds,
                calibrated_over_probability=prediction.calibration_adjusted_probability,
                calibrated_under_probability=calibrated_under,
            )
            if recommendation is None:
                continue
            recommended_side, hit_probability, no_vig_market_probability = recommendation
            key = (
                board_date,
                int(home_team_id),
                int(away_team_id),
                prediction.player_id,
                market_key,
                str(sportsbook_key),
                float(line_value),
            )
            attr = _coerce_feature_summary(prediction.feature_attribution_summary)
            candidate = _QuotePredictionRow(
                game_id=prediction.game_id,
                player_id=prediction.player_id,
                player_name=player_name,
                player_team_abbreviation=str(player_team_abbreviation) if player_team_abbreviation else None,
                player_position=str(player_position) if player_position else None,
                market_key=market_key,
                game_label=f"{away_abbreviation} @ {home_abbreviation}",
                game_start_time=start_time.isoformat() if start_time is not None else None,
                projected_mean=prediction.projected_mean,
                line_value=float(line_value),
                over_odds=int(over_odds) if over_odds is not None else None,
                under_odds=int(under_odds) if under_odds is not None else None,
                timestamp=timestamp.isoformat(),
                sportsbook_key=str(sportsbook_key),
                sportsbook_name=str(sportsbook_name),
                is_live_quote=bool(snapshot_payload.get("is_live_quote", False)),
                is_alternate_line=bool(snapshot_payload.get("is_alternate_line", False)),
                source_market_key=str(snapshot_payload.get("source_market_key", market_key)),
                verification_status=str(snapshot_payload.get("odds_verification_status", "unknown")),
                odds_source_provider=str(snapshot_payload.get("odds_source_provider", "unknown")),
                predicted_at=prediction.predicted_at.isoformat(),
                projected_variance=prediction.projected_variance,
                confidence_interval_low=prediction.confidence_interval_low,
                confidence_interval_high=prediction.confidence_interval_high,
                over_probability=prediction.over_probability,
                under_probability=prediction.under_probability,
                push_probability=push_probability,
                calibration_adjusted_probability=prediction.calibration_adjusted_probability,
                calibrated_under_probability=calibrated_under,
                recommended_side=recommended_side,
                hit_probability=hit_probability,
                no_vig_market_probability=no_vig_market_probability,
                top_features=list(
                    attr.get(
                        "signal_summary",
                        attr.get("top_features", []),
                    )
                ),
                data_sufficiency_tier=str(
                    attr.get("data_sufficiency_tier", "A")
                ),
                data_confidence_score=float(
                    attr.get("data_confidence_score", 1.0)
                ),
                percentile_25=_float_attr(attr, "percentile_25"),
                percentile_75=_float_attr(attr, "percentile_75"),
                dnp_risk=_float_attr(attr, "dnp_risk"),
                boom_probability=_float_attr(attr, "boom_probability"),
                bust_probability=_float_attr(attr, "bust_probability"),
                availability_branches=_int_attr(attr, "availability_branches", 1),
            )
            existing = latest.get(key)
            if existing is None or (candidate.predicted_at, candidate.timestamp) > (existing.predicted_at, existing.timestamp):
                latest[key] = candidate
        line_counts: dict[tuple[date, int, int, int, str, str], set[float]] = {}
        for (
            board_date,
            home_team_id,
            away_team_id,
            player_id,
            market_key,
            sportsbook_key,
            line_value,
        ) in latest:
            line_counts.setdefault(
                (board_date, home_team_id, away_team_id, player_id, market_key, sportsbook_key),
                set(),
            ).add(line_value)
        grouped: dict[tuple[date, int, int, int, str, float], list[_QuotePredictionRow]] = {}
        for (
            board_date,
            home_team_id,
            away_team_id,
            player_id,
            market_key,
            sportsbook_key,
            line_value,
        ), row in latest.items():
            has_multiple_lines_for_book = len(
                line_counts[(board_date, home_team_id, away_team_id, player_id, market_key, sportsbook_key)]
            ) > 1
            line_group_key = line_value if row.is_alternate_line or has_multiple_lines_for_book else -1.0
            grouped.setdefault(
                (board_date, home_team_id, away_team_id, player_id, market_key, line_group_key),
                [],
            ).append(row)
        return grouped

    def _prediction_row_to_quote(self, row: _QuotePredictionRow) -> SportsbookQuote:
        return SportsbookQuote(
            game_id=row.game_id,
            sportsbook_key=row.sportsbook_key,
            sportsbook_name=row.sportsbook_name,
            icon=sportsbook_icon(row.sportsbook_key),
            market_key=row.market_key,
            line_value=row.line_value,
            over_odds=row.over_odds,
            under_odds=row.under_odds,
            timestamp=row.timestamp,
            is_live_quote=row.is_live_quote,
            verification_status=row.verification_status,
            odds_source_provider=row.odds_source_provider,
            over_probability=row.over_probability,
            under_probability=row.under_probability,
            push_probability=row.push_probability,
            calibrated_over_probability=row.calibration_adjusted_probability,
            calibrated_under_probability=row.calibrated_under_probability,
            recommended_side=row.recommended_side,
            hit_probability=row.hit_probability,
            no_vig_market_probability=row.no_vig_market_probability,
            source_market_key=row.source_market_key,
            is_alternate_line=row.is_alternate_line,
        )


def _is_prediction_snapshot_usable(
    snapshot_meta: dict[str, object],
    over_odds: int | None,
    under_odds: int | None,
) -> bool:
    if over_odds is None and under_odds is None:
        return False
    return (
        bool(snapshot_meta.get("is_live_quote", False))
        and str(snapshot_meta.get("odds_verification_status", "")).lower() == "provider_live"
    )


@dataclass(frozen=True)
class _QuotePredictionRow:
    game_id: int
    player_id: int
    player_name: str
    player_team_abbreviation: str | None
    player_position: str | None
    market_key: str
    game_label: str
    game_start_time: str | None
    projected_mean: float
    line_value: float
    over_odds: int | None
    under_odds: int | None
    timestamp: str
    sportsbook_key: str
    sportsbook_name: str
    is_live_quote: bool
    is_alternate_line: bool
    source_market_key: str
    verification_status: str
    odds_source_provider: str
    predicted_at: str
    projected_variance: float
    confidence_interval_low: float
    confidence_interval_high: float
    over_probability: float
    under_probability: float
    push_probability: float
    calibration_adjusted_probability: float
    calibrated_under_probability: float
    recommended_side: str
    hit_probability: float
    no_vig_market_probability: float
    top_features: list[str]
    data_sufficiency_tier: str
    data_confidence_score: float
    percentile_25: float = 0.0
    percentile_75: float = 0.0
    dnp_risk: float = 0.0
    boom_probability: float = 0.0
    bust_probability: float = 0.0
    availability_branches: int = 1


def _push_probability(over_probability: float, under_probability: float) -> float:
    return max(0.0, 1.0 - float(over_probability) - float(under_probability))


def _calibrated_under_probability(
    calibrated_over_probability: float,
    push_probability: float,
) -> float:
    return max(0.0, 1.0 - float(calibrated_over_probability) - float(push_probability))


def _coerce_snapshot_meta(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return {}
        if isinstance(parsed, dict):
            return parsed
    return {}


def _coerce_feature_summary(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _float_attr(payload: dict[str, Any], key: str, default: float = 0.0) -> float:
    try:
        return float(payload.get(key, default))
    except (TypeError, ValueError):
        return default


def _int_attr(payload: dict[str, Any], key: str, default: int = 0) -> int:
    try:
        return int(payload.get(key, default))
    except (TypeError, ValueError):
        return default


def _quote_recommendation(
    *,
    over_odds: int | None,
    under_odds: int | None,
    calibrated_over_probability: float,
    calibrated_under_probability: float,
) -> tuple[str, float, float] | None:
    if over_odds is None and under_odds is None:
        return None
    decision = price_prop(
        prediction={
            "calibration_adjusted_probability": float(calibrated_over_probability),
            "under_probability": float(calibrated_under_probability),
        },
        line_snapshot={
            "market_key": "",
            "line_value": 0.0,
            "over_odds": over_odds,
            "under_odds": under_odds,
        },
    )
    return decision.recommendation, decision.model_prob, decision.no_vig_market_prob

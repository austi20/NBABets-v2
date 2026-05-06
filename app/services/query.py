from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from sqlalchemy import func, select
from sqlalchemy.orm import Session, aliased

from app.models.all import Game, LineSnapshot, Player, Prediction, PropMarket, Sportsbook, Team
from app.schemas.api import CalibrationMetricsResponse, PlayerSearchResult, UpcomingGameResult, UpcomingPropResult
from app.schemas.domain import PropPrediction
from app.services.board_date import to_local_board_date
from app.services.live_games import sync_live_games_from_nba_api


@dataclass(frozen=True)
class BoardAvailability:
    board_date: date
    scheduled_games: int
    live_games: int
    final_games: int

    @property
    def has_pregame_options(self) -> bool:
        return self.scheduled_games > 0


class QueryService:
    def __init__(self, session: Session) -> None:
        self._session = session

    def resolve_board_date(self, preferred_date: date | None = None) -> date | None:
        preferred_date = preferred_date or date.today()
        sync_live_games_from_nba_api(self._session, preferred_date)
        rows = self._session.execute(
            select(Game.game_date, Game.start_time, Game.status)
            .where(Game.status == "scheduled")
            .order_by(Game.game_date, Game.start_time)
        ).all()
        board_dates = sorted({to_local_board_date(game_date, start_time) for game_date, start_time, _ in rows})
        if preferred_date in board_dates:
            return preferred_date
        future_dates = [value for value in board_dates if value > preferred_date]
        if future_dates:
            return future_dates[0]
        if board_dates:
            return board_dates[-1]

        prediction_rows = self._session.execute(
            select(Game.game_date, Game.start_time)
            .join(Prediction, Prediction.game_id == Game.game_id)
            .order_by(Game.game_date.desc(), Game.start_time.desc())
        ).all()
        if prediction_rows:
            return to_local_board_date(prediction_rows[0][0], prediction_rows[0][1])
        return None

    def search_players(self, query: str, limit: int = 20) -> list[PlayerSearchResult]:
        team = aliased(Team)
        rows = self._session.execute(
            select(Player, team.abbreviation)
            .join(team, Player.team_id == team.team_id, isouter=True)
            .where(Player.full_name.ilike(f"%{query}%"))
            .order_by(Player.full_name)
            .limit(limit)
        ).all()
        return [
            PlayerSearchResult(
                player_id=player.player_id,
                full_name=player.full_name,
                team_abbreviation=team_abbreviation,
                position=player.position,
                status=player.status,
            )
            for player, team_abbreviation in rows
        ]

    def upcoming_games(self) -> list[UpcomingGameResult]:
        sync_live_games_from_nba_api(self._session, date.today())
        home_team = aliased(Team)
        away_team = aliased(Team)
        rows = self._session.execute(
            select(Game, home_team.abbreviation, away_team.abbreviation)
            .join(home_team, Game.home_team_id == home_team.team_id)
            .join(away_team, Game.away_team_id == away_team.team_id)
            .where(Game.status == "scheduled")
            .order_by(Game.start_time)
        ).all()
        deduped: dict[tuple[object, ...], UpcomingGameResult] = {}
        for game, home, away in rows:
            key = (game.game_date, home, away)
            deduped.setdefault(
                key,
                UpcomingGameResult(
                    game_id=game.game_id,
                    game_date=game.game_date,
                    start_time=game.start_time,
                    home_team=home,
                    away_team=away,
                    spread=game.spread,
                    total=game.total,
                ),
            )
        return list(deduped.values())

    def upcoming_props(self) -> list[UpcomingPropResult]:
        sync_live_games_from_nba_api(self._session, date.today())
        rows = self._session.execute(
            select(
                LineSnapshot,
                Player.full_name,
                PropMarket.key,
                Sportsbook.display_name,
                Game.game_date,
                Game.home_team_id,
                Game.away_team_id,
            )
            .join(Player, LineSnapshot.player_id == Player.player_id)
            .join(PropMarket, LineSnapshot.market_id == PropMarket.market_id)
            .join(Sportsbook, LineSnapshot.sportsbook_id == Sportsbook.sportsbook_id)
            .join(Game, LineSnapshot.game_id == Game.game_id)
            .where(Game.status == "scheduled")
            .order_by(LineSnapshot.timestamp.desc())
        ).all()
        seen: set[tuple[object, ...]] = set()
        results: list[UpcomingPropResult] = []
        for snapshot, player_name, market_key, sportsbook, game_date, home_team_id, away_team_id in rows:
            dedupe = (
                game_date,
                home_team_id,
                away_team_id,
                snapshot.player_id,
                snapshot.market_id,
                snapshot.sportsbook_id,
                snapshot.line_value,
            )
            if dedupe in seen:
                continue
            seen.add(dedupe)
            results.append(
                UpcomingPropResult(
                    game_id=snapshot.game_id,
                    player_id=snapshot.player_id,
                    player_name=player_name,
                    market_key=market_key,
                    sportsbook=sportsbook,
                    line_value=snapshot.line_value,
                    over_odds=snapshot.over_odds,
                    under_odds=snapshot.under_odds,
                    timestamp=snapshot.timestamp,
                )
            )
        return results

    def board_availability(self, target_date: date) -> BoardAvailability:
        sync_live_games_from_nba_api(self._session, target_date)
        rows = self._session.execute(
            select(Game.game_date, Game.start_time, Game.status)
            .where(Game.status != "superseded")
            .order_by(Game.game_date, Game.start_time)
        ).all()
        scheduled_games = 0
        live_games = 0
        final_games = 0
        for game_date, start_time, status in rows:
            if to_local_board_date(game_date, start_time) != target_date:
                continue
            normalized_status = str(status or "").strip().lower()
            if normalized_status == "scheduled":
                scheduled_games += 1
            elif normalized_status == "live":
                live_games += 1
            else:
                final_games += 1
        return BoardAvailability(
            board_date=target_date,
            scheduled_games=scheduled_games,
            live_games=live_games,
            final_games=final_games,
        )

    def predictions_for_player(self, player_id: int, target_date: date | None = None) -> list[PropPrediction]:
        effective_date = target_date or date.today()
        rows = self._session.execute(
            select(
                Prediction,
                Player.full_name,
                PropMarket.key,
                Game.game_date,
                Game.start_time,
                LineSnapshot.line_value,
            )
            .join(Player, Prediction.player_id == Player.player_id)
            .join(PropMarket, Prediction.market_id == PropMarket.market_id)
            .join(Game, Prediction.game_id == Game.game_id)
            .join(LineSnapshot, Prediction.line_snapshot_id == LineSnapshot.snapshot_id, isouter=True)
            .where(Prediction.player_id == player_id)
            .order_by(Prediction.predicted_at.desc())
        ).all()
        latest: dict[tuple[int, str, int], PropPrediction] = {}
        for prediction, player_name, market_key, game_date, start_time, sportsbook_line in rows:
            if to_local_board_date(game_date, start_time) != effective_date:
                continue
            key = (prediction.game_id, market_key, prediction.line_snapshot_id or 0)
            if key in latest:
                continue
            latest[key] = self._prediction_to_schema(
                prediction,
                player_name,
                market_key,
                float(sportsbook_line) if sportsbook_line is not None else 0.0,
            )
        return sorted(latest.values(), key=lambda item: (item.player_name, item.market_key))

    def predictions_for_game(self, game_id: int) -> list[PropPrediction]:
        rows = self._session.execute(
            select(Prediction, Player.full_name, PropMarket.key, LineSnapshot.line_value)
            .join(Player, Prediction.player_id == Player.player_id)
            .join(PropMarket, Prediction.market_id == PropMarket.market_id)
            .join(LineSnapshot, Prediction.line_snapshot_id == LineSnapshot.snapshot_id, isouter=True)
            .where(Prediction.game_id == game_id)
            .order_by(Prediction.predicted_at.desc())
        ).all()
        latest: dict[tuple[int, str, int], PropPrediction] = {}
        for prediction, player_name, market_key, sportsbook_line in rows:
            key = (prediction.player_id, market_key, prediction.line_snapshot_id or 0)
            if key in latest:
                continue
            latest[key] = self._prediction_to_schema(
                prediction,
                player_name,
                market_key,
                float(sportsbook_line) if sportsbook_line is not None else 0.0,
            )
        return sorted(
            latest.values(),
            key=lambda item: (item.calibrated_over_probability, item.sportsbook_line, item.player_name, item.market_key),
            reverse=True,
        )

    def calibration_metrics(self) -> list[CalibrationMetricsResponse]:
        rows = self._session.execute(
            select(
                PropMarket.key,
                func.avg(Prediction.calibration_adjusted_probability),
                func.avg(Prediction.over_probability),
            )
            .join(PropMarket, Prediction.market_id == PropMarket.market_id)
            .group_by(PropMarket.key)
            .order_by(PropMarket.key)
        ).all()
        return [
            CalibrationMetricsResponse(
                market_key=market_key,
                avg_calibrated_probability=float(avg_calibrated or 0.0),
                avg_raw_probability=float(avg_raw or 0.0),
            )
            for market_key, avg_calibrated, avg_raw in rows
        ]

    def odds_history(self, player_id: int, market_key: str) -> list[UpcomingPropResult]:
        rows = self._session.execute(
            select(LineSnapshot, Player.full_name, PropMarket.key, Sportsbook.display_name)
            .join(Player, LineSnapshot.player_id == Player.player_id)
            .join(PropMarket, LineSnapshot.market_id == PropMarket.market_id)
            .join(Sportsbook, LineSnapshot.sportsbook_id == Sportsbook.sportsbook_id)
            .where(LineSnapshot.player_id == player_id, PropMarket.key == market_key)
            .order_by(LineSnapshot.timestamp)
        ).all()
        return [
            UpcomingPropResult(
                game_id=snapshot.game_id,
                player_id=snapshot.player_id,
                player_name=player_name,
                market_key=resolved_market_key,
                sportsbook=sportsbook,
                line_value=snapshot.line_value,
                over_odds=snapshot.over_odds,
                under_odds=snapshot.under_odds,
                timestamp=snapshot.timestamp,
            )
            for snapshot, player_name, resolved_market_key, sportsbook in rows
        ]

    def _prediction_to_schema(
        self,
        prediction: Prediction,
        player_name: str,
        market_key: str,
        sportsbook_line: float,
    ) -> PropPrediction:
        top_features = prediction.feature_attribution_summary.get(
            "signal_summary",
            prediction.feature_attribution_summary.get("top_features", []),
        )
        return PropPrediction(
            player_id=prediction.player_id,
            player_name=player_name,
            game_id=prediction.game_id,
            market_key=market_key,
            sportsbook_line=sportsbook_line,
            projected_mean=prediction.projected_mean,
            projected_variance=prediction.projected_variance,
            projected_median=prediction.projected_median,
            over_probability=prediction.over_probability,
            under_probability=prediction.under_probability,
            calibrated_over_probability=prediction.calibration_adjusted_probability,
            percentile_10=prediction.confidence_interval_low,
            percentile_50=prediction.projected_median,
            percentile_90=prediction.confidence_interval_high,
            confidence_interval_low=prediction.confidence_interval_low,
            confidence_interval_high=prediction.confidence_interval_high,
            top_features=list(top_features),
            model_version="stored",
            feature_version="stored",
            data_freshness={"predicted_at": prediction.predicted_at},
            data_sufficiency_tier=str(
                prediction.feature_attribution_summary.get("data_sufficiency_tier", "A")
            ),
            data_confidence_score=float(
                prediction.feature_attribution_summary.get("data_confidence_score", 1.0)
            ),
        )

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import date, datetime, timedelta
from typing import Any

from dateutil.parser import isoparse
from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session

from app.models.all import (
    ClosingLine,
    Game,
    GamePlayerAvailability,
    InjuryReport,
    LineSnapshot,
    Player,
    PlayerGameLog,
    PropMarket,
    Season,
    Sportsbook,
    Team,
)
from app.schemas.domain import (
    GamePayload,
    InjuryPayload,
    LineSnapshotPayload,
    PlayerAvailabilityPayload,
    PlayerGameLogPayload,
    PlayerPayload,
    TeamPayload,
)
from app.services.agents.run_service import AgentRunService
from app.services.board_date import to_local_board_date
from app.services.deduplication import (
    dedupe_game_payloads,
    dedupe_injury_payloads,
    dedupe_line_snapshot_payloads,
    dedupe_player_game_log_payloads,
    dedupe_player_payloads,
    dedupe_team_payloads,
)
from app.services.name_matching import PlayerMatcher, normalize_name
from app.services.raw_payloads import RawPayloadService
from app.services.repositories import ReferenceRepository

logger = logging.getLogger(__name__)

DEFAULT_MARKETS = {
    "points": ("points", "Player Points"),
    "rebounds": ("rebounds", "Player Rebounds"),
    "assists": ("assists", "Player Assists"),
    "threes": ("threes", "Player Threes"),
    "turnovers": ("turnovers", "Player Turnovers"),
    "pra": ("pra", "Player Points + Rebounds + Assists"),
}


class IngestionService:
    def __init__(self, session: Session) -> None:
        self._session = session
        self._raw_payloads = RawPayloadService(session)
        self._references = ReferenceRepository(session)
        self._player_matcher = PlayerMatcher(session)

    def ensure_default_markets(self) -> None:
        for key, (stat_type, display_name) in DEFAULT_MARKETS.items():
            market = self._references.get_market_by_key(key)
            if market is None:
                self._session.add(
                    PropMarket(
                        key=key,
                        stat_type=stat_type,
                        display_name=display_name,
                        distribution_family="negative_binomial",
                    )
                )
        self._session.flush()

    def ingest_teams(self, payloads: Iterable[TeamPayload]) -> int:
        count = 0
        for payload in dedupe_team_payloads(payloads):
            team = self._references.get_team_by_abbreviation(payload.abbreviation)
            if team is None:
                team = Team(
                    provider_team_id=payload.provider_team_id,
                    abbreviation=payload.abbreviation,
                    name=payload.name,
                    city=payload.city,
                )
                self._session.add(team)
            else:
                team.provider_team_id = payload.provider_team_id
                team.name = payload.name
                team.city = payload.city
            count += 1
        self._session.flush()
        return count

    def ingest_players(
        self,
        payloads: Iterable[PlayerPayload],
        source_provider_name: str | None = None,
    ) -> int:
        count = 0
        for payload in dedupe_player_payloads(payloads):
            team = (
                self._references.get_team_by_abbreviation(payload.team_abbreviation)
                if payload.team_abbreviation
                else None
            )
            normalized_name = normalize_name(payload.full_name)
            team_id = team.team_id if team else None
            player = self._find_existing_player(
                provider_player_id=payload.provider_player_id,
                normalized_name=normalized_name,
                team_id=team_id,
            )
            if player is None:
                player = Player(
                    provider_player_id=payload.provider_player_id,
                    full_name=payload.full_name,
                    normalized_name=normalized_name,
                    team_id=team_id,
                    position=payload.position,
                    status=payload.status,
                    meta={},
                )
                self._session.add(player)
            self._merge_player_provider_identity(player, payload.provider_player_id, source_provider_name)
            player.full_name = payload.full_name
            player.normalized_name = normalized_name
            if team is not None:
                player.team_id = team_id
            if payload.position:
                player.position = payload.position
            if payload.status:
                player.status = payload.status
            count += 1
        self._session.flush()
        return count

    def ingest_games(self, payloads: Iterable[GamePayload]) -> int:
        count = 0
        for payload in dedupe_game_payloads(payloads):
            season = self._ensure_season(payload.season_code, payload.game_date)
            home_team = self._references.get_team_by_abbreviation(payload.home_team_abbreviation)
            away_team = self._references.get_team_by_abbreviation(payload.away_team_abbreviation)
            if home_team is None or away_team is None:
                continue
            game = self._find_existing_game(payload, home_team.team_id, away_team.team_id)
            if game is None:
                game = Game(
                    provider_game_id=payload.provider_game_id,
                    season_id=season.season_id if season else None,
                    game_date=payload.game_date,
                    start_time=payload.start_time,
                    home_team_id=home_team.team_id,
                    away_team_id=away_team.team_id,
                    spread=payload.spread,
                    total=payload.total,
                    status=payload.status,
                    meta=payload.meta,
                )
                self._session.add(game)
            else:
                self._merge_game_provider_identity(game, payload.provider_game_id, payload.meta)
                game.season_id = season.season_id if season else None
                game.game_date = payload.game_date
                game.start_time = payload.start_time
                game.home_team_id = home_team.team_id
                game.away_team_id = away_team.team_id
                game.spread = payload.spread
                game.total = payload.total
                game.status = payload.status
                game.meta = payload.meta
            count += 1
        self._session.flush()
        return count

    def archive_missing_games(self, game_date: date, active_provider_game_ids: set[str]) -> int:
        archived = 0
        games = self._session.scalars(select(Game).where(Game.game_date == game_date)).all()
        for game in games:
            provider_game_id = game.provider_game_id or ""
            if provider_game_id in active_provider_game_ids:
                continue
            if game.status == "superseded":
                continue
            game.status = "superseded"
            archived += 1
        self._session.flush()
        return archived

    def ingest_player_game_logs(
        self,
        payloads: Iterable[PlayerGameLogPayload],
        source_provider_name: str | None = None,
    ) -> int:
        # Pre-load all existing PlayerGameLog rows into a dict so the per-row
        # lookup below is an O(1) dict access instead of a SELECT per row.
        existing_logs: dict[tuple[int, int], PlayerGameLog] = {
            (row.player_id, row.game_id): row
            for row in self._session.scalars(select(PlayerGameLog)).all()
        }
        count = 0
        for payload in dedupe_player_game_log_payloads(payloads):
            player = self._references.get_player_by_provider_id(payload.provider_player_id)
            game = self._references.get_game_by_provider_id(payload.provider_game_id)
            team = self._references.get_team_by_abbreviation(payload.team_abbreviation)
            opponent = (
                self._references.get_team_by_abbreviation(payload.opponent_abbreviation)
                if payload.opponent_abbreviation
                else None
            )
            player_name = str(payload.meta.get("player_name", payload.provider_player_id))
            normalized_name = normalize_name(player_name)
            if player is None:
                match = self._player_matcher.match(
                    source_provider_name, payload.provider_player_id, player_name
                )
                if match.player_id is not None:
                    player = self._session.get(Player, match.player_id)
            if player is None:
                player = Player(
                    provider_player_id=payload.provider_player_id,
                    full_name=player_name,
                    normalized_name=normalized_name,
                    team_id=team.team_id if team else None,
                    position=str(payload.meta.get("position")) if payload.meta.get("position") else None,
                    status="active",
                    meta={},
                )
                self._session.add(player)
                self._session.flush()
                logger.debug(
                    "Created new player from game log",
                    extra={"provider_player_id": payload.provider_player_id, "player_name": player_name},
                )
            self._merge_player_provider_identity(player, payload.provider_player_id, source_provider_name)
            if team is not None:
                player.team_id = team.team_id
            position = payload.meta.get("position")
            if position:
                player.position = str(position)
            if game is None:
                continue
            key = (player.player_id, game.game_id)
            existing = existing_logs.get(key)
            if existing is None:
                existing = PlayerGameLog(player_id=player.player_id, game_id=game.game_id)
                self._session.add(existing)
                existing_logs[key] = existing
            existing.team_id = team.team_id if team else None
            existing.opponent_team_id = opponent.team_id if opponent else None
            existing.minutes = payload.minutes
            existing.points = payload.points
            existing.rebounds = payload.rebounds
            existing.assists = payload.assists
            existing.threes = payload.threes
            existing.steals = payload.steals
            existing.blocks = payload.blocks
            existing.turnovers = payload.turnovers
            existing.fouls = payload.fouls
            existing.field_goal_attempts = payload.field_goal_attempts
            existing.field_goals_made = payload.field_goals_made
            existing.free_throw_attempts = payload.free_throw_attempts
            existing.free_throws_made = payload.free_throws_made
            existing.offensive_rebounds = payload.offensive_rebounds
            existing.defensive_rebounds = payload.defensive_rebounds
            existing.plus_minus = payload.plus_minus
            existing.starter_flag = payload.starter_flag
            existing.overtime_flag = payload.overtime_flag
            existing.meta = payload.meta
            count += 1
        self._session.flush()
        return count

    def restore_logged_games(self, max_game_date: date) -> int:
        games = self._session.scalars(
            select(Game)
            .join(PlayerGameLog, PlayerGameLog.game_id == Game.game_id)
            .where(Game.status == "superseded")
            .where(Game.game_date <= max_game_date)
            .distinct()
        ).all()
        restored = 0
        for game in games:
            game.status = "final"
            restored += 1
        self._session.flush()
        return restored

    def ingest_injuries(self, payloads: Iterable[InjuryPayload], source_payload_id: int | None = None) -> int:
        count = 0
        for payload in dedupe_injury_payloads(payloads):
            player = self._references.get_player_by_provider_id(payload.provider_player_id)
            team = self._references.get_team_by_abbreviation(payload.team_abbreviation)
            if player is None or team is None:
                continue
            game = (
                self._references.get_game_by_provider_id(payload.provider_game_id)
                if payload.provider_game_id
                else None
            )
            self._session.add(
                InjuryReport(
                    player_id=player.player_id,
                    team_id=team.team_id,
                    report_timestamp=payload.report_timestamp,
                    game_id=game.game_id if game else None,
                    status=payload.status,
                    designation=payload.designation,
                    body_part=payload.body_part,
                    notes=payload.notes,
                    expected_availability_flag=payload.expected_availability_flag,
                    source_payload_id=source_payload_id,
                )
            )
            count += 1
        self._session.flush()
        return count

    def ingest_game_availability(
        self, payloads: Iterable[PlayerAvailabilityPayload]
    ) -> tuple[int, set[int]]:
        """Upsert official NBA inactive-list entries into game_player_availability.

        Each call fully replaces the inactive records for every game_id present
        in the payload batch so stale entries (players reinstated since last
        fetch) are removed.  Players who are NOT on the inactive list are not
        recorded — absence of a row means active or list not yet submitted.

        Returns ``(count_inserted, changed_game_ids)`` where ``changed_game_ids``
        is the set of internal game_ids whose inactive list changed since the
        previous fetch.  Callers can use this to trigger selective re-prediction.
        """
        from collections import defaultdict
        payloads_list = list(payloads)
        changed_game_ids: set[int] = set()

        # Group by provider_game_id so we can delete-then-insert per game
        by_game: dict[str, list[PlayerAvailabilityPayload]] = defaultdict(list)
        for p in payloads_list:
            by_game[p.provider_game_id].append(p)

        # For games with an empty payload batch (no inactives returned from API)
        # we still need to clear any previously stored inactives in case they
        # were reinstated.  Collect all resolved game_ids we need to process.
        resolved_games: dict[str, Game] = {}
        for provider_game_id in by_game:
            game = self._references.get_game_by_provider_id(provider_game_id)
            if game is not None:
                resolved_games[provider_game_id] = game

        count = 0
        for provider_game_id, game_payloads in by_game.items():
            game = resolved_games.get(provider_game_id)
            if game is None:
                continue

            # Snapshot the current inactive player_ids before deletion so we
            # can detect whether the list actually changed.
            existing_rows = (
                self._session.query(GamePlayerAvailability)
                .filter(GamePlayerAvailability.game_id == game.game_id)
                .all()
            )
            previous_inactive_ids = {r.provider_player_id for r in existing_rows}
            new_inactive_ids = {p.provider_player_id for p in game_payloads}

            # Delete stale records first (handles reinstatements cleanly)
            self._session.query(GamePlayerAvailability).filter(
                GamePlayerAvailability.game_id == game.game_id
            ).delete(synchronize_session=False)

            for payload in game_payloads:
                player = self._references.get_player_by_provider_id(payload.provider_player_id)
                self._session.add(
                    GamePlayerAvailability(
                        game_id=game.game_id,
                        player_id=player.player_id if player else None,
                        provider_player_id=payload.provider_player_id,
                        player_name=payload.player_name,
                        team_abbreviation=payload.team_abbreviation,
                        is_active=payload.is_active,
                        reason=payload.reason,
                        fetched_at=payload.fetched_at,
                    )
                )
                count += 1

            if previous_inactive_ids != new_inactive_ids:
                changed_game_ids.add(game.game_id)

        self._session.flush()
        return count, changed_game_ids

    def ingest_line_snapshots(self, payloads: Iterable[LineSnapshotPayload], source_payload_id: int | None = None) -> int:
        self.ensure_default_markets()
        count = 0
        pending_snapshots: dict[tuple[datetime, int, int, int, int, float], LineSnapshot] = {}
        for payload in dedupe_line_snapshot_payloads(payloads):
            game = self._resolve_game_for_line(payload)
            if game is None:
                continue
            sportsbook = self._ensure_sportsbook(payload.sportsbook_key)
            market = self._references.get_market_by_key(payload.market_key)
            if market is None:
                continue
            player = self._resolve_player_for_line(payload)
            if player is None:
                continue
            snapshot_key = (
                payload.timestamp,
                game.game_id,
                sportsbook.sportsbook_id,
                player.player_id,
                market.market_id,
                float(payload.line_value),
            )
            existing = pending_snapshots.get(snapshot_key)
            if existing is None:
                existing = self._session.scalar(
                    select(LineSnapshot).where(
                        LineSnapshot.timestamp == payload.timestamp,
                        LineSnapshot.game_id == game.game_id,
                        LineSnapshot.sportsbook_id == sportsbook.sportsbook_id,
                        LineSnapshot.player_id == player.player_id,
                        LineSnapshot.market_id == market.market_id,
                        LineSnapshot.line_value == payload.line_value,
                    )
                )
            if existing is None:
                existing = LineSnapshot(
                    timestamp=payload.timestamp,
                    game_id=game.game_id,
                    sportsbook_id=sportsbook.sportsbook_id,
                    player_id=player.player_id,
                    market_id=market.market_id,
                    line_value=payload.line_value,
                )
                self._session.add(existing)
            pending_snapshots[snapshot_key] = existing
            existing.over_odds = payload.over.odds
            existing.under_odds = payload.under.odds
            existing.event_status = payload.event_status
            existing.source_payload_id = source_payload_id
            existing.meta = payload.meta
            count += 1
        self._session.flush()
        return count

    def mark_closing_lines(self, game_date: date) -> int:
        games = self._session.scalars(select(Game).where(Game.game_date == game_date)).all()
        count = 0
        for game in games:
            snapshots = self._session.scalars(
                select(LineSnapshot).where(LineSnapshot.game_id == game.game_id).order_by(LineSnapshot.timestamp.desc())
            ).all()
            seen_keys: set[tuple[int, int, int]] = set()
            for snapshot in snapshots:
                key = (snapshot.sportsbook_id, snapshot.player_id, snapshot.market_id)
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                closing_line = self._session.scalar(
                    select(ClosingLine).where(
                        ClosingLine.game_id == snapshot.game_id,
                        ClosingLine.sportsbook_id == snapshot.sportsbook_id,
                        ClosingLine.player_id == snapshot.player_id,
                        ClosingLine.market_id == snapshot.market_id,
                    )
                )
                if closing_line is None:
                    closing_line = ClosingLine(
                        game_id=snapshot.game_id,
                        sportsbook_id=snapshot.sportsbook_id,
                        player_id=snapshot.player_id,
                        market_id=snapshot.market_id,
                    )
                    self._session.add(closing_line)
                closing_line.line_value = snapshot.line_value
                closing_line.over_odds = snapshot.over_odds
                closing_line.under_odds = snapshot.under_odds
                closing_line.captured_at = snapshot.timestamp
                count += 1
        self._session.flush()
        return count

    def mark_historical_training_quotes(
        self,
        game_date: date | None = None,
    ) -> int:
        """For all final games (optionally filtered to game_date), select the
        latest pre-game snapshot for each (game, player, market, sportsbook)
        group and stamp it with ``is_historical_training_quote: true`` in its
        meta JSON.  This makes those snapshots usable for calibration and
        backtest evaluation via ``_is_training_usable_meta``.

        Uses event_start_time from snapshot meta (not game.start_time, which
        is stored as midnight for most games) to determine the pre-game cutoff.
        Idempotent — re-stamping already-marked snapshots is harmless.
        """
        # Match the case-insensitive status stored by different ingestion paths
        games_query = select(Game).where(Game.status.in_(["final", "Final", "FINAL"]))
        if game_date is not None:
            games_query = games_query.where(Game.game_date == game_date)
        games = self._session.scalars(games_query).all()
        count = 0
        for game in games:
            snapshots = self._session.scalars(
                select(LineSnapshot)
                .where(LineSnapshot.game_id == game.game_id)
                .order_by(LineSnapshot.timestamp.desc())
            ).all()
            if not snapshots:
                continue
            # Determine the actual game start from meta (game.start_time is midnight)
            event_start: datetime | None = None
            for snap in snapshots:
                est_str = (snap.meta or {}).get("event_start_time")
                if est_str:
                    try:
                        from dateutil.parser import isoparse as _isoparse
                        event_start = _isoparse(est_str).replace(tzinfo=None)
                        break
                    except Exception:
                        pass
            # A "pre-game" snapshot is any snapshot captured before actual tipoff.
            # Use a small 5-minute guard to exclude snapshots taken mid-game.
            if event_start is not None:
                pre_game_cutoff = event_start - timedelta(minutes=5)
                eligible = [s for s in snapshots if s.timestamp < pre_game_cutoff]
            else:
                # No event_start_time in meta — fall back to all snapshots for
                # this game (assumes system only collected odds before games).
                eligible = list(snapshots)
            seen_keys: set[tuple[int, int, int]] = set()
            for snapshot in eligible:
                key = (snapshot.sportsbook_id, snapshot.player_id, snapshot.market_id)
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                meta = dict(snapshot.meta) if snapshot.meta else {}
                if not meta.get("is_historical_training_quote"):
                    meta["is_historical_training_quote"] = True
                    snapshot.meta = meta
                    count += 1
        self._session.flush()
        return count

    def store_raw_payload(
        self,
        provider_type: str,
        provider_name: str,
        endpoint: str,
        fetched_at: datetime,
        payload: dict[str, object],
    ) -> int:
        return self._raw_payloads.store(provider_type, provider_name, endpoint, fetched_at, payload).payload_id

    def _ensure_season(self, season_code: str | None, game_date: date) -> Season | None:
        if season_code is None:
            return None
        season = self._session.scalar(select(Season).where(Season.code == season_code))
        if season is None:
            season = Season(
                code=season_code,
                start_date=date(game_date.year, 10, 1),
                end_date=date(game_date.year + 1, 6, 30),
            )
            self._session.add(season)
            self._session.flush()
        return season

    def _ensure_sportsbook(self, key: str) -> Sportsbook:
        sportsbook = self._references.get_sportsbook_by_key(key)
        if sportsbook is None:
            sportsbook = Sportsbook(key=key, display_name=key.replace("_", " ").title(), is_active=True)
            self._session.add(sportsbook)
            self._session.flush()
        return sportsbook

    def _resolve_player_for_line(self, payload: LineSnapshotPayload) -> Player | None:
        player = self._references.get_player_by_provider_id(payload.provider_player_id)
        if player is not None:
            return player
        player_name = str(payload.meta.get("player_name", payload.provider_player_id))
        match = self._player_matcher.match("odds", payload.provider_player_id, player_name)
        if match.player_id is None:
            logger.warning(
                "Unmatched player in odds line — snapshot skipped",
                extra={"provider_player_id": payload.provider_player_id, "player_name": player_name},
            )
            return None
        return self._session.get(Player, match.player_id)

    def _resolve_game_for_line(self, payload: LineSnapshotPayload) -> Game | None:
        game = self._references.get_game_by_provider_id(payload.provider_game_id)
        if game is not None:
            return game

        home_abbreviation = str(payload.meta.get("home_team_abbreviation") or "").strip().upper()
        away_abbreviation = str(payload.meta.get("away_team_abbreviation") or "").strip().upper()
        if not home_abbreviation or not away_abbreviation:
            return None

        event_date = self._line_event_date(payload)
        clauses = [
            or_(
                and_(Team.team_id == Game.home_team_id, Team.abbreviation == home_abbreviation),
                and_(Team.team_id == Game.away_team_id, Team.abbreviation == home_abbreviation),
            )
        ]
        query = (
            select(Game)
            .join(Team, or_(Game.home_team_id == Team.team_id, Game.away_team_id == Team.team_id))
            .where(*clauses)
        )
        if event_date is not None:
            query = query.where(Game.game_date == event_date)
        candidates = self._session.scalars(query).all()
        for candidate in candidates:
            home_team = self._session.get(Team, candidate.home_team_id)
            away_team = self._session.get(Team, candidate.away_team_id)
            if home_team is None or away_team is None:
                continue
            if {home_team.abbreviation, away_team.abbreviation} == {home_abbreviation, away_abbreviation}:
                return candidate
        return None

    @staticmethod
    def _line_event_date(payload: LineSnapshotPayload) -> date | None:
        event_start = payload.meta.get("event_start_time")
        if event_start:
            parsed = isoparse(str(event_start))
            return to_local_board_date(parsed.date(), parsed)
        return payload.timestamp.date()

    def _find_existing_player(
        self,
        provider_player_id: str,
        normalized_name: str,
        team_id: int | None,
    ) -> Player | None:
        player = self._references.get_player_by_provider_id(provider_player_id)
        if player is not None:
            return player
        return self._references.get_player_by_normalized_name_and_team(normalized_name, team_id)

    def _find_existing_game(
        self,
        payload: GamePayload,
        home_team_id: int,
        away_team_id: int,
    ) -> Game | None:
        game = self._references.get_game_by_provider_id(payload.provider_game_id)
        if game is not None:
            return game
        return self._references.get_game_by_matchup(payload.game_date, home_team_id, away_team_id)

    @staticmethod
    def _merge_player_provider_identity(
        player: Player,
        provider_player_id: str,
        source_provider_name: str | None,
    ) -> None:
        provider_player_id = str(provider_player_id)
        if not player.provider_player_id:
            player.provider_player_id = provider_player_id
        meta: dict[str, Any] = dict(player.meta or {})
        provider_ids = {
            str(item)
            for item in meta.get("provider_player_ids", [])
            if item is not None
        }
        if player.provider_player_id:
            provider_ids.add(str(player.provider_player_id))
        provider_ids.add(provider_player_id)
        meta["provider_player_ids"] = sorted(provider_ids)
        if source_provider_name:
            by_source = meta.get("provider_player_ids_by_source")
            if not isinstance(by_source, dict):
                by_source = {}
            source_ids = {
                str(item)
                for item in by_source.get(source_provider_name, [])
                if item is not None
            }
            source_ids.add(provider_player_id)
            by_source[source_provider_name] = sorted(source_ids)
            meta["provider_player_ids_by_source"] = by_source
        player.meta = meta

    @staticmethod
    def _merge_game_provider_identity(
        game: Game,
        provider_game_id: str,
        payload_meta: dict[str, Any] | None,
    ) -> None:
        provider_game_id = str(provider_game_id)
        if not game.provider_game_id:
            game.provider_game_id = provider_game_id
        meta: dict[str, Any] = dict(game.meta or {})
        provider_ids = {
            str(item)
            for item in meta.get("provider_game_ids", [])
            if item is not None
        }
        if game.provider_game_id:
            provider_ids.add(str(game.provider_game_id))
        provider_ids.add(provider_game_id)
        meta["provider_game_ids"] = sorted(provider_ids)
        if isinstance(payload_meta, dict):
            merged_meta = dict(payload_meta)
            merged_meta.update(meta)
            meta = merged_meta
        game.meta = meta


class IngestionOrchestrator:
    def __init__(self, session: Session) -> None:
        self._session = session
        self._ingestion = IngestionService(session)
        self._agent_runs = AgentRunService(session)

    async def refresh_reference_data(self, stats_provider: object, target_date: date) -> dict[str, int]:
        provider_name = str(getattr(stats_provider, "provider_name", "unknown"))
        try:
            game_result, games = await stats_provider.fetch_schedule(target_date)
            team_result, teams = await stats_provider.fetch_teams()
        except Exception as exc:
            self._record_api_monitor_event(
                provider_name=provider_name,
                event_type="refresh_reference_data",
                status="error",
                detail=str(exc),
            )
            raise
        self._ingestion.store_raw_payload("stats", stats_provider.provider_name, team_result.endpoint, team_result.fetched_at, team_result.payload)
        self._ingestion.store_raw_payload("stats", stats_provider.provider_name, game_result.endpoint, game_result.fetched_at, game_result.payload)
        self._record_api_monitor_event(
            provider_name=provider_name,
            event_type="refresh_reference_data",
            status="ok",
            payload={
                "schedule_endpoint": game_result.endpoint,
                "teams_endpoint": team_result.endpoint,
                "schedule_keys": sorted(game_result.payload.keys()) if isinstance(game_result.payload, dict) else [],
                "teams_keys": sorted(team_result.payload.keys()) if isinstance(team_result.payload, dict) else [],
            },
        )
        players: list[PlayerPayload] = []
        if getattr(stats_provider, "provider_name", "") != "balldontlie":
            try:
                player_result, players = await stats_provider.fetch_rosters()
            except Exception as exc:
                self._record_api_monitor_event(
                    provider_name=provider_name,
                    event_type="fetch_rosters",
                    status="error",
                    detail=str(exc),
                )
                raise
            self._ingestion.store_raw_payload(
                "stats",
                stats_provider.provider_name,
                player_result.endpoint,
                player_result.fetched_at,
                player_result.payload,
            )
            self._record_api_monitor_event(
                provider_name=provider_name,
                event_type="fetch_rosters",
                status="ok",
                payload={"endpoint": player_result.endpoint},
            )
        metrics = {
            "teams": self._ingestion.ingest_teams(teams),
            "players": self._ingestion.ingest_players(players, source_provider_name=getattr(stats_provider, "provider_name", None)),
            "games": self._ingestion.ingest_games(games),
        }
        metrics["archived_games"] = self._ingestion.archive_missing_games(
            target_date,
            {payload.provider_game_id for payload in games},
        )
        self._session.commit()
        return metrics

    async def ingest_game_logs(self, stats_provider: object, start_date: date, end_date: date) -> dict[str, int]:
        provider_name = str(getattr(stats_provider, "provider_name", "unknown"))
        try:
            result, logs = await stats_provider.fetch_player_game_logs(start_date, end_date)
        except Exception as exc:
            self._record_api_monitor_event(
                provider_name=provider_name,
                event_type="fetch_player_game_logs",
                status="error",
                detail=str(exc),
            )
            raise
        self._ingestion.store_raw_payload("stats", stats_provider.provider_name, result.endpoint, result.fetched_at, result.payload)
        self._record_api_monitor_event(
            provider_name=provider_name,
            event_type="fetch_player_game_logs",
            status="ok",
            payload={"endpoint": result.endpoint, "start_date": start_date.isoformat(), "end_date": end_date.isoformat()},
        )
        metrics = {
            "player_game_logs": self._ingestion.ingest_player_game_logs(
                logs,
                source_provider_name=getattr(stats_provider, "provider_name", None),
            )
        }
        metrics["restored_games"] = self._ingestion.restore_logged_games(end_date)
        metrics["historical_training_quotes"] = self._ingestion.mark_historical_training_quotes(end_date)
        self._session.commit()
        return metrics

    async def refresh_reference_history(
        self,
        stats_provider: object,
        start_date: date,
        end_date: date,
    ) -> dict[str, int]:
        metrics = {"historical_games": 0}
        if hasattr(stats_provider, "fetch_schedule_range"):
            try:
                game_result, games = await stats_provider.fetch_schedule_range(start_date, end_date)
            except Exception as exc:
                self._record_api_monitor_event(
                    provider_name=str(getattr(stats_provider, "provider_name", "unknown")),
                    event_type="fetch_schedule_range",
                    status="error",
                    detail=str(exc),
                )
                raise
            self._ingestion.store_raw_payload(
                "stats",
                stats_provider.provider_name,
                game_result.endpoint,
                game_result.fetched_at,
                game_result.payload,
            )
            metrics["historical_games"] += self._ingestion.ingest_games(games)
            self._session.commit()
            return metrics
        current_date = start_date
        while current_date <= end_date:
            try:
                game_result, games = await stats_provider.fetch_schedule(current_date)
            except Exception as exc:
                self._record_api_monitor_event(
                    provider_name=str(getattr(stats_provider, "provider_name", "unknown")),
                    event_type="fetch_schedule",
                    status="error",
                    detail=str(exc),
                    payload={"target_date": current_date.isoformat()},
                )
                raise
            self._ingestion.store_raw_payload(
                "stats",
                stats_provider.provider_name,
                game_result.endpoint,
                game_result.fetched_at,
                game_result.payload,
            )
            metrics["historical_games"] += self._ingestion.ingest_games(games)
            self._ingestion.archive_missing_games(
                current_date,
                {payload.provider_game_id for payload in games},
            )
            current_date = date.fromordinal(current_date.toordinal() + 1)
        self._session.commit()
        return metrics

    async def ingest_injuries(self, injuries_provider: object, target_date: date | None) -> dict[str, int]:
        provider_name = str(getattr(injuries_provider, "provider_name", "unknown"))
        try:
            result, injuries = await injuries_provider.fetch_injuries(target_date)
        except Exception as exc:
            self._record_api_monitor_event(
                provider_name=provider_name,
                event_type="fetch_injuries",
                status="error",
                detail=str(exc),
            )
            raise
        payload_id = self._ingestion.store_raw_payload("injuries", injuries_provider.provider_name, result.endpoint, result.fetched_at, result.payload)
        self._record_api_monitor_event(
            provider_name=provider_name,
            event_type="fetch_injuries",
            status="ok",
            payload={"endpoint": result.endpoint, "target_date": target_date.isoformat() if target_date else None},
        )
        metrics = {"injury_reports": self._ingestion.ingest_injuries(injuries, source_payload_id=payload_id)}
        self._session.commit()
        return metrics

    async def ingest_odds(
        self,
        odds_provider: object,
        target_date: date,
        prefetched_result: object | None = None,
        prefetched_lines: list[LineSnapshotPayload] | None = None,
    ) -> dict[str, int]:
        provider_name = str(getattr(odds_provider, "provider_name", "unknown"))
        if prefetched_result is None or prefetched_lines is None:
            try:
                result, lines = await odds_provider.fetch_upcoming_player_props(target_date)
            except Exception as exc:
                self._record_api_monitor_event(
                    provider_name=provider_name,
                    event_type="fetch_upcoming_player_props",
                    status="error",
                    detail=str(exc),
                )
                raise
        else:
            result, lines = prefetched_result, prefetched_lines
        payload = result.payload if isinstance(result.payload, dict) else {"items": result.payload}
        payload_id = self._ingestion.store_raw_payload("odds", odds_provider.provider_name, result.endpoint, result.fetched_at, payload)
        self._record_api_monitor_event(
            provider_name=provider_name,
            event_type="fetch_upcoming_player_props",
            status="ok",
            payload={"endpoint": result.endpoint, "line_count": len(lines)},
        )
        metrics = {"line_snapshots": self._ingestion.ingest_line_snapshots(lines, source_payload_id=payload_id)}
        self._session.commit()
        return metrics

    async def ingest_game_availability(
        self,
        stats_provider: object,
        target_date: date,
    ) -> dict[str, int]:
        """Fetch and store the official NBA inactive list for today's games.

        Queries the DB for all games scheduled on target_date (using the
        America/New_York board-date logic so games stored with a UTC date of
        tomorrow are still included), then calls the provider for each
        provider_game_id.  Always re-fetches on every startup — no stale-cache
        guard — so the inactive list is as current as possible.
        """
        from sqlalchemy import select as sa_select

        from app.services.board_date import to_local_board_date

        games = self._session.scalars(
            sa_select(Game).where(Game.status.in_(["scheduled", "live"]))
        ).all()
        # Filter to games that fall on target_date in Eastern time
        target_games = [
            g for g in games
            if to_local_board_date(g.game_date, g.start_time) == target_date
        ]
        if not target_games:
            return {"player_availability": 0}

        provider_game_ids = [
            g.provider_game_id for g in target_games if g.provider_game_id
        ]
        provider_name = str(getattr(stats_provider, "provider_name", "unknown"))
        try:
            result, payloads = await stats_provider.fetch_game_availability(provider_game_ids)
        except Exception as exc:
            self._record_api_monitor_event(
                provider_name=provider_name,
                event_type="fetch_game_availability",
                status="error",
                detail=str(exc),
                payload={"provider_game_ids": len(provider_game_ids)},
            )
            raise
        self._ingestion.store_raw_payload(
            "stats",
            getattr(stats_provider, "provider_name", "nba_api"),
            result.endpoint,
            result.fetched_at,
            result.payload if isinstance(result.payload, dict) else {"items": result.payload},
        )
        self._record_api_monitor_event(
            provider_name=provider_name,
            event_type="fetch_game_availability",
            status="ok",
            payload={"endpoint": result.endpoint, "provider_game_ids": len(provider_game_ids), "rows": len(payloads)},
        )
        count, changed_game_ids = self._ingestion.ingest_game_availability(payloads)
        self._session.commit()
        # changed_game_ids is carried as a non-int value under a private key so
        # callers can extract it before merging into the int-typed metrics dict.
        return {"player_availability": count, "_changed_game_ids": changed_game_ids}  # type: ignore[dict-item]

    def mark_closing_lines(self, game_date: date) -> dict[str, int]:
        # v1.2.2 Step 1: Removed full historical backfill from this hot path.
        # mark_historical_training_quotes() is now called only from ingest_game_logs
        # (scoped to the target date), which keeps closing-line ingestion fast and
        # prevents a full table-scan on every daily cron run.
        metrics = {"closing_lines": self._ingestion.mark_closing_lines(game_date)}
        self._session.commit()
        return metrics

    def _record_api_monitor_event(
        self,
        *,
        provider_name: str,
        event_type: str,
        status: str,
        detail: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        self._agent_runs.record(
            task_id=f"{provider_name}:{event_type}",
            agent_role="api_monitor",
            event_type=event_type,
            status=status,
            detail=detail,
            payload={"provider_name": provider_name, **(payload or {})},
            error_category="provider_error" if status == "error" else None,
        )

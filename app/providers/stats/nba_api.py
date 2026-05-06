from __future__ import annotations

import asyncio
import logging
import time as time_module
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

from dateutil.parser import isoparse
from nba_api.stats.endpoints import (
    boxscoreadvancedv3,
    boxscoreplayertrackv3,
    boxscorescoringv3,
    boxscoresummaryv2,
    commonteamroster,
    leaguegamefinder,
    playergamelogs,
    scoreboardv3,
)
from nba_api.stats.static import teams as static_teams

from app.config.settings import get_settings
from app.providers.base import StatsProvider
from app.providers.canonical_schema import (
    NBA_API_ADVANCED_FIELD_MAP,
    NBA_API_SCORING_FIELD_MAP,
    NBA_API_TRACKING_FIELD_MAP,
    normalize_provider_boxscore_row,
)
from app.schemas.domain import (
    GamePayload,
    PlayerAvailabilityPayload,
    PlayerGameLogPayload,
    PlayerPayload,
    ProviderFetchResult,
    TeamPayload,
)

SEASON_TYPES = ("Regular Season", "Playoffs")
LOG_WINDOW_DAYS = 5
PLAYER_LOG_FETCH_RETRIES = 3
BOXSCORE_TRANSIENT_ERROR_MARKERS = (
    "timeout",
    "timed out",
    "readtimeout",
    "connection reset",
    "connection aborted",
    "connection refused",
    "temporarily unavailable",
    "remote disconnected",
    "too many requests",
    "429",
    "503",
)
logger = logging.getLogger(__name__)


class NbaApiStatsProvider(StatsProvider):
    provider_name = "nba_api"

    def __init__(self) -> None:
        settings = get_settings()
        self._timeout = settings.request_timeout_seconds
        self._enable_boxscore_enrichment = settings.nba_api_enable_boxscore_enrichment
        self._boxscore_timeout = max(1, settings.nba_api_boxscore_timeout_seconds)
        self._boxscore_fetch_retries = max(1, settings.nba_api_boxscore_fetch_retries)
        self._boxscore_fetch_concurrency = max(1, settings.nba_api_boxscore_fetch_concurrency)
        self._request_delay_seconds = max(0.0, float(settings.nba_api_request_delay_seconds))
        self._retry_attempts = max(1, int(settings.nba_api_retry_attempts))
        self._retry_max_backoff_seconds = max(0.5, float(settings.nba_api_retry_max_backoff_seconds))
        self._team_scope_abbreviations: set[str] = set()
        team_rows = static_teams.get_teams()
        self._team_rows_by_abbreviation = {
            str(team["abbreviation"]).upper(): dict(team)
            for team in team_rows
            if team.get("abbreviation")
        }
        self._team_abbreviation_by_id = {
            int(team["id"]): str(team["abbreviation"]).upper()
            for team in team_rows
            if team.get("id") is not None and team.get("abbreviation")
        }
        self._scheduled_team_abbreviations: set[str] = set()
        self._game_lookup: dict[str, tuple[str, str]] = {}
        self._game_date_lookup: dict[str, date] = {}
        self._player_name_lookup: dict[str, str] = {}
        self._player_position_lookup: dict[str, str] = {}
        self._game_boxscore_cache: dict[str, dict[str, dict[str, Any]]] = {}
        self._game_boxscore_status: dict[str, dict[str, Any]] = {}
        # In-session cache for availability so the same game isn't fetched twice
        # within one startup.  Keyed by provider_game_id.
        self._availability_cache: dict[str, list[PlayerAvailabilityPayload]] = {}

    async def healthcheck(self) -> bool:
        payload = await asyncio.to_thread(self._scoreboard_payload, date.today())
        return "scoreboard" in payload

    async def verify_required_access(self) -> None:
        end_date = date.today()
        start_date = end_date - timedelta(days=1)
        await asyncio.to_thread(
            self._player_log_rows_with_retry,
            start_date,
            end_date,
            _season_code_for_date(end_date),
            "Regular Season",
        )

    async def fetch_teams(self) -> tuple[ProviderFetchResult, list[TeamPayload]]:
        payload_rows = [
            {
                "id": team["id"],
                "abbreviation": team["abbreviation"],
                "full_name": team["full_name"],
                "city": team["city"],
            }
            for team in self._team_rows_by_abbreviation.values()
        ]
        result = ProviderFetchResult(
            endpoint="nba_api.stats.static.teams.get_teams",
            fetched_at=datetime.now(UTC),
            payload={"data": payload_rows, "meta": {"record_count": len(payload_rows)}},
        )
        teams = [
            TeamPayload(
                provider_team_id=str(row["id"]),
                abbreviation=str(row["abbreviation"]).upper(),
                name=str(row["full_name"]),
                city=str(row["city"]),
            )
            for row in payload_rows
        ]
        return result, teams

    async def fetch_rosters(self) -> tuple[ProviderFetchResult, list[PlayerPayload]]:
        season_code = _season_code_for_date(date.today())
        team_ids = self._scoped_team_ids()
        roster_rows: list[dict[str, Any]] = []
        for team_id in team_ids:
            roster_rows.extend(
                await asyncio.to_thread(
                    self._common_team_roster_rows,
                    team_id,
                    season_code,
                )
            )
        players: list[PlayerPayload] = []
        for row in roster_rows:
            provider_player_id = str(row["PLAYER_ID"])
            team_abbreviation = self._team_abbreviation_by_id.get(int(row["TeamID"]))
            player_name = str(row["PLAYER"]).strip()
            position = self._normalize_position(row.get("POSITION"))
            self._player_name_lookup[provider_player_id] = player_name
            if position:
                self._player_position_lookup[provider_player_id] = position
            players.append(
                PlayerPayload(
                    provider_player_id=provider_player_id,
                    full_name=player_name,
                    team_abbreviation=team_abbreviation,
                    position=position,
                    status="active",
                )
            )
        result = ProviderFetchResult(
            endpoint="nba_api.stats.endpoints.commonteamroster.CommonTeamRoster",
            fetched_at=datetime.now(UTC),
            payload={
                "data": roster_rows,
                "meta": {
                    "team_count": len(team_ids),
                    "record_count": len(roster_rows),
                    "season": season_code,
                },
            },
        )
        return result, players

    async def fetch_schedule(self, target_date: date) -> tuple[ProviderFetchResult, list[GamePayload]]:
        payload = await asyncio.to_thread(self._scoreboard_payload, target_date)
        raw_games = payload.get("scoreboard", {}).get("games", [])
        games = [self._scoreboard_game_to_payload(item) for item in raw_games]
        self._cache_games(games)
        self._scheduled_team_abbreviations = {
            value
            for game in games
            for value in (game.home_team_abbreviation, game.away_team_abbreviation)
        }
        result = ProviderFetchResult(
            endpoint="nba_api.stats.endpoints.scoreboardv3.ScoreboardV3",
            fetched_at=datetime.now(UTC),
            payload={"data": raw_games, "meta": {"record_count": len(raw_games)}},
        )
        return result, games

    async def fetch_game_availability(
        self,
        provider_game_ids: list[str],
    ) -> tuple[ProviderFetchResult, list[PlayerAvailabilityPayload]]:
        """Fetch the official NBA pre-game inactive list for each game.

        The NBA requires teams to submit their inactive list ~90 minutes before
        tip-off.  This uses BoxScoreSummaryV2 which exposes the
        ``InactivePlayers`` result set for both scheduled and live/final games.
        Before the list is submitted the result set will be empty — callers
        should treat an empty result for a game as "not yet available" rather
        than "everyone is active".

        Results are always re-fetched on every startup (no provider-cache TTL)
        so the caller always gets the freshest list, but an in-session dict
        prevents duplicate requests for the same game within one startup.
        """
        fetched_at = datetime.now(UTC)
        all_payloads: list[PlayerAvailabilityPayload] = []
        raw_records: list[dict[str, Any]] = []

        semaphore = asyncio.Semaphore(self._boxscore_fetch_concurrency)

        async def _fetch_one(game_id: str) -> list[PlayerAvailabilityPayload]:
            if game_id in self._availability_cache:
                return self._availability_cache[game_id]
            async with semaphore:
                await asyncio.sleep(self._request_delay_seconds)
                rows = await asyncio.to_thread(self._availability_rows_for_game, game_id)
            payloads = [
                PlayerAvailabilityPayload(
                    provider_game_id=game_id,
                    provider_player_id=str(row["PLAYER_ID"]),
                    player_name=f"{row.get('FIRST_NAME', '')} {row.get('LAST_NAME', '')}".strip() or None,
                    team_abbreviation=self._team_abbreviation_by_id.get(int(row["TEAM_ID"]))
                    if row.get("TEAM_ID") else row.get("TEAM_ABBREVIATION"),
                    is_active=False,
                    reason=str(row["JERSEY_NUM"]).strip() if row.get("JERSEY_NUM") is not None else None,
                    fetched_at=fetched_at,
                )
                for row in rows
                if row.get("PLAYER_ID")
            ]
            self._availability_cache[game_id] = payloads
            return payloads

        results = await asyncio.gather(
            *(_fetch_one(game_id) for game_id in provider_game_ids),
            return_exceptions=True,
        )
        for game_id, result in zip(provider_game_ids, results, strict=False):
            if isinstance(result, Exception):
                logger.warning(
                    "nba_api availability fetch failed for game %s: %s",
                    game_id,
                    result,
                )
                continue
            all_payloads.extend(result)
            raw_records.extend(
                {"game_id": game_id, "player_id": p.provider_player_id, "player_name": p.player_name}
                for p in result
            )

        fetch_result = ProviderFetchResult(
            endpoint="nba_api.stats.endpoints.boxscoresummaryv2.BoxScoreSummaryV2",
            fetched_at=fetched_at,
            payload={
                "game_ids": provider_game_ids,
                "inactive_players": raw_records,
                "meta": {"game_count": len(provider_game_ids), "inactive_count": len(all_payloads)},
            },
        )
        return fetch_result, all_payloads

    def _availability_rows_for_game(self, game_id: str) -> list[dict[str, Any]]:
        """Synchronous inner call — runs in a thread via asyncio.to_thread."""
        try:
            response = self._retry_nba_api_call(
                "BoxScoreSummaryV2",
                lambda timeout: boxscoresummaryv2.BoxScoreSummaryV2(
                    game_id=game_id,
                    timeout=timeout,
                ),
            )
            normalized = response.get_normalized_dict()
            inactive = normalized.get("InactivePlayers", [])
            return list(inactive) if inactive else []
        except Exception as exc:
            logger.warning(
                "nba_api BoxScoreSummaryV2 unavailable for game %s (inactive list not yet submitted?): %s",
                game_id,
                exc,
            )
            return []

    async def fetch_schedule_range(self, start_date: date, end_date: date) -> tuple[ProviderFetchResult, list[GamePayload]]:
        raw_records: list[dict[str, Any]] = []
        games_by_id: dict[str, GamePayload] = {}

        past_end = min(end_date, date.today() - timedelta(days=1))
        if start_date <= past_end:
            for season_code, segment_start, segment_end in _season_segments(start_date, past_end):
                for season_type in SEASON_TYPES:
                    rows = await asyncio.to_thread(
                        self._league_game_rows,
                        segment_start,
                        segment_end,
                        season_code,
                        season_type,
                    )
                    raw_records.extend(rows)
                    for game in self._league_game_rows_to_payloads(rows, season_code):
                        games_by_id[game.provider_game_id] = game

        current_date = max(start_date, date.today())
        while current_date <= end_date:
            _, games = await self.fetch_schedule(current_date)
            for game in games:
                games_by_id[game.provider_game_id] = game
            current_date += timedelta(days=1)

        games = sorted(
            games_by_id.values(),
            key=lambda item: (item.game_date, item.start_time, item.provider_game_id),
        )
        self._cache_games(games)
        result = ProviderFetchResult(
            endpoint="nba_api.stats.endpoints.leaguegamefinder.LeagueGameFinder",
            fetched_at=datetime.now(UTC),
            payload={"data": raw_records, "meta": {"record_count": len(raw_records)}},
        )
        return result, games

    async def fetch_player_game_logs(
        self,
        start_date: date,
        end_date: date,
    ) -> tuple[ProviderFetchResult, list[PlayerGameLogPayload]]:
        raw_records: list[dict[str, Any]] = []
        logs: list[PlayerGameLogPayload] = []
        seen: set[tuple[str, str]] = set()

        for season_code, segment_start, segment_end in _season_segments(start_date, end_date):
            for season_type in _season_types_for_range(segment_start, segment_end):
                for window_start, window_end in _date_windows(segment_start, segment_end, LOG_WINDOW_DAYS):
                    try:
                        rows = await asyncio.to_thread(
                            self._player_log_rows_with_retry,
                            window_start,
                            window_end,
                            season_code,
                            season_type,
                        )
                    except Exception as exc:
                        logger.warning(
                            "nba_api player game log window failed",
                            extra={
                                "season_code": season_code,
                                "season_type": season_type,
                                "window_start": window_start.isoformat(),
                                "window_end": window_end.isoformat(),
                                "error": str(exc),
                            },
                        )
                        continue
                    raw_records.extend(rows)
                    for row in rows:
                        provider_player_id = str(row["PLAYER_ID"])
                        provider_game_id = str(row["GAME_ID"])
                        key = (provider_player_id, provider_game_id)
                        if key in seen:
                            continue
                        seen.add(key)
                        team_abbreviation = str(row["TEAM_ABBREVIATION"]).upper()
                        player_name = str(
                            row.get("PLAYER_NAME") or self._player_name_lookup.get(provider_player_id, "")
                        ).strip()
                        if player_name:
                            self._player_name_lookup[provider_player_id] = player_name
                        logs.append(
                            PlayerGameLogPayload(
                                provider_game_id=provider_game_id,
                                provider_player_id=provider_player_id,
                                team_abbreviation=team_abbreviation,
                                opponent_abbreviation=self._opponent_abbreviation(
                                    provider_game_id,
                                    team_abbreviation,
                                ),
                                minutes=_parse_minutes(row.get("MIN_SEC") or row.get("MIN")),
                                points=int(row.get("PTS") or 0),
                                rebounds=int(row.get("REB") or 0),
                                assists=int(row.get("AST") or 0),
                                threes=int(row.get("FG3M") or 0),
                                steals=int(row.get("STL") or 0),
                                blocks=int(row.get("BLK") or 0),
                                turnovers=int(row.get("TOV") or 0),
                                fouls=int(row.get("PF") or 0),
                                field_goal_attempts=int(row.get("FGA") or 0),
                                field_goals_made=int(row.get("FGM") or 0),
                                free_throw_attempts=int(row.get("FTA") or 0),
                                free_throws_made=int(row.get("FTM") or 0),
                                offensive_rebounds=int(row.get("OREB") or 0),
                                defensive_rebounds=int(row.get("DREB") or 0),
                                plus_minus=float(row["PLUS_MINUS"]) if row.get("PLUS_MINUS") is not None else None,
                                starter_flag=False,
                                overtime_flag=False,
                                meta={
                                    "season_year": row.get("SEASON_YEAR"),
                                    "season_type": season_type,
                                    "player_name": player_name or provider_player_id,
                                    "position": self._player_position_lookup.get(provider_player_id),
                                    "game_date": _log_game_date(row, self._game_date_lookup.get(provider_game_id)),
                                },
                            )
                        )

        result = ProviderFetchResult(
            endpoint="nba_api.stats.endpoints.playergamelogs.PlayerGameLogs",
            fetched_at=datetime.now(UTC),
            payload={"data": raw_records, "meta": {"record_count": len(raw_records)}},
        )
        if logs and self._enable_boxscore_enrichment:
            await self._enrich_logs_with_boxscore_context(logs)
        return result, logs

    def set_team_scope(self, team_abbreviations: set[str]) -> None:
        self._team_scope_abbreviations = {value.upper() for value in team_abbreviations if value}

    def _scoped_team_ids(self) -> list[int]:
        abbreviations = self._team_scope_abbreviations or self._scheduled_team_abbreviations
        if abbreviations:
            return [
                int(self._team_rows_by_abbreviation[abbreviation]["id"])
                for abbreviation in sorted(abbreviations)
                if abbreviation in self._team_rows_by_abbreviation
            ]
        return sorted(self._team_abbreviation_by_id)

    def _scoreboard_payload(self, target_date: date) -> dict[str, Any]:
        response = self._retry_nba_api_call(
            "ScoreboardV3",
            lambda timeout: scoreboardv3.ScoreboardV3(
                game_date=target_date.strftime("%m/%d/%Y"),
                timeout=timeout,
            ),
        )
        return response.get_dict()

    def _common_team_roster_rows(self, team_id: int, season_code: str) -> list[dict[str, Any]]:
        response = self._retry_nba_api_call(
            "CommonTeamRoster",
            lambda timeout: commonteamroster.CommonTeamRoster(
                team_id=team_id,
                season=season_code,
                timeout=timeout,
            ),
        )
        return response.get_data_frames()[0].to_dict("records")

    def _league_game_rows(
        self,
        start_date: date,
        end_date: date,
        season_code: str,
        season_type: str,
    ) -> list[dict[str, Any]]:
        response = self._retry_nba_api_call(
            "LeagueGameFinder",
            lambda timeout: leaguegamefinder.LeagueGameFinder(
                player_or_team_abbreviation="T",
                date_from_nullable=start_date.strftime("%m/%d/%Y"),
                date_to_nullable=end_date.strftime("%m/%d/%Y"),
                season_nullable=season_code,
                season_type_nullable=season_type,
                timeout=timeout,
            ),
        )
        return response.get_data_frames()[0].to_dict("records")

    def _player_log_rows(
        self,
        start_date: date,
        end_date: date,
        season_code: str,
        season_type: str,
    ) -> list[dict[str, Any]]:
        response = self._retry_nba_api_call(
            "PlayerGameLogs",
            lambda timeout: playergamelogs.PlayerGameLogs(
                date_from_nullable=start_date.strftime("%m/%d/%Y"),
                date_to_nullable=end_date.strftime("%m/%d/%Y"),
                season_nullable=season_code,
                season_type_nullable=season_type,
                timeout=timeout,
            ),
            retries=PLAYER_LOG_FETCH_RETRIES,
        )
        return response.get_data_frames()[0].to_dict("records")

    def _player_log_rows_with_retry(
        self,
        start_date: date,
        end_date: date,
        season_code: str,
        season_type: str,
    ) -> list[dict[str, Any]]:
        return self._player_log_rows(start_date, end_date, season_code, season_type)

    async def _enrich_logs_with_boxscore_context(self, logs: list[PlayerGameLogPayload]) -> None:
        game_ids = sorted({log.provider_game_id for log in logs})
        if not game_ids:
            return
        semaphore = asyncio.Semaphore(self._boxscore_fetch_concurrency)

        async def fetch(game_id: str) -> tuple[str, dict[str, dict[str, Any]]]:
            async with semaphore:
                return game_id, await asyncio.to_thread(self._boxscore_context_for_game, game_id)

        results = await asyncio.gather(*(fetch(game_id) for game_id in game_ids), return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logger.warning(
                    "nba_api boxscore enrichment skipped after unexpected game-level failure: %s",
                    result,
                )
                continue
            game_id, context = result
            self._game_boxscore_cache[game_id] = context

        for log in logs:
            player_context = self._game_boxscore_cache.get(log.provider_game_id, {}).get(log.provider_player_id)
            if not player_context:
                continue
            merged_meta = dict(log.meta)
            merged_meta.update(player_context)
            log.meta = merged_meta

    def _boxscore_context_for_game(self, game_id: str) -> dict[str, dict[str, Any]]:
        cached = self._game_boxscore_cache.get(game_id)
        if cached is not None:
            return cached
        context: dict[str, dict[str, Any]] = {}
        failures: list[tuple[str, str, str]] = []
        endpoints = (
            (boxscoreadvancedv3.BoxScoreAdvancedV3, NBA_API_ADVANCED_FIELD_MAP),
            (boxscoreplayertrackv3.BoxScorePlayerTrackV3, NBA_API_TRACKING_FIELD_MAP),
            (boxscorescoringv3.BoxScoreScoringV3, NBA_API_SCORING_FIELD_MAP),
        )
        with ThreadPoolExecutor(max_workers=len(endpoints)) as executor:
            future_map = {
                executor.submit(self._fetch_boxscore_rows, endpoint, game_id): (endpoint.__name__, field_map)
                for endpoint, field_map in endpoints
            }
            for future in as_completed(future_map):
                endpoint_name, field_map = future_map[future]
                try:
                    rows = future.result()
                except Exception as exc:
                    failures.append((endpoint_name, _nba_api_error_kind(exc), str(exc)))
                    continue
                for row in rows:
                    player_id = str(row.get("personId") or "")
                    if not player_id:
                        continue
                    entry = context.setdefault(player_id, {})
                    entry.update(normalize_provider_boxscore_row(row, field_map))
        if failures:
            self._log_boxscore_enrichment_failure(game_id, failures, partial=bool(context))
        return context

    def _fetch_boxscore_rows(self, endpoint: Any, game_id: str) -> list[dict[str, Any]]:
        response = self._retry_nba_api_call(
            endpoint.__name__,
            lambda timeout: endpoint(game_id=game_id, timeout=timeout),
            retries=self._boxscore_fetch_retries,
            timeout=self._boxscore_timeout,
        )
        return response.get_data_frames()[0].to_dict("records")

    def _retry_nba_api_call(
        self,
        endpoint_name: str,
        builder: Any,
        *,
        retries: int | None = None,
        timeout: int | None = None,
    ) -> Any:
        max_attempts = max(1, retries if retries is not None else self._retry_attempts)
        timeout_value = timeout if timeout is not None else self._timeout
        last_error: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            try:
                if self._request_delay_seconds > 0 and attempt > 1:
                    time_module.sleep(self._request_delay_seconds)
                return builder(timeout_value)
            except Exception as exc:  # pragma: no cover - exercised with live endpoints
                last_error = exc
                error_kind = _nba_api_error_kind(exc)
                if error_kind not in {"timeout", "rate_limit"}:
                    break
                if attempt >= max_attempts:
                    break
                retry_after_seconds = _nba_api_retry_after_seconds(exc)
                if retry_after_seconds is None:
                    sleep_seconds = min(self._retry_max_backoff_seconds, max(1.0, float(attempt)))
                else:
                    sleep_seconds = min(self._retry_max_backoff_seconds, max(0.0, float(retry_after_seconds)))
                logger.warning(
                    "nba_api call retry %s/%s for %s (%s): %s",
                    attempt,
                    max_attempts,
                    endpoint_name,
                    error_kind,
                    exc,
                )
                time_module.sleep(sleep_seconds)
        if last_error is None:
            return builder(timeout_value)
        raise last_error

    def _log_boxscore_enrichment_failure(
        self,
        game_id: str,
        failures: list[tuple[str, str, str]],
        *,
        partial: bool,
    ) -> None:
        failure_summary = "; ".join(
            f"{endpoint} [{kind}] {message}"
            for endpoint, kind, message in failures
        )
        message = "nba_api boxscore enrichment partially degraded" if partial else "nba_api boxscore enrichment unavailable"
        logger.warning(
            "%s for game %s: %s",
            message,
            game_id,
            failure_summary,
        )

    def _league_game_rows_to_payloads(
        self,
        rows: list[dict[str, Any]],
        season_code: str,
    ) -> list[GamePayload]:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault(str(row["GAME_ID"]), []).append(row)
        games: list[GamePayload] = []
        for provider_game_id, grouped_rows in grouped.items():
            home_abbreviation: str | None = None
            away_abbreviation: str | None = None
            game_date: date | None = None
            for row in grouped_rows:
                matchup = str(row.get("MATCHUP") or "")
                team_abbreviation = str(row.get("TEAM_ABBREVIATION") or "").upper()
                if " vs. " in matchup:
                    home_abbreviation = team_abbreviation
                    away_abbreviation = matchup.split(" vs. ", 1)[1].strip().upper()
                elif " @ " in matchup:
                    away_abbreviation = team_abbreviation
                    home_abbreviation = matchup.split(" @ ", 1)[1].strip().upper()
                game_date = date.fromisoformat(str(row["GAME_DATE"]))
            if game_date is None or home_abbreviation is None or away_abbreviation is None:
                continue
            games.append(
                GamePayload(
                    provider_game_id=provider_game_id,
                    game_date=game_date,
                    start_time=datetime.combine(game_date, time.min, tzinfo=UTC),
                    home_team_abbreviation=home_abbreviation,
                    away_team_abbreviation=away_abbreviation,
                    season_code=season_code,
                    status="Final",
                )
            )
        return games

    def _scoreboard_game_to_payload(self, item: dict[str, Any]) -> GamePayload:
        start_time = isoparse(str(item.get("gameTimeUTC") or item.get("gameEt")))
        status_id = int(item.get("gameStatus") or 0)
        return GamePayload(
            provider_game_id=str(item["gameId"]),
            game_date=start_time.date(),
            start_time=start_time,
            home_team_abbreviation=str(item["homeTeam"]["teamTricode"]).upper(),
            away_team_abbreviation=str(item["awayTeam"]["teamTricode"]).upper(),
            season_code=_season_code_for_date(start_time.date()),
            status=_scoreboard_status(status_id),
            meta={"game_status_text": item.get("gameStatusText")},
        )

    def _cache_games(self, games: list[GamePayload]) -> None:
        for game in games:
            self._game_lookup[game.provider_game_id] = (
                game.home_team_abbreviation.upper(),
                game.away_team_abbreviation.upper(),
            )
            self._game_date_lookup[game.provider_game_id] = game.game_date

    def _opponent_abbreviation(self, provider_game_id: str, team_abbreviation: str) -> str | None:
        matchup = self._game_lookup.get(provider_game_id)
        if matchup is None:
            return None
        home_abbreviation, away_abbreviation = matchup
        if team_abbreviation == home_abbreviation:
            return away_abbreviation
        if team_abbreviation == away_abbreviation:
            return home_abbreviation
        return None

    @staticmethod
    def _normalize_position(raw_position: Any) -> str | None:
        if raw_position is None:
            return None
        position = str(raw_position).strip()
        return position or None


def _scoreboard_status(status_id: int) -> str:
    if status_id <= 1:
        return "scheduled"
    if status_id == 2:
        return "live"
    if status_id >= 3:
        return "final"
    return "scheduled"


def _season_code_for_date(target_date: date) -> str:
    start_year = target_date.year if target_date.month >= 10 else target_date.year - 1
    return f"{start_year}-{str(start_year + 1)[-2:]}"


def _season_segments(start_date: date, end_date: date) -> list[tuple[str, date, date]]:
    segments: list[tuple[str, date, date]] = []
    cursor = start_date
    while cursor <= end_date:
        season_code = _season_code_for_date(cursor)
        start_year = cursor.year if cursor.month >= 10 else cursor.year - 1
        season_end = date(start_year + 1, 9, 30)
        segment_end = min(end_date, season_end)
        segments.append((season_code, cursor, segment_end))
        cursor = segment_end + timedelta(days=1)
    return segments


def _season_types_for_range(start_date: date, end_date: date) -> tuple[str, ...]:
    if start_date.month >= 4 or end_date.month >= 4:
        return SEASON_TYPES
    return ("Regular Season",)


def _date_windows(start_date: date, end_date: date, window_days: int) -> list[tuple[date, date]]:
    windows: list[tuple[date, date]] = []
    cursor = start_date
    step = max(window_days, 1)
    while cursor <= end_date:
        window_end = min(end_date, cursor + timedelta(days=step - 1))
        windows.append((cursor, window_end))
        cursor = window_end + timedelta(days=1)
    return windows


def _parse_minutes(raw_minutes: str | int | float | None) -> float:
    if raw_minutes is None:
        return 0.0
    if isinstance(raw_minutes, (int, float)):
        return float(raw_minutes)
    text = str(raw_minutes).strip()
    if ":" not in text:
        return float(text or 0.0)
    minutes, seconds = text.split(":", 1)
    return float(minutes) + float(seconds) / 60.0


def _log_game_date(row: dict[str, Any], fallback: date | None) -> str | None:
    raw_value = row.get("GAME_DATE")
    if raw_value not in (None, ""):
        raw_text = str(raw_value).strip()
        try:
            return date.fromisoformat(raw_text).isoformat()
        except ValueError:
            return isoparse(raw_text).date().isoformat()
    if fallback is not None:
        return fallback.isoformat()
    return None


def _nba_api_error_kind(exc: Exception) -> str:
    message = str(exc).lower()
    if any(marker in message for marker in BOXSCORE_TRANSIENT_ERROR_MARKERS):
        if "429" in message or "too many requests" in message:
            return "rate_limit"
        return "timeout"
    if "dataframe" in message or "columns" in message or "keyerror" in message or "indexerror" in message:
        return "schema"
    return "unexpected"


def _nba_api_retry_after_seconds(exc: Exception) -> float | None:
    response = getattr(exc, "response", None)
    if response is None:
        return None
    headers = getattr(response, "headers", None)
    if headers is None:
        return None
    retry_after_raw = headers.get("Retry-After") or headers.get("retry-after")
    if retry_after_raw in (None, ""):
        return None
    try:
        return float(str(retry_after_raw).strip())
    except (TypeError, ValueError):
        return None

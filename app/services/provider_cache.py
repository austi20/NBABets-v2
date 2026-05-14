from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from collections.abc import Iterable
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, TypeVar

from pydantic import BaseModel

from app.config.settings import get_settings
from app.schemas.domain import PlayerGameLogPayload, ProviderFetchResult
from app.services.deduplication import dedupe_player_game_log_payloads
from app.services.name_matching import normalize_name

ModelT = TypeVar("ModelT", bound=BaseModel)


@dataclass(frozen=True)
class CachedCollectionRecord[ModelT]:
    provider_name: str
    fetched_at: datetime
    result: ProviderFetchResult
    items: list[ModelT]


class LocalProviderCache:
    def __init__(self, path: Path | None = None) -> None:
        settings = get_settings()
        self._path = path or settings.provider_cache_db_path
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def get_collection(
        self,
        *,
        provider_type: str,
        provider_name: str | None = None,
        provider_names: Iterable[str] | None = None,
        method_name: str,
        scope_key: str,
        model_type: type[ModelT],
    ) -> tuple[ProviderFetchResult, list[ModelT]] | None:
        record = self.get_collection_with_metadata(
            provider_type=provider_type,
            provider_name=provider_name,
            provider_names=provider_names,
            method_name=method_name,
            scope_key=scope_key,
            model_type=model_type,
        )
        if record is None:
            return None
        return record.result, record.items

    def get_collection_with_metadata(
        self,
        *,
        provider_type: str,
        provider_name: str | None = None,
        provider_names: Iterable[str] | None = None,
        method_name: str,
        scope_key: str,
        model_type: type[ModelT],
    ) -> CachedCollectionRecord[ModelT] | None:
        names = _provider_name_candidates(provider_name=provider_name, provider_names=provider_names)
        with self._connect() as connection:
            for candidate_name in names:
                row = connection.execute(
                    """
                    SELECT provider_name, fetched_at, result_json, items_json
                    FROM cached_fetches
                    WHERE provider_type = ? AND provider_name = ? AND method_name = ? AND scope_key = ?
                    """,
                    (provider_type, candidate_name, method_name, scope_key),
                ).fetchone()
                if row is None:
                    continue
                result = ProviderFetchResult.model_validate(json.loads(row["result_json"]))
                items = [model_type.model_validate(item) for item in json.loads(row["items_json"])]
                return CachedCollectionRecord(
                    provider_name=str(row["provider_name"]),
                    fetched_at=_parse_timestamp(str(row["fetched_at"])),
                    result=result,
                    items=items,
                )
        return None

    def put_collection(
        self,
        *,
        provider_type: str,
        provider_name: str,
        method_name: str,
        scope_key: str,
        result: ProviderFetchResult,
        items: Iterable[BaseModel],
    ) -> None:
        serialized_items = [item.model_dump(mode="json") for item in items]
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO cached_fetches (
                    provider_type,
                    provider_name,
                    method_name,
                    scope_key,
                    fetched_at,
                    result_json,
                    items_json,
                    item_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(provider_type, provider_name, method_name, scope_key)
                DO UPDATE SET
                    fetched_at = excluded.fetched_at,
                    result_json = excluded.result_json,
                    items_json = excluded.items_json,
                    item_count = excluded.item_count
                """,
                (
                    provider_type,
                    provider_name,
                    method_name,
                    scope_key,
                    _isoformat(result.fetched_at),
                    json.dumps(_serialize_fetch_result(result)),
                    json.dumps(serialized_items),
                    len(serialized_items),
                ),
            )

    def delete_collection(
        self,
        *,
        provider_type: str,
        provider_name: str | None = None,
        provider_names: Iterable[str] | None = None,
        method_name: str,
        scope_key: str,
    ) -> int:
        deleted = 0
        names = _provider_name_candidates(provider_name=provider_name, provider_names=provider_names)
        with self._connect() as connection:
            for candidate_name in names:
                cursor = connection.execute(
                    """
                    DELETE FROM cached_fetches
                    WHERE provider_type = ? AND provider_name = ? AND method_name = ? AND scope_key = ?
                    """,
                    (provider_type, candidate_name, method_name, scope_key),
                )
                deleted += int(cursor.rowcount or 0)
        return deleted

    def is_collection_stale(
        self,
        *,
        record: CachedCollectionRecord[Any] | None,
        ttl: timedelta,
        now: datetime | None = None,
    ) -> bool:
        if record is None:
            return True
        effective_now = _utc_now(now)
        return record.fetched_at + ttl <= effective_now

    def is_collection_usable(
        self,
        *,
        record: CachedCollectionRecord[Any] | None,
        target_date: date | None = None,
        ttl: timedelta | None = None,
        now: datetime | None = None,
        allow_past_reuse: bool = False,
    ) -> bool:
        if record is None:
            return False
        effective_now = _utc_now(now)
        if target_date is not None and allow_past_reuse and target_date < effective_now.date():
            return True
        if ttl is None:
            return True
        return not self.is_collection_stale(record=record, ttl=ttl, now=effective_now)

    def get_cached_log_days(
        self,
        *,
        provider_name: str | None = None,
        provider_names: Iterable[str] | None = None,
        requested_days: Iterable[date],
    ) -> set[date]:
        requested = [item.isoformat() for item in requested_days]
        if not requested:
            return set()
        names = _provider_name_candidates(provider_name=provider_name, provider_names=provider_names)
        if not names:
            return set()
        provider_placeholders = ",".join("?" for _ in names)
        day_placeholders = ",".join("?" for _ in requested)
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT game_date
                FROM cached_log_days
                WHERE provider_name IN ({provider_placeholders}) AND game_date IN ({day_placeholders})
                """,
                (*names, *requested),
            ).fetchall()
        return {date.fromisoformat(row["game_date"]) for row in rows}

    def describe_log_day_coverage(
        self,
        *,
        provider_name: str | None = None,
        provider_names: Iterable[str] | None = None,
        requested_days: Iterable[date],
    ) -> tuple[list[date], list[date]]:
        ordered_days = sorted(set(requested_days))
        cached_day_set = self.get_cached_log_days(
            provider_name=provider_name,
            provider_names=provider_names,
            requested_days=ordered_days,
        )
        cached_days = [item for item in ordered_days if item in cached_day_set]
        missing_days = [item for item in ordered_days if item not in cached_day_set]
        return cached_days, missing_days

    def get_missing_log_days(
        self,
        *,
        provider_name: str | None = None,
        provider_names: Iterable[str] | None = None,
        requested_days: Iterable[date],
    ) -> list[date]:
        _, missing_days = self.describe_log_day_coverage(
            provider_name=provider_name,
            provider_names=provider_names,
            requested_days=requested_days,
        )
        return missing_days

    def latest_cached_log_day(
        self,
        *,
        provider_name: str | None = None,
        provider_names: Iterable[str] | None = None,
    ) -> date | None:
        names = _provider_name_candidates(provider_name=provider_name, provider_names=provider_names)
        query = "SELECT MAX(game_date) AS game_date FROM cached_log_days"
        params: tuple[object, ...] = ()
        if names:
            placeholders = ",".join("?" for _ in names)
            query += f" WHERE provider_name IN ({placeholders})"
            params = tuple(names)
        with self._connect() as connection:
            row = connection.execute(query, params).fetchone()
        if row is None or row["game_date"] in (None, ""):
            return None
        return date.fromisoformat(str(row["game_date"]))

    def get_player_game_logs(
        self,
        *,
        provider_name: str | None = None,
        provider_names: Iterable[str] | None = None,
        requested_days: Iterable[date],
    ) -> list[PlayerGameLogPayload]:
        requested = [item.isoformat() for item in requested_days]
        if not requested:
            return []
        names = _provider_name_candidates(provider_name=provider_name, provider_names=provider_names)
        if not names:
            return []
        provider_placeholders = ",".join("?" for _ in names)
        day_placeholders = ",".join("?" for _ in requested)
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT payload_json
                FROM cached_player_game_logs
                WHERE provider_name IN ({provider_placeholders}) AND game_date IN ({day_placeholders})
                ORDER BY normalized_player_name, provider_player_id, game_date, provider_game_id
                """,
                (*names, *requested),
            ).fetchall()
        return dedupe_player_game_log_payloads(
            PlayerGameLogPayload.model_validate(json.loads(row["payload_json"]))
            for row in rows
        )

    def put_player_game_logs(
        self,
        *,
        provider_name: str,
        requested_days: Iterable[date],
        result: ProviderFetchResult,
        logs: Iterable[PlayerGameLogPayload],
    ) -> bool:
        requested = sorted(set(requested_days))
        deduped_logs = dedupe_player_game_log_payloads(logs)
        if any(_payload_game_date(payload) is None for payload in deduped_logs):
            return False

        grouped_logs: dict[date, list[PlayerGameLogPayload]] = defaultdict(list)
        for payload in deduped_logs:
            game_date = _payload_game_date(payload)
            if game_date is None:
                continue
            grouped_logs[game_date].append(payload)

        stored_any_days = False
        with self._connect() as connection:
            for game_day in requested:
                connection.execute(
                    """
                    DELETE FROM cached_player_game_logs
                    WHERE provider_name = ? AND game_date = ?
                    """,
                    (provider_name, game_day.isoformat()),
                )
                day_payloads = dedupe_player_game_log_payloads(grouped_logs.get(game_day, []))
                for payload in sorted(day_payloads, key=_player_log_sort_key):
                    player_name = str(payload.meta.get("player_name") or payload.provider_player_id)
                    connection.execute(
                        """
                        INSERT INTO cached_player_game_logs (
                            provider_name,
                            game_date,
                            provider_game_id,
                            provider_player_id,
                            team_abbreviation,
                            normalized_player_name,
                            payload_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(provider_name, game_date, provider_game_id, normalized_player_name, team_abbreviation)
                        DO UPDATE SET
                            provider_player_id = excluded.provider_player_id,
                            payload_json = excluded.payload_json
                        """,
                        (
                            provider_name,
                            game_day.isoformat(),
                            payload.provider_game_id,
                            payload.provider_player_id,
                            payload.team_abbreviation.strip().upper(),
                            normalize_name(player_name),
                            json.dumps(payload.model_dump(mode="json")),
                        ),
                    )
                connection.execute(
                    """
                    INSERT INTO cached_log_days (
                        provider_name,
                        game_date,
                        fetched_at,
                        endpoint,
                        payload_json,
                        item_count
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(provider_name, game_date)
                    DO UPDATE SET
                        fetched_at = excluded.fetched_at,
                        endpoint = excluded.endpoint,
                        payload_json = excluded.payload_json,
                        item_count = excluded.item_count
                    """,
                    (
                        provider_name,
                        game_day.isoformat(),
                        _isoformat(result.fetched_at),
                        result.endpoint,
                        json.dumps(_log_day_payload_summary(result, game_day, len(day_payloads))),
                        len(day_payloads),
                    ),
                )
                stored_any_days = True
        return stored_any_days

    def repair_legacy_scoped_log_cache(self) -> dict[str, int | bool]:
        """Clear pre-repair player-log cache entries once.

        Older cache rows did not record whether the source provider was scoped
        to the current slate. Those rows can look like complete historical log
        days while actually containing only a handful of teams, so the safest
        migration is to evict the legacy log-day tables and let them refill
        from unscoped history fetches.
        """
        marker_key = "legacy_scoped_log_cache_repair_v1"
        with self._connect() as connection:
            existing = connection.execute(
                "SELECT value FROM cache_metadata WHERE key = ?",
                (marker_key,),
            ).fetchone()
            if existing is not None:
                return {
                    "ran": False,
                    "deleted_provider_cached_log_days": 0,
                    "deleted_provider_cached_logs": 0,
                }
            deleted_log_days = int(
                connection.execute("DELETE FROM cached_log_days").rowcount or 0
            )
            deleted_logs = int(
                connection.execute("DELETE FROM cached_player_game_logs").rowcount or 0
            )
            connection.execute(
                """
                INSERT INTO cache_metadata (key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    updated_at = excluded.updated_at
                """,
                (
                    marker_key,
                    json.dumps(
                        {
                            "deleted_provider_cached_log_days": deleted_log_days,
                            "deleted_provider_cached_logs": deleted_logs,
                        }
                    ),
                    _isoformat(datetime.now(UTC)),
                ),
            )
        return {
            "ran": True,
            "deleted_provider_cached_log_days": deleted_log_days,
            "deleted_provider_cached_logs": deleted_logs,
        }

    def prune_odds_cache(
        self,
        *,
        keep_date: date,
        provider_name: str | None = None,
    ) -> int:
        deleted = 0
        allow_past_reuse = get_settings().provider_cache_allow_past_odds_reuse
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT provider_name, method_name, scope_key
                FROM cached_fetches
                WHERE provider_type = 'odds' AND method_name = 'fetch_upcoming_player_props'
                """
            ).fetchall()
            for row in rows:
                row_provider_name = str(row["provider_name"])
                if provider_name is not None and row_provider_name != provider_name:
                    continue
                try:
                    scope = json.loads(str(row["scope_key"]))
                except json.JSONDecodeError:
                    scope = {}
                raw_target_date = scope.get("target_date")
                if raw_target_date == keep_date.isoformat():
                    continue
                parsed_target_date = _parse_scope_date(raw_target_date)
                if parsed_target_date is not None and allow_past_reuse and parsed_target_date < keep_date:
                    continue
                connection.execute(
                    """
                    DELETE FROM cached_fetches
                    WHERE provider_type = 'odds' AND provider_name = ? AND method_name = ? AND scope_key = ?
                    """,
                    (
                        row_provider_name,
                        str(row["method_name"]),
                        str(row["scope_key"]),
                    ),
                )
                deleted += 1
        return deleted

    def delete_day_scoped_entries(self, *, target_date: date) -> dict[str, int]:
        deleted_fetches = 0
        deleted_log_days = 0
        deleted_logs = 0
        target_date_text = target_date.isoformat()
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT provider_type, provider_name, method_name, scope_key, fetched_at
                FROM cached_fetches
                WHERE provider_type IN ('stats', 'odds', 'injuries')
                """
            ).fetchall()
            for row in rows:
                scope_key = str(row["scope_key"])
                fetched_at = _parse_timestamp(str(row["fetched_at"])).date()
                try:
                    scope = json.loads(scope_key)
                except json.JSONDecodeError:
                    scope = {}
                scope_date = _parse_scope_date(scope.get("target_date"))
                if scope_date != target_date and fetched_at != target_date:
                    continue
                cursor = connection.execute(
                    """
                    DELETE FROM cached_fetches
                    WHERE provider_type = ? AND provider_name = ? AND method_name = ? AND scope_key = ?
                    """,
                    (
                        str(row["provider_type"]),
                        str(row["provider_name"]),
                        str(row["method_name"]),
                        scope_key,
                    ),
                )
                deleted_fetches += int(cursor.rowcount or 0)

            deleted_log_days = int(
                connection.execute(
                    """
                    DELETE FROM cached_log_days
                    WHERE game_date = ?
                    """,
                    (target_date_text,),
                ).rowcount
                or 0
            )
            deleted_logs = int(
                connection.execute(
                    """
                    DELETE FROM cached_player_game_logs
                    WHERE game_date = ?
                    """,
                    (target_date_text,),
                ).rowcount
                or 0
            )
        return {
            "deleted_provider_cached_fetches": deleted_fetches,
            "deleted_provider_cached_log_days": deleted_log_days,
            "deleted_provider_cached_logs": deleted_logs,
        }

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS cache_metadata (
                    key TEXT NOT NULL PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS cached_fetches (
                    provider_type TEXT NOT NULL,
                    provider_name TEXT NOT NULL,
                    method_name TEXT NOT NULL,
                    scope_key TEXT NOT NULL,
                    fetched_at TEXT NOT NULL,
                    result_json TEXT NOT NULL,
                    items_json TEXT NOT NULL,
                    item_count INTEGER NOT NULL,
                    PRIMARY KEY (provider_type, provider_name, method_name, scope_key)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS cached_log_days (
                    provider_name TEXT NOT NULL,
                    game_date TEXT NOT NULL,
                    fetched_at TEXT NOT NULL,
                    endpoint TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    item_count INTEGER NOT NULL,
                    PRIMARY KEY (provider_name, game_date)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS cached_player_game_logs (
                    provider_name TEXT NOT NULL,
                    game_date TEXT NOT NULL,
                    provider_game_id TEXT NOT NULL,
                    provider_player_id TEXT NOT NULL,
                    team_abbreviation TEXT NOT NULL,
                    normalized_player_name TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    PRIMARY KEY (
                        provider_name,
                        game_date,
                        provider_game_id,
                        normalized_player_name,
                        team_abbreviation
                    )
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_cached_player_game_logs_lookup
                ON cached_player_game_logs (
                    provider_name,
                    game_date,
                    normalized_player_name,
                    provider_player_id
                )
                """
            )

    @contextmanager
    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._path)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=NORMAL")
            yield connection
            connection.commit()
        finally:
            connection.close()


def _payload_game_date(payload: PlayerGameLogPayload) -> date | None:
    raw_value = payload.meta.get("game_date")
    if raw_value in (None, ""):
        return None
    return date.fromisoformat(str(raw_value))


def _provider_name_candidates(
    *,
    provider_name: str | None = None,
    provider_names: Iterable[str] | None = None,
) -> list[str]:
    ordered = [provider_name] if provider_name else []
    if provider_names is not None:
        ordered.extend(provider_names)
    return [item for item in dict.fromkeys(str(value) for value in ordered if value)]


def _player_log_sort_key(payload: PlayerGameLogPayload) -> tuple[str, str, str, str]:
    player_name = str(payload.meta.get("player_name") or payload.provider_player_id)
    game_day = str(payload.meta.get("game_date") or "")
    return (
        normalize_name(player_name),
        payload.provider_player_id,
        game_day,
        payload.provider_game_id,
    )


def _isoformat(value: datetime) -> str:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC).isoformat()
    return value.astimezone(UTC).isoformat()


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _utc_now(now: datetime | None = None) -> datetime:
    if now is None:
        return datetime.now(UTC)
    if now.tzinfo is None:
        return now.replace(tzinfo=UTC)
    return now.astimezone(UTC)


def _parse_scope_date(raw_value: object) -> date | None:
    if raw_value in (None, ""):
        return None
    try:
        return date.fromisoformat(str(raw_value))
    except ValueError:
        return None


def _log_day_payload_summary(
    result: ProviderFetchResult,
    game_day: date,
    item_count: int,
) -> dict[str, Any]:
    return {
        "game_date": game_day.isoformat(),
        "endpoint": result.endpoint,
        "fetched_at": result.fetched_at.astimezone(UTC).isoformat()
        if result.fetched_at.tzinfo is not None
        else result.fetched_at.replace(tzinfo=UTC).isoformat(),
        "item_count": item_count,
    }


def _serialize_fetch_result(result: ProviderFetchResult | Any) -> dict[str, Any]:
    if isinstance(result, BaseModel):
        return result.model_dump(mode="json")
    endpoint = getattr(result, "endpoint", None)
    fetched_at = getattr(result, "fetched_at", None)
    payload = getattr(result, "payload", None)
    if endpoint is None or fetched_at is None:
        raise TypeError(f"Unsupported provider fetch result type: {type(result)!r}")
    return {
        "endpoint": str(endpoint),
        "fetched_at": _isoformat(fetched_at) if isinstance(fetched_at, datetime) else str(fetched_at),
        "payload": payload,
    }

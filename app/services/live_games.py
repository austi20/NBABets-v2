from __future__ import annotations

import asyncio
import logging
import time
from datetime import date
from threading import Lock, Thread

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.all import Game
from app.providers.base import StatsProvider
from app.providers.factory import get_stats_provider, iter_configured_stats_provider_chain

logger = logging.getLogger(__name__)

_LIVE_CACHE_TTL_SECONDS = 60.0
_live_game_cache: dict[date, tuple[float, set[str]]] = {}
_live_game_cache_lock = Lock()


def live_provider_game_ids_from_nba_api(target_date: date | None = None) -> set[str]:
    effective_date = target_date or date.today()
    cached = _cached_live_provider_game_ids(effective_date)
    if cached is not None:
        logger.debug("live_games: date=%s source=cache", effective_date.isoformat())
        return cached

    try:
        provider = get_stats_provider()
    except ValueError as exc:
        logger.warning(
            "live_games: date=%s fallback=empty reason=stats_provider_config: %s",
            effective_date.isoformat(),
            exc,
        )
        return set()

    chain = ",".join(iter_configured_stats_provider_chain())
    logger.debug("live_games: date=%s stats_chain=%s", effective_date.isoformat(), chain or "(none)")

    try:
        _, games = _run_provider_schedule(provider, effective_date)
    except Exception as exc:  # pragma: no cover - exercised with live provider
        resolved = getattr(provider, "provider_name", type(provider).__name__)
        logger.warning(
            "live_games: date=%s provider=%s fallback=empty reason=schedule_fetch_failed: %s",
            effective_date.isoformat(),
            resolved,
            exc,
        )
        return set()

    resolved = getattr(provider, "provider_name", type(provider).__name__)
    live_ids = {game.provider_game_id for game in games if game.status == "live" and game.provider_game_id}
    if not live_ids:
        if games:
            logger.debug(
                "live_games: date=%s provider=%s fallback=empty reason=no_live_status_in_schedule games=%d",
                effective_date.isoformat(),
                resolved,
                len(games),
            )
        else:
            logger.debug(
                "live_games: date=%s provider=%s fallback=empty reason=empty_schedule",
                effective_date.isoformat(),
                resolved,
            )
    else:
        logger.debug(
            "live_games: date=%s provider=%s live_ids=%d",
            effective_date.isoformat(),
            resolved,
            len(live_ids),
        )

    with _live_game_cache_lock:
        _live_game_cache[effective_date] = (time.monotonic(), live_ids)
    return set(live_ids)


def sync_live_games_from_nba_api(session: Session, target_date: date | None = None) -> set[int]:
    live_provider_ids = live_provider_game_ids_from_nba_api(target_date)
    if not live_provider_ids:
        return set()

    games = session.scalars(
        select(Game).where(
            Game.provider_game_id.in_(live_provider_ids),
            Game.status.in_(("scheduled", "live")),
        )
    ).all()

    updated_game_ids: set[int] = set()
    for game in games:
        if game.status == "live":
            continue
        game.status = "live"
        updated_game_ids.add(game.game_id)
    if updated_game_ids:
        session.flush()
    return updated_game_ids


def _cached_live_provider_game_ids(target_date: date) -> set[str] | None:
    with _live_game_cache_lock:
        cached = _live_game_cache.get(target_date)
    if cached is None:
        return None
    cached_at, game_ids = cached
    if time.monotonic() - cached_at > _LIVE_CACHE_TTL_SECONDS:
        return None
    return set(game_ids)


def _run_provider_schedule(provider: StatsProvider, target_date: date):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(provider.fetch_schedule(target_date))

    result: tuple[object, object] | None = None
    error: BaseException | None = None

    def runner() -> None:
        nonlocal result, error
        try:
            result = asyncio.run(provider.fetch_schedule(target_date))
        except BaseException as exc:  # pragma: no cover - exercised via async callers
            error = exc

    thread = Thread(target=runner, daemon=True)
    thread.start()
    thread.join()
    if error is not None:
        raise error
    if result is None:
        raise RuntimeError("stats live game check returned no schedule result")
    return result

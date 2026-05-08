"""Resolve the in-memory board cache for API handlers."""

from __future__ import annotations

from typing import cast

from fastapi import HTTPException, Request

from app.server.services.board_cache import BoardCache, BoardCacheEntry


def board_cache_entry_or_503(request: Request) -> BoardCacheEntry:
    """Return cached board data, or build lazily if startup finished but warmup missed.

    StartupCoordinator calls ``board_cache.populate`` after the pipeline completes.
    If that call raises, ``snapshot.completed`` can still be true while ``get_cached()``
    is empty; all board-backed endpoints would otherwise 503 forever.
    """
    cache = cast(BoardCache, request.app.state.board_cache)
    get_cached = getattr(cache, "get_cached", None)
    if not callable(get_cached):
        return cast(BoardCacheEntry, cache.get_or_build())
    entry = get_cached()
    if entry is not None:
        return cast(BoardCacheEntry, entry)
    coordinator = getattr(request.app.state, "startup_coordinator", None)
    if coordinator is not None:
        snapshot = coordinator.snapshot()
        if snapshot.completed and not snapshot.failed:
            return cache.get_or_build()
    raise HTTPException(
        status_code=503,
        detail="Board cache is not ready. Run startup first.",
    )

"""Per-player over_probability bias offsets, loaded from
data/player_bias_offsets.json (produced by scripts/derive_player_bias_offsets.py).

The offsets are Bayesian-shrunk over the per-market prior, so they degrade
gracefully when a player has few samples. At runtime we look them up by the
local DB player_id keyed against the provider_player_id (BallDontLie ID) used
in the historical grading set. Callers should treat this lookup as
informational: if no entry exists, fall back to the per-market offset
(see Settings.per_market_bias_offsets) and finally the global offset.
"""
from __future__ import annotations

import json
import logging
from functools import lru_cache
from pathlib import Path

from app.config.settings import get_settings

_log = logging.getLogger(__name__)


def _default_offsets_path() -> Path:
    # Repo-root/data/player_bias_offsets.json; ride the same root as Settings.
    candidate = Path(__file__).resolve().parents[2] / "data" / "player_bias_offsets.json"
    return candidate


@lru_cache(maxsize=1)
def _load_offsets() -> dict[str, float]:
    """Return a flat {provider_player_id: offset} dict, or empty when missing."""
    path = _default_offsets_path()
    if not path.exists():
        _log.info("player_bias: %s not found; per-player offsets disabled", path)
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001 - never break the request path
        _log.warning("player_bias: failed to load %s (%s); skipping", path, exc)
        return {}
    rows = payload.get("offsets") or {}
    flat: dict[str, float] = {}
    for pid, entry in rows.items():
        offset = entry.get("offset")
        if isinstance(offset, (int, float)):
            flat[str(pid)] = float(offset)
    _log.info("player_bias: loaded %d per-player offsets from %s", len(flat), path)
    return flat


@lru_cache(maxsize=4096)
def _provider_id_for_db(db_player_id: int) -> str | None:
    """Translate a local DB player_id to the provider_player_id used in the offsets file."""
    # Local-import to avoid pulling SQLAlchemy at module import time.
    from sqlalchemy import select

    from app.db.session import session_scope
    from app.models.all import Player

    with session_scope() as session:
        provider_id = session.scalar(
            select(Player.provider_player_id).where(Player.player_id == db_player_id)
        )
    if provider_id is None:
        return None
    return str(provider_id)


def get_player_bias_offset(db_player_id: int | None) -> float | None:
    """Return the per-player over_probability offset for a local DB player_id,
    or None when no per-player entry exists."""
    if db_player_id is None:
        return None
    if not get_settings().player_bias_enabled:
        return None
    offsets = _load_offsets()
    if not offsets:
        return None
    provider_id = _provider_id_for_db(int(db_player_id))
    if provider_id is None:
        return None
    return offsets.get(provider_id)


def reset_caches() -> None:
    """Test helper: clear the lru_caches so subsequent calls re-read the file."""
    _load_offsets.cache_clear()
    _provider_id_for_db.cache_clear()


def effective_over_bias_offset(player_id: int | None, market_key: str | None) -> float:
    """Return the over-probability bias offset for a (player, market) pair.

    Precedence (highest to lowest):
      1. Per-player learned offset (data/player_bias_offsets.json)
      2. Per-market offset (Settings.per_market_bias_offsets)
      3. Global Settings.over_probability_bias_offset

    Positive offset => tilt over_probability DOWN toward UNDER (model is
    bullish here). Negative offset => tilt UP toward OVER. The same number
    is used by both prop_analysis._quote_recommendation (which picks the
    recommended side) and board_cache (which composes the volatility-shrunk
    probability), so the chain stays consistent end-to-end.
    """
    settings = get_settings()
    offset = settings.over_probability_bias_offset
    if market_key is not None:
        per_market = settings.per_market_bias_offsets.get(market_key.lower())
        if per_market is not None:
            offset = per_market
    player_offset = get_player_bias_offset(player_id)
    if player_offset is not None:
        offset = player_offset
    return offset

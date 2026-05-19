"""Disk cache for FeatureEngineer.build_training_frame output.

Hash-based: cache key derived from historical DataFrame content + kwargs.
Storage: parquet (frame) + json (feature_columns) under <cache_dir>/<hex>/
On any I/O failure the cache is bypassed transparently.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import pandas as pd

if TYPE_CHECKING:
    from app.training.features import FeatureSet

logger = logging.getLogger(__name__)


def _hash_dataframe(df: pd.DataFrame) -> str:
    """Cheap stable hash of a DataFrame (shape + columns + content)."""
    h = hashlib.blake2b(digest_size=16)
    h.update(str(df.shape).encode())
    h.update(str(list(df.columns)).encode())
    row_hashes = pd.util.hash_pandas_object(df, index=True)
    h.update(row_hashes.values.tobytes())
    return h.hexdigest()


def _cache_key(df_hex: str, kwargs: dict[str, Any], builder: Callable[..., Any]) -> str:
    h = hashlib.blake2b(digest_size=20)
    h.update(df_hex.encode())
    h.update(repr(sorted(kwargs.items())).encode())
    h.update(getattr(builder, "__qualname__", repr(builder)).encode())
    return h.hexdigest()


def cached_build(
    historical: pd.DataFrame,
    builder: Callable[..., FeatureSet],
    **kwargs: Any,
) -> FeatureSet:
    """Return builder(historical, **kwargs), reading/writing a disk cache.

    Cache is skipped entirely when feature_cache_enabled=False.
    Any I/O error falls back to a live build with a logged warning.
    """
    from app.config.settings import get_settings
    from app.training.features import FeatureSet

    settings = get_settings()
    if not settings.feature_cache_enabled:
        return builder(historical, **kwargs)

    cache_dir = settings.feature_cache_dir

    try:
        df_hex = _hash_dataframe(historical)
        key = _cache_key(df_hex, kwargs, builder)
        entry_dir = cache_dir / key

        frame_path = entry_dir / "frame.parquet"
        cols_path = entry_dir / "feature_columns.json"

        if frame_path.is_file() and cols_path.is_file():
            frame = pd.read_parquet(frame_path)
            feature_columns: list[str] = json.loads(cols_path.read_text(encoding="utf-8"))
            logger.debug("feature_cache: hit key=%s", key[:12])
            return FeatureSet(frame=frame, feature_columns=feature_columns)
    except Exception as exc:  # noqa: BLE001
        logger.warning("feature_cache: load failed, rebuilding (%s)", exc)
        return builder(historical, **kwargs)

    result = builder(historical, **kwargs)

    try:
        entry_dir.mkdir(parents=True, exist_ok=True)
        result.frame.to_parquet(frame_path, index=False)
        cols_path.write_text(json.dumps(result.feature_columns), encoding="utf-8")
        logger.debug("feature_cache: wrote key=%s", key[:12])
    except Exception as exc:  # noqa: BLE001
        logger.warning("feature_cache: write failed, continuing without cache (%s)", exc)

    return result

from __future__ import annotations

from unittest.mock import patch

from app.config.settings import Settings
from app.providers.factory import get_stats_provider


def test_get_stats_provider_returns_wrapped_single_provider() -> None:
    settings = Settings.model_construct(stats_provider="nba_api")
    with patch("app.providers.factory.get_settings", return_value=settings):
        provider = get_stats_provider()

    assert provider is not None
    # Single-provider reality: no multi-entry rotation aliases should be attached.
    aliases = getattr(provider, "_cache_provider_aliases", None)
    assert aliases is None or len(aliases) <= 1

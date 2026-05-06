from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from app.config.settings import Settings, get_settings
from app.providers.factory import (
    ODDS_PROVIDER_PROFILES,
    STATS_PROVIDER_PROFILES,
    get_injuries_provider,
    get_odds_provider,
    get_provider_capability_profile,
    iter_configured_odds_provider_chain,
    iter_configured_stats_provider_chain,
)
from app.providers.payload_schema import (
    ProviderPayloadSchemaError,
    validate_odds_provider_props_response,
    validate_stats_provider_response,
)
from app.schemas.domain import ProviderFetchResult


def _fr(payload: dict[str, Any] | list[Any]) -> ProviderFetchResult:
    return ProviderFetchResult(
        endpoint="unit://test",
        fetched_at=datetime.now(UTC),
        payload=payload,
    )


def test_validate_stats_rejects_missing_wrapped_keys() -> None:
    bad = _fr({"meta": {}})
    with pytest.raises(ProviderPayloadSchemaError, match="missing required keys"):
        validate_stats_provider_response("nba_api", "fetch_teams", bad)


def test_validate_stats_accepts_wrapped_tabular() -> None:
    good = _fr({"data": [], "meta": {"record_count": 0}})
    validate_stats_provider_response("nba_api", "fetch_player_game_logs", good)


def test_validate_odds_balldontlie_rejects_missing_keys() -> None:
    with pytest.raises(ProviderPayloadSchemaError, match="missing required keys"):
        validate_odds_provider_props_response("balldontlie", _fr({"success": True}))


def test_capability_profiles_cover_configured_chains() -> None:
    stats_cfg = Settings.model_construct(
        stats_provider="nba_api",
    )
    for name in iter_configured_stats_provider_chain(stats_cfg):
        assert name in STATS_PROVIDER_PROFILES
        assert get_provider_capability_profile(name) is not None

    bdl_odds_cfg = Settings.model_construct(
        odds_provider="balldontlie",
        balldontlie_api_key="unit-bdl",
    )
    for name in iter_configured_odds_provider_chain(bdl_odds_cfg):
        assert name in ODDS_PROVIDER_PROFILES


def test_factory_rejects_noop_odds_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ODDS_PROVIDER", "none")
    get_settings.cache_clear()
    try:
        with pytest.raises(ValueError, match="ODDS_PROVIDER must be set to a real provider"):
            get_odds_provider()
    finally:
        get_settings.cache_clear()


def test_factory_rejects_noop_injuries_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INJURY_PROVIDER", "none")
    get_settings.cache_clear()
    try:
        with pytest.raises(ValueError, match="INJURY_PROVIDER must be set to a real provider"):
            get_injuries_provider()
    finally:
        get_settings.cache_clear()

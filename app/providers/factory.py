from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Literal, cast

from app.config.settings import Settings, get_settings
from app.providers.base import BaseProvider, InjuriesProvider, OddsProvider, StatsProvider
from app.providers.cached import wrap_with_provider_cache
from app.providers.live import BallDontLieInjuriesProvider, BallDontLieOddsProvider, BallDontLieStatsProvider
from app.providers.stats.nba_api import NbaApiStatsProvider

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProviderCapabilityProfile:
    """Static capability map for diagnostics, routing, and API-drift triage."""

    name: str
    kind: Literal["stats", "odds", "injuries"]
    implemented_fetch_methods: frozenset[str]
    optional_features: frozenset[str] = frozenset()
    notes: str = ""


_STATS_CORE = frozenset(
    {
        "fetch_teams",
        "fetch_rosters",
        "fetch_schedule",
        "fetch_player_game_logs",
        "healthcheck",
    }
)

STATS_PROVIDER_PROFILES: dict[str, ProviderCapabilityProfile] = {
    "nba_api": ProviderCapabilityProfile(
        name="nba_api",
        kind="stats",
        implemented_fetch_methods=_STATS_CORE
        | frozenset({"fetch_schedule_range", "verify_required_access", "fetch_game_availability"}),
        optional_features=frozenset({"boxscore_enrichment"}),
        notes="Historical box-score source; parquet loader wraps this for training data.",
    ),
    "balldontlie": ProviderCapabilityProfile(
        name="balldontlie",
        kind="stats",
        implemented_fetch_methods=_STATS_CORE | frozenset({"fetch_schedule_range", "verify_required_access"}),
        notes="Live stats source; requires GOAT tier for /stats endpoint.",
    ),
}

_ODDS_CORE = frozenset({"fetch_upcoming_player_props", "healthcheck"})

ODDS_PROVIDER_PROFILES: dict[str, ProviderCapabilityProfile] = {
    "balldontlie": ProviderCapabilityProfile(
        name="balldontlie",
        kind="odds",
        implemented_fetch_methods=_ODDS_CORE,
        notes="v2 /odds/player_props per game_id; requires tier with betting odds.",
    ),
}

INJURIES_PROVIDER_PROFILES: dict[str, ProviderCapabilityProfile] = {
    "balldontlie": ProviderCapabilityProfile(
        name="balldontlie",
        kind="injuries",
        implemented_fetch_methods=frozenset({"fetch_injuries", "healthcheck"}),
        notes="v1 /player_injuries feed filtered to scheduled teams when a target date is provided.",
    ),
}


def get_provider_capability_profile(provider_name: str) -> ProviderCapabilityProfile | None:
    name = provider_name.lower()
    if name in STATS_PROVIDER_PROFILES:
        return STATS_PROVIDER_PROFILES[name]
    if name in ODDS_PROVIDER_PROFILES:
        return ODDS_PROVIDER_PROFILES[name]
    if name in INJURIES_PROVIDER_PROFILES:
        return INJURIES_PROVIDER_PROFILES[name]
    return None


def iter_configured_stats_provider_chain(settings: Settings | None = None) -> tuple[str, ...]:
    """Ordered upstream stats providers for the active settings."""
    cfg = settings or get_settings()
    return _configured_stats_provider_names(cfg)


def iter_configured_odds_provider_chain(settings: Settings | None = None) -> tuple[str, ...]:
    """Ordered upstream odds providers."""
    cfg = settings or get_settings()
    return _configured_odds_provider_names(cfg)


def get_stats_provider() -> StatsProvider:
    settings = get_settings()
    provider = settings.stats_provider.lower()
    if provider == "nba_api":
        return cast(StatsProvider, wrap_with_provider_cache(NbaApiStatsProvider()))
    if provider == "balldontlie":
        if not settings.balldontlie_api_key:
            raise ValueError("STATS_PROVIDER=balldontlie requires BALLDONTLIE_API_KEY to be set")
        return cast(StatsProvider, wrap_with_provider_cache(BallDontLieStatsProvider()))
    raise ValueError(f"Unsupported stats provider: {settings.stats_provider}")


def get_odds_provider() -> OddsProvider:
    settings = get_settings()
    provider = settings.odds_provider.lower()
    if provider in {"", "none", "null"}:
        raise ValueError("ODDS_PROVIDER must be set to a real provider (balldontlie)")
    if provider == "balldontlie":
        if not settings.balldontlie_api_key:
            raise ValueError("ODDS_PROVIDER=balldontlie requires BALLDONTLIE_API_KEY to be set")
        return cast(OddsProvider, wrap_with_provider_cache(BallDontLieOddsProvider()))
    raise ValueError(f"Unsupported odds provider: {settings.odds_provider}")


def get_injuries_provider() -> InjuriesProvider:
    settings = get_settings()
    provider = settings.injury_provider.lower()
    if provider in {"", "none", "null"}:
        raise ValueError("INJURY_PROVIDER must be set to a real provider (balldontlie)")
    if provider == "balldontlie":
        if not settings.balldontlie_api_key:
            raise ValueError("INJURY_PROVIDER=balldontlie requires BALLDONTLIE_API_KEY to be set")
        return cast(InjuriesProvider, wrap_with_provider_cache(BallDontLieInjuriesProvider()))
    raise ValueError(f"Unsupported injuries provider: {settings.injury_provider}")


def _configured_stats_provider_names(settings: Settings) -> tuple[str, ...]:
    provider = settings.stats_provider.lower()
    if provider in {"nba_api", "balldontlie"}:
        return (provider,)
    return tuple()


def _configured_odds_provider_names(settings: Settings) -> tuple[str, ...]:
    provider = settings.odds_provider.lower()
    if provider in {"", "none", "null"}:
        return tuple()
    if provider == "balldontlie":
        return ("balldontlie",)
    return tuple()


def _attach_cache_aliases(provider: BaseProvider, upstream_providers: Sequence[BaseProvider]) -> None:
    provider_names = [item.provider_name for item in upstream_providers if item.provider_name]
    cache_alias_provider = cast(Any, provider)
    cache_alias_provider._cache_provider_aliases = provider_names + [provider.provider_name]
    if provider_names:
        cache_alias_provider._cache_primary_provider_name = provider_names[0]

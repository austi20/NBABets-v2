from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date

from app.schemas.domain import (
    GamePayload,
    InjuryPayload,
    LineSnapshotPayload,
    PlayerGameLogPayload,
    PlayerPayload,
    ProviderFetchResult,
    TeamPayload,
)


class BaseProvider(ABC):
    provider_name: str
    provider_type: str

    @abstractmethod
    async def healthcheck(self) -> bool:
        raise NotImplementedError


class StatsProvider(BaseProvider, ABC):
    provider_type = "stats"

    @abstractmethod
    async def fetch_teams(self) -> tuple[ProviderFetchResult, list[TeamPayload]]:
        raise NotImplementedError

    @abstractmethod
    async def fetch_rosters(self) -> tuple[ProviderFetchResult, list[PlayerPayload]]:
        raise NotImplementedError

    @abstractmethod
    async def fetch_schedule(self, target_date: date) -> tuple[ProviderFetchResult, list[GamePayload]]:
        raise NotImplementedError

    @abstractmethod
    async def fetch_player_game_logs(
        self,
        start_date: date,
        end_date: date,
    ) -> tuple[ProviderFetchResult, list[PlayerGameLogPayload]]:
        raise NotImplementedError


class OddsProvider(BaseProvider, ABC):
    provider_type = "odds"

    @abstractmethod
    async def fetch_upcoming_player_props(
        self,
        target_date: date,
    ) -> tuple[ProviderFetchResult, list[LineSnapshotPayload]]:
        raise NotImplementedError


class InjuriesProvider(BaseProvider, ABC):
    provider_type = "injuries"

    @abstractmethod
    async def fetch_injuries(
        self,
        target_date: date | None = None,
    ) -> tuple[ProviderFetchResult, list[InjuryPayload]]:
        raise NotImplementedError

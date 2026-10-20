from __future__ import annotations

import logging
from datetime import date
from time import time

import httpx

from app.providers.base import OddsProvider
from app.schemas.domain import LineSnapshotPayload, ProviderFetchResult

logger = logging.getLogger(__name__)

# BallDontLie odds and player props need the GOAT tier. Once a key is refused,
# skip it for a while instead of failing every refresh.
_RECHECK_SECONDS = 6 * 60 * 60
_denied_until = 0.0


class OddsWithKalshiFallback(OddsProvider):
    """Use the primary odds feed, or Kalshi when the key has no odds access."""

    provider_name = "balldontlie"

    def __init__(self, primary: OddsProvider, fallback: OddsProvider) -> None:
        self._primary = primary
        self._fallback = fallback
        self.provider_name = primary.provider_name

    async def healthcheck(self) -> bool:
        return await self._primary.healthcheck() or await self._fallback.healthcheck()

    async def fetch_upcoming_player_props(
        self,
        target_date: date,
    ) -> tuple[ProviderFetchResult, list[LineSnapshotPayload]]:
        global _denied_until
        if time() >= _denied_until:
            try:
                self.provider_name = self._primary.provider_name
                return await self._primary.fetch_upcoming_player_props(target_date)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code not in (401, 403):
                    raise
            except RuntimeError as exc:
                # repeated denials open the HTTP circuit breaker
                if "Circuit open" not in str(exc):
                    raise
            _denied_until = time() + _RECHECK_SECONDS
            logger.warning(
                "%s key has no odds access (needs GOAT tier). Using %s prices instead.",
                self._primary.provider_name,
                self._fallback.provider_name,
            )
        self.provider_name = self._fallback.provider_name
        return await self._fallback.fetch_upcoming_player_props(target_date)

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from typing import Any

import httpx

from app.config.settings import get_settings
from app.providers.base import InjuriesProvider
from app.providers.http import HttpProviderMixin
from app.schemas.domain import InjuryPayload, ProviderFetchResult

logger = logging.getLogger(__name__)


class BallDontLieInjuriesProvider(HttpProviderMixin, InjuriesProvider):
    provider_name = "balldontlie"
    base_url = "https://api.balldontlie.io/v1"

    def __init__(self) -> None:
        super().__init__()
        self._api_key = get_settings().balldontlie_api_key
        self._team_lookup: dict[int, str] = {}

    async def healthcheck(self) -> bool:
        try:
            result = await self._get("/player_injuries", headers=self._headers(), params={"per_page": 1})
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in {401, 403, 404}:
                logger.warning("balldontlie injuries healthcheck unavailable: %s", exc)
                return False
            raise
        return "data" in result.payload

    async def fetch_injuries(
        self,
        target_date: date | None = None,
    ) -> tuple[ProviderFetchResult, list[InjuryPayload]]:
        await self._load_team_lookup()
        game_lookup: dict[int, str] = {}
        params: dict[str, Any] = {}

        if target_date is not None:
            game_result = await self._get_paginated(
                "/games",
                params={"dates[]": [target_date.isoformat()], "per_page": 100},
            )
            scheduled_team_ids: set[int] = set()
            for item in game_result.payload.get("data", []):
                game_id = item.get("id")
                if game_id is None:
                    continue
                for side_key in ("home_team", "visitor_team"):
                    team = item.get(side_key) or {}
                    team_id = team.get("id")
                    if team_id is None:
                        continue
                    scheduled_team_ids.add(int(team_id))
                    game_lookup[int(team_id)] = str(game_id)
            if scheduled_team_ids:
                params["team_ids[]"] = sorted(scheduled_team_ids)

        try:
            result = await self._get_paginated("/player_injuries", params=params)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in {401, 403, 404}:
                logger.warning("balldontlie injuries endpoint unavailable, continuing without injuries: %s", exc)
                return self._empty_result(reason=f"endpoint_unavailable_{exc.response.status_code}"), []
            raise

        injuries: list[InjuryPayload] = []
        for item in result.payload.get("data", []):
            player = item.get("player") or {}
            player_id = player.get("id")
            team_id = player.get("team_id")
            if player_id is None or team_id is None:
                continue
            team_abbreviation = self._team_lookup.get(int(team_id))
            if not team_abbreviation:
                continue
            status = str(item.get("status") or "unknown").strip() or "unknown"
            description = str(item.get("description") or "").strip() or None
            injuries.append(
                InjuryPayload(
                    provider_player_id=str(player_id),
                    team_abbreviation=team_abbreviation,
                    report_timestamp=result.fetched_at,
                    status=status,
                    designation=None,
                    body_part=_extract_body_part(description),
                    notes=description,
                    expected_availability_flag=_expected_availability_flag(status),
                    provider_game_id=game_lookup.get(int(team_id)),
                )
            )
        return result, injuries

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if self._api_key:
            headers["Authorization"] = self._api_key
        return headers

    async def _load_team_lookup(self) -> None:
        if self._team_lookup:
            return
        result = await self._get_paginated("/teams")
        self._team_lookup = {
            int(item["id"]): str(item["abbreviation"])
            for item in result.payload.get("data", [])
            if item.get("id") is not None and item.get("abbreviation")
        }

    async def _get_paginated(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
    ) -> ProviderFetchResult:
        query_params = dict(params or {})
        query_params.setdefault("per_page", 100)
        all_rows: list[dict[str, Any]] = []
        pages = 0
        cursor = query_params.pop("cursor", None)
        final_result: ProviderFetchResult | None = None

        while True:
            page_params = dict(query_params)
            if cursor is not None:
                page_params["cursor"] = cursor
            result = await self._get(endpoint, headers=self._headers(), params=page_params)
            final_result = result
            payload = result.payload
            all_rows.extend(payload.get("data", []))
            pages += 1
            cursor = payload.get("meta", {}).get("next_cursor")
            if cursor in (None, ""):
                break

        if final_result is None:
            raise RuntimeError(f"No response returned for endpoint {endpoint}")
        return final_result.model_copy(
            update={
                "payload": {
                    "data": all_rows,
                    "meta": {
                        "page_count": pages,
                        "record_count": len(all_rows),
                        "per_page": query_params.get("per_page"),
                    },
                }
            }
        )

    def _empty_result(self, *, reason: str) -> ProviderFetchResult:
        return ProviderFetchResult(
            endpoint=f"{self.base_url}/player_injuries",
            fetched_at=datetime.now(UTC),
            payload={
                "data": [],
                "meta": {
                    "record_count": 0,
                    "reason": reason,
                },
            },
        )


def _expected_availability_flag(status: str) -> bool | None:
    normalized = status.strip().lower()
    if normalized in {"out", "inactive", "suspended"}:
        return False
    if normalized in {"available", "active", "probable"}:
        return True
    return None


def _extract_body_part(description: str | None) -> str | None:
    if not description:
        return None
    start = description.find("(")
    end = description.find(")", start + 1)
    if start == -1 or end == -1 or end <= start + 1:
        return None
    return description[start + 1 : end].strip() or None

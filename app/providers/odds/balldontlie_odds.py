from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from typing import Any, cast

import httpx
from dateutil.parser import isoparse

from app.config.settings import get_settings
from app.providers.base import OddsProvider
from app.providers.http import HttpProviderMixin
from app.schemas.domain import LineOutcomePayload, LineSnapshotPayload, ProviderFetchResult

logger = logging.getLogger(__name__)

# Base host; call v1 and v2 paths explicitly (stats use v1, odds props are v2).
_BASE = "https://api.balldontlie.io"

_PROP_TYPE_TO_MARKET: dict[str, str] = {
    "points": "points",
    "rebounds": "rebounds",
    "assists": "assists",
    "threes": "threes",
    "points_rebounds_assists": "pra",
}


class BallDontLieOddsProvider(HttpProviderMixin, OddsProvider):
    """Fetches NBA player props from Ball Dont Lie v2 (per-game player_props)."""

    provider_name = "balldontlie"
    base_url = _BASE

    def __init__(self) -> None:
        super().__init__()
        self._api_key = get_settings().balldontlie_api_key

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if self._api_key:
            headers["Authorization"] = self._api_key
        return headers

    async def healthcheck(self) -> bool:
        result = await self._get("/v1/teams", headers=self._headers(), params={"per_page": 1})
        return isinstance(result.payload, dict) and "data" in result.payload

    async def fetch_upcoming_player_props(self, target_date: date) -> tuple[ProviderFetchResult, list[LineSnapshotPayload]]:
        if not self._api_key:
            raise ValueError("ODDS_PROVIDER=balldontlie requires BALLDONTLIE_API_KEY to be set")

        games = await self._fetch_games_for_date(target_date)
        logger.debug(
            "balldontlie odds: games for date",
            extra={"target_date": target_date.isoformat(), "game_count": len(games)},
        )

        raw_rows: list[dict[str, Any]] = []
        snapshots: list[LineSnapshotPayload] = []
        for game in games:
            gid = game.get("id")
            if gid is None:
                continue
            props = await self._fetch_all_player_props(int(gid))
            raw_rows.extend(props)
            snapshots.extend(parse_bdl_player_props(game, props))

        merged = {
            "data": raw_rows,
            "meta": {
                "record_count": len(raw_rows),
                "snapshot_count": len(snapshots),
                "game_count": len(games),
                "target_date": target_date.isoformat(),
            },
        }
        return (
            ProviderFetchResult(
                endpoint=f"{_BASE}/v2/odds/player_props",
                fetched_at=datetime.now(UTC),
                payload=merged,
            ),
            snapshots,
        )

    async def _fetch_games_for_date(self, target_date: date) -> list[dict[str, Any]]:
        date_str = target_date.isoformat()
        all_rows: list[dict[str, Any]] = []
        cursor: str | None = None
        final_endpoint = ""
        while True:
            params: dict[str, Any] = {"dates[]": [date_str], "per_page": 100}
            if cursor is not None:
                params["cursor"] = cursor
            result = await self._get("/v1/games", headers=self._headers(), params=params)
            final_endpoint = result.endpoint
            payload = result.payload if isinstance(result.payload, dict) else {}
            for item in payload.get("data") or []:
                if _game_calendar_date(item) == target_date:
                    all_rows.append(item)
            cursor = (payload.get("meta") or {}).get("next_cursor")
            if cursor in (None, ""):
                break
        logger.debug("balldontlie odds: loaded games", extra={"endpoint": final_endpoint, "count": len(all_rows)})
        return all_rows

    async def _fetch_all_player_props(self, game_id: int) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            params: dict[str, Any] = {"game_id": game_id, "per_page": 100}
            if cursor is not None:
                params["cursor"] = cursor
            try:
                result = await self._get("/v2/odds/player_props", headers=self._headers(), params=params)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404:
                    logger.debug(
                        "balldontlie odds: no player_props for game",
                        extra={"game_id": game_id},
                    )
                    break
                raise
            payload = result.payload if isinstance(result.payload, dict) else {}
            batch = list(payload.get("data") or [])
            rows.extend(batch)
            cursor = (payload.get("meta") or {}).get("next_cursor")
            if cursor in (None, ""):
                break
        return rows


def _game_calendar_date(item: dict[str, Any]) -> date | None:
    raw = item.get("date") or item.get("datetime")
    if not raw:
        return None
    try:
        return cast(date, isoparse(str(raw)).date())
    except (TypeError, ValueError):
        return None


def _team_abbrev(team: object) -> str | None:
    if not isinstance(team, dict):
        return None
    abbr = team.get("abbreviation")
    return str(abbr).strip() if abbr else None


def _player_name_from_row(row: dict[str, Any]) -> str:
    direct = row.get("player_name") or row.get("name")
    if direct:
        return str(direct).strip()
    player = row.get("player")
    if isinstance(player, dict):
        full = player.get("full_name")
        if full:
            return str(full).strip()
        fn = str(player.get("first_name") or "").strip()
        ln = str(player.get("last_name") or "").strip()
        combined = f"{fn} {ln}".strip()
        if combined:
            return combined
    return ""


def _coerce_prop_type(row: dict[str, Any]) -> str | None:
    pt = row.get("prop_type") or row.get("stat") or row.get("stat_id")
    if pt is None:
        return None
    return str(pt).strip().lower() or None


def _market_dict(row: dict[str, Any]) -> dict[str, Any]:
    m = row.get("market")
    return m if isinstance(m, dict) else {}


def _vendor_key(row: dict[str, Any]) -> str:
    v = row.get("vendor") or row.get("sportsbook") or row.get("bookmaker") or "unknown"
    return str(v).strip().lower() or "unknown"


def parse_bdl_player_props(game: dict[str, Any], rows: list[dict[str, Any]]) -> list[LineSnapshotPayload]:
    """Map BDL player_prop rows into LineSnapshotPayload (over_under only)."""
    game_id = str(game.get("id") or "")
    home = _team_abbrev(game.get("home_team"))
    away = _team_abbrev(game.get("visitor_team"))
    event_start = str(game.get("datetime") or game.get("date") or "")
    status = str(game.get("status") or "scheduled").lower()

    out: list[LineSnapshotPayload] = []
    for row in rows:
        market = _market_dict(row)
        mtype = str(market.get("type") or row.get("market_type") or "").lower()
        if mtype and mtype != "over_under":
            continue

        prop_type = _coerce_prop_type(row)
        market_key = _PROP_TYPE_TO_MARKET.get(prop_type or "") if prop_type else None
        if market_key is None:
            continue

        line_raw = row.get("line_value") if row.get("line_value") is not None else row.get("line") or row.get("point")
        if line_raw in (None, ""):
            continue
        try:
            line_value = float(str(line_raw).strip())
        except ValueError:
            continue

        over_raw = market.get("over_odds") if "over_odds" in market else market.get("over")
        under_raw = market.get("under_odds") if "under_odds" in market else market.get("under")
        over_odds = _safe_int_odds(over_raw)
        under_odds = _safe_int_odds(under_raw)
        if over_odds is None and under_odds is None:
            continue

        player_id = row.get("player_id")
        provider_player_id = str(player_id).strip() if player_id is not None else ""
        if not provider_player_id:
            continue

        ts_raw = row.get("updated_at") or row.get("updatedAt") or game.get("date")
        timestamp = _parse_ts(ts_raw) or datetime.now(UTC)

        player_name = _player_name_from_row(row)
        meta = {
            "player_name": player_name,
            "home_team_abbreviation": home,
            "away_team_abbreviation": away,
            "event_start_time": event_start,
            "odds_source_provider": "balldontlie",
            "odds_verification_status": "provider_live",
            "is_live_quote": True,
            "is_alternate_line": False,
            "source_prop_type": prop_type,
            "sportsbook_key": _vendor_key(row),
            "provider_event_id": game_id,
        }

        out.append(
            LineSnapshotPayload(
                timestamp=timestamp,
                provider_game_id=game_id,
                sportsbook_key=_vendor_key(row),
                provider_player_id=provider_player_id,
                market_key=market_key,
                line_value=line_value,
                over=LineOutcomePayload(side="over", odds=over_odds),
                under=LineOutcomePayload(side="under", odds=under_odds),
                event_status=status,
                meta=meta,
            )
        )
    return out


def _safe_int_odds(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _parse_ts(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = isoparse(str(value))
        if parsed.tzinfo is None:
            return cast(datetime, parsed.replace(tzinfo=UTC))
        return cast(datetime, parsed.astimezone(UTC))
    except (TypeError, ValueError):
        return None

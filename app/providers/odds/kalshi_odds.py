from __future__ import annotations

import logging
import re
from collections import defaultdict
from datetime import UTC, date, datetime
from typing import Any

from app.config.settings import get_settings
from app.providers.base import OddsProvider
from app.providers.http import HttpProviderMixin
from app.schemas.domain import LineOutcomePayload, LineSnapshotPayload, ProviderFetchResult

logger = logging.getLogger(__name__)

# Kalshi series -> our market key. No turnovers series exists.
SERIES_TO_MARKET: dict[str, str] = {
    "KXNBAPTS": "points",
    "KXNBAREB": "rebounds",
    "KXNBAAST": "assists",
    "KXNBA3PT": "threes",
    "KXNBAPRA": "pra",
}

_MONTH_NAMES = ("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC")
_MONTHS = {name: number for number, name in enumerate(_MONTH_NAMES, start=1)}
_EVENT_RE = re.compile(r"^(?P<series>[A-Z0-9]+)-(?P<yy>\d{2})(?P<mon>[A-Z]{3})(?P<dd>\d{2})(?P<teams>[A-Z]{6})$")
_TITLE_RE = re.compile(r"^(?P<name>.+?):\s*\d+\+")
_PAGE_LIMIT = 1000


def parse_event_ticker(event_ticker: str) -> tuple[date, str, str] | None:
    """KXNBAPTS-26OCT21DALHOU -> (2026-10-21, away DAL, home HOU)."""
    match = _EVENT_RE.match(event_ticker)
    if match is None or match["mon"] not in _MONTHS:
        return None
    try:
        game_date = date(2000 + int(match["yy"]), _MONTHS[match["mon"]], int(match["dd"]))
    except ValueError:
        return None
    teams = match["teams"]
    return game_date, teams[:3], teams[3:]


def american_from_price(price: float) -> int | None:
    """Dollar price of a $1 contract -> American odds for buying it."""
    if price <= 0.0 or price >= 1.0:
        return None
    if price < 0.5:
        return round(100 * (1 - price) / price)
    return round(-100 * price / (1 - price))


def _price(market: dict[str, Any], key: str) -> float | None:
    raw = market.get(key)
    if raw in (None, ""):
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def parse_kalshi_player_props(
    markets: list[dict[str, Any]],
    target_date: date,
    now: datetime,
) -> list[LineSnapshotPayload]:
    """Map Kalshi "X+" player markets into over/under snapshots.

    Over is the cost of buying YES, under the cost of buying NO. Asks are used
    on both sides because that is what you would actually pay.
    """
    rows: list[tuple[tuple[str, str, str], float, LineSnapshotPayload]] = []
    for market in markets:
        event_ticker = str(market.get("event_ticker") or "")
        event = parse_event_ticker(event_ticker)
        if event is None:
            continue
        game_date, away, home = event
        if game_date != target_date:
            continue
        series = event_ticker.split("-", 1)[0]
        market_key = SERIES_TO_MARKET.get(series)
        title = _TITLE_RE.match(str(market.get("title") or ""))
        strike = _price(market, "floor_strike")
        if market_key is None or title is None or strike is None:
            continue
        yes_ask = _price(market, "yes_ask_dollars")
        no_ask = _price(market, "no_ask_dollars")
        if yes_ask is None or no_ask is None:
            continue
        over_odds = american_from_price(yes_ask)
        under_odds = american_from_price(no_ask)
        if over_odds is None or under_odds is None:
            continue

        player_name = title["name"].strip()
        custom = market.get("custom_strike")
        player_ref = str(custom.get("basketball_player")) if isinstance(custom, dict) else ""
        ticker = str(market.get("ticker") or "")
        snapshot = LineSnapshotPayload(
            timestamp=now,
            provider_game_id=event_ticker,
            sportsbook_key="kalshi",
            provider_player_id=player_ref or player_name,
            market_key=market_key,
            line_value=strike,
            over=LineOutcomePayload(side="over", odds=over_odds),
            under=LineOutcomePayload(side="under", odds=under_odds),
            event_status="scheduled",
            meta={
                "player_name": player_name,
                "home_team_abbreviation": home,
                "away_team_abbreviation": away,
                "event_date": game_date.isoformat(),
                "odds_source_provider": "kalshi",
                "odds_verification_status": "provider_live",
                "is_live_quote": True,
                "is_alternate_line": True,
                "source_prop_type": series,
                "sportsbook_key": "kalshi",
                "provider_event_id": event_ticker,
                "kalshi_ticker": ticker,
                "kalshi_volume": market.get("volume_fp"),
                "kalshi_open_interest": market.get("open_interest_fp"),
            },
        )
        group = (event_ticker, player_name, market_key)
        rows.append((group, abs(yes_ask - 0.5), snapshot))

    # Kalshi lists a ladder of thresholds per player. Treat the one nearest even
    # odds as the main line and the rest as alternates.
    best: dict[tuple[str, str, str], float] = defaultdict(lambda: 2.0)
    for group, distance, _ in rows:
        best[group] = min(best[group], distance)
    snapshots: list[LineSnapshotPayload] = []
    claimed: set[tuple[str, str, str]] = set()
    for group, distance, snapshot in rows:
        if distance == best[group] and group not in claimed:
            claimed.add(group)
            snapshot.meta["is_alternate_line"] = False
        snapshots.append(snapshot)
    return snapshots


class KalshiOddsProvider(HttpProviderMixin, OddsProvider):
    """NBA player prop prices from Kalshi's public market data (no key needed)."""

    provider_name = "kalshi"

    def __init__(self) -> None:
        super().__init__()
        self.base_url = get_settings().kalshi_market_data_base_url.rstrip("/")

    async def healthcheck(self) -> bool:
        result = await self._get("/markets", params={"limit": 1, "status": "open"})
        return isinstance(result.payload, dict) and "markets" in result.payload

    async def fetch_upcoming_player_props(
        self,
        target_date: date,
    ) -> tuple[ProviderFetchResult, list[LineSnapshotPayload]]:
        kept: list[dict[str, Any]] = []
        for series in SERIES_TO_MARKET:
            kept.extend(await self._fetch_series(series, target_date))
        snapshots = parse_kalshi_player_props(kept, target_date, datetime.now(UTC))
        logger.info(
            "kalshi odds: target_date=%s markets=%s snapshots=%s",
            target_date.isoformat(),
            len(kept),
            len(snapshots),
        )
        payload = {
            "data": kept,
            "meta": {
                "record_count": len(kept),
                "snapshot_count": len(snapshots),
                "target_date": target_date.isoformat(),
            },
        }
        return (
            ProviderFetchResult(
                endpoint=f"{self.base_url}/markets",
                fetched_at=datetime.now(UTC),
                payload=payload,
            ),
            snapshots,
        )

    async def _fetch_series(self, series: str, target_date: date) -> list[dict[str, Any]]:
        kept: list[dict[str, Any]] = []
        cursor = ""
        while True:
            params: dict[str, Any] = {"series_ticker": series, "status": "open", "limit": _PAGE_LIMIT}
            if cursor:
                params["cursor"] = cursor
            result = await self._get("/markets", params=params)
            payload = result.payload if isinstance(result.payload, dict) else {}
            for market in payload.get("markets") or []:
                event = parse_event_ticker(str(market.get("event_ticker") or ""))
                if event is not None and event[0] == target_date:
                    kept.append(market)
            cursor = str(payload.get("cursor") or "")
            if not cursor:
                return kept

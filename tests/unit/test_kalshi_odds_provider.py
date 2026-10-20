from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from app.providers.odds.kalshi_odds import (
    american_from_price,
    parse_event_ticker,
    parse_kalshi_player_props,
)

NOW = datetime(2026, 10, 20, 15, 0, tzinfo=UTC)


def _market(
    *,
    series: str = "KXNBAPTS",
    event: str = "26OCT21DALHOU",
    player_code: str = "HOUKDURANT7",
    name: str = "Kevin Durant",
    strike: float = 34.5,
    unit: str = "points",
    yes_ask: str | None = "0.1100",
    no_ask: str | None = "0.9800",
) -> dict:
    threshold = int(strike + 0.5)
    market = {
        "ticker": f"{series}-{event}-{player_code}-{threshold}",
        "event_ticker": f"{series}-{event}",
        "title": f"{name}: {threshold}+ {unit}",
        "floor_strike": strike,
        "strike_type": "greater",
        "status": "active",
        "volume_fp": "1934.05",
        "open_interest_fp": "12.00",
        "custom_strike": {"basketball_player": "uuid-1"},
    }
    if yes_ask is not None:
        market["yes_ask_dollars"] = yes_ask
    if no_ask is not None:
        market["no_ask_dollars"] = no_ask
    return market


def test_parse_event_ticker_reads_date_and_teams():
    parsed = parse_event_ticker("KXNBAPTS-26OCT21DALHOU")
    assert parsed == (date(2026, 10, 21), "DAL", "HOU")


def test_parse_event_ticker_rejects_garbage():
    assert parse_event_ticker("KXNFLGAME-26OCT21") is None


def test_american_from_price_underdog_and_favorite():
    assert american_from_price(0.25) == 300
    assert american_from_price(0.75) == -300
    assert american_from_price(0.5) == -100


def test_american_from_price_rejects_unpriceable():
    assert american_from_price(0.0) is None
    assert american_from_price(1.0) is None


def test_parse_builds_snapshot_with_over_from_yes_and_under_from_no():
    snaps = parse_kalshi_player_props([_market(yes_ask="0.2500", no_ask="0.7800")], date(2026, 10, 21), NOW)

    assert len(snaps) == 1
    snap = snaps[0]
    assert snap.sportsbook_key == "kalshi"
    assert snap.market_key == "points"
    assert snap.line_value == 34.5
    assert snap.over.odds == 300
    assert snap.under.odds == -355
    assert snap.meta["player_name"] == "Kevin Durant"
    assert snap.meta["home_team_abbreviation"] == "HOU"
    assert snap.meta["away_team_abbreviation"] == "DAL"
    assert snap.meta["event_date"] == "2026-10-21"
    assert snap.meta["odds_source_provider"] == "kalshi"
    assert snap.meta["kalshi_ticker"].startswith("KXNBAPTS-26OCT21DALHOU")


def test_parse_filters_to_target_date():
    snaps = parse_kalshi_player_props([_market(event="26OCT22DALHOU")], date(2026, 10, 21), NOW)
    assert snaps == []


@pytest.mark.parametrize(
    ("series", "unit", "market_key"),
    [
        ("KXNBAREB", "rebounds", "rebounds"),
        ("KXNBAAST", "assists", "assists"),
        ("KXNBA3PT", "threes", "threes"),
        ("KXNBAPRA", "points, rebounds, and assists", "pra"),
    ],
)
def test_parse_maps_series_to_market(series, unit, market_key):
    snaps = parse_kalshi_player_props([_market(series=series, unit=unit, strike=4.5)], date(2026, 10, 21), NOW)
    assert [s.market_key for s in snaps] == [market_key]


def test_parse_skips_unknown_series_and_unpriceable_quotes():
    markets = [
        _market(series="KXNBAGAME"),
        _market(yes_ask=None),
        _market(no_ask="1.0000"),
        _market(yes_ask="0.0000"),
    ]
    assert parse_kalshi_player_props(markets, date(2026, 10, 21), NOW) == []


def test_main_line_is_threshold_closest_to_even_odds():
    markets = [
        _market(strike=19.5, yes_ask="0.9000", no_ask="0.1500"),
        _market(strike=24.5, yes_ask="0.5200", no_ask="0.5000"),
        _market(strike=29.5, yes_ask="0.1500", no_ask="0.9000"),
    ]
    snaps = parse_kalshi_player_props(markets, date(2026, 10, 21), NOW)
    flags = {s.line_value: s.meta["is_alternate_line"] for s in snaps}
    assert flags == {19.5: True, 24.5: False, 29.5: True}


def test_parse_keeps_punctuated_names_whole():
    market = _market(name="Jaren Jackson Jr.", player_code="MEMJJACKSONJR13", strike=29.5)
    snaps = parse_kalshi_player_props([market], date(2026, 10, 21), NOW)
    assert snaps[0].meta["player_name"] == "Jaren Jackson Jr."

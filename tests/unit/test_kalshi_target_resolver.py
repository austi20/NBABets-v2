from __future__ import annotations

from typing import Any

from scripts.resolve_kalshi_targets import _load_candidate_markets, resolve_targets


class _FakeResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return self._payload


class _FakeClient:
    def __init__(self, markets_by_series: dict[str | None, list[dict[str, Any]]]) -> None:
        self._markets_by_series = markets_by_series
        self.calls: list[dict[str, Any]] = []

    def get(self, _url: str, *, params: dict[str, Any]) -> _FakeResponse:
        self.calls.append(dict(params))
        return _FakeResponse({"markets": self._markets_by_series.get(params.get("series_ticker"), []), "cursor": ""})


def test_load_candidate_markets_uses_target_specific_player_prop_series() -> None:
    target = {
        "target_id": "donovan-assists",
        "series_ticker": "KXNBAAST",
        "line_value": 3.5,
        "recommendation": "buy_yes",
        "match_rules": {},
    }
    market = {"ticker": "KXNBAAST-26MAY09DETCLE-CLEDMITCHELL45-4"}
    client = _FakeClient({"KXNBA": [], "KXNBAAST": [market]})

    markets = _load_candidate_markets(
        client=client,  # type: ignore[arg-type]
        base_url="https://example.test",
        targets=[target],
        status="open",
        mve_filter="exclude",
        series_ticker="KXNBA",
    )

    assert markets == [market]
    assert [call.get("series_ticker") for call in client.calls] == ["KXNBAAST"]


def test_resolve_targets_matches_kalshi_threshold_for_sportsbook_half_line() -> None:
    target = {
        "target_id": "donovan-assists",
        "market_key": "assists",
        "game_date": "2026-05-09",
        "player_id": "88",
        "line_value": 3.5,
        "recommendation": "buy_yes",
        "match_rules": {
            "title_contains_all": ["DET", "CLE"],
            "player_name_contains_any": ["Donovan Mitchell", "Mitchell"],
            "stat_contains_any": ["assists", "ast"],
            "acceptable_line_values": [3.5, 4.0],
            "status": "open",
            "mve_filter": "exclude",
        },
    }
    market = {
        "ticker": "KXNBAAST-26MAY09DETCLE-CLEDMITCHELL45-4",
        "event_ticker": "KXNBAAST-26MAY09DETCLE",
        "status": "open",
        "title": "Donovan Mitchell: 4+ assists",
    }

    resolved = resolve_targets([target], [market], min_score=70)

    assert resolved["unresolved"] == []
    assert resolved["symbols"][0]["kalshi_ticker"] == market["ticker"]
    assert resolved["symbols"][0]["match_quality"] == "exact"


def test_resolve_targets_rejects_wrong_player_even_when_line_matches() -> None:
    target = {
        "target_id": "duncan-assists",
        "market_key": "assists",
        "game_date": "2026-05-09",
        "player_id": "395",
        "line_value": 1.5,
        "recommendation": "buy_yes",
        "match_rules": {
            "title_contains_all": ["DET", "CLE"],
            "player_name_contains_any": ["Duncan Robinson", "Robinson"],
            "stat_contains_any": ["assists", "ast"],
            "acceptable_line_values": [1.5, 2.0],
            "status": "open",
            "mve_filter": "exclude",
        },
    }
    market = {
        "ticker": "KXNBAAST-26MAY09DETCLE-CLEDSCHRODER8-2",
        "event_ticker": "KXNBAAST-26MAY09DETCLE",
        "status": "open",
        "title": "Dennis Schroder: 2+ assists",
    }

    resolved = resolve_targets([target], [market], min_score=70)

    assert resolved["symbols"] == []
    assert resolved["unresolved"][0]["target_id"] == "duncan-assists"
    assert resolved["unresolved"][0]["reason"] == "player_not_listed_for_series"


def test_resolve_targets_rejects_wrong_threshold_even_when_player_matches() -> None:
    target = {
        "target_id": "lebron-points",
        "market_key": "points",
        "game_date": "2026-05-09",
        "player_id": "208",
        "line_value": 23.5,
        "recommendation": "buy_no",
        "match_rules": {
            "title_contains_all": ["OKC", "LAL"],
            "player_name_contains_any": ["LeBron James", "James"],
            "stat_contains_any": ["points", "pts"],
            "acceptable_line_values": [23.5, 24.0],
            "status": "open",
            "mve_filter": "exclude",
        },
    }
    market = {
        "ticker": "KXNBAPTS-26MAY09OKCLAL-LALLJAMES23-30",
        "event_ticker": "KXNBAPTS-26MAY09OKCLAL",
        "status": "open",
        "title": "LeBron James: 30+ points",
        "floor_strike": 29.5,
    }

    resolved = resolve_targets([target], [market], min_score=70)

    assert resolved["symbols"] == []
    assert resolved["unresolved"][0]["target_id"] == "lebron-points"
    assert resolved["unresolved"][0]["reason"] == "exact_threshold_not_listed"
    assert resolved["unresolved"][0]["adjacent_line_values"][0] == 29.5


def test_resolve_targets_classifies_closed_matching_market() -> None:
    target = {
        "target_id": "lebron-points",
        "market_key": "points",
        "game_date": "2026-05-09",
        "player_id": "208",
        "line_value": 23.5,
        "recommendation": "buy_yes",
        "match_rules": {
            "title_contains_all": ["OKC", "LAL"],
            "player_name_contains_any": ["LeBron James", "James"],
            "stat_contains_any": ["points", "pts"],
            "acceptable_line_values": [23.5, 24.0],
            "status": "open",
            "mve_filter": "exclude",
        },
    }
    market = {
        "ticker": "KXNBAPTS-26MAY09OKCLAL-LALLJAMES23-24",
        "event_ticker": "KXNBAPTS-26MAY09OKCLAL",
        "status": "closed",
        "title": "LeBron James: 24+ points",
    }

    resolved = resolve_targets([target], [market], min_score=70)

    assert resolved["symbols"] == []
    assert resolved["unresolved"][0]["reason"] == "market_closed"

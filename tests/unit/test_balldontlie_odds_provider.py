from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.config.settings import Settings
from app.providers.factory import iter_configured_odds_provider_chain
from app.providers.odds.balldontlie_odds import parse_bdl_player_props
from app.providers.payload_schema import ProviderPayloadSchemaError, validate_odds_provider_props_response
from app.schemas.domain import ProviderFetchResult


def _fr(payload: dict) -> ProviderFetchResult:
    return ProviderFetchResult(
        endpoint="unit://balldontlie",
        fetched_at=datetime.now(UTC),
        payload=payload,
    )


def test_validate_odds_balldontlie_requires_wrapped_envelope() -> None:
    with pytest.raises(ProviderPayloadSchemaError, match="missing required keys"):
        validate_odds_provider_props_response("balldontlie", _fr({"data": []}))


def test_validate_odds_balldontlie_accepts_data_meta() -> None:
    validate_odds_provider_props_response("balldontlie", _fr({"data": [], "meta": {"record_count": 0}}))


def test_iter_odds_chain_balldontlie_only() -> None:
    cfg = Settings.model_construct(
        odds_provider="balldontlie",
        balldontlie_api_key="unit-bdl",
    )
    assert iter_configured_odds_provider_chain(cfg) == ("balldontlie",)


def test_parse_bdl_player_props_maps_over_under() -> None:
    game = {
        "id": 18447073,
        "date": "2025-11-24",
        "datetime": "2025-11-25T01:00:00Z",
        "status": "scheduled",
        "home_team": {"abbreviation": "LAL"},
        "visitor_team": {"abbreviation": "BOS"},
    }
    rows = [
        {
            "game_id": 18447073,
            "player_id": 246,
            "vendor": "draftkings",
            "prop_type": "points",
            "line_value": "30.5",
            "market": {"type": "over_under", "over_odds": -111, "under_odds": -115},
            "updated_at": "2025-11-24T23:46:46.653Z",
            "player": {"first_name": "LeBron", "last_name": "James"},
        }
    ]
    snaps = parse_bdl_player_props(game, rows)
    assert len(snaps) == 1
    s = snaps[0]
    assert s.provider_game_id == "18447073"
    assert s.provider_player_id == "246"
    assert s.market_key == "points"
    assert s.line_value == 30.5
    assert s.sportsbook_key == "draftkings"
    assert s.over.odds == -111
    assert s.under.odds == -115
    assert s.meta.get("player_name") == "LeBron James"
    assert s.meta.get("odds_source_provider") == "balldontlie"
    assert s.meta.get("event_start_time") == "2025-11-25T01:00:00Z"


def test_parse_bdl_skips_milestone_and_unknown_prop() -> None:
    game = {"id": 1, "date": "2025-01-01T00:00:00Z", "home_team": {}, "visitor_team": {}}
    rows = [
        {
            "player_id": 1,
            "vendor": "x",
            "prop_type": "points",
            "line_value": "18",
            "market": {"type": "milestone", "odds": -100},
        },
        {
            "player_id": 2,
            "vendor": "x",
            "prop_type": "steals",
            "line_value": "1.5",
            "market": {"type": "over_under", "over_odds": -110, "under_odds": -110},
        },
    ]
    assert parse_bdl_player_props(game, rows) == []

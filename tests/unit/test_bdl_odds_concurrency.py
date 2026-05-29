from __future__ import annotations

import asyncio
from datetime import date

import pytest

import app.providers.odds.balldontlie_odds as bdl
from app.providers.odds.balldontlie_odds import BallDontLieOddsProvider


@pytest.mark.asyncio
async def test_at_most_8_concurrent_fetches(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = BallDontLieOddsProvider()
    provider._api_key = "unit-key"

    games = [{"id": i} for i in range(20)]

    async def _fake_games(_self: object, _target: date) -> list[dict[str, object]]:
        return games

    state = {"current": 0, "max": 0}

    async def _fake_fetch(_self: object, _game_id: int) -> list[dict[str, object]]:
        state["current"] += 1
        state["max"] = max(state["max"], state["current"])
        for _ in range(3):
            await asyncio.sleep(0)
        state["current"] -= 1
        return []

    monkeypatch.setattr(BallDontLieOddsProvider, "_fetch_games_for_date", _fake_games)
    monkeypatch.setattr(BallDontLieOddsProvider, "_fetch_all_player_props", _fake_fetch)
    monkeypatch.setattr(bdl, "parse_bdl_player_props", lambda _game, _props: [])

    await provider.fetch_upcoming_player_props(date(2025, 11, 25))

    assert state["max"] <= 8
    assert state["max"] > 1

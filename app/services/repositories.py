from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.all import Game, Player, PropMarket, Sportsbook, Team


class ReferenceRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def get_team_by_abbreviation(self, abbreviation: str) -> Team | None:
        return self._session.scalar(select(Team).where(Team.abbreviation == abbreviation))

    def get_player_by_provider_id(self, provider_player_id: str) -> Player | None:
        player = self._session.scalar(select(Player).where(Player.provider_player_id == provider_player_id))
        if player is not None:
            return player
        for candidate in self._session.scalars(select(Player)).all():
            if str(provider_player_id) in _provider_player_aliases(candidate.meta):
                return candidate
        return None

    def get_player_by_normalized_name_and_team(
        self,
        normalized_name: str,
        team_id: int | None,
    ) -> Player | None:
        query = select(Player).where(Player.normalized_name == normalized_name)
        if team_id is None:
            player = self._session.scalar(query.where(Player.team_id.is_(None)))
        else:
            player = self._session.scalar(query.where(Player.team_id == team_id))
        if player is not None:
            return player
        candidates = self._session.scalars(query).all()
        if team_id is not None:
            unassigned = next((candidate for candidate in candidates if candidate.team_id is None), None)
            if unassigned is not None:
                return unassigned
        if len(candidates) == 1:
            return candidates[0]
        return None

    def get_game_by_provider_id(self, provider_game_id: str) -> Game | None:
        game = self._session.scalar(select(Game).where(Game.provider_game_id == provider_game_id))
        if game is not None:
            return game
        for candidate in self._session.scalars(select(Game)).all():
            if str(provider_game_id) in _provider_game_aliases(candidate.meta):
                return candidate
        return game

    def get_game_by_matchup(
        self,
        game_date: object,
        home_team_id: int,
        away_team_id: int,
    ) -> Game | None:
        return self._session.scalar(
            select(Game).where(
                Game.game_date == game_date,
                Game.home_team_id == home_team_id,
                Game.away_team_id == away_team_id,
            )
        )

    def get_market_by_key(self, key: str) -> PropMarket | None:
        return self._session.scalar(select(PropMarket).where(PropMarket.key == key))

    def get_sportsbook_by_key(self, key: str) -> Sportsbook | None:
        return self._session.scalar(select(Sportsbook).where(Sportsbook.key == key))


def _provider_player_aliases(meta: dict[str, Any] | None) -> set[str]:
    if not isinstance(meta, dict):
        return set()
    aliases: set[str] = set()
    raw_aliases = meta.get("provider_player_ids")
    if isinstance(raw_aliases, list):
        aliases.update(str(item) for item in raw_aliases if item is not None)
    by_source = meta.get("provider_player_ids_by_source")
    if isinstance(by_source, dict):
        for values in by_source.values():
            if not isinstance(values, list):
                continue
            aliases.update(str(item) for item in values if item is not None)
    return aliases


def _provider_game_aliases(meta: dict[str, Any] | None) -> set[str]:
    if not isinstance(meta, dict):
        return set()
    raw_aliases = meta.get("provider_game_ids")
    if not isinstance(raw_aliases, list):
        return set()
    return {str(item) for item in raw_aliases if item is not None}

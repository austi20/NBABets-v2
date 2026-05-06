from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

from Levenshtein import ratio
from sqlalchemy.orm import Session

from app.models.reference import ManualEntityOverride, Player

SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}


def normalize_name(value: str) -> str:
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    value = re.sub(r"[^a-zA-Z0-9\s]", " ", value).lower()
    parts = [part for part in value.split() if part and part not in SUFFIXES]
    return " ".join(parts)


@dataclass(frozen=True)
class MatchResult:
    player_id: int | None
    confidence: float
    matched_name: str | None


class PlayerMatcher:
    def __init__(self, session: Session, threshold: float = 0.9) -> None:
        self._session = session
        self._threshold = threshold

    def match(self, provider: str, provider_entity_id: str, provider_name: str) -> MatchResult:
        override = (
            self._session.query(ManualEntityOverride)
            .filter(
                ManualEntityOverride.provider == provider,
                ManualEntityOverride.provider_entity_id == provider_entity_id,
            )
            .one_or_none()
        )
        if override and override.canonical_player_id:
            player = self._session.get(Player, override.canonical_player_id)
            return MatchResult(player_id=override.canonical_player_id, confidence=1.0, matched_name=player.full_name if player else None)

        normalized_target = normalize_name(provider_name)
        players = self._session.query(Player).all()
        exact = next((player for player in players if player.normalized_name == normalized_target), None)
        if exact:
            return MatchResult(player_id=exact.player_id, confidence=1.0, matched_name=exact.full_name)

        best_player: Player | None = None
        best_score = 0.0
        for player in players:
            score = ratio(normalized_target, player.normalized_name)
            if score > best_score:
                best_score = score
                best_player = player
        if best_player and best_score >= self._threshold:
            return MatchResult(player_id=best_player.player_id, confidence=best_score, matched_name=best_player.full_name)
        return MatchResult(player_id=None, confidence=best_score, matched_name=best_player.full_name if best_player else None)


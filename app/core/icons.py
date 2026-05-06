from __future__ import annotations

SPORTSBOOK_ICONS = {
    "draftkings": "[DK]",
    "fanduel": "[FD]",
    "betmgm": "[MGM]",
    "caesars": "[CZ]",
    "espnbet": "[ESPN]",
    "bet365": "[365]",
    "hardrock": "[HR]",
    "consensus": "[AVG]",
}


def player_icon(_: str) -> str:
    return "[P]"


def sportsbook_icon(key: str) -> str:
    return SPORTSBOOK_ICONS.get(key.lower(), "[BOOK]")

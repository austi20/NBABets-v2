from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.trading.types import ExecutionIntent

_REQUIRED = ("market_key", "side", "line_value", "player_id", "game_date", "kalshi_ticker")


class SymbolResolverConfigError(RuntimeError):
    """Raised when the symbol map JSON cannot be loaded."""


def _key(market_key: str, side: str, line_value: float, player_id: int, game_date: str) -> tuple:
    return (market_key.lower(), side.lower(), round(float(line_value), 2), int(player_id), game_date)


class SymbolResolver:
    def __init__(self, entries: list[dict[str, Any]]) -> None:
        self._table: dict[tuple, str] = {}
        for entry in entries:
            for field in _REQUIRED:
                if field not in entry:
                    raise SymbolResolverConfigError(f"symbol entry missing field: {field}")
            key = _key(
                entry["market_key"],
                entry["side"],
                entry["line_value"],
                entry["player_id"],
                str(entry["game_date"]),
            )
            self._table[key] = str(entry["kalshi_ticker"])

    @property
    def ticker_count(self) -> int:
        return len(self._table)

    def resolve(self, intent: ExecutionIntent) -> str | None:
        signal = intent.signal
        player_id = signal.metadata.get("player_id")
        if player_id is None:
            return None
        game_date_raw = signal.metadata.get("game_date")
        if game_date_raw is None:
            return None
        try:
            key = _key(
                signal.market_key,
                signal.side,
                signal.line_value,
                int(player_id),
                str(game_date_raw),
            )
        except (TypeError, ValueError):
            return None
        return self._table.get(key)


def load_symbol_resolver(path: Path | str) -> SymbolResolver:
    config_path = Path(path)
    if not config_path.is_file():
        raise SymbolResolverConfigError(
            f"symbol map not found at {config_path}; "
            "copy config/kalshi_symbols.example.json to that path."
        )
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SymbolResolverConfigError(f"malformed JSON in {config_path}: {exc}") from exc
    if not isinstance(payload, list):
        raise SymbolResolverConfigError(f"symbol map in {config_path} must be a JSON array")
    return SymbolResolver(entries=payload)

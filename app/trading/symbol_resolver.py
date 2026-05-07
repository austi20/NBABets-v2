from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.trading.types import ExecutionIntent

_OBSERVE_ONLY_RECOMMENDATIONS = {"observe", "observe_only", "watch"}


class SymbolResolverConfigError(RuntimeError):
    """Raised when the symbol map JSON cannot be loaded."""


def _key(market_key: str, side: str, line_value: float, player_id: int | str, game_date: str) -> tuple:
    return (market_key.lower(), side.lower(), round(float(line_value), 2), str(player_id).strip().lower(), game_date)


def _side_from_entry(entry: dict[str, Any]) -> str | None:
    raw = entry.get("side", entry.get("recommendation"))
    if raw is None:
        raise SymbolResolverConfigError("symbol entry missing field: side")
    value = str(raw).strip().lower()
    if value in {"over", "buy_yes", "yes"}:
        return "over"
    if value in {"under", "buy_no", "no"}:
        return "under"
    if value in _OBSERVE_ONLY_RECOMMENDATIONS:
        return None
    raise SymbolResolverConfigError(f"unsupported symbol side/recommendation: {raw}")


def _entry_value(entry: dict[str, Any], field: str) -> Any:
    if field not in entry or entry[field] is None or entry[field] == "":
        raise SymbolResolverConfigError(f"symbol entry missing field: {field}")
    return entry[field]


def _entries_from_payload(payload: Any, config_path: Path) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        raise SymbolResolverConfigError(f"symbol map in {config_path} must be a JSON array or object")
    unresolved = payload.get("unresolved") or []
    if unresolved:
        raise SymbolResolverConfigError(
            f"symbol map in {config_path} contains unresolved targets; refusing live resolver load"
        )
    entries = payload.get("symbols")
    if not isinstance(entries, list):
        raise SymbolResolverConfigError(f"symbol map object in {config_path} must contain a symbols array")
    return entries


class SymbolResolver:
    def __init__(self, entries: list[dict[str, Any]]) -> None:
        self._table: dict[tuple, str] = {}
        for entry in entries:
            side = _side_from_entry(entry)
            if side is None:
                continue
            key = _key(
                _entry_value(entry, "market_key"),
                side,
                _entry_value(entry, "line_value"),
                _entry_value(entry, "player_id"),
                str(_entry_value(entry, "game_date")),
            )
            self._table[key] = str(_entry_value(entry, "kalshi_ticker"))

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
                player_id,
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
    return SymbolResolver(entries=_entries_from_payload(payload, config_path))

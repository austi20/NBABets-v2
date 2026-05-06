from __future__ import annotations

from datetime import date
from zoneinfo import ZoneInfo

LOCAL_TIMEZONE = ZoneInfo("America/New_York")
UTC_TIMEZONE = ZoneInfo("UTC")


def to_local_board_date(game_date: object, start_time: object) -> date:
    base_date = _coerce_date(game_date)
    if hasattr(start_time, "astimezone"):
        timestamp = start_time
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=UTC_TIMEZONE)
        return timestamp.astimezone(LOCAL_TIMEZONE).date()
    return base_date


def matches_board_date(game_date: object, start_time: object, target_date: date) -> bool:
    return to_local_board_date(game_date, start_time) == target_date


def _coerce_date(value: object) -> date:
    if isinstance(value, date):
        return value
    if hasattr(value, "date"):
        return value.date()
    raise TypeError(f"Cannot coerce {type(value)!r} to date")

from __future__ import annotations

import logging

_log = logging.getLogger(__name__)


def safe_int(value: object) -> int | None:
    """Convert an external-payload value to int, returning None on bad input.

    Direct ``int(payload[...])`` on API data crashes the whole batch when a
    provider returns a non-numeric or missing id.  Use this for external
    payload conversions only, not trusted internal ints.
    """
    if value is None:
        return None
    try:
        return int(value)  # type: ignore[call-overload]
    except (TypeError, ValueError):
        _log.warning("safe_int: cannot convert %r to int", value)
        return None

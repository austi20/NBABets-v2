from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.trading.stream_publisher import TradingStreamPublisher


@pytest.mark.parametrize("level", ["info", "warn", "error"])
def test_log_event_accepts_allowed_levels(level: str) -> None:
    publisher = TradingStreamPublisher()
    line = publisher.log_event(level=level, message="ok")
    assert line.level == level


def test_log_event_rejects_level_outside_literal() -> None:
    publisher = TradingStreamPublisher()
    with pytest.raises(ValidationError):
        publisher.log_event(level="debug", message="bad level")

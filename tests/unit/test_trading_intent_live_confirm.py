from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.config.settings import get_settings
from app.server.main import create_app
from app.trading.risk import ExposureRiskEngine, RiskLimits
from app.trading.types import ExecutionIntent, Fill, MarketRef, OrderEvent

HEADERS = {"X-App-Token": "t"}

_VALID_PAYLOAD = {
    "game_id": 1,
    "player_id": 42,
    "market": "points",
    "line": 24.5,
    "side": "over",
    "sportsbook_key": "draftkings",
    "stake": 10.0,
}


class _AcceptAdapter:
    """Fake adapter that always accepts and fills."""

    def place_order(self, intent: ExecutionIntent) -> tuple[list[OrderEvent], list[Fill]]:
        fill = Fill(
            fill_id="f-test",
            intent_id=intent.intent_id,
            market=MarketRef(
                exchange="paper",
                symbol="sym",
                market_key=intent.signal.market_key,
                side=intent.side,
                line_value=intent.signal.line_value,
            ),
            side="buy",
            stake=intent.stake,
            price=0.5,
        )
        event = OrderEvent(
            intent_id=intent.intent_id,
            event_type="accepted",
            status="ok",
            message="accepted",
        )
        return [event], [fill]


def _client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, *, exchange: str = "paper") -> TestClient:
    monkeypatch.setenv("APP_DATA_DIR", str(tmp_path / "app"))
    monkeypatch.setenv("TRADING_EXCHANGE", exchange)
    monkeypatch.setenv("TRADING_PAPER_ADAPTER", "fake")
    monkeypatch.setenv("TRADING_LIVE_ENABLED", "false")
    monkeypatch.setenv("KALSHI_LIVE_TRADING", "false")
    monkeypatch.setenv("KALSHI_WS_ENABLED", "false")
    monkeypatch.setenv("KALSHI_DECISION_BRAIN_ENABLED", "false")
    monkeypatch.setenv("AUTO_INIT_BUDGET_FROM_WALLET", "false")
    get_settings.cache_clear()
    app = create_app(app_token="t")
    app.state.trading_risk = ExposureRiskEngine(RiskLimits(per_order_cap=10_000))
    app.state.trading_adapter = _AcceptAdapter()
    if exchange != "paper":
        # Override what create_app set so the endpoint sees a live exchange name.
        app.state.trading_exchange = exchange
    return TestClient(app, raise_server_exceptions=False)


def test_paper_intent_does_not_require_confirm_live(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    c = _client(monkeypatch, tmp_path, exchange="paper")
    r = c.post("/api/trading/intent", json=_VALID_PAYLOAD, headers=HEADERS)
    # Paper mode: no confirm_live needed. 200/202 accepted, or 422 for model-level errors.
    # Must NOT be 400 (which would indicate the confirm_live gate fired).
    assert r.status_code != 400, f"Got 400 in paper mode: {r.text}"


def test_live_intent_without_confirm_is_rejected(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    c = _client(monkeypatch, tmp_path, exchange="kalshi")
    payload = {**_VALID_PAYLOAD}  # no confirm_live
    r = c.post("/api/trading/intent", json=payload, headers=HEADERS)
    assert r.status_code == 400
    assert "confirm_live" in r.text.lower()


def test_live_intent_with_wrong_confirm_is_rejected(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    c = _client(monkeypatch, tmp_path, exchange="kalshi")
    payload = {**_VALID_PAYLOAD, "confirm_live": "yes"}
    r = c.post("/api/trading/intent", json=payload, headers=HEADERS)
    assert r.status_code == 400
    assert "confirm_live" in r.text.lower()


def test_live_intent_with_correct_confirm_is_accepted(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    c = _client(monkeypatch, tmp_path, exchange="kalshi")
    payload = {**_VALID_PAYLOAD, "confirm_live": "LIVE"}
    r = c.post("/api/trading/intent", json=payload, headers=HEADERS)
    # Risk engine accepts, adapter accepts: should succeed (200 accepted=True).
    assert r.status_code == 200


def test_live_intent_stake_too_large_rejected(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    c = _client(monkeypatch, tmp_path, exchange="kalshi")
    payload = {**_VALID_PAYLOAD, "stake": 10_000_001, "confirm_live": "LIVE"}
    r = c.post("/api/trading/intent", json=payload, headers=HEADERS)
    assert r.status_code == 422


def test_paper_intent_stake_too_large_rejected(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    c = _client(monkeypatch, tmp_path, exchange="paper")
    payload = {**_VALID_PAYLOAD, "stake": 99_999}
    r = c.post("/api/trading/intent", json=payload, headers=HEADERS)
    assert r.status_code == 422

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from app.config.settings import Settings
from app.trading.readiness import build_trading_readiness


class _FakeMarketClient:
    def __init__(self, *, status: str = "open") -> None:
        self._status = status

    def get_market(self, ticker: str) -> dict[str, Any]:
        return {
            "market": {
                "ticker": ticker,
                "status": self._status,
                "yes_bid_dollars": "0.4000",
                "yes_ask_dollars": "0.4200",
                "no_bid_dollars": "0.5700",
                "no_ask_dollars": "0.5900",
            }
        }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _settings(tmp_path: Path, *, decisions: Path, symbols: Path, key_path: Path) -> Settings:
    key_path.write_text("fake-key", encoding="utf-8")
    return Settings(
        KALSHI_LIVE_TRADING=True,
        KALSHI_API_KEY_ID="key-id",
        KALSHI_PRIVATE_KEY_PATH=key_path,
        KALSHI_DECISIONS_PATH=str(decisions),
        KALSHI_SYMBOLS_PATH=str(symbols),
        KALSHI_RESOLUTION_TARGETS_PATH=str(tmp_path / "targets.json"),
    )


def _ready_decision(*, game_date: str = "2026-05-08") -> dict[str, Any]:
    return {
        "version": 1,
        "decisions": [
            {
                "decision_id": "live",
                "mode": "live",
                "market_key": "nba.game.total_points",
                "recommendation": "buy_yes",
                "line_value": 222.5,
                "player_id": "game_total",
                "game_date": game_date,
                "kalshi": {"ticker": "KX-TEST"},
                "gates": {
                    "symbol_resolved": True,
                    "fresh_market_snapshot": True,
                    "market_open": True,
                    "event_not_stale": True,
                    "spread_within_limit": True,
                    "one_order_cap_ok": True,
                    "price_within_limit": True,
                },
                "execution": {"allow_live_submit": True},
            }
        ],
    }


def _symbols(*, game_date: str = "2026-05-08") -> dict[str, Any]:
    return {
        "version": 1,
        "symbols": [
            {
                "target_id": "target",
                "market_key": "nba.game.total_points",
                "game_date": game_date,
                "line_value": 222.5,
                "recommendation": "buy_yes",
                "kalshi_ticker": "KX-TEST",
            }
        ],
        "unresolved": [],
    }


def test_trading_readiness_ready_when_pack_symbols_credentials_and_market_pass(tmp_path: Path) -> None:
    decisions = tmp_path / "decisions.json"
    symbols = tmp_path / "symbols.json"
    _write_json(decisions, _ready_decision())
    _write_json(symbols, _symbols())

    readiness = build_trading_readiness(
        settings=_settings(tmp_path, decisions=decisions, symbols=symbols, key_path=tmp_path / "key.pem"),
        market_client=_FakeMarketClient(),
        today=date(2026, 5, 8),
    )

    assert readiness.state == "ready"
    assert readiness.ticker == "KX-TEST"
    assert readiness.market_status == "open"


def test_trading_readiness_blocks_stale_or_finalized_pack(tmp_path: Path) -> None:
    decisions = tmp_path / "decisions.json"
    symbols = tmp_path / "symbols.json"
    _write_json(decisions, _ready_decision(game_date="2026-05-07"))
    _write_json(symbols, _symbols(game_date="2026-05-07"))

    readiness = build_trading_readiness(
        settings=_settings(tmp_path, decisions=decisions, symbols=symbols, key_path=tmp_path / "key.pem"),
        market_client=_FakeMarketClient(status="finalized"),
        today=date(2026, 5, 8),
    )

    assert readiness.state == "blocked"
    failed = {check.key for check in readiness.checks if check.status == "fail"}
    assert "game_date" in failed
    assert "market_open" in failed


def test_settings_resolves_relative_kalshi_paths_from_explicit_env(monkeypatch, tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("", encoding="utf-8")
    monkeypatch.setenv("NBA_PROP_ENV_FILE", str(env_file))

    settings = Settings(
        KALSHI_SYMBOLS_PATH="config/kalshi_symbols.json",
        KALSHI_DECISIONS_PATH="data/decisions/decisions.json",
        KALSHI_RESOLUTION_TARGETS_PATH="config/kalshi_resolution_targets.json",
        TRADING_LIMITS_PATH="config/trading_limits.json",
    )

    assert Path(settings.kalshi_symbols_path) == tmp_path / "config" / "kalshi_symbols.json"
    assert Path(settings.kalshi_decisions_path) == tmp_path / "data" / "decisions" / "decisions.json"

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.run_trading_loop import _load_live_decisions


def test_load_live_decisions_rejects_symbol_map_payload(tmp_path: Path) -> None:
    payload = [
        {
            "market_key": "points",
            "side": "over",
            "line_value": 25.5,
            "player_id": 237,
            "game_date": "2026-05-06",
            "kalshi_ticker": "KXNBASGPL-26MAY06LAL-LEBRON-OPTS25",
        }
    ]
    path = tmp_path / "symbols-not-decisions.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="recommendation"):
        _load_live_decisions(path)


def test_load_live_decisions_rejects_missing_file(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="could not be read"):
        _load_live_decisions(tmp_path / "missing.json")


def test_load_live_decisions_caps_to_first_decision(tmp_path: Path) -> None:
    base = {
        "model_prob": 0.58,
        "market_prob": 0.51,
        "no_vig_market_prob": 0.50,
        "ev": 0.05,
        "recommendation": "OVER",
        "confidence": "high",
        "driver": "test",
        "market_key": "points",
        "line_value": 25.5,
        "over_odds": -110,
        "under_odds": -110,
        "player_id": 237,
        "game_id": 1001,
        "game_date": "2026-05-07",
    }
    path = tmp_path / "decisions.json"
    path.write_text(json.dumps([base, {**base, "player_id": 999}]), encoding="utf-8")

    decisions, decision_count = _load_live_decisions(path)

    assert decision_count == 2
    assert len(decisions) == 1
    assert decisions[0].player_id == 237


def test_load_live_decisions_rejects_observe_only_decision_pack(tmp_path: Path) -> None:
    path = tmp_path / "observe.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "decisions": [
                    {
                        "decision_id": "observe",
                        "mode": "observe",
                        "market_key": "nba.game.total_points",
                        "recommendation": "observe_only",
                        "line_value": 222.5,
                        "player_id": "game_total",
                        "game_date": "2026-05-07",
                        "kalshi": {"ticker": "KXNBATOTAL-26MAY07LALOKC-222"},
                        "gates": {"symbol_resolved": True},
                        "execution": {"allow_live_submit": False},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="observe-only"):
        _load_live_decisions(path)


def test_load_live_decisions_accepts_live_decision_pack(tmp_path: Path) -> None:
    path = tmp_path / "live.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "decisions": [
                    {
                        "decision_id": "live",
                        "mode": "live",
                        "source_model": "test_model",
                        "market_key": "nba.player.points",
                        "recommendation": "buy_yes",
                        "confidence": 0.61,
                        "edge_bps": 480,
                        "line_value": 20.5,
                        "player_id": "lebron_james",
                        "game_date": "2026-05-07",
                        "kalshi": {
                            "target_id": "target1",
                            "ticker": "KX-LEBRON-20",
                            "max_price_dollars": "0.6200",
                            "contracts": "1.00",
                            "post_only": True,
                            "time_in_force": "good_till_canceled",
                        },
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
        ),
        encoding="utf-8",
    )

    decisions, decision_count = _load_live_decisions(path)

    assert decision_count == 1
    assert decisions[0].recommendation == "OVER"
    assert decisions[0].player_id == "lebron_james"
    assert decisions[0].ev == pytest.approx(0.048)
    assert decisions[0].metadata["max_price_dollars"] == "0.6200"
    assert decisions[0].metadata["post_only"] is True
    assert decisions[0].metadata["max_contracts"] == "1.00"
    assert decisions[0].metadata["kalshi_ticker_verified"] is True


def test_load_live_decisions_live_mode_requires_rich_blocks(tmp_path: Path) -> None:
    path = tmp_path / "incomplete_live.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "decisions": [
                    {
                        "decision_id": "x",
                        "mode": "live",
                        "market_key": "nba.player.points",
                        "recommendation": "buy_yes",
                        "line_value": 20.5,
                        "player_id": "a",
                        "game_date": "2026-05-07",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="must include execution"):
        _load_live_decisions(path)

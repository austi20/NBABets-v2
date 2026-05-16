from __future__ import annotations

import json
from dataclasses import replace
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from app.config.settings import Settings
from app.trading.decision_brain import (
    DecisionBrainError,
    FrontmatterError,
    candidates_from_board,
    export_resolution_targets,
    load_manual_candidates,
    load_policy,
    merge_candidates,
    parse_frontmatter_text,
    rank_and_enrich_symbols,
    sync_decision_brain,
)
from app.trading.selections import SelectionStore


class _FakeMarketClient:
    def __init__(self, markets: dict[str, dict[str, Any]]) -> None:
        self._markets = markets

    def get_market(self, ticker: str) -> dict[str, Any]:
        return {"market": self._markets[ticker]}


def _settings(tmp_path: Path, brain_root: Path) -> Settings:
    return Settings(
        APP_DATA_DIR=str(tmp_path / "app"),
        SNAPSHOT_DIR=str(tmp_path / "snapshots"),
        BRAIN_VAULT_ROOT=str(tmp_path / "vault"),
        KALSHI_DECISION_BRAIN_ROOT=str(brain_root),
        KALSHI_RESOLUTION_TARGETS_PATH=str(tmp_path / "config" / "targets.json"),
        KALSHI_SYMBOLS_PATH=str(tmp_path / "config" / "symbols.json"),
        KALSHI_DECISIONS_PATH=str(tmp_path / "decisions" / "decisions.json"),
        TRADING_LIMITS_PATH=str(tmp_path / "config" / "limits.json"),
    )


def _write_policy(root: Path, *, allow_live_submit: bool = False) -> None:
    policy_path = root / "00 System" / "Policy Core.md"
    policy_path.parent.mkdir(parents=True, exist_ok=True)
    policy_path.write_text(
        f"""---
brain_type: policy_core
policy_version: 2026-05-08-a
schema_version: 1
allow_live_submit: {str(allow_live_submit).lower()}
allowed_market_keys:
  - points
  - rebounds
blocked_market_keys:
  - turnovers
min_edge_bps: 450
min_model_prob: 0.57
min_confidence: 0.60
max_price_dollars_default: 0.57
max_spread_dollars: 0.03
max_contracts: 1.00
post_only: true
time_in_force: good_till_canceled
same_day_only: true
ranking_weight_edge_bps: 0.45
ranking_weight_ev: 0.20
ranking_weight_liquidity: 0.15
ranking_weight_calibration: 0.10
ranking_weight_freshness: 0.10
---
# Policy
""",
        encoding="utf-8",
    )


def _write_limits(settings: Settings) -> None:
    path = Path(settings.trading_limits_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "per_order_cap": 0.75,
                "per_market_cap": 1.0,
                "max_open_notional": 2.0,
                "daily_loss_cap": 2.0,
                "reject_cooldown_seconds": 300,
            }
        ),
        encoding="utf-8",
    )


def _board_entry(*, edge: float = 0.074, ev: float = 0.034, confidence: int = 71) -> SimpleNamespace:
    quote = SimpleNamespace(
        recommended_side="OVER",
        hit_probability=0.612,
        no_vig_market_probability=0.538,
        line_value=25.5,
    )
    opportunity = SimpleNamespace(
        game_id=123,
        player_id=237,
        player_name="LeBron James",
        market_key="points",
        consensus_line=25.5,
        recommended_side="OVER",
        hit_probability=0.612,
        data_confidence_score=0.71,
        game_label="LAL @ GSW",
        game_start_time="2026-05-09T01:00:00+00:00",
        quotes=[quote],
    )
    insight = SimpleNamespace(
        best_quote=quote,
        implied_probability=0.546,
        edge=edge,
        expected_profit_per_unit=ev,
        confidence_score=confidence,
    )
    return SimpleNamespace(
        board_date=date(2026, 5, 8),
        opportunities=[opportunity],
        opportunity_insights={(123, 237, "points", 25.5): insight},
    )


def test_frontmatter_parser_accepts_policy_shape_and_rejects_nested_map() -> None:
    fields, raw = parse_frontmatter_text(
        """---
brain_type: policy_core
allow_live_submit: false
allowed_market_keys: [points, rebounds]
blocked_market_keys:
  - turnovers
max_contracts: 1.00
---
Body
"""
    )

    assert fields["allow_live_submit"] is False
    assert fields["allowed_market_keys"] == ["points", "rebounds"]
    assert fields["blocked_market_keys"] == ["turnovers"]
    assert "brain_type" in raw

    with pytest.raises(FrontmatterError):
        parse_frontmatter_text(
            """---
brain_type: policy_core
ranking_weights:
  edge_bps: 0.45
---
"""
        )


def test_missing_policy_fails_closed(tmp_path: Path) -> None:
    settings = _settings(tmp_path, tmp_path / "brain")

    with pytest.raises(DecisionBrainError):
        load_policy(settings)


def test_board_opportunity_becomes_kalshi_candidate(tmp_path: Path) -> None:
    brain_root = tmp_path / "brain"
    _write_policy(brain_root)
    settings = _settings(tmp_path, brain_root)
    policy = load_policy(settings)

    candidates = candidates_from_board(_board_entry(), policy=policy, limit=25)

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.recommendation == "buy_yes"
    assert candidate.outcome_side == "yes"
    assert candidate.book_side == "bid"
    assert candidate.game_date == date(2026, 5, 8)
    assert candidate.edge_bps == 740
    assert candidate.stable_id == "2026-05-08_123_237_points_25.5_buy_yes"
    assert candidate.acceptable_line_values == [25.5, 26.0]


def test_vault_candidate_overrides_board_candidate_by_stable_id(tmp_path: Path) -> None:
    brain_root = tmp_path / "brain"
    _write_policy(brain_root)
    settings = _settings(tmp_path, brain_root)
    policy = load_policy(settings)
    board_candidate = candidates_from_board(_board_entry(), policy=policy, limit=25)[0]
    manual_dir = brain_root / "40 Candidates" / "2026-05-08"
    manual_dir.mkdir(parents=True, exist_ok=True)
    (manual_dir / "override.md").write_text(
        f"""---
brain_type: candidate
stable_id: {board_candidate.stable_id}
board_date: 2026-05-08
candidate_status: candidate
market_key: points
player_id: 237
player_name: LeBron James
game_id: 123
game_date: 2026-05-08
line_value: 25.5
recommendation: buy_yes
outcome_side: yes
book_side: bid
model_prob: 0.612
market_prob: 0.546
no_vig_market_prob: 0.538
edge_bps: 740
ev: 0.034
confidence: 0.71
contracts: 1.00
max_price_dollars: 0.49
title_contains_all: [LAL, GSW]
player_name_contains_any: [LeBron James, LeBron]
stat_contains_any: [points, pts]
acceptable_line_values: [25.5]
driver: manual_override
---
""",
        encoding="utf-8",
    )

    manual = load_manual_candidates(settings, board_date=date(2026, 5, 8), policy=policy)
    merged = merge_candidates([board_candidate], manual)

    assert len(merged) == 1
    assert merged[0].source == "board+vault"
    assert merged[0].max_price_dollars == 0.49
    assert merged[0].driver == "manual_override"


def test_export_writes_resolver_compatible_targets(tmp_path: Path) -> None:
    brain_root = tmp_path / "brain"
    _write_policy(brain_root)
    settings = _settings(tmp_path, brain_root)
    policy = load_policy(settings)
    candidate = candidates_from_board(_board_entry(), policy=policy, limit=25)[0]

    payload, exportable, checks = export_resolution_targets(
        settings=settings,
        policy=policy,
        board_date=date(2026, 5, 8),
        candidates=[candidate],
    )

    assert exportable == [candidate]
    assert payload["defaults"]["series_tickers"] == ["KXNBAPTS"]
    assert "series_ticker" not in payload["defaults"]
    assert payload["targets"][0]["series_ticker"] == "KXNBAPTS"
    assert payload["targets"][0]["recommendation"] == "buy_yes"
    assert payload["targets"][0]["outcome_side"] == "yes"
    assert payload["targets"][0]["book_side"] == "bid"
    assert payload["targets"][0]["match_rules"]["acceptable_line_values"] == [25.5, 26.0]
    assert Path(settings.kalshi_resolution_targets_path).is_file()
    assert {check.key for check in checks} >= {"target_export"}


def test_ranked_symbol_row_zero_is_deterministic_and_blocks_bad_spread(tmp_path: Path) -> None:
    brain_root = tmp_path / "brain"
    _write_policy(brain_root, allow_live_submit=True)
    settings = _settings(tmp_path, brain_root)
    _write_limits(settings)
    policy = load_policy(settings)
    high = candidates_from_board(_board_entry(), policy=policy, limit=25)[0]
    low = candidates_from_board(_board_entry(edge=0.050, ev=0.020, confidence=65), policy=policy, limit=25)[0]
    low = type(high)(**{**low.__dict__, "stable_id": low.stable_id.replace("123", "124"), "game_id": "124"})
    symbols_path = Path(settings.kalshi_symbols_path)
    symbols_path.parent.mkdir(parents=True, exist_ok=True)
    symbols_path.write_text(
        json.dumps(
            {
                "version": 1,
                "symbols": [
                    {
                        "target_id": low.stable_id,
                        "market_key": "points",
                        "game_date": "2026-05-08",
                        "player_id": "237",
                        "line_value": 25.5,
                        "recommendation": "buy_yes",
                        "kalshi_ticker": "KX-LOW",
                    },
                    {
                        "target_id": high.stable_id,
                        "market_key": "points",
                        "game_date": "2026-05-08",
                        "player_id": "237",
                        "line_value": 25.5,
                        "recommendation": "buy_yes",
                        "kalshi_ticker": "KX-HIGH",
                    },
                ],
                "unresolved": [],
            }
        ),
        encoding="utf-8",
    )
    client = _FakeMarketClient(
        {
            "KX-HIGH": {
                "status": "open",
                "yes_bid_dollars": "0.48",
                "yes_ask_dollars": "0.50",
                "no_bid_dollars": "0.49",
                "no_ask_dollars": "0.52",
            },
            "KX-LOW": {
                "status": "open",
                "yes_bid_dollars": "0.20",
                "yes_ask_dollars": "0.30",
                "no_bid_dollars": "0.69",
                "no_ask_dollars": "0.80",
            },
        }
    )

    ranked, selected, _checks = rank_and_enrich_symbols(
        settings=settings,
        policy=policy,
        candidates=[high, low],
        market_client=client,
        today=date(2026, 5, 8),
    )

    assert selected == high
    assert ranked["symbols"][0]["stable_id"] == high.stable_id
    assert ranked["symbols"][0]["recommendation"] == "buy_yes"
    assert ranked["symbols"][0]["candidate_status"] == "selected_live"
    assert ranked["symbols"][1]["recommendation"] == "observe_only"
    assert ranked["symbols"][1]["candidate_status"] == "blocked"
    assert "failed_spread_gate" in ranked["symbols"][1]["brain_blockers"]


def test_ranked_symbols_select_all_eligible_live_rows(tmp_path: Path) -> None:
    brain_root = tmp_path / "brain"
    _write_policy(brain_root, allow_live_submit=True)
    settings = _settings(tmp_path, brain_root)
    _write_limits(settings)
    policy = load_policy(settings)
    first = candidates_from_board(_board_entry(edge=0.080, ev=0.035, confidence=75), policy=policy, limit=25)[0]
    second = candidates_from_board(_board_entry(edge=0.055, ev=0.025, confidence=68), policy=policy, limit=25)[0]
    second = type(first)(**{**second.__dict__, "stable_id": second.stable_id.replace("123", "124"), "game_id": "124"})
    Path(settings.kalshi_symbols_path).parent.mkdir(parents=True, exist_ok=True)
    Path(settings.kalshi_symbols_path).write_text(
        json.dumps(
            {
                "version": 1,
                "symbols": [
                    {"target_id": first.stable_id, "market_key": "points", "game_date": "2026-05-08", "player_id": "237", "line_value": 25.5, "recommendation": "buy_yes", "kalshi_ticker": "KX-1"},
                    {"target_id": second.stable_id, "market_key": "points", "game_date": "2026-05-08", "player_id": "237", "line_value": 25.5, "recommendation": "buy_yes", "kalshi_ticker": "KX-2"},
                ],
                "unresolved": [],
            }
        ),
        encoding="utf-8",
    )
    client = _FakeMarketClient(
        {
            "KX-1": {"status": "open", "yes_bid_dollars": "0.48", "yes_ask_dollars": "0.50", "no_bid_dollars": "0.49", "no_ask_dollars": "0.52"},
            "KX-2": {"status": "open", "yes_bid_dollars": "0.45", "yes_ask_dollars": "0.47", "no_bid_dollars": "0.51", "no_ask_dollars": "0.53"},
        }
    )

    ranked, selected, _checks = rank_and_enrich_symbols(
        settings=settings,
        policy=policy,
        candidates=[first, second],
        market_client=client,
        today=date(2026, 5, 8),
    )

    assert selected == first
    assert [row["candidate_status"] for row in ranked["symbols"]] == ["selected_live", "selected_live"]
    assert ranked["brain"]["selected_candidate_ids"] == [first.stable_id, second.stable_id]
    assert ranked["brain"]["live_candidate_count"] == 2


def test_ranked_symbols_prefer_higher_consistency_score(tmp_path: Path) -> None:
    brain_root = tmp_path / "brain"
    _write_policy(brain_root, allow_live_submit=True)
    settings = _settings(tmp_path, brain_root)
    _write_limits(settings)
    policy = load_policy(settings)
    base_board = _board_entry(edge=0.080, ev=0.035, confidence=75)
    high_cs = candidates_from_board(base_board, policy=policy, limit=25)[0]
    low_cs = candidates_from_board(base_board, policy=policy, limit=25)[0]
    low_cs = type(high_cs)(
        **{**low_cs.__dict__, "stable_id": low_cs.stable_id.replace("123", "124"), "game_id": "124"}
    )
    high_cs = replace(high_cs, consistency_score=0.95)
    low_cs = replace(low_cs, consistency_score=0.02)
    Path(settings.kalshi_symbols_path).parent.mkdir(parents=True, exist_ok=True)
    Path(settings.kalshi_symbols_path).write_text(
        json.dumps(
            {
                "version": 1,
                "symbols": [
                    {
                        "target_id": low_cs.stable_id,
                        "market_key": "points",
                        "game_date": "2026-05-08",
                        "player_id": "237",
                        "line_value": 25.5,
                        "recommendation": "buy_yes",
                        "kalshi_ticker": "KX-LOW",
                    },
                    {
                        "target_id": high_cs.stable_id,
                        "market_key": "points",
                        "game_date": "2026-05-08",
                        "player_id": "237",
                        "line_value": 25.5,
                        "recommendation": "buy_yes",
                        "kalshi_ticker": "KX-HIGH",
                    },
                ],
                "unresolved": [],
            }
        ),
        encoding="utf-8",
    )
    client = _FakeMarketClient(
        {
            "KX-HIGH": {
                "status": "open",
                "yes_bid_dollars": "0.48",
                "yes_ask_dollars": "0.50",
                "no_bid_dollars": "0.49",
                "no_ask_dollars": "0.52",
            },
            "KX-LOW": {
                "status": "open",
                "yes_bid_dollars": "0.48",
                "yes_ask_dollars": "0.50",
                "no_bid_dollars": "0.49",
                "no_ask_dollars": "0.52",
            },
        }
    )

    ranked, selected, _checks = rank_and_enrich_symbols(
        settings=settings,
        policy=policy,
        candidates=[high_cs, low_cs],
        market_client=client,
        today=date(2026, 5, 8),
    )

    assert selected.stable_id == high_cs.stable_id
    assert ranked["symbols"][0]["stable_id"] == high_cs.stable_id
    assert ranked["symbols"][0]["consistency_score"] == 0.95


def test_ranked_symbols_honor_trading_thresholds(tmp_path: Path) -> None:
    brain_root = tmp_path / "brain"
    _write_policy(brain_root, allow_live_submit=True)
    settings = _settings(tmp_path, brain_root)
    _write_limits(settings)
    policy = load_policy(settings)
    high = candidates_from_board(_board_entry(edge=0.080, ev=0.035, confidence=75), policy=policy, limit=25)[0]
    high = type(high)(**{**high.__dict__, "model_prob": 0.70})
    low = candidates_from_board(_board_entry(edge=0.055, ev=0.025, confidence=68), policy=policy, limit=25)[0]
    low = type(high)(**{**low.__dict__, "stable_id": low.stable_id.replace("123", "124"), "game_id": "124", "model_prob": 0.58})
    Path(settings.kalshi_symbols_path).parent.mkdir(parents=True, exist_ok=True)
    Path(settings.kalshi_symbols_path).write_text(
        json.dumps(
            {
                "version": 1,
                "symbols": [
                    {"target_id": high.stable_id, "market_key": "points", "game_date": "2026-05-08", "player_id": "237", "line_value": 25.5, "recommendation": "buy_yes", "kalshi_ticker": "KX-HIGH"},
                    {"target_id": low.stable_id, "market_key": "points", "game_date": "2026-05-08", "player_id": "237", "line_value": 25.5, "recommendation": "buy_yes", "kalshi_ticker": "KX-LOW"},
                ],
                "unresolved": [],
            }
        ),
        encoding="utf-8",
    )
    store = SelectionStore(path=tmp_path / "selections.json")
    store.update_thresholds(min_hit_pct=0.60, min_edge_bps=450)
    client = _FakeMarketClient(
        {
            "KX-HIGH": {"status": "open", "yes_bid_dollars": "0.48", "yes_ask_dollars": "0.50", "no_bid_dollars": "0.49", "no_ask_dollars": "0.52"},
            "KX-LOW": {"status": "open", "yes_bid_dollars": "0.45", "yes_ask_dollars": "0.47", "no_bid_dollars": "0.51", "no_ask_dollars": "0.53"},
        }
    )

    ranked, selected, _checks = rank_and_enrich_symbols(
        settings=settings,
        policy=policy,
        candidates=[high, low],
        market_client=client,
        selection_store=store,
        today=date(2026, 5, 8),
    )

    assert selected == high
    by_id = {row["stable_id"]: row for row in ranked["symbols"]}
    assert by_id[high.stable_id]["candidate_status"] == "selected_live"
    assert by_id[low.stable_id]["candidate_status"] == "watchlist"
    assert by_id[low.stable_id]["recommendation"] == "observe_only"
    assert "trading_min_hit" in by_id[low.stable_id]["brain_blockers"]


def test_min_edge_policy_blocks_candidate_before_resolver(tmp_path: Path) -> None:
    brain_root = tmp_path / "brain"
    _write_policy(brain_root, allow_live_submit=True)
    settings = _settings(tmp_path, brain_root)
    policy = load_policy(settings)
    candidate = candidates_from_board(_board_entry(edge=0.040), policy=policy, limit=25)[0]

    payload, exportable, checks = export_resolution_targets(
        settings=settings,
        policy=policy,
        board_date=date(2026, 5, 8),
        candidates=[candidate],
    )

    assert exportable == []
    assert payload["targets"] == []
    assert any(check.key == "candidate_policy_blocks" for check in checks)
    assert any(check.key == "target_export" and check.status == "fail" for check in checks)


def test_supervised_live_sync_stays_observe_when_policy_blocks_live(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    brain_root = tmp_path / "brain"
    _write_policy(brain_root, allow_live_submit=False)
    settings = _settings(tmp_path, brain_root)
    _write_limits(settings)
    policy = load_policy(settings)
    candidate = candidates_from_board(_board_entry(), policy=policy, limit=25)[0]
    Path(settings.kalshi_symbols_path).parent.mkdir(parents=True, exist_ok=True)
    Path(settings.kalshi_symbols_path).write_text(
        json.dumps(
            {
                "version": 1,
                "symbols": [
                    {
                        "target_id": candidate.stable_id,
                        "market_key": "points",
                        "game_date": "2026-05-08",
                        "player_id": "237",
                        "line_value": 25.5,
                        "recommendation": "buy_yes",
                        "kalshi_ticker": "KX-HIGH",
                    }
                ],
                "unresolved": [],
            }
        ),
        encoding="utf-8",
    )
    calls: list[dict[str, Any]] = []

    def fake_write_pack(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        decisions_path = Path(kwargs["decisions_path"])
        decisions_path.parent.mkdir(parents=True, exist_ok=True)
        decisions_path.write_text(json.dumps({"version": 1, "decisions": []}), encoding="utf-8")
        return {"version": 1, "decisions": []}

    monkeypatch.setattr("app.trading.decision_brain.write_live_decision_pack", fake_write_pack)
    client = _FakeMarketClient(
        {
            "KX-HIGH": {
                "status": "open",
                "yes_bid_dollars": "0.48",
                "yes_ask_dollars": "0.50",
                "no_bid_dollars": "0.49",
                "no_ask_dollars": "0.52",
            }
        }
    )

    result = sync_decision_brain(
        settings=settings,
        board_entry=_board_entry(),
        board_date=date(2026, 5, 8),
        mode="supervised-live",
        resolve_markets=False,
        build_pack=True,
        market_client=client,
        today=date(2026, 5, 8),
    )

    assert result.state == "observe_only"
    assert calls[0]["arm_live"] is False
    assert any(check.key == "policy_live_submit" for check in result.checks)
    assert datetime.fromisoformat(result.synced_at.isoformat())


def test_blocked_sync_clears_stale_decision_pack(tmp_path: Path) -> None:
    brain_root = tmp_path / "brain"
    _write_policy(brain_root, allow_live_submit=True)
    settings = _settings(tmp_path, brain_root)
    _write_limits(settings)
    decisions_path = Path(settings.kalshi_decisions_path)
    decisions_path.parent.mkdir(parents=True, exist_ok=True)
    decisions_path.write_text(
        json.dumps(
            {
                "version": 1,
                "decisions": [
                    {
                        "decision_id": "old-live-row",
                        "mode": "live",
                        "game_date": "2026-05-07",
                        "kalshi": {"ticker": "OLD"},
                        "execution": {"allow_live_submit": True},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    symbols_path = Path(settings.kalshi_symbols_path)
    symbols_path.parent.mkdir(parents=True, exist_ok=True)
    symbols_path.write_text(json.dumps({"version": 1, "symbols": [], "unresolved": []}), encoding="utf-8")

    result = sync_decision_brain(
        settings=settings,
        board_entry=_board_entry(),
        board_date=date(2026, 5, 8),
        mode="supervised-live",
        resolve_markets=False,
        build_pack=True,
        today=date(2026, 5, 8),
    )

    pack = json.loads(decisions_path.read_text(encoding="utf-8"))
    assert result.state == "blocked"
    assert pack["decisions"] == []
    assert pack["board_date"] == "2026-05-08"
    assert "no eligible row-zero candidate" in pack["blocked_reason"]


def test_consistency_table_reloads_when_file_mtime_changes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import os
    import time

    import joblib

    from app.trading.decision_brain import _consistency_table, _consistency_table_cached
    from app.training.artifacts import ArtifactPaths

    _consistency_table_cached.cache_clear()

    artifact_path = tmp_path / "consistency_scores.joblib"
    joblib.dump({("p1", "points"): {"consistency_score": 0.5}}, artifact_path)

    fake_paths = ArtifactPaths(
        root=tmp_path,
        minutes_model=tmp_path / "minutes_model.joblib",
        stat_models=tmp_path / "stat_models.joblib",
        calibrators=tmp_path / "calibrators.joblib",
        metadata=tmp_path / "metadata.joblib",
        population_priors=tmp_path / "population_priors.joblib",
        consistency_scores=artifact_path,
    )
    monkeypatch.setattr("app.trading.decision_brain.artifact_paths", lambda *_args, **_kw: fake_paths)

    first = _consistency_table("v1", "ns1")
    assert first[("p1", "points")] == 0.5

    # Rewrite with a different score and bump mtime so the cache key changes
    time.sleep(0.05)
    joblib.dump({("p1", "points"): {"consistency_score": 0.9}}, artifact_path)
    new_mtime = artifact_path.stat().st_mtime + 1
    os.utime(artifact_path, (new_mtime, new_mtime))

    second = _consistency_table("v1", "ns1")
    assert second[("p1", "points")] == 0.9


def test_rank_sort_key_prefers_higher_consistency_score() -> None:
    from app.trading.decision_brain import DecisionBrainCandidate, _rank_sort_key

    base: dict[str, Any] = dict(
        stable_id="x",
        source="board",
        board_date=date(2026, 1, 1),
        candidate_status="candidate",
        market_key="points",
        player_id="p1",
        player_name="Player",
        game_id="g1",
        game_date=date(2026, 1, 1),
        line_value=20.5,
        recommendation="buy_yes",
        outcome_side="yes",
        book_side="buy",
        model_prob=0.55,
        market_prob=0.50,
        no_vig_market_prob=0.50,
        edge_bps=100,
        ev=0.05,
        confidence=0.6,
        contracts=10.0,
        max_price_dollars=0.6,
        post_only=True,
        time_in_force="GTC",
        title_contains_all=[],
        player_name_contains_any=[],
        stat_contains_any=[],
        acceptable_line_values=[],
        event_or_page_hint=None,
        exclude_multivariate=False,
        driver="board",
    )
    high = DecisionBrainCandidate(**base, consistency_score=0.85)
    low = DecisionBrainCandidate(**{**base, "stable_id": "y"}, consistency_score=0.20)
    row = {
        "rank_score": 0.5,
        "entry_price_dollars": 0.5,
        "spread_dollars": 0.05,
        "brain_blockers": [],
    }
    assert _rank_sort_key(row, high) < _rank_sort_key(row, low)


def test_decision_brain_uses_adjusted_over_probability_when_present(tmp_path: Path) -> None:
    """When the opportunity carries an adjusted_over_probability (set by the
    volatility pipeline), the candidate uses it as model_prob instead of the
    raw hit_probability."""
    brain_root = tmp_path / "brain"
    _write_policy(brain_root)
    settings = _settings(tmp_path, brain_root)
    policy = load_policy(settings)

    board = _board_entry()
    opportunity = board.opportunities[0]
    # Inject volatility-adjusted fields onto the fake opportunity.
    object.__setattr__(opportunity, "adjusted_over_probability", 0.55)
    object.__setattr__(opportunity, "volatility_coefficient", 0.4)
    object.__setattr__(opportunity, "volatility_tier", "medium")

    candidates = candidates_from_board(board, policy=policy, limit=25)
    assert len(candidates) == 1
    # adjusted_over_probability (0.55) is below the un-adjusted hit_probability
    # (0.612), so model_prob should reflect the adjustment.
    assert candidates[0].model_prob == pytest.approx(0.55, abs=1e-6)


def test_decision_brain_blocks_candidate_above_max_volatility(tmp_path: Path) -> None:
    """When policy.max_volatility_coefficient is set, opportunities with
    volatility above it are dropped."""
    brain_root = tmp_path / "brain"
    _write_policy(brain_root)
    # Patch the policy file to include a max_volatility_coefficient gate.
    policy_path = brain_root / "00 System" / "Policy Core.md"
    existing = policy_path.read_text(encoding="utf-8")
    policy_path.write_text(
        existing.replace(
            "ranking_weight_freshness: 0.10",
            "ranking_weight_freshness: 0.10\nmax_volatility_coefficient: 0.50",
        ),
        encoding="utf-8",
    )

    settings = _settings(tmp_path, brain_root)
    policy = load_policy(settings)
    assert policy.max_volatility_coefficient == 0.50

    board = _board_entry()
    opportunity = board.opportunities[0]
    object.__setattr__(opportunity, "volatility_coefficient", 0.80)  # above 0.50 gate
    object.__setattr__(opportunity, "adjusted_over_probability", 0.55)
    object.__setattr__(opportunity, "volatility_tier", "high")

    candidates = candidates_from_board(board, policy=policy, limit=25)
    assert candidates == []


def test_decision_brain_passes_candidate_below_max_volatility(tmp_path: Path) -> None:
    """An opportunity below the gate is still admitted."""
    brain_root = tmp_path / "brain"
    _write_policy(brain_root)
    policy_path = brain_root / "00 System" / "Policy Core.md"
    existing = policy_path.read_text(encoding="utf-8")
    policy_path.write_text(
        existing.replace(
            "ranking_weight_freshness: 0.10",
            "ranking_weight_freshness: 0.10\nmax_volatility_coefficient: 0.90",
        ),
        encoding="utf-8",
    )

    settings = _settings(tmp_path, brain_root)
    policy = load_policy(settings)

    board = _board_entry()
    opportunity = board.opportunities[0]
    object.__setattr__(opportunity, "volatility_coefficient", 0.30)
    object.__setattr__(opportunity, "adjusted_over_probability", 0.60)

    candidates = candidates_from_board(board, policy=policy, limit=25)
    assert len(candidates) == 1

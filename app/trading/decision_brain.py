from __future__ import annotations

import hashlib
import json
import re
import shutil
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any

import httpx

from app.config.settings import Settings
from app.trading.live_limits import load_live_limits
from app.trading.live_pack_builder import (
    LivePackBuildError,
    build_pack_document,
    evaluate_gates_for_row,
    row_to_monitored,
    write_live_decision_pack,
)
from app.trading.monitoring import KalshiPublicMarketDataClient, MarketDataClient, fetch_quote_snapshot
from app.trading.selections import SelectionStore
from app.training.artifacts import artifact_exists, artifact_paths, load_artifact, resolve_artifact_namespace

BRAIN_SECTION_RELATIVE_PATH = Path("05 Knowledge and Skills") / "Data Analysis" / "Kalshi Market Decision Brain"
POLICY_RELATIVE_PATH = Path("00 System") / "Policy Core.md"
CANDIDATES_RELATIVE_PATH = Path("40 Candidates")
SNAPSHOTS_RELATIVE_PATH = Path("50 Snapshots")

_ALLOWED_CANDIDATE_STATUSES = {"candidate", "selected_observe_only", "selected_live"}
_BLOCKING_CANDIDATE_STATUSES = {"blocked", "disabled", "manual_review", "postmortem_ready"}
_OPEN_MARKET_STATUSES = {"open", "active"}
_KALSHI_PLAYER_PROP_SERIES_BY_ALIAS = {
    "points": "KXNBAPTS",
    "pts": "KXNBAPTS",
    "player_points": "KXNBAPTS",
    "rebounds": "KXNBAREB",
    "rebs": "KXNBAREB",
    "player_rebounds": "KXNBAREB",
    "assists": "KXNBAAST",
    "asts": "KXNBAAST",
    "player_assists": "KXNBAAST",
    "threes": "KXNBA3PT",
    "three_pointers": "KXNBA3PT",
    "3pt": "KXNBA3PT",
    "pra": "KXNBAPRA",
    "points_rebounds_assists": "KXNBAPRA",
    "points_rebounds": "KXNBAPR",
    "pr": "KXNBAPR",
    "points_assists": "KXNBAPA",
    "pa": "KXNBAPA",
    "rebounds_assists": "KXNBARA",
    "ra": "KXNBARA",
}


class FrontmatterError(ValueError):
    """Raised when a brain note contains unsupported machine frontmatter."""


class DecisionBrainError(RuntimeError):
    """Raised when the decision brain cannot safely produce runtime artifacts."""


@dataclass(frozen=True)
class BrainCheck:
    key: str
    label: str
    status: str
    detail: str


@dataclass(frozen=True)
class DecisionBrainPolicy:
    policy_version: str
    policy_hash: str
    allow_live_submit: bool
    allowed_market_keys: set[str]
    blocked_market_keys: set[str]
    min_edge_bps: int
    min_model_prob: float
    min_confidence: float
    max_price_dollars_default: float
    max_spread_dollars: float
    max_contracts: float
    post_only: bool
    time_in_force: str
    same_day_only: bool
    ranking_weight_edge_bps: float
    ranking_weight_ev: float
    ranking_weight_liquidity: float
    ranking_weight_calibration: float
    ranking_weight_freshness: float
    require_injury_refresh_minutes: int = 30
    require_projection_refresh_minutes: int = 60


@dataclass(frozen=True)
class DecisionBrainCandidate:
    stable_id: str
    source: str
    board_date: date
    candidate_status: str
    market_key: str
    player_id: str | None
    player_name: str | None
    game_id: str | None
    game_date: date
    line_value: float
    recommendation: str
    outcome_side: str
    book_side: str
    model_prob: float | None
    market_prob: float | None
    no_vig_market_prob: float | None
    edge_bps: int | None
    ev: float | None
    confidence: float | None
    contracts: float
    max_price_dollars: float
    post_only: bool
    time_in_force: str
    title_contains_all: list[str]
    player_name_contains_any: list[str]
    stat_contains_any: list[str]
    acceptable_line_values: list[float]
    event_or_page_hint: str | None
    exclude_multivariate: bool
    driver: str
    policy_version: str | None = None
    consistency_score: float = 0.0


@dataclass(frozen=True)
class TradingBrainSyncResult:
    state: str
    policy_version: str | None
    policy_hash: str | None
    board_date: str
    mode: str
    generated_candidate_count: int
    manual_candidate_count: int
    exported_target_count: int
    resolved_symbol_count: int
    unresolved_symbol_count: int
    selected_candidate_id: str | None
    selected_ticker: str | None
    selected_candidate_ids: list[str]
    selected_tickers: list[str]
    live_candidate_count: int
    targets_path: str
    symbols_path: str
    decisions_path: str
    snapshot_dir: str | None
    checks: list[BrainCheck]
    synced_at: datetime

    def to_payload(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "policy_version": self.policy_version,
            "policy_hash": self.policy_hash,
            "board_date": self.board_date,
            "mode": self.mode,
            "generated_candidate_count": self.generated_candidate_count,
            "manual_candidate_count": self.manual_candidate_count,
            "exported_target_count": self.exported_target_count,
            "resolved_symbol_count": self.resolved_symbol_count,
            "unresolved_symbol_count": self.unresolved_symbol_count,
            "selected_candidate_id": self.selected_candidate_id,
            "selected_ticker": self.selected_ticker,
            "selected_candidate_ids": self.selected_candidate_ids,
            "selected_tickers": self.selected_tickers,
            "live_candidate_count": self.live_candidate_count,
            "targets_path": self.targets_path,
            "symbols_path": self.symbols_path,
            "decisions_path": self.decisions_path,
            "snapshot_dir": self.snapshot_dir,
            "checks": [check.__dict__ for check in self.checks],
            "synced_at": self.synced_at.isoformat(),
        }


def decision_brain_root(settings: Settings) -> Path:
    configured = settings.kalshi_decision_brain_root
    if configured is not None:
        return Path(configured).expanduser()
    return Path(settings.brain_vault_root).expanduser() / BRAIN_SECTION_RELATIVE_PATH


def brain_status_path(settings: Settings) -> Path:
    return Path(settings.snapshot_dir) / "kalshi_decision_brain_status.json"


def _brain_artifact_namespace(settings: Settings) -> str:
    """Namespace slug used by ``TrainingPipeline`` / ``artifact_paths`` for on-disk bundles."""

    return resolve_artifact_namespace(settings.database_url, settings.app_env)


def _consistency_table(model_version: str, namespace: str) -> dict[tuple[str, str], float]:
    paths = artifact_paths(model_version, namespace)
    if not artifact_exists(paths.consistency_scores):
        return {}
    try:
        mtime_ns = paths.consistency_scores.stat().st_mtime_ns
    except OSError:
        return {}
    return _consistency_table_cached(str(paths.consistency_scores), mtime_ns)


@lru_cache(maxsize=4)
def _consistency_table_cached(path_str: str, _mtime_ns: int) -> dict[tuple[str, str], float]:
    raw = load_artifact(Path(path_str))
    if not isinstance(raw, dict):
        return {}
    out: dict[tuple[str, str], float] = {}
    for key, value in raw.items():
        if not isinstance(key, tuple) or len(key) != 2:
            continue
        if isinstance(value, dict):
            score = float(value.get("consistency_score", 0.0))
        else:
            try:
                score = float(value)
            except (TypeError, ValueError):
                continue
        out[(str(key[0]), str(key[1]))] = score
    return out


def trading_selections_path(settings: Settings) -> Path:
    return Path(settings.app_data_dir) / "trading_selections.json"


def load_last_brain_status(settings: Settings) -> TradingBrainSyncResult | None:
    path = brain_status_path(settings)
    if not path.is_file():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    return _result_from_payload(payload)


def default_brain_status(settings: Settings) -> TradingBrainSyncResult:
    return TradingBrainSyncResult(
        state="blocked",
        policy_version=None,
        policy_hash=None,
        board_date=date.today().isoformat(),
        mode="observe",
        generated_candidate_count=0,
        manual_candidate_count=0,
        exported_target_count=0,
        resolved_symbol_count=0,
        unresolved_symbol_count=0,
        selected_candidate_id=None,
        selected_ticker=None,
        selected_candidate_ids=[],
        selected_tickers=[],
        live_candidate_count=0,
        targets_path=str(Path(settings.kalshi_resolution_targets_path)),
        symbols_path=str(Path(settings.kalshi_symbols_path)),
        decisions_path=str(Path(settings.kalshi_decisions_path)),
        snapshot_dir=None,
        checks=[
            BrainCheck(
                key="brain_sync",
                label="Decision brain sync",
                status="warn",
                detail="No decision-brain sync has been recorded yet.",
            )
        ],
        synced_at=datetime.now(UTC),
    )


def write_blocked_decision_pack(
    *,
    settings: Settings,
    board_date: date,
    mode: str,
    reason: str,
    policy: DecisionBrainPolicy | None = None,
) -> None:
    """Overwrite any stale executable pack with a current empty blocked pack."""
    path = Path(settings.kalshi_decisions_path)
    payload = build_pack_document([])
    payload["state"] = "blocked"
    payload["board_date"] = board_date.isoformat()
    payload["mode"] = mode
    payload["blocked_reason"] = reason
    payload["brain"] = {
        "policy_version": policy.policy_version if policy else None,
        "policy_hash": policy.policy_hash if policy else None,
        "selected_candidate_id": None,
        "selected_candidate_ids": [],
        "live_candidate_count": 0,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def parse_frontmatter_file(path: Path) -> tuple[dict[str, Any], str]:
    text = path.read_text(encoding="utf-8")
    return parse_frontmatter_text(text, source=str(path))


def parse_frontmatter_text(text: str, *, source: str = "<memory>") -> tuple[dict[str, Any], str]:
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        raise FrontmatterError(f"{source}: missing opening frontmatter marker")
    end_index: int | None = None
    for index, line in enumerate(lines[1:], start=1):
        if line.strip() == "---":
            end_index = index
            break
    if end_index is None:
        raise FrontmatterError(f"{source}: missing closing frontmatter marker")
    frontmatter_lines = lines[1:end_index]
    raw = "\n".join(frontmatter_lines)
    return _parse_simple_frontmatter(frontmatter_lines, source=source), raw


def load_policy(settings: Settings) -> DecisionBrainPolicy:
    root = decision_brain_root(settings)
    policy_path = root / POLICY_RELATIVE_PATH
    if not policy_path.is_file():
        raise DecisionBrainError(f"policy note missing at {policy_path}")
    fields, raw = parse_frontmatter_file(policy_path)
    if fields.get("brain_type") != "policy_core":
        raise DecisionBrainError("Policy Core.md is not marked brain_type=policy_core")
    policy_hash = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    policy_version = _required_str(fields, "policy_version")
    allow_live_submit = _required_bool(fields, "allow_live_submit")
    return DecisionBrainPolicy(
        policy_version=policy_version,
        policy_hash=policy_hash,
        allow_live_submit=allow_live_submit,
        allowed_market_keys={_norm_key(value) for value in _list_str(fields.get("allowed_market_keys"))},
        blocked_market_keys={_norm_key(value) for value in _list_str(fields.get("blocked_market_keys"))},
        min_edge_bps=_required_int(fields, "min_edge_bps"),
        min_model_prob=_required_float(fields, "min_model_prob"),
        min_confidence=_required_float(fields, "min_confidence"),
        max_price_dollars_default=_required_float(fields, "max_price_dollars_default"),
        max_spread_dollars=_required_float(fields, "max_spread_dollars"),
        max_contracts=_required_float(fields, "max_contracts"),
        post_only=_bool_or(fields.get("post_only"), True),
        time_in_force=str(fields.get("time_in_force") or "good_till_canceled"),
        same_day_only=_bool_or(fields.get("same_day_only"), True),
        ranking_weight_edge_bps=_float_or(fields.get("ranking_weight_edge_bps"), 0.45),
        ranking_weight_ev=_float_or(fields.get("ranking_weight_ev"), 0.20),
        ranking_weight_liquidity=_float_or(fields.get("ranking_weight_liquidity"), 0.15),
        ranking_weight_calibration=_float_or(fields.get("ranking_weight_calibration"), 0.10),
        ranking_weight_freshness=_float_or(fields.get("ranking_weight_freshness"), 0.10),
        require_injury_refresh_minutes=int(fields.get("require_injury_refresh_minutes") or 30),
        require_projection_refresh_minutes=int(fields.get("require_projection_refresh_minutes") or 60),
    )


def load_manual_candidates(
    settings: Settings,
    *,
    board_date: date,
    policy: DecisionBrainPolicy,
) -> list[DecisionBrainCandidate]:
    root = decision_brain_root(settings)
    base = root / CANDIDATES_RELATIVE_PATH
    candidate_dirs = [base / board_date.isoformat()]
    if base.is_dir():
        candidate_dirs.append(base)
    paths: list[Path] = []
    for directory in candidate_dirs:
        if directory.is_dir():
            paths.extend(sorted(path for path in directory.glob("*.md") if path.is_file()))

    candidates: list[DecisionBrainCandidate] = []
    seen: set[Path] = set()
    for path in paths:
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        fields, _raw = parse_frontmatter_file(path)
        if fields.get("brain_type") != "candidate":
            continue
        candidate = _candidate_from_frontmatter(fields, board_date=board_date, policy=policy, source="vault")
        if candidate.board_date == board_date:
            candidates.append(candidate)
    return candidates


def candidates_from_board(
    board_entry: Any | None,
    *,
    policy: DecisionBrainPolicy,
    limit: int,
) -> list[DecisionBrainCandidate]:
    if board_entry is None:
        return []
    board_date = _coerce_date(getattr(board_entry, "board_date", None), fallback=date.today())
    opportunities = list(getattr(board_entry, "opportunities", []) or [])[:limit]
    insights = getattr(board_entry, "opportunity_insights", {}) or {}
    candidates: list[DecisionBrainCandidate] = []
    for opportunity in opportunities:
        candidate = _candidate_from_opportunity(
            opportunity,
            board_date=board_date,
            insights=insights,
            policy=policy,
        )
        if candidate is not None:
            candidates.append(candidate)
    return candidates


def merge_candidates(
    board_candidates: list[DecisionBrainCandidate],
    manual_candidates: list[DecisionBrainCandidate],
) -> list[DecisionBrainCandidate]:
    merged: dict[str, DecisionBrainCandidate] = {candidate.stable_id: candidate for candidate in board_candidates}
    for manual in manual_candidates:
        existing = merged.get(manual.stable_id)
        if existing is None:
            merged[manual.stable_id] = manual
            continue
        merged[manual.stable_id] = replace(manual, source=f"{existing.source}+vault")
    return list(merged.values())


def export_resolution_targets(
    *,
    settings: Settings,
    policy: DecisionBrainPolicy,
    board_date: date,
    candidates: list[DecisionBrainCandidate],
) -> tuple[dict[str, Any], list[DecisionBrainCandidate], list[BrainCheck]]:
    checks: list[BrainCheck] = []
    exportable: list[DecisionBrainCandidate] = []
    blocked: list[str] = []
    for candidate in candidates:
        reasons = _candidate_policy_blockers(candidate, policy=policy, board_date=board_date)
        if reasons:
            blocked.append(f"{candidate.stable_id}: {', '.join(reasons)}")
            continue
        exportable.append(candidate)

    if blocked:
        checks.append(
            BrainCheck(
                key="candidate_policy_blocks",
                label="Candidate policy blocks",
                status="warn",
                detail=f"{len(blocked)} candidate(s) blocked before resolver",
            )
        )
    checks.append(
        BrainCheck(
            key="target_export",
            label="Resolution target export",
            status="pass" if exportable else "fail",
            detail=f"{len(exportable)} target(s) exported",
        )
    )

    payload = {
        "version": 1,
        "generated_at": datetime.now(UTC).isoformat(),
        "source": "app.trading.decision_brain",
        "brain": {
            "policy_version": policy.policy_version,
            "policy_hash": policy.policy_hash,
            "board_date": board_date.isoformat(),
        },
        "defaults": {
            "market_status": "open",
            "series_tickers": sorted(
                {series for candidate in exportable if (series := _kalshi_series_ticker(candidate.market_key))}
            ),
            "exclude_multivariate": True,
            "contracts": f"{policy.max_contracts:.2f}",
            "max_price_dollars": f"{policy.max_price_dollars_default:.4f}",
            "post_only": policy.post_only,
            "time_in_force": policy.time_in_force,
        },
        "targets": [_candidate_to_target(candidate) for candidate in exportable],
    }
    path = Path(settings.kalshi_resolution_targets_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return payload, exportable, checks


def resolve_brain_targets(
    *,
    settings: Settings,
    targets_payload: dict[str, Any],
    min_score: int = 70,
) -> dict[str, Any]:
    targets = targets_payload.get("targets", [])
    if not isinstance(targets, list) or not all(isinstance(target, dict) for target in targets):
        raise DecisionBrainError("target export must contain a targets array")
    defaults = targets_payload.get("defaults", {}) if isinstance(targets_payload.get("defaults"), dict) else {}
    from scripts.resolve_kalshi_targets import _load_candidate_markets, resolve_targets

    with httpx.Client(timeout=float(settings.request_timeout_seconds)) as client:
        markets = _load_candidate_markets(
            client=client,
            base_url=settings.kalshi_market_data_base_url,
            targets=targets,
            status=str(defaults.get("market_status") or "open"),
            mve_filter="exclude" if defaults.get("exclude_multivariate", True) else "only",
            series_ticker=(str(defaults["series_ticker"]) if defaults.get("series_ticker") else None),
            series_tickers=defaults.get("series_tickers"),
        )
    resolved = resolve_targets(targets, markets, min_score)
    resolved["source"] = "app.trading.decision_brain+scripts.resolve_kalshi_targets"
    resolved["brain"] = targets_payload.get("brain", {})
    path = Path(settings.kalshi_symbols_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(resolved, indent=2) + "\n", encoding="utf-8")
    return resolved



def _stale_context_blockers(settings: Settings, policy: DecisionBrainPolicy) -> list[str]:
    """Return veto keys for stale injury or projection context."""
    import sqlite3
    from datetime import timedelta

    blockers: list[str] = []
    db_url: str = settings.database_url
    if not db_url.startswith("sqlite:///"):
        return blockers
    db_path = db_url[len("sqlite:///"):]
    now = datetime.now(UTC)
    try:
        con = sqlite3.connect(db_path)
        try:
            cur = con.cursor()
            cur.execute("SELECT MAX(report_timestamp) FROM injury_reports")
            row = cur.fetchone()
            latest_injury = row[0] if row and row[0] else None
            if latest_injury is None:
                blockers.append("stale_injury_context")
            else:
                ts = datetime.fromisoformat(latest_injury)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=UTC)
                cutoff = now - timedelta(minutes=policy.require_injury_refresh_minutes)
                if ts < cutoff:
                    blockers.append("stale_injury_context")

            cur.execute("SELECT MAX(predicted_at) FROM predictions")
            row = cur.fetchone()
            latest_pred = row[0] if row and row[0] else None
            if latest_pred is None:
                blockers.append("stale_projection_context")
            else:
                ts = datetime.fromisoformat(latest_pred)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=UTC)
                cutoff = now - timedelta(minutes=policy.require_projection_refresh_minutes)
                if ts < cutoff:
                    blockers.append("stale_projection_context")
        finally:
            con.close()
    except Exception:  # pragma: no cover - best-effort check
        pass
    return blockers


def rank_and_enrich_symbols(
    *,
    settings: Settings,
    policy: DecisionBrainPolicy,
    candidates: list[DecisionBrainCandidate],
    market_client: MarketDataClient | None = None,
    selection_store: SelectionStore | None = None,
    today: date | None = None,
    session_blockers: list[str] | None = None,
) -> tuple[dict[str, Any], DecisionBrainCandidate | None, list[BrainCheck]]:
    symbols_path = Path(settings.kalshi_symbols_path)
    if not symbols_path.is_file():
        raise DecisionBrainError(f"symbol map missing at {symbols_path}")
    payload = json.loads(symbols_path.read_text(encoding="utf-8"))
    symbols = payload.get("symbols", []) if isinstance(payload, dict) else []
    unresolved = payload.get("unresolved", []) if isinstance(payload, dict) else []
    if not isinstance(symbols, list):
        raise DecisionBrainError("symbol map must contain a symbols array")
    if not isinstance(unresolved, list):
        unresolved = []

    limits = load_live_limits(settings.trading_limits_path)
    candidate_by_id = {candidate.stable_id: candidate for candidate in candidates}
    rows: list[dict[str, Any]] = []
    checks: list[BrainCheck] = [
        BrainCheck(
            key="market_resolution",
            label="Market resolution",
            status="pass" if symbols else "fail",
            detail=f"{len(symbols)} resolved, {len(unresolved)} unresolved",
        )
    ]
    selected_candidate: DecisionBrainCandidate | None = None
    selected_candidates: list[DecisionBrainCandidate] = []

    def with_client(client: MarketDataClient) -> None:
        nonlocal selected_candidate
        enriched: list[tuple[dict[str, Any], float, tuple[Any, ...], DecisionBrainCandidate | None, bool, bool]] = []
        for raw in symbols:
            if not isinstance(raw, dict):
                continue
            row = dict(raw)
            candidate = candidate_by_id.get(str(row.get("target_id") or ""))
            if candidate is None:
                row["brain_blockers"] = ["missing_candidate_metadata"]
                row["recommendation"] = "observe_only"
                row["rank_score"] = 0.0
                enriched.append((
                    row,
                    0.0,
                    (1, -0.0, -0.0, -0.0, -0.0, 999.0, 999.0, -0.0, str(row.get("target_id") or "")),
                    None,
                    False,
                    False,
                ))
                continue
            row.update(_candidate_symbol_fields(candidate, policy))
            row["consistency_score"] = candidate.consistency_score
            blockers = _candidate_policy_blockers(candidate, policy=policy, board_date=candidate.board_date)
            if session_blockers:
                blockers = list(session_blockers) + blockers
            selection_blockers = _trading_selection_blockers(candidate, selection_store)
            quote = None
            monitored = row_to_monitored(row)
            if monitored is None:
                blockers.append("unresolved_ticker")
            else:
                quote = fetch_quote_snapshot(monitored, client)
                gate = evaluate_gates_for_row(
                    row,
                    limits=limits,
                    risk=_candidate_risk(candidate),
                    quote=quote,
                    max_spread=policy.max_spread_dollars,
                    today=today,
                )
                for key, passed in gate.gates.items():
                    if not passed:
                        blockers.append(_gate_blocker_key(key))
                row["entry_price_dollars"] = gate.entry
                row["spread_dollars"] = gate.spread
                row["quote_error"] = gate.quote_error
                row["rank_gate_snapshot"] = dict(gate.gates)
                if quote is not None:
                    row["rank_quote_observed_at"] = quote.observed_at.isoformat()
                    row["rank_market_status"] = quote.status

            score = 0.0
            eligible = not blockers and not selection_blockers
            if eligible:
                score = _rank_score(
                    policy=policy,
                    edge_bps=float(candidate.edge_bps or 0),
                    ev=float(candidate.ev or 0.0),
                    spread=_float_or(row.get("spread_dollars"), 0.0),
                    confidence=float(candidate.confidence or 0.0),
                )
            row["rank_score"] = round(score, 6)
            row["brain_blockers"] = sorted(set(blockers + selection_blockers))
            row["original_recommendation"] = candidate.recommendation
            if not eligible:
                row["recommendation"] = "observe_only"
            sort_key = _rank_sort_key(row, candidate)
            enriched.append((row, score, sort_key, candidate, eligible, bool(blockers)))

        enriched.sort(key=lambda item: item[2])
        for index, (row, _score, _sort_key, candidate, eligible, hard_blocked) in enumerate(enriched, start=1):
            row["selection_rank"] = index
            if candidate is not None and eligible:
                if selected_candidate is None:
                    selected_candidate = candidate
                selected_candidates.append(candidate)
                row["candidate_status"] = (
                    "selected_live" if policy.allow_live_submit else "selected_observe_only"
                )
                row["recommendation"] = candidate.recommendation if policy.allow_live_submit else "observe_only"
            elif candidate is not None:
                row["recommendation"] = "observe_only"
                row["candidate_status"] = "blocked" if hard_blocked else "watchlist"
            else:
                row["recommendation"] = "observe_only"
                row["candidate_status"] = "blocked"
            rows.append(row)

    if market_client is not None:
        with_client(market_client)
    else:
        with KalshiPublicMarketDataClient(base_url=settings.kalshi_market_data_base_url) as client:
            with_client(client)

    selected_ids = [candidate.stable_id for candidate in selected_candidates]
    selected_ticker_values = _selected_tickers({"symbols": rows}, selected_ids)
    output = {
        "version": 1,
        "generated_at": datetime.now(UTC).isoformat(),
        "source": "app.trading.decision_brain.rank_and_enrich_symbols",
        "brain": {
            "policy_version": policy.policy_version,
            "policy_hash": policy.policy_hash,
            "selected_candidate_id": selected_candidate.stable_id if selected_candidate else None,
            "selected_candidate_ids": selected_ids,
            "selected_tickers": selected_ticker_values,
            "live_candidate_count": len(selected_candidates) if policy.allow_live_submit else 0,
        },
        "symbols": rows,
        "unresolved": unresolved,
    }
    symbols_path.parent.mkdir(parents=True, exist_ok=True)
    symbols_path.write_text(json.dumps(output, indent=2) + "\n", encoding="utf-8")
    checks.append(
        BrainCheck(
            key="ranked_winner",
            label="Ranked row zero",
            status="pass" if selected_candidate else "fail",
            detail=(
                f"{len(selected_candidates)} eligible row(s); first={selected_candidate.stable_id}"
                if selected_candidate
                else "No eligible ranked symbol row"
            ),
        )
    )
    return output, selected_candidate, checks


def sync_decision_brain(
    *,
    settings: Settings,
    board_entry: Any | None = None,
    board_date: date | None = None,
    mode: str = "observe",
    candidate_limit: int | None = None,
    resolve_markets: bool = True,
    build_pack: bool = True,
    market_client: MarketDataClient | None = None,
    today: date | None = None,
) -> TradingBrainSyncResult:
    normalized_mode = mode.strip().lower().replace("_", "-")
    if normalized_mode not in {"observe", "supervised-live"}:
        raise DecisionBrainError("mode must be observe or supervised-live")
    sync_date = board_date or _coerce_date(getattr(board_entry, "board_date", None), fallback=date.today())
    checks: list[BrainCheck] = []
    policy: DecisionBrainPolicy | None = None
    selected: DecisionBrainCandidate | None = None
    symbols_payload: dict[str, Any] = {}
    snapshot_dir: Path | None = None
    exported_count = 0
    generated_count = 0
    manual_count = 0
    selected_candidate_ids: list[str] = []
    selected_tickers: list[str] = []
    state = "failed"

    try:
        if build_pack:
            write_blocked_decision_pack(
                settings=settings,
                board_date=sync_date,
                mode=normalized_mode,
                reason="decision brain sync started",
            )
        if not settings.kalshi_decision_brain_enabled:
            raise DecisionBrainError("KALSHI_DECISION_BRAIN_ENABLED is false")
        policy = load_policy(settings)
        stale_blockers = _stale_context_blockers(settings, policy)
        for stale_key in stale_blockers:
            checks.append(
                BrainCheck(
                    key=stale_key,
                    label=stale_key.replace("_", " ").title(),
                    status="fail",
                    detail="Context data is stale; refresh before submitting live orders",
                )
            )
        checks.append(
            BrainCheck(
                key="policy_loaded",
                label="Policy loaded",
                status="pass",
                detail=f"{policy.policy_version} ({policy.policy_hash})",
            )
        )
        limit = candidate_limit or settings.kalshi_decision_brain_candidate_limit
        board_candidates = candidates_from_board(board_entry, policy=policy, limit=limit)
        manual_candidates = load_manual_candidates(settings, board_date=sync_date, policy=policy)
        generated_count = len(board_candidates)
        manual_count = len(manual_candidates)
        candidates = merge_candidates(board_candidates, manual_candidates)
        table = _consistency_table(settings.model_version, _brain_artifact_namespace(settings))
        candidates = [
            replace(
                c,
                consistency_score=float(
                    table.get((str(c.player_id or ""), _norm_key(c.market_key).split(".")[-1]), 0.0)
                ),
            )
            for c in candidates
        ]
        checks.append(
            BrainCheck(
                key="candidate_sources",
                label="Candidate sources",
                status="pass" if candidates else "warn",
                detail=f"{generated_count} board, {manual_count} vault",
            )
        )
        targets_payload, exportable, export_checks = export_resolution_targets(
            settings=settings,
            policy=policy,
            board_date=sync_date,
            candidates=candidates,
        )
        checks.extend(export_checks)
        exported_count = len(exportable)
        if not exportable:
            raise DecisionBrainError("no candidates passed policy validation")

        if resolve_markets:
            symbols_payload = resolve_brain_targets(settings=settings, targets_payload=targets_payload)
        else:
            symbols_path = Path(settings.kalshi_symbols_path)
            if not symbols_path.is_file():
                raise DecisionBrainError("resolve_markets=false but no symbol map exists")
            symbols_payload = json.loads(symbols_path.read_text(encoding="utf-8"))

        symbols_payload, selected, rank_checks = rank_and_enrich_symbols(
            settings=settings,
            policy=policy,
            candidates=exportable,
            market_client=market_client,
            selection_store=SelectionStore.load(trading_selections_path(settings)),
            today=today,
            session_blockers=stale_blockers,
        )
        selected_candidate_ids = _selected_candidate_ids(symbols_payload)
        selected_tickers = _selected_tickers(symbols_payload, selected_candidate_ids)
        checks.extend(rank_checks)
        top_by_cs = sorted(exportable, key=lambda cand: cand.consistency_score, reverse=True)[:3]
        top_detail = "; ".join(
            f"{cand.player_name or cand.player_id or '?'} "
            f"market={_norm_key(cand.market_key).split('.')[-1]} cs={cand.consistency_score:.2f}"
            for cand in top_by_cs
        )
        checks.append(
            BrainCheck(
                key="top_consistency",
                label="Top by consistency",
                status="pass" if top_detail else "warn",
                detail=top_detail or "No exported candidates",
            )
        )
        stk_pick = selected_tickers[0] if selected_tickers else None
        if selected is not None:
            suf_sel = _norm_key(selected.market_key).split(".")[-1]
            sel_detail = (
                f"{selected.player_name or selected.player_id or '?'} market={suf_sel} "
                f"cs={selected.consistency_score:.2f}"
            )
            if stk_pick:
                sel_detail += f" → ticker={stk_pick}"
            sel_status = "pass"
        else:
            sel_detail = "none"
            sel_status = "fail"
        checks.append(
            BrainCheck(
                key="selected_summary",
                label="Selected for execution",
                status=sel_status,
                detail=sel_detail,
            )
        )
        if selected is None:
            raise DecisionBrainError("no eligible row-zero candidate after ranking")

        arm_live = normalized_mode == "supervised-live" and policy.allow_live_submit
        if normalized_mode == "supervised-live" and not policy.allow_live_submit:
            checks.append(
                BrainCheck(
                    key="policy_live_submit",
                    label="Policy live submit",
                    status="warn",
                    detail="Vault policy allow_live_submit=false; pack will remain observe-only.",
                )
            )
        if build_pack:
            write_live_decision_pack(
                decisions_path=Path(settings.kalshi_decisions_path),
                settings=settings,
                arm_live=arm_live,
                max_spread=policy.max_spread_dollars,
            )
            checks.append(
                BrainCheck(
                    key="decision_pack",
                    label="Decision pack",
                    status="pass",
                    detail="live-armed" if arm_live else "observe-only",
                )
        )
        state = "synced" if arm_live else "observe_only"
        snapshot_dir = _write_snapshot(settings=settings, board_date=sync_date)
    except Exception as exc:  # noqa: BLE001 - the public sync surface must fail closed
        if build_pack:
            try:
                write_blocked_decision_pack(
                    settings=settings,
                    board_date=sync_date,
                    mode=normalized_mode,
                    reason=str(exc),
                    policy=policy,
                )
            except Exception as pack_exc:  # noqa: BLE001 - preserve original sync failure
                checks.append(
                    BrainCheck(
                        key="blocked_pack",
                        label="Blocked decision pack",
                        status="warn",
                        detail=f"Could not clear stale decision pack: {pack_exc}",
                    )
                )
        checks.append(
            BrainCheck(
                key="sync_failed",
                label="Decision brain sync",
                status="fail",
                detail=str(exc),
            )
        )
        state = "blocked" if isinstance(exc, (DecisionBrainError, LivePackBuildError)) else "failed"

    result = TradingBrainSyncResult(
        state=state,
        policy_version=policy.policy_version if policy else None,
        policy_hash=policy.policy_hash if policy else None,
        board_date=sync_date.isoformat(),
        mode=normalized_mode,
        generated_candidate_count=generated_count,
        manual_candidate_count=manual_count,
        exported_target_count=exported_count,
        resolved_symbol_count=len(symbols_payload.get("symbols", [])) if isinstance(symbols_payload, dict) else 0,
        unresolved_symbol_count=len(symbols_payload.get("unresolved", [])) if isinstance(symbols_payload, dict) else 0,
        selected_candidate_id=selected.stable_id if selected else None,
        selected_ticker=_selected_ticker(symbols_payload, selected),
        selected_candidate_ids=selected_candidate_ids,
        selected_tickers=selected_tickers,
        live_candidate_count=len(selected_tickers) if state == "synced" else 0,
        targets_path=str(Path(settings.kalshi_resolution_targets_path)),
        symbols_path=str(Path(settings.kalshi_symbols_path)),
        decisions_path=str(Path(settings.kalshi_decisions_path)),
        snapshot_dir=str(snapshot_dir) if snapshot_dir is not None else None,
        checks=checks,
        synced_at=datetime.now(UTC),
    )
    _write_status(settings, result)
    return result


def _parse_simple_frontmatter(lines: list[str], *, source: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    index = 0
    while index < len(lines):
        raw_line = lines[index]
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            index += 1
            continue
        if raw_line[:1].isspace():
            raise FrontmatterError(f"{source}: unsupported nested frontmatter near '{stripped}'")
        if ":" not in raw_line:
            raise FrontmatterError(f"{source}: invalid frontmatter line '{raw_line}'")
        key, raw_value = raw_line.split(":", 1)
        key = key.strip()
        raw_value = raw_value.strip()
        if not key:
            raise FrontmatterError(f"{source}: empty frontmatter key")
        if raw_value:
            out[key] = _parse_scalar_or_inline_list(raw_value)
            index += 1
            continue
        items: list[Any] = []
        index += 1
        while index < len(lines):
            item_line = lines[index]
            if not item_line.strip():
                index += 1
                continue
            if not item_line[:1].isspace():
                break
            item_text = item_line.strip()
            if not item_text.startswith("- "):
                raise FrontmatterError(f"{source}: unsupported nested frontmatter under '{key}'")
            value_text = item_text[2:].strip()
            items.append(_parse_scalar_or_inline_list(value_text))
            index += 1
        out[key] = items
    return out


def _parse_scalar_or_inline_list(raw: str) -> Any:
    value = raw.strip()
    if value.startswith("[") and value.endswith("]"):
        inner = value[1:-1].strip()
        if not inner:
            return []
        return [_parse_scalar_or_inline_list(part) for part in _split_inline_list(inner)]
    if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
        return value[1:-1]
    lower = value.lower()
    if lower in {"true", "false"}:
        return lower == "true"
    if lower in {"null", "none"}:
        return None
    try:
        if re.fullmatch(r"[-+]?\d+", value):
            return int(value)
        if re.fullmatch(r"[-+]?\d+\.\d+", value):
            return float(value)
    except ValueError:
        pass
    return value


def _split_inline_list(value: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    quote: str | None = None
    for char in value:
        if char in {"'", '"'}:
            quote = None if quote == char else char if quote is None else quote
        if char == "," and quote is None:
            parts.append("".join(current).strip())
            current = []
            continue
        current.append(char)
    parts.append("".join(current).strip())
    return [part for part in parts if part]


def _candidate_from_frontmatter(
    fields: dict[str, Any],
    *,
    board_date: date,
    policy: DecisionBrainPolicy,
    source: str,
) -> DecisionBrainCandidate:
    note_board_date = _required_date(fields, "board_date")
    recommendation = _normalize_recommendation(fields.get("recommendation"))
    line_value = _required_float(fields, "line_value")
    stable_id = str(fields.get("stable_id") or _stable_id(
        board_date=note_board_date,
        game_id=fields.get("game_id"),
        player_id=fields.get("player_id"),
        market_key=fields.get("market_key"),
        line_value=line_value,
        recommendation=recommendation,
    ))
    candidate_status = fields.get("candidate_status")
    if candidate_status is None:
        legacy_status = str(fields.get("status") or "").strip().lower()
        candidate_status = (
            legacy_status
            if legacy_status in (_ALLOWED_CANDIDATE_STATUSES | _BLOCKING_CANDIDATE_STATUSES)
            else "candidate"
        )

    return DecisionBrainCandidate(
        stable_id=stable_id,
        source=source,
        board_date=note_board_date,
        candidate_status=str(candidate_status).strip().lower(),
        market_key=_required_str(fields, "market_key"),
        player_id=_str_or_none(fields.get("player_id")),
        player_name=_str_or_none(fields.get("player_name")),
        game_id=_str_or_none(fields.get("game_id")),
        game_date=_required_date(fields, "game_date"),
        line_value=line_value,
        recommendation=recommendation,
        outcome_side=str(fields.get("outcome_side") or _outcome_side(recommendation)),
        book_side=str(fields.get("book_side") or "bid"),
        model_prob=_float_or_none(fields.get("model_prob")),
        market_prob=_float_or_none(fields.get("market_prob")),
        no_vig_market_prob=_float_or_none(fields.get("no_vig_market_prob")),
        edge_bps=_int_or_none(fields.get("edge_bps")),
        ev=_float_or_none(fields.get("ev")),
        confidence=_float_or_none(fields.get("confidence")),
        contracts=_float_or(fields.get("contracts"), policy.max_contracts),
        max_price_dollars=_float_or(fields.get("max_price_dollars"), policy.max_price_dollars_default),
        post_only=_bool_or(fields.get("post_only"), policy.post_only),
        time_in_force=str(fields.get("time_in_force") or policy.time_in_force),
        title_contains_all=_list_str(fields.get("title_contains_all")),
        player_name_contains_any=_list_str(fields.get("player_name_contains_any")),
        stat_contains_any=_list_str(fields.get("stat_contains_any")),
        acceptable_line_values=_list_float(fields.get("acceptable_line_values")) or _acceptable_line_values(line_value),
        event_or_page_hint=_str_or_none(fields.get("event_or_page_hint")),
        exclude_multivariate=_bool_or(fields.get("exclude_multivariate"), True),
        driver=str(fields.get("driver") or "vault_manual"),
        policy_version=_str_or_none(fields.get("policy_version")),
    )


def _candidate_from_opportunity(
    opportunity: Any,
    *,
    board_date: date,
    insights: Any,
    policy: DecisionBrainPolicy,
) -> DecisionBrainCandidate | None:
    quotes = list(getattr(opportunity, "quotes", []) or [])
    best_quote = quotes[0] if quotes else None
    insight = _matching_insight(opportunity, insights)
    if insight is not None and getattr(insight, "best_quote", None) is not None:
        best_quote = insight.best_quote
    if best_quote is None:
        return None

    recommendation = _recommendation_from_side(
        getattr(best_quote, "recommended_side", None) or getattr(opportunity, "recommended_side", None)
    )
    line_value = _float_or_none(getattr(best_quote, "line_value", None))
    if line_value is None:
        line_value = _float_or_none(getattr(opportunity, "consensus_line", None))
    if line_value is None or recommendation is None:
        return None

    model_prob = _float_or_none(getattr(best_quote, "hit_probability", None))
    if model_prob is None:
        model_prob = _float_or_none(getattr(opportunity, "hit_probability", None))
    no_vig = _float_or_none(getattr(best_quote, "no_vig_market_probability", None))
    market_prob = _float_or_none(getattr(insight, "implied_probability", None)) if insight is not None else None
    if market_prob is None:
        market_prob = no_vig
    edge = _float_or_none(getattr(insight, "edge", None)) if insight is not None else None
    if edge is None and model_prob is not None and no_vig is not None:
        edge = model_prob - no_vig
    edge_bps = int(round(edge * 10000)) if edge is not None else None
    ev = _float_or_none(getattr(insight, "expected_profit_per_unit", None)) if insight is not None else None
    if ev is None:
        ev = edge
    confidence = _confidence_from_opportunity(opportunity, insight)
    player_name = _str_or_none(getattr(opportunity, "player_name", None))
    market_key = str(getattr(opportunity, "market_key", "") or "").strip()
    game_id = _str_or_none(getattr(opportunity, "game_id", None))
    player_id = _str_or_none(getattr(opportunity, "player_id", None))
    # Board-derived candidates trade the app's local board date. NBA evening starts
    # may be stored as next-day UTC timestamps, which must not trip same-day policy.
    game_date = board_date

    return DecisionBrainCandidate(
        stable_id=_stable_id(
            board_date=board_date,
            game_id=game_id,
            player_id=player_id,
            market_key=market_key,
            line_value=line_value,
            recommendation=recommendation,
        ),
        source="board",
        board_date=board_date,
        candidate_status="candidate",
        market_key=market_key,
        player_id=player_id,
        player_name=player_name,
        game_id=game_id,
        game_date=game_date,
        line_value=line_value,
        recommendation=recommendation,
        outcome_side=_outcome_side(recommendation),
        book_side="bid",
        model_prob=model_prob,
        market_prob=market_prob,
        no_vig_market_prob=no_vig,
        edge_bps=edge_bps,
        ev=ev,
        confidence=confidence,
        contracts=policy.max_contracts,
        max_price_dollars=policy.max_price_dollars_default,
        post_only=policy.post_only,
        time_in_force=policy.time_in_force,
        title_contains_all=_game_label_terms(getattr(opportunity, "game_label", None)),
        player_name_contains_any=_player_terms(player_name),
        stat_contains_any=_stat_terms(market_key),
        acceptable_line_values=_acceptable_line_values(line_value),
        event_or_page_hint=None,
        exclude_multivariate=True,
        driver="board_prop_analysis",
        policy_version=policy.policy_version,
    )


def _candidate_to_target(candidate: DecisionBrainCandidate) -> dict[str, Any]:
    target = {
        "target_id": candidate.stable_id,
        "stable_id": candidate.stable_id,
        "sport": "nba",
        "market_type": "player_prop",
        "market_key": candidate.market_key,
        "game_date": candidate.game_date.isoformat(),
        "game_id": candidate.game_id,
        "player_id": candidate.player_id,
        "player_name": candidate.player_name,
        "line_value": candidate.line_value,
        "recommendation": candidate.recommendation,
        "outcome_side": candidate.outcome_side,
        "book_side": candidate.book_side,
        "model_prob": candidate.model_prob,
        "market_prob": candidate.market_prob,
        "no_vig_market_prob": candidate.no_vig_market_prob,
        "edge_bps": candidate.edge_bps,
        "ev": candidate.ev,
        "confidence": candidate.confidence,
        "driver": candidate.driver,
        "event_or_page_hint": candidate.event_or_page_hint,
        "match_rules": {
            "title_contains_all": candidate.title_contains_all,
            "player_name_contains_any": candidate.player_name_contains_any,
            "stat_contains_any": candidate.stat_contains_any,
            "acceptable_line_values": candidate.acceptable_line_values,
            "status": "open",
            "mve_filter": "exclude" if candidate.exclude_multivariate else "only",
        },
        "metadata": {
            "source": candidate.source,
            "policy_version": candidate.policy_version,
            "contracts": candidate.contracts,
            "max_price_dollars": candidate.max_price_dollars,
            "post_only": candidate.post_only,
            "time_in_force": candidate.time_in_force,
        },
    }
    series_ticker = _kalshi_series_ticker(candidate.market_key)
    if series_ticker:
        target["series_ticker"] = series_ticker
        target["match_rules"]["series_ticker"] = series_ticker
        target["metadata"]["series_ticker"] = series_ticker
    return target


def _candidate_symbol_fields(candidate: DecisionBrainCandidate, policy: DecisionBrainPolicy) -> dict[str, Any]:
    return {
        "stable_id": candidate.stable_id,
        "source": candidate.source,
        "game_id": candidate.game_id,
        "player_name": candidate.player_name,
        "recommendation": candidate.recommendation,
        "outcome_side": candidate.outcome_side,
        "book_side": candidate.book_side,
        "model_prob": candidate.model_prob,
        "market_prob": candidate.market_prob,
        "no_vig_market_prob": candidate.no_vig_market_prob,
        "edge_bps": candidate.edge_bps,
        "ev": candidate.ev,
        "confidence": candidate.confidence,
        "contracts": f"{candidate.contracts:.2f}",
        "max_price_dollars": f"{candidate.max_price_dollars:.4f}",
        "post_only": candidate.post_only,
        "time_in_force": candidate.time_in_force,
        "driver": candidate.driver,
        "policy_version": policy.policy_version,
        "policy_hash": policy.policy_hash,
    }


def _candidate_risk(candidate: DecisionBrainCandidate) -> dict[str, Any]:
    return {
        "contracts": f"{candidate.contracts:.2f}",
        "max_price_dollars": f"{candidate.max_price_dollars:.4f}",
        "post_only": candidate.post_only,
        "time_in_force": candidate.time_in_force,
    }


def _candidate_policy_blockers(
    candidate: DecisionBrainCandidate,
    *,
    policy: DecisionBrainPolicy,
    board_date: date,
) -> list[str]:
    blockers: list[str] = []
    status = candidate.candidate_status.strip().lower()
    if status in _BLOCKING_CANDIDATE_STATUSES or status not in _ALLOWED_CANDIDATE_STATUSES:
        blockers.append(f"candidate_status={status or 'missing'}")
    if _market_blocked(candidate.market_key, policy):
        blockers.append("blocked_market_key")
    if candidate.game_date < board_date:
        blockers.append("game_already_played")
    elif policy.same_day_only and candidate.game_date != board_date:
        blockers.append("not_same_day")
    if candidate.model_prob is None or candidate.model_prob < policy.min_model_prob:
        blockers.append("min_model_prob")
    if candidate.confidence is None or candidate.confidence < policy.min_confidence:
        blockers.append("min_confidence")
    if candidate.edge_bps is None or candidate.edge_bps < policy.min_edge_bps:
        blockers.append("min_edge_bps")
    if abs(candidate.contracts - 1.0) > 0.0001:
        blockers.append("one_contract_required")
    return blockers


def _trading_selection_blockers(
    candidate: DecisionBrainCandidate,
    selection_store: SelectionStore | None,
) -> list[str]:
    if selection_store is None:
        return []
    blockers: list[str] = []
    if not selection_store.is_selected(candidate.board_date, candidate.stable_id):
        blockers.append("trading_selection_excluded")
    thresholds = selection_store.thresholds
    if candidate.model_prob is None or candidate.model_prob < thresholds.min_hit_pct:
        blockers.append("trading_min_hit")
    if candidate.edge_bps is None or candidate.edge_bps < thresholds.min_edge_bps:
        blockers.append("trading_min_edge")
    return blockers


def _market_blocked(market_key: str, policy: DecisionBrainPolicy) -> bool:
    aliases = _market_aliases(market_key)
    if aliases & policy.blocked_market_keys:
        return True
    return bool(policy.allowed_market_keys) and not bool(aliases & policy.allowed_market_keys)


def _rank_score(
    *,
    policy: DecisionBrainPolicy,
    edge_bps: float,
    ev: float,
    spread: float,
    confidence: float,
) -> float:
    max_spread = max(policy.max_spread_dollars, 0.0001)
    return (
        policy.ranking_weight_edge_bps * _clamp(edge_bps / 1500.0)
        + policy.ranking_weight_ev * _clamp(ev / 0.10)
        + policy.ranking_weight_liquidity * _clamp(1.0 - spread / max_spread)
        + policy.ranking_weight_calibration * _clamp(confidence)
        + policy.ranking_weight_freshness
    )


def _rank_sort_key(row: dict[str, Any], candidate: DecisionBrainCandidate) -> tuple[Any, ...]:
    blockers = row.get("brain_blockers") or []
    return (
        1 if blockers else 0,
        -float(candidate.consistency_score),
        -float(row.get("rank_score") or 0.0),
        -float(candidate.edge_bps or 0),
        -float(candidate.ev or 0.0),
        float(row.get("entry_price_dollars") or 999.0),
        float(row.get("spread_dollars") or 999.0),
        -float(candidate.confidence or 0.0),
        candidate.stable_id,
    )


def _gate_blocker_key(key: str) -> str:
    return {
        "symbol_resolved": "unresolved_ticker",
        "fresh_market_snapshot": "fresh_market_snapshot",
        "market_open": "market_open",
        "event_not_stale": "stale_event",
        "spread_within_limit": "failed_spread_gate",
        "one_order_cap_ok": "failed_one_order_cap",
        "price_within_limit": "failed_price_gate",
    }.get(key, key)


def _write_snapshot(*, settings: Settings, board_date: date) -> Path:
    root = decision_brain_root(settings)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    snapshot_dir = root / SNAPSHOTS_RELATIVE_PATH / board_date.isoformat() / stamp
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    for source, name in (
        (Path(settings.kalshi_resolution_targets_path), "targets_export.json"),
        (Path(settings.kalshi_symbols_path), "symbols_export.json"),
        (Path(settings.kalshi_decisions_path), "decision_pack_snapshot.json"),
    ):
        if source.is_file():
            shutil.copyfile(source, snapshot_dir / name)
    return snapshot_dir


def _write_status(settings: Settings, result: TradingBrainSyncResult) -> None:
    path = brain_status_path(settings)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result.to_payload(), indent=2) + "\n", encoding="utf-8")


def _result_from_payload(payload: dict[str, Any]) -> TradingBrainSyncResult:
    checks = [
        BrainCheck(
            key=str(check.get("key") or ""),
            label=str(check.get("label") or ""),
            status=str(check.get("status") or "warn"),
            detail=str(check.get("detail") or ""),
        )
        for check in payload.get("checks", [])
        if isinstance(check, dict)
    ]
    synced_at_raw = payload.get("synced_at")
    try:
        synced_at = datetime.fromisoformat(str(synced_at_raw))
    except (TypeError, ValueError):
        synced_at = datetime.now(UTC)
    return TradingBrainSyncResult(
        state=str(payload.get("state") or "blocked"),
        policy_version=_str_or_none(payload.get("policy_version")),
        policy_hash=_str_or_none(payload.get("policy_hash")),
        board_date=str(payload.get("board_date") or date.today().isoformat()),
        mode=str(payload.get("mode") or "observe"),
        generated_candidate_count=int(payload.get("generated_candidate_count") or 0),
        manual_candidate_count=int(payload.get("manual_candidate_count") or 0),
        exported_target_count=int(payload.get("exported_target_count") or 0),
        resolved_symbol_count=int(payload.get("resolved_symbol_count") or 0),
        unresolved_symbol_count=int(payload.get("unresolved_symbol_count") or 0),
        selected_candidate_id=_str_or_none(payload.get("selected_candidate_id")),
        selected_ticker=_str_or_none(payload.get("selected_ticker")),
        selected_candidate_ids=_list_str(payload.get("selected_candidate_ids")),
        selected_tickers=_list_str(payload.get("selected_tickers")),
        live_candidate_count=int(payload.get("live_candidate_count") or 0),
        targets_path=str(payload.get("targets_path") or ""),
        symbols_path=str(payload.get("symbols_path") or ""),
        decisions_path=str(payload.get("decisions_path") or ""),
        snapshot_dir=_str_or_none(payload.get("snapshot_dir")),
        checks=checks,
        synced_at=synced_at,
    )


def _selected_ticker(payload: dict[str, Any], candidate: DecisionBrainCandidate | None) -> str | None:
    if candidate is None or not isinstance(payload, dict):
        return None
    for row in payload.get("symbols", []):
        if isinstance(row, dict) and row.get("stable_id") == candidate.stable_id:
            return _str_or_none(row.get("kalshi_ticker"))
    return None


def _selected_candidate_ids(payload: dict[str, Any]) -> list[str]:
    if not isinstance(payload, dict):
        return []
    out: list[str] = []
    for row in payload.get("symbols", []):
        if not isinstance(row, dict):
            continue
        if row.get("candidate_status") not in {"selected_live", "selected_observe_only"}:
            continue
        stable_id = _str_or_none(row.get("stable_id") or row.get("target_id"))
        if stable_id:
            out.append(stable_id)
    return out


def _selected_tickers(payload: dict[str, Any], selected_candidate_ids: list[str]) -> list[str]:
    selected = set(selected_candidate_ids)
    if not selected or not isinstance(payload, dict):
        return []
    out: list[str] = []
    for row in payload.get("symbols", []):
        if not isinstance(row, dict):
            continue
        stable_id = _str_or_none(row.get("stable_id") or row.get("target_id"))
        ticker = _str_or_none(row.get("kalshi_ticker"))
        if stable_id in selected and ticker:
            out.append(ticker)
    return out


def _matching_insight(opportunity: Any, insights: Any) -> Any | None:
    if not isinstance(insights, dict):
        return None
    game_id = getattr(opportunity, "game_id", None)
    player_id = getattr(opportunity, "player_id", None)
    market_key = getattr(opportunity, "market_key", None)
    consensus_line = _float_or_none(getattr(opportunity, "consensus_line", None))
    direct_key = (game_id, player_id, market_key, consensus_line)
    if direct_key in insights:
        return insights[direct_key]
    for key, value in insights.items():
        if (
            isinstance(key, tuple)
            and len(key) == 4
            and key[0] == game_id
            and key[1] == player_id
            and key[2] == market_key
        ):
            return value
    return None


def _confidence_from_opportunity(opportunity: Any, insight: Any | None) -> float | None:
    if insight is not None:
        raw = _float_or_none(getattr(insight, "confidence_score", None))
        if raw is not None:
            return raw / 100.0 if raw > 1.0 else raw
    raw = _float_or_none(getattr(opportunity, "data_confidence_score", None))
    if raw is not None:
        return raw / 100.0 if raw > 1.0 else raw
    raw = _float_or_none(getattr(opportunity, "hit_probability", None))
    return raw


def _date_from_game_start(value: Any) -> date | None:
    if value in (None, ""):
        return None
    text = str(value)
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            return None


def _recommendation_from_side(value: Any) -> str | None:
    raw = str(value or "").strip().lower()
    if raw in {"over", "yes", "buy_yes"}:
        return "buy_yes"
    if raw in {"under", "no", "buy_no"}:
        return "buy_no"
    return None


def _normalize_recommendation(value: Any) -> str:
    rec = _recommendation_from_side(value)
    if rec is None:
        raise DecisionBrainError(f"unsupported recommendation: {value}")
    return rec


def _outcome_side(recommendation: str) -> str:
    return "yes" if recommendation == "buy_yes" else "no"


def _player_terms(player_name: str | None) -> list[str]:
    if not player_name:
        return []
    terms = [player_name]
    last = player_name.split()[-1]
    if last and last.lower() != player_name.lower():
        terms.append(last)
    return terms


def _stat_terms(market_key: str) -> list[str]:
    key = _norm_key(market_key).split(".")[-1]
    if key in {"points", "pts"}:
        return ["points", "pts"]
    if key in {"rebounds", "rebs"}:
        return ["rebounds", "reb"]
    if key in {"assists", "asts"}:
        return ["assists", "ast"]
    if key == "pra":
        return ["points rebounds assists", "pra"]
    return [key.replace("_", " ")]


def _acceptable_line_values(line_value: float) -> list[float]:
    values = [float(line_value)]
    # Kalshi player-prop contracts are threshold markets such as "25+ points".
    # A sportsbook half-line of 24.5 maps to the equivalent 25+ threshold.
    threshold = line_value + 0.5
    if abs(threshold - round(threshold)) < 0.0001:
        values.append(float(round(threshold)))
    return sorted(set(values))


def _game_label_terms(value: Any) -> list[str]:
    text = str(value or "")
    terms = re.findall(r"\b[A-Z]{2,4}\b", text)
    return terms[:2]


def _stable_id(
    *,
    board_date: date,
    game_id: Any,
    player_id: Any,
    market_key: Any,
    line_value: float,
    recommendation: str,
) -> str:
    raw = f"{board_date.isoformat()}_{game_id or 'game'}_{player_id or 'player'}_{market_key}_{line_value:g}_{recommendation}"
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", raw).strip("_")


def _market_aliases(market_key: str) -> set[str]:
    normalized = _norm_key(market_key)
    aliases = {normalized}
    if "." in normalized:
        aliases.add(normalized.split(".")[-1])
    aliases.add(normalized.replace("nba.player.", ""))
    aliases.add(normalized.replace("nba.", ""))
    return {alias for alias in aliases if alias}


def _kalshi_series_ticker(market_key: str) -> str | None:
    for alias in _market_aliases(market_key):
        normalized = alias.replace("-", "_").replace(" ", "_")
        if normalized in _KALSHI_PLAYER_PROP_SERIES_BY_ALIAS:
            return _KALSHI_PLAYER_PROP_SERIES_BY_ALIAS[normalized]
    return None


def _norm_key(value: Any) -> str:
    return str(value or "").strip().lower()


def _required_str(fields: dict[str, Any], key: str) -> str:
    value = fields.get(key)
    if value in (None, ""):
        raise DecisionBrainError(f"missing required field: {key}")
    return str(value)


def _required_bool(fields: dict[str, Any], key: str) -> bool:
    value = fields.get(key)
    if not isinstance(value, bool):
        raise DecisionBrainError(f"missing or invalid boolean field: {key}")
    return value


def _required_float(fields: dict[str, Any], key: str) -> float:
    value = _float_or_none(fields.get(key))
    if value is None:
        raise DecisionBrainError(f"missing or invalid numeric field: {key}")
    return value


def _required_int(fields: dict[str, Any], key: str) -> int:
    value = _int_or_none(fields.get(key))
    if value is None:
        raise DecisionBrainError(f"missing or invalid integer field: {key}")
    return value


def _required_date(fields: dict[str, Any], key: str) -> date:
    value = fields.get(key)
    parsed = _coerce_date(value)
    if parsed is None:
        raise DecisionBrainError(f"missing or invalid date field: {key}")
    return parsed


def _coerce_date(value: Any, fallback: date | None = None) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if value in (None, ""):
        return fallback
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return fallback


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _float_or(value: Any, fallback: float) -> float:
    parsed = _float_or_none(value)
    return fallback if parsed is None else parsed


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return None


def _bool_or(value: Any, fallback: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "on"}:
            return True
        if normalized in {"false", "0", "no", "off"}:
            return False
    return fallback


def _list_str(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, list):
        return [str(item) for item in value if item not in (None, "")]
    return [str(value)]


def _list_float(value: Any) -> list[float]:
    out: list[float] = []
    for item in _list_str(value):
        parsed = _float_or_none(item)
        if parsed is not None:
            out.append(parsed)
    return out


def _str_or_none(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))

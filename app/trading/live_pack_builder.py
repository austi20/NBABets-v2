"""Generate `data/decisions/*.json` from resolved Kalshi symbol rows + live quotes.

Pipeline:
1. ``scripts/resolve_kalshi_targets.py`` writes ``config/kalshi_symbols.json`` from targets.
2. This module reads executable symbol rows, fetches a fresh public quote per primary row,
   evaluates the same mechanical gates the supervised runner expects, and writes the pack.

Operators who only want start/stop can run ``run_trading_loop.py --sync-pack`` so the
pack is refreshed immediately before load. Discovery of *which* markets to track still
comes from ``kalshi_resolution_targets.json`` (edit or generate that list for new games).
"""

from __future__ import annotations

import json
import logging
import math
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from app.config.settings import Settings
from app.trading.live_limits import load_live_limits
from app.trading.monitoring import KalshiPublicMarketDataClient, MonitoredSymbol, fetch_quote_snapshot
from app.trading.risk import RiskLimits

_log = logging.getLogger(__name__)

_VAULT_PROFILES_SUBPATH = "05 Knowledge and Skills/Data Analysis/NBA Prop Engine Learning/Market Profiles"

def _read_market_profile(market_key: str, vault_root: Path) -> dict[str, Any] | None:
    """Read brain market profile for *market_key* from the Obsidian vault.

    Returns a dict with ``calibration_strategy``, ``recent_ece``, and ``failure_modes``
    when the profile file exists and is parseable; ``None`` on any I/O or parse failure.
    Callers must treat ``None`` as "brain context unavailable" and continue normally.
    """
    profile_path = vault_root / _VAULT_PROFILES_SUBPATH / f"{market_key.title()} Profile.md"
    try:
        text = profile_path.read_text(encoding="utf-8")
    except OSError:
        return None

    result: dict[str, Any] = {"market": market_key}
    ece_history: list[float] = []
    failure_modes: list[str] = []
    in_ece = False
    in_failures = False

    for line in text.splitlines():
        s = line.strip()
        if s.startswith("## ECE History"):
            in_ece, in_failures = True, False
            continue
        if s.startswith("## Known Failure Modes"):
            in_ece, in_failures = False, True
            continue
        if s.startswith("##"):
            in_ece, in_failures = False, False
            continue
        if s.startswith("- **Calibration strategy**:"):
            result["calibration_strategy"] = s.split(":", 1)[-1].strip()
        elif s.startswith("- **Corrections applied**:"):
            try:
                result["corrections_applied"] = int(s.split(":", 1)[-1].strip())
            except ValueError:
                pass
        elif s.startswith("- **Average ECE improvement**:"):
            try:
                result["avg_ece_improvement"] = float(s.split(":", 1)[-1].strip())
            except ValueError:
                pass
        if in_ece and s.startswith("- ") and ":" in s:
            try:
                ece_history.append(float(s[2:].split(":", 1)[-1].strip()))
            except ValueError:
                pass
        if in_failures and s.startswith("- ") and s != "- none observed":
            failure_modes.append(s[2:].strip())

    if ece_history:
        result["recent_ece"] = ece_history[-1]
    if failure_modes:
        result["failure_modes"] = failure_modes
    return result if len(result) > 1 else None


_EXECUTABLE_REC = frozenset({"buy_yes", "buy_no", "over", "under", "yes", "no"})
_OPEN_MARKET_STATUSES = frozenset({"open", "active"})


class LivePackBuildError(RuntimeError):
    """The pack cannot be armed for live submission."""


def _norm_rec(value: object) -> str:
    return str(value or "").strip().lower().replace(" ", "_")


def _float_or_none(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _date_or_none(value: object) -> date | None:
    if value in (None, ""):
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def load_symbols_entries(symbols_path: Path) -> list[dict[str, Any]]:
    payload = json.loads(symbols_path.read_text(encoding="utf-8"))
    raw = payload.get("symbols", [])
    if not isinstance(raw, list):
        return []
    return [row for row in raw if isinstance(row, dict)]


def pick_executable_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in entries:
        rec = _norm_rec(row.get("recommendation"))
        original_rec = _norm_rec(row.get("original_recommendation"))
        status = _norm_rec(row.get("candidate_status"))
        selected_observe = (
            rec == "observe_only"
            and status in {"selected_observe_only", "watchlist"}
            and original_rec in _EXECUTABLE_REC
        )
        if rec not in _EXECUTABLE_REC and not selected_observe:
            continue
        if not row.get("kalshi_ticker"):
            continue
        if row.get("line_value") in (None, ""):
            continue
        out.append(row)
    return out


def load_targets_defaults(targets_path: Path) -> dict[str, Any]:
    if not targets_path.is_file():
        return {}
    payload = json.loads(targets_path.read_text(encoding="utf-8"))
    defaults = payload.get("defaults")
    return defaults if isinstance(defaults, dict) else {}


def row_to_monitored(row: dict[str, Any]) -> MonitoredSymbol | None:
    ticker = row.get("kalshi_ticker")
    if not ticker:
        return None
    rec = _norm_rec(row.get("side", row.get("recommendation")))
    if rec not in _EXECUTABLE_REC:
        rec = _norm_rec(row.get("original_recommendation"))
    if rec in {"buy_yes", "over", "yes"}:
        side = "OVER"
    elif rec in {"buy_no", "under", "no"}:
        side = "UNDER"
    else:
        return None
    line_f = _float_or_none(row.get("line_value"))
    if line_f is None:
        return None
    pid = row.get("player_id")
    player_id = "game_total" if pid is None else str(pid)
    game_date = str(row["game_date"]) if row.get("game_date") else None
    if game_date is None:
        return None
    return MonitoredSymbol(
        ticker=str(ticker),
        market_key=str(row.get("market_key", "")),
        side=side,
        line_value=line_f,
        player_id=player_id,
        game_date=game_date,
        title=(str(row["title"]) if row.get("title") else None),
    )


def _risk_block(defaults: dict[str, Any], row: dict[str, Any]) -> dict[str, Any]:
    keys = ("contracts", "max_price_dollars", "post_only", "time_in_force")
    merged: dict[str, Any] = {k: defaults.get(k) for k in keys}
    for k in keys:
        if row.get(k) is not None:
            merged[k] = row[k]
    return merged


def _contract_order_count(
    entry: float,
    limits: RiskLimits,
    max_contracts: float | None,
) -> int:
    raw = int(math.floor(float(limits.per_order_cap) / entry))
    if max_contracts is not None:
        cap = int(math.floor(float(max_contracts)))
        return min(raw, max(cap, 0))
    return raw


@dataclass(frozen=True)
class GateResult:
    gates: dict[str, bool]
    entry: float | None
    spread: float | None
    quote_error: str | None


def evaluate_gates_for_row(
    row: dict[str, Any],
    *,
    limits: RiskLimits,
    risk: dict[str, Any],
    quote,
    max_spread: float,
    today: date | None = None,
) -> GateResult:
    entry = quote.entry_price if quote is not None else None
    spread = quote.spread if quote is not None else None
    max_price = _float_or_none(risk.get("max_price_dollars"))
    max_contracts = _float_or_none(risk.get("contracts"))
    market_status = str(quote.status or "").strip().lower() if quote is not None else ""
    game_date = _date_or_none(row.get("game_date"))
    current_date = today or date.today()

    sym_ok = bool(row.get("kalshi_ticker"))
    fresh_ok = quote is not None and not quote.error
    market_open_ok = fresh_ok and market_status in _OPEN_MARKET_STATUSES
    event_not_stale_ok = game_date is not None and game_date >= current_date
    entry_ok = entry is not None and entry > 0
    spread_ok = spread is None or spread <= max_spread
    price_ok = max_price is None or (entry is not None and entry <= max_price)
    count_ok = False
    if entry_ok and max_contracts is not None:
        count_ok = _contract_order_count(entry, limits, max_contracts) == 1
    elif entry_ok:
        count_ok = _contract_order_count(entry, limits, None) == 1

    gates = {
        "symbol_resolved": sym_ok,
        "fresh_market_snapshot": fresh_ok,
        "market_open": market_open_ok,
        "event_not_stale": event_not_stale_ok,
        "spread_within_limit": spread_ok,
        "one_order_cap_ok": count_ok if entry_ok else False,
        "price_within_limit": price_ok if entry_ok else False,
    }
    err = quote.error if quote is not None else None
    return GateResult(gates=gates, entry=entry, spread=spread, quote_error=err)


def _normalize_recommendation(row: dict[str, Any]) -> str:
    rec = _norm_rec(row.get("recommendation"))
    if rec in {"over", "yes"}:
        return "buy_yes"
    if rec in {"under", "no"}:
        return "buy_no"
    return rec


def build_primary_decision_row(
    primary: dict[str, Any],
    *,
    defaults: dict[str, Any],
    gate_result: GateResult,
    arm_live: bool,
    brain_context: dict[str, Any] | None = None,
    ece_threshold: float | None = None,
) -> dict[str, Any]:
    risk = _risk_block(defaults, primary)
    monitored = row_to_monitored(primary)
    if monitored is None:
        raise LivePackBuildError("primary symbol row could not be converted (check line_value, game_date, side)")
    # brain_health_ok fails only when a threshold is set AND brain data is present AND ECE exceeds it.
    # Vault unreachable (brain_context=None) or no threshold configured -> fail-open (True).
    brain_health_ok = (
        ece_threshold is None
        or brain_context is None
        or brain_context.get("recent_ece", 0.0) <= ece_threshold
    )
    all_gates = {**gate_result.gates, "brain_health_ok": brain_health_ok}
    failed = [k for k, v in all_gates.items() if not v]
    recommendation = _normalize_recommendation(primary)
    live_ok = arm_live and recommendation in _EXECUTABLE_REC and not failed
    notes: list[str] = [
        f"pack_builder_at={datetime.now(UTC).isoformat()}",
        f"entry={gate_result.entry} spread={gate_result.spread}",
    ]
    if arm_live and recommendation not in _EXECUTABLE_REC:
        notes.append(f"not_live_executable={recommendation}")
    if gate_result.quote_error:
        notes.append(f"quote_error={gate_result.quote_error}")
    if not brain_health_ok and brain_context is not None:
        notes.append(f"brain_ece={brain_context.get('recent_ece')} exceeds threshold={ece_threshold}")
    if failed:
        notes.append(f"gates_failed={','.join(failed)}")

    kalshi_block: dict[str, Any] = {
        "target_id": primary.get("target_id"),
        "ticker": primary.get("kalshi_ticker"),
        "event_ticker": primary.get("event_ticker"),
        "contracts": str(risk.get("contracts") or "1.00"),
        "max_price_dollars": risk.get("max_price_dollars"),
        "post_only": bool(risk.get("post_only", True)),
        "time_in_force": str(risk.get("time_in_force") or "good_till_canceled"),
    }
    brain_block: dict[str, Any] = {k: v for k, v in (brain_context or {}).items() if k != "market"}

    display_block: dict[str, Any] = {
        "player_name": primary.get("player_name"),
        "market_label": primary.get("title") or primary.get("market_key"),
        "model_prob": primary.get("model_prob"),
        "market_prob": primary.get("market_prob"),
        "side": primary.get("side"),
    }

    if live_ok:
        return {
            "decision_id": str(primary.get("target_id") or primary.get("kalshi_ticker")),
            "mode": "live",
            "source_model": "live_pack_builder",
            "market_key": str(primary["market_key"]),
            "recommendation": recommendation,
            "confidence": primary.get("confidence", 0.0),
            "edge_bps": primary.get("edge_bps", 0),
            "line_value": float(primary["line_value"]),
            "player_id": monitored.player_id,
            "game_date": monitored.game_date,
            "kalshi": kalshi_block,
            "brain": brain_block,
            "gates": all_gates,
            "execution": {"allow_live_submit": True, "client_order_id": None},
            "notes": notes,
            **display_block,
        }

    return {
        "decision_id": str(primary.get("target_id") or primary.get("kalshi_ticker")),
        "mode": "observe",
        "source_model": "live_pack_builder",
        "market_key": str(primary["market_key"]),
        "recommendation": "observe_only",
        "confidence": primary.get("confidence", 0.0),
        "edge_bps": primary.get("edge_bps", 0),
        "line_value": float(primary["line_value"]),
        "player_id": monitored.player_id,
        "game_date": monitored.game_date,
        "kalshi": kalshi_block,
        "brain": brain_block,
        "gates": all_gates,
        "execution": {"allow_live_submit": False, "client_order_id": None},
        "notes": notes,
        **display_block,
    }


def build_pack_document(decisions: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "version": 1,
        "created_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "decisions": decisions,
    }


def write_live_decision_pack(
    *,
    decisions_path: Path,
    settings: Settings,
    arm_live: bool = True,
    max_spread: float | None = None,
) -> dict[str, Any]:
    """Write a decision pack; raise :class:`LivePackBuildError` if ``arm_live`` and gates fail."""
    symbols_path = Path(settings.kalshi_symbols_path)
    if not symbols_path.is_file():
        raise LivePackBuildError(
            f"symbols file missing at {symbols_path}; run scripts/resolve_kalshi_targets.py first",
        )
    entries = load_symbols_entries(symbols_path)
    executable = pick_executable_entries(entries)
    if not executable:
        raise LivePackBuildError(
            "no executable symbol rows (need recommendation buy_yes/buy_no/over/under, ticker, line_value). "
            "Edit config/kalshi_resolution_targets.json, then run scripts/resolve_kalshi_targets.py",
        )

    limits = load_live_limits(settings.trading_limits_path)
    defaults = load_targets_defaults(Path(settings.kalshi_resolution_targets_path))
    spread_cap = max_spread
    if spread_cap is None:
        spread_cap = float(os.getenv("LIVE_PACK_MAX_SPREAD", "0.25"))
    vault_root = Path(settings.brain_vault_root)
    _ece_env = os.getenv("LIVE_PACK_MAX_ECE")
    ece_threshold: float | None = float(_ece_env) if _ece_env else None

    primary = executable[0]
    monitored = row_to_monitored(primary)
    if monitored is None:
        raise LivePackBuildError("primary symbol row invalid")

    risk = _risk_block(defaults, primary)
    rows_to_quote: list[tuple[dict[str, Any], MonitoredSymbol, dict[str, Any]]] = [
        (primary, monitored, risk),
    ]
    for other in executable[1:]:
        om = row_to_monitored(other)
        if om is None:
            continue
        rows_to_quote.append((other, om, _risk_block(defaults, other)))

    quotes_and_gates: list[tuple[dict[str, Any], GateResult]] = []
    brain_contexts: list[dict[str, Any] | None] = []
    with KalshiPublicMarketDataClient(base_url=settings.kalshi_market_data_base_url) as client:
        for nrow, nmon, nrisk in rows_to_quote:
            nquote = fetch_quote_snapshot(nmon, client)
            ng = evaluate_gates_for_row(
                nrow,
                limits=limits,
                risk=nrisk,
                quote=nquote,
                max_spread=spread_cap,
            )
            quotes_and_gates.append((nrow, ng))
            mk = str(nrow.get("market_key", ""))
            brain_contexts.append(_read_market_profile(mk, vault_root) if mk else None)

    gate_result = quotes_and_gates[0][1]
    if arm_live:
        failed = [k for k, v in gate_result.gates.items() if not v]
        if failed:
            raise LivePackBuildError(
                "refusing live arm: gates failed for primary symbol "
                f"{primary.get('kalshi_ticker')}: {', '.join(failed)} "
                f"(entry={gate_result.entry} spread={gate_result.spread})",
            )

    row = build_primary_decision_row(
        quotes_and_gates[0][0],
        defaults=defaults,
        gate_result=gate_result,
        arm_live=arm_live,
        brain_context=brain_contexts[0],
        ece_threshold=ece_threshold,
    )
    extra: list[dict[str, Any]] = []
    for (nrow, ng), bc in zip(quotes_and_gates[1:], brain_contexts[1:], strict=True):
        extra.append(
            build_primary_decision_row(
                nrow, defaults=defaults, gate_result=ng, arm_live=False,
                brain_context=bc, ece_threshold=ece_threshold,
            ),
        )

    doc = build_pack_document([row, *extra])
    decisions_path.parent.mkdir(parents=True, exist_ok=True)
    decisions_path.write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
    return doc

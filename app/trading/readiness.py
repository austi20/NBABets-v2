from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from app.config.settings import Settings
from app.trading.decision_brain import load_last_brain_status
from app.trading.live_pack_builder import load_symbols_entries, pick_executable_entries
from app.trading.monitoring import MarketDataClient, MonitoredSymbol, fetch_quote_snapshot

_LIVE_GATE_FIELDS = (
    "symbol_resolved",
    "fresh_market_snapshot",
    "market_open",
    "event_not_stale",
    "spread_within_limit",
    "one_order_cap_ok",
    "price_within_limit",
)
_OPEN_MARKET_STATUSES = frozenset({"open", "active"})


@dataclass(frozen=True)
class ReadinessCheck:
    key: str
    label: str
    status: str
    detail: str


@dataclass(frozen=True)
class TradingReadiness:
    observed_at: datetime
    state: str
    summary: str
    live_trading_enabled: bool
    credentials_configured: bool
    account_sync_enabled: bool
    decisions_path: str
    symbols_path: str
    decision_id: str | None
    ticker: str | None
    game_date: str | None
    market_status: str | None
    executable_symbol_count: int
    unresolved_symbol_count: int
    brain_state: str | None
    brain_policy_version: str | None
    brain_selected_candidate_id: str | None
    brain_last_sync_at: datetime | None
    brain_snapshot_dir: str | None
    checks: list[ReadinessCheck]


def build_trading_readiness(
    *,
    settings: Settings,
    market_client: MarketDataClient | None = None,
    today: date | None = None,
) -> TradingReadiness:
    current_date = today or date.today()
    checks: list[ReadinessCheck] = []
    decisions_path = Path(settings.kalshi_decisions_path)
    symbols_path = Path(settings.kalshi_symbols_path)
    key_path = Path(settings.kalshi_private_key_path) if settings.kalshi_private_key_path else None
    credentials_ok = bool(settings.kalshi_api_key_id and key_path is not None and key_path.exists())
    brain_state: str | None = None
    brain_policy_version: str | None = None
    brain_selected_candidate_id: str | None = None
    brain_last_sync_at: datetime | None = None
    brain_snapshot_dir: str | None = None
    try:
        brain_status = load_last_brain_status(settings)
    except Exception as exc:  # noqa: BLE001 - readiness should report partial status
        brain_status = None
        checks.append(
            ReadinessCheck(
                key="brain_status",
                label="Decision brain",
                status="warn",
                detail=f"Could not read last sync status: {exc}",
            )
        )
    if brain_status is not None:
        brain_state = brain_status.state
        brain_policy_version = brain_status.policy_version
        brain_selected_candidate_id = brain_status.selected_candidate_id
        brain_last_sync_at = brain_status.synced_at
        brain_snapshot_dir = brain_status.snapshot_dir
        checks.append(
            ReadinessCheck(
                key="brain_status",
                label="Decision brain",
                status="pass" if brain_status.state in {"synced", "observe_only"} else "warn",
                detail=f"{brain_status.state}; selected={brain_status.selected_candidate_id or 'none'}",
            )
        )

    _add_check(
        checks,
        key="credentials",
        label="Kalshi credentials",
        ok=credentials_ok,
        detail="API key and private key file are available" if credentials_ok else "API key or private key file is missing",
    )
    _add_check(
        checks,
        key="live_flag",
        label="Live trading flag",
        ok=settings.kalshi_live_trading,
        detail="KALSHI_LIVE_TRADING is enabled"
        if settings.kalshi_live_trading
        else "KALSHI_LIVE_TRADING is not enabled",
    )

    executable_symbol_count = 0
    unresolved_symbol_count = 0
    symbols_error: str | None = None
    try:
        symbol_payload = _json_file(symbols_path)
        unresolved = symbol_payload.get("unresolved", []) if isinstance(symbol_payload, dict) else []
        unresolved_symbol_count = len(unresolved) if isinstance(unresolved, list) else 0
        executable_symbol_count = len(pick_executable_entries(load_symbols_entries(symbols_path)))
    except Exception as exc:  # noqa: BLE001 - readiness should report partial status
        symbols_error = str(exc)
    if symbols_error:
        checks.append(
            ReadinessCheck(
                key="sync_pack_source",
                label="Sync-pack source",
                status="fail",
                detail=f"Could not read symbol map: {symbols_error}",
            )
        )
    else:
        checks.append(
            ReadinessCheck(
                key="sync_pack_source",
                label="Sync-pack source",
                status="pass" if executable_symbol_count > 0 else "fail",
                detail=(
                    f"{executable_symbol_count} executable symbol row(s)"
                    if executable_symbol_count > 0
                    else "No executable current symbol rows for --sync-pack"
                ),
            )
        )

    first: dict[str, Any] | None = None
    decision_count = 0
    decision_error: str | None = None
    try:
        first, decision_count = _first_decision(decisions_path)
    except Exception as exc:  # noqa: BLE001 - readiness should report partial status
        decision_error = str(exc)

    decision_id = str(first.get("decision_id")) if first and first.get("decision_id") is not None else None
    game_date = str(first.get("game_date")) if first and first.get("game_date") is not None else None
    kalshi = first.get("kalshi") if first and isinstance(first.get("kalshi"), dict) else {}
    ticker = str(kalshi.get("ticker")) if isinstance(kalshi, dict) and kalshi.get("ticker") else None
    market_status: str | None = None

    if decision_error:
        checks.append(
            ReadinessCheck(
                key="decision_pack",
                label="Decision pack",
                status="fail",
                detail=decision_error,
            )
        )
    elif first is not None:
        checks.append(
            ReadinessCheck(
                key="decision_pack",
                label="Decision pack",
                status="pass",
                detail=f"{decision_count} decision row(s); first={decision_id or 'unnamed'}",
            )
        )
        mode = str(first.get("mode", "")).strip().lower()
        _add_check(
            checks,
            key="decision_live_mode",
            label="Decision mode",
            ok=mode == "live",
            detail=f"mode={mode or 'missing'}",
        )
        execution = first.get("execution") if isinstance(first.get("execution"), dict) else {}
        _add_check(
            checks,
            key="submit_allowed",
            label="Submit permission",
            ok=isinstance(execution, dict) and execution.get("allow_live_submit") is True,
            detail="allow_live_submit=true"
            if isinstance(execution, dict) and execution.get("allow_live_submit") is True
            else "allow_live_submit is not true",
        )
        side = _live_side(first.get("recommendation"))
        _add_check(
            checks,
            key="executable_side",
            label="Executable side",
            ok=side is not None,
            detail=str(first.get("recommendation") or "missing"),
        )
        _add_check(
            checks,
            key="decision_ticker",
            label="Decision ticker",
            ok=bool(ticker),
            detail=ticker or "missing",
        )
        parsed_game_date = _date_or_none(game_date)
        _add_check(
            checks,
            key="game_date",
            label="Game date",
            ok=parsed_game_date is not None and parsed_game_date >= current_date,
            detail=game_date or "missing",
        )
        gates = first.get("gates") if isinstance(first.get("gates"), dict) else {}
        failed_gates = [field for field in _LIVE_GATE_FIELDS if gates.get(field) is not True]
        checks.append(
            ReadinessCheck(
                key="decision_gates",
                label="Decision gates",
                status="pass" if not failed_gates else "fail",
                detail="all pass" if not failed_gates else ", ".join(failed_gates),
            )
        )

        if ticker and side and market_client is not None:
            quote = fetch_quote_snapshot(
                MonitoredSymbol(
                    ticker=ticker,
                    market_key=str(first.get("market_key") or ""),
                    side=side,
                    line_value=_float_or_none(first.get("line_value")),
                    player_id=(str(first["player_id"]) if first.get("player_id") is not None else None),
                    game_date=game_date,
                ),
                market_client,
            )
            market_status = quote.status
            _add_check(
                checks,
                key="market_quote",
                label="Market quote",
                ok=quote.error is None and quote.entry_price is not None and quote.entry_price > 0,
                detail=quote.error or f"entry={quote.entry_price} exit={quote.exit_price}",
            )
            normalized_status = str(quote.status or "").strip().lower()
            _add_check(
                checks,
                key="market_open",
                label="Market status",
                ok=normalized_status in _OPEN_MARKET_STATUSES,
                detail=quote.status or "missing",
            )

    checks.append(
        ReadinessCheck(
            key="runner_control",
            label="Runner control",
            status="warn",
            detail="This tab monitors state; order submission still requires starting the live runner.",
        )
    )

    failures = [check for check in checks if check.status == "fail"]
    state = "blocked" if failures else "ready"
    summary = (
        f"Blocked: {failures[0].detail}"
        if failures
        else "Ready to start the live runner"
    )

    return TradingReadiness(
        observed_at=datetime.now(UTC),
        state=state,
        summary=summary,
        live_trading_enabled=bool(settings.kalshi_live_trading),
        credentials_configured=credentials_ok,
        account_sync_enabled=credentials_ok,
        decisions_path=str(decisions_path),
        symbols_path=str(symbols_path),
        decision_id=decision_id,
        ticker=ticker,
        game_date=game_date,
        market_status=market_status,
        executable_symbol_count=executable_symbol_count,
        unresolved_symbol_count=unresolved_symbol_count,
        brain_state=brain_state,
        brain_policy_version=brain_policy_version,
        brain_selected_candidate_id=brain_selected_candidate_id,
        brain_last_sync_at=brain_last_sync_at,
        brain_snapshot_dir=brain_snapshot_dir,
        checks=checks,
    )


def _add_check(checks: list[ReadinessCheck], *, key: str, label: str, ok: bool, detail: str) -> None:
    checks.append(ReadinessCheck(key=key, label=label, status="pass" if ok else "fail", detail=detail))


def _json_file(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _first_decision(path: Path) -> tuple[dict[str, Any], int]:
    payload = _json_file(path)
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict) and isinstance(payload.get("decisions"), list):
        rows = payload["decisions"]
    else:
        raise ValueError("decisions file must contain a JSON list or decisions array")
    if not rows:
        raise ValueError("decisions file is empty")
    if not isinstance(rows[0], dict):
        raise ValueError("first decision is not a JSON object")
    return rows[0], len(rows)


def _live_side(raw: object) -> str | None:
    value = str(raw or "").strip().lower()
    if value in {"over", "buy_yes", "yes"}:
        return "OVER"
    if value in {"under", "buy_no", "no"}:
        return "UNDER"
    return None


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

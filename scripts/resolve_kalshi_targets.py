"""Resolve Kalshi decision targets into exact market tickers.

This script uses Kalshi's public market-data API only. It does not read API
keys, does not sign requests, and cannot place orders.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import unicodedata
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

DEFAULT_BASE_URL = "https://external-api.kalshi.com/trade-api/v2"
ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TARGETS_PATH = ROOT / "config" / "kalshi_resolution_targets.json"
DEFAULT_SYMBOLS_PATH = ROOT / "config" / "kalshi_symbols.json"
OPEN_STATUSES = {"open", "active"}
EXECUTABLE_RECOMMENDATIONS = {"buy_yes", "buy_no", "over", "under"}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Resolve Kalshi targets to exact public market tickers.")
    parser.add_argument("--targets", type=Path, default=DEFAULT_TARGETS_PATH)
    parser.add_argument("--symbols-out", type=Path, default=DEFAULT_SYMBOLS_PATH)
    parser.add_argument("--base-url", default=os.getenv("KALSHI_MARKET_DATA_BASE_URL", DEFAULT_BASE_URL))
    parser.add_argument("--min-score", type=int, default=70)
    return parser.parse_args()


def norm(text: object) -> str:
    if text is None:
        return ""
    cleaned = unicodedata.normalize("NFKD", str(text)).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", " ", cleaned.lower()).strip()


def _numeric(value: object) -> float | None:
    if value in (None, "", "MODEL_OUTPUT_REQUIRED"):
        return None
    try:
        return float(str(value))
    except ValueError:
        return None


def extract_numeric_labels(*parts: object) -> set[float]:
    found: set[float] = set()
    for part in parts:
        if part is None:
            continue
        text = str(part)
        for raw in re.findall(r"(\d+(?:\.\d+)?)\s*\+", text):
            found.add(float(raw))
        for raw in re.findall(r"over\s+(\d+(?:\.\d+)?)", text.lower()):
            found.add(float(raw))
        for raw in re.findall(r"under\s+(\d+(?:\.\d+)?)", text.lower()):
            found.add(float(raw))
    return found


def _base_url(value: str) -> str:
    return value.rstrip("/")


def fetch_markets(
    *,
    client: httpx.Client,
    base_url: str,
    status: str = "open",
    event_ticker: str | None = None,
    series_ticker: str | None = None,
    tickers: str | None = None,
    mve_filter: str = "exclude",
) -> list[dict[str, Any]]:
    markets: list[dict[str, Any]] = []
    cursor: str | None = None
    while True:
        params: dict[str, str | int] = {
            "limit": 1000,
            "status": status,
            "mve_filter": mve_filter,
        }
        if cursor:
            params["cursor"] = cursor
        if event_ticker:
            params["event_ticker"] = event_ticker
        if series_ticker:
            params["series_ticker"] = series_ticker
        if tickers:
            params["tickers"] = tickers
        response = client.get(f"{_base_url(base_url)}/markets", params=params)
        response.raise_for_status()
        payload = response.json()
        batch = payload.get("markets", [])
        if not isinstance(batch, list):
            raise ValueError("Kalshi /markets response did not contain a markets array")
        markets.extend([market for market in batch if isinstance(market, dict)])
        cursor = payload.get("cursor") or None
        if not cursor:
            return markets


def _combined_market_text(market: dict[str, Any]) -> str:
    parts = [
        market.get("title"),
        market.get("subtitle"),
        market.get("yes_sub_title"),
        market.get("no_sub_title"),
        market.get("rules_primary"),
    ]
    return " ".join(norm(part) for part in parts if part)


def _contains_all(haystack: str, needles: list[object]) -> bool:
    return all(norm(needle) in haystack for needle in needles if norm(needle))


def _contains_any(haystack: str, needles: list[object]) -> bool:
    normalized = [norm(needle) for needle in needles if norm(needle)]
    return not normalized or any(needle in haystack for needle in normalized)


def _market_line_values(market: dict[str, Any]) -> set[float]:
    values = extract_numeric_labels(
        market.get("title"),
        market.get("subtitle"),
        market.get("yes_sub_title"),
        market.get("no_sub_title"),
        market.get("rules_primary"),
    )
    strike = _numeric(market.get("floor_strike"))
    if strike is not None:
        values.add(strike)
    return values


def _status_ok(target: dict[str, Any], market: dict[str, Any]) -> bool:
    desired = norm(target.get("match_rules", {}).get("status") or target.get("market_status") or "open")
    actual = norm(market.get("status"))
    if desired == "open":
        return actual in OPEN_STATUSES
    return not desired or actual == desired


def score_market(target: dict[str, Any], market: dict[str, Any]) -> tuple[int, str]:
    if not _status_ok(target, market):
        return 0, "status"
    if target.get("match_rules", {}).get("mve_filter") == "exclude":
        if market.get("mve_collection_ticker") or market.get("mve_selected_legs"):
            return 0, "mve"

    rules = target.get("match_rules", {})
    haystack = _combined_market_text(market)
    reasons: list[str] = []
    score = 0

    title_terms = rules.get("title_contains_all") or []
    if title_terms and _contains_all(haystack, title_terms):
        score += 40
        reasons.append("title")

    player_terms = rules.get("player_name_contains_any") or []
    if player_terms and _contains_any(haystack, player_terms):
        score += 35
        reasons.append("player")

    stat_terms = rules.get("stat_contains_any") or []
    if stat_terms and _contains_any(haystack, stat_terms):
        score += 25
        reasons.append("stat")

    target_line = _numeric(target.get("line_value"))
    acceptable: set[float] = set()
    for value in rules.get("acceptable_line_values", []):
        numeric_value = _numeric(value)
        if numeric_value is not None:
            acceptable.add(numeric_value)
    if target_line is not None:
        acceptable.add(target_line)
    if acceptable:
        market_lines = _market_line_values(market)
        if any(any(abs(want - got) < 0.01 for got in market_lines) for want in acceptable):
            score += 40
            reasons.append("line")

    hint = norm(target.get("event_or_page_hint"))
    if hint and hint in norm(market.get("event_ticker")):
        score += 20
        reasons.append("event")

    return score, ",".join(reasons)


def _side_from_recommendation(recommendation: object) -> str | None:
    value = norm(recommendation)
    if value in {"buy yes", "over", "yes"}:
        return "over"
    if value in {"buy no", "under", "no"}:
        return "under"
    return None


def _is_executable(target: dict[str, Any]) -> bool:
    return norm(target.get("recommendation")).replace(" ", "_") in EXECUTABLE_RECOMMENDATIONS


def _market_snapshot(target: dict[str, Any], market: dict[str, Any], reasons: str) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "target_id": target["target_id"],
        "market_key": target["market_key"],
        "game_date": target["game_date"],
        "player_id": target.get("player_id"),
        "line_value": _numeric(target.get("line_value")) or _numeric(market.get("floor_strike")),
        "recommendation": target.get("recommendation"),
        "kalshi_ticker": market["ticker"],
        "event_ticker": market.get("event_ticker"),
        "title": market.get("title"),
        "subtitle": market.get("subtitle"),
        "yes_sub_title": market.get("yes_sub_title"),
        "no_sub_title": market.get("no_sub_title"),
        "last_price_dollars": market.get("last_price_dollars"),
        "yes_bid_dollars": market.get("yes_bid_dollars"),
        "yes_ask_dollars": market.get("yes_ask_dollars"),
        "matched_at": datetime.now(UTC).isoformat(),
        "match_quality": "exact" if "line" in reasons else reasons,
    }
    side = _side_from_recommendation(target.get("recommendation"))
    if side is not None:
        entry["side"] = side
    return entry


def _unresolved(target: dict[str, Any], reason: str) -> dict[str, Any]:
    return {
        "target_id": target.get("target_id"),
        "market_key": target.get("market_key"),
        "player_id": target.get("player_id"),
        "game_date": target.get("game_date"),
        "line_value": target.get("line_value"),
        "reason": reason,
    }


def resolve_targets(targets: list[dict[str, Any]], markets: list[dict[str, Any]], min_score: int) -> dict[str, Any]:
    out: dict[str, Any] = {
        "version": 1,
        "generated_at": datetime.now(UTC).isoformat(),
        "source": "scripts/resolve_kalshi_targets.py",
        "symbols": [],
        "unresolved": [],
    }
    for target in targets:
        if _is_executable(target) and _numeric(target.get("line_value")) is None:
            out["unresolved"].append(_unresolved(target, "line_value required before executable resolution"))
            continue
        ranked: list[tuple[int, str, dict[str, Any]]] = []
        for market in markets:
            score, reasons = score_market(target, market)
            if score > 0:
                ranked.append((score, reasons, market))
        ranked.sort(key=lambda row: row[0], reverse=True)
        if not ranked or ranked[0][0] < min_score:
            out["unresolved"].append(_unresolved(target, "no exact live market match"))
            continue
        _score, reasons, market = ranked[0]
        out["symbols"].append(_market_snapshot(target, market, reasons))
    return out


def _market_scan_needed(target: dict[str, Any]) -> bool:
    return not (_is_executable(target) and _numeric(target.get("line_value")) is None)


def _load_candidate_markets(
    *,
    client: httpx.Client,
    base_url: str,
    targets: list[dict[str, Any]],
    status: str,
    mve_filter: str,
) -> list[dict[str, Any]]:
    markets_by_ticker: dict[str, dict[str, Any]] = {}
    hinted_targets = [target for target in targets if _market_scan_needed(target) and target.get("event_or_page_hint")]
    for target in hinted_targets:
        hint = str(target["event_or_page_hint"])
        for market in fetch_markets(
            client=client,
            base_url=base_url,
            status=status,
            event_ticker=hint,
            mve_filter=mve_filter,
        ):
            ticker = str(market.get("ticker") or "")
            if ticker:
                markets_by_ticker[ticker] = market

    needs_broad_scan = any(
        _market_scan_needed(target) and not target.get("event_or_page_hint")
        for target in targets
    )
    if needs_broad_scan:
        for market in fetch_markets(
            client=client,
            base_url=base_url,
            status=status,
            mve_filter=mve_filter,
        ):
            ticker = str(market.get("ticker") or "")
            if ticker:
                markets_by_ticker[ticker] = market

    return list(markets_by_ticker.values())


def main() -> int:
    args = _parse_args()
    payload = json.loads(args.targets.read_text(encoding="utf-8"))
    targets = payload.get("targets", [])
    if not isinstance(targets, list) or not all(isinstance(target, dict) for target in targets):
        raise ValueError("targets file must contain a targets array")
    defaults = payload.get("defaults", {}) if isinstance(payload.get("defaults"), dict) else {}
    with httpx.Client(timeout=30.0) as client:
        markets = _load_candidate_markets(
            client=client,
            base_url=args.base_url,
            targets=targets,
            status=str(defaults.get("market_status") or "open"),
            mve_filter="exclude" if defaults.get("exclude_multivariate", True) else "only",
        )
    resolved = resolve_targets(targets, markets, args.min_score)
    args.symbols_out.parent.mkdir(parents=True, exist_ok=True)
    args.symbols_out.write_text(json.dumps(resolved, indent=2) + "\n", encoding="utf-8")
    print(
        f"wrote {args.symbols_out} "
        f"symbols={len(resolved['symbols'])} unresolved={len(resolved['unresolved'])}"
    )
    return 0 if not resolved["unresolved"] else 2


if __name__ == "__main__":
    raise SystemExit(main())

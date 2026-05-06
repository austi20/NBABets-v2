from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.all import ModelRun
from app.services.model_quality.release_compare import _avg_ece, _data_quality_status

ECE_DETERIORATION_DELTA = 0.02
WATCH_MARKET_ECE_DELTAS: dict[str, float] = {
    "pra": 0.02,
    "turnovers": 0.015,
}
WATCH_MARKET_ECE_ABSOLUTE: dict[str, float] = {
    "pra": 0.08,
    "turnovers": 0.08,
}


@dataclass(frozen=True)
class AutomationTrendSnapshot:
    lines: list[str]
    alerts: list[str]


def _prior_automation_reports(reports_dir: Path, *, limit: int = 5) -> list[Path]:
    paths = sorted(reports_dir.glob("automation_daily_*.md"), key=lambda p: p.name)
    if not paths:
        return []
    return paths[-limit:]


def parse_api_tier_from_report_markdown(content: str) -> str | None:
    """Extract API coverage tier (A/B/C) from a daily automation report body."""
    idx = content.find("## API Coverage Tier")
    if idx < 0:
        return None
    segment = content[idx : idx + 800]
    m = re.search(r"- tier:\s*([ABC])\b", segment)
    if m:
        return m.group(1)
    return None


def _extract_market_ece(metrics: object) -> dict[str, float]:
    if not isinstance(metrics, dict):
        return {}
    diagnostics = metrics.get("calibration_diagnostics")
    if not isinstance(diagnostics, dict):
        return {}
    out: dict[str, float] = {}
    for market, payload in diagnostics.items():
        if not isinstance(payload, dict):
            continue
        raw_ece = payload.get("ece")
        try:
            ece = float(raw_ece)
        except (TypeError, ValueError):
            continue
        out[str(market)] = ece
    return out


def _model_run_trend_rows(
    session: Session,
    *,
    limit: int = 5,
) -> list[tuple[datetime | None, float | None, str, dict[str, float]]]:
    rows = session.execute(
        select(ModelRun.completed_at, ModelRun.metrics)
        .where(ModelRun.model_version.not_like("%_backtest"))
        .order_by(ModelRun.completed_at.desc())
        .limit(limit)
    ).all()
    out: list[tuple[datetime | None, float | None, str, dict[str, float]]] = []
    for completed_at, metrics in rows:
        m = metrics if isinstance(metrics, dict) else {}
        out.append(
            (
                completed_at,
                _avg_ece(m),
                _data_quality_status(m),
                _extract_market_ece(m),
            )
        )
    return out


def build_automation_trend_snapshot(
    session: Session,
    reports_dir: Path,
    *,
    report_date: date,
) -> AutomationTrendSnapshot:
    """Compare recent model metrics and prior reports for deterioration signals."""
    now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    lines: list[str] = [
        f"- report_anchor_date: {report_date.isoformat()}",
        f"- as_of_utc: {now}",
    ]
    alerts: list[str] = []

    runs = _model_run_trend_rows(session, limit=5)
    if not runs:
        lines.append("- model_run_history: insufficient (no non-backtest model runs)")
    else:
        latest_ece, prev_ece = runs[0][1], runs[1][1] if len(runs) > 1 else None
        latest_dq, prev_dq = runs[0][2], runs[1][2] if len(runs) > 1 else None
        latest_market_ece = runs[0][3]
        prev_market_ece = runs[1][3] if len(runs) > 1 else {}
        lines.append(f"- latest_model_run_at: {runs[0][0]}")
        lines.append(f"- latest_avg_ece: {latest_ece}")
        if prev_ece is not None and latest_ece is not None:
            delta = latest_ece - prev_ece
            lines.append(f"- ece_delta_vs_prior_run: {delta:+.4f}")
            if delta > ECE_DETERIORATION_DELTA:
                alerts.append(
                    f"Average ECE worsened by {delta:.4f} vs prior run "
                    f"(threshold {ECE_DETERIORATION_DELTA:.2f})."
                )
        elif prev_ece is None:
            lines.append("- ece_delta_vs_prior_run: n/a (single run in window)")
        if prev_dq is not None:
            lines.append(f"- data_quality_status: prior={prev_dq} latest={latest_dq}")
            if prev_dq != "degraded" and latest_dq == "degraded":
                alerts.append("Training data quality status flipped to degraded vs prior run.")
        for market in ("pra", "turnovers"):
            latest_market = latest_market_ece.get(market)
            prior_market = prev_market_ece.get(market) if prev_market_ece else None
            if latest_market is None:
                lines.append(f"- watch_market_{market}_ece: unavailable")
                continue
            lines.append(f"- watch_market_{market}_ece: {latest_market:.4f}")
            absolute_threshold = WATCH_MARKET_ECE_ABSOLUTE[market]
            if latest_market > absolute_threshold:
                alerts.append(
                    f"{market.upper()} ECE is {latest_market:.4f}, above watch threshold {absolute_threshold:.3f}."
                )
            if prior_market is not None:
                delta = latest_market - prior_market
                lines.append(f"- watch_market_{market}_ece_delta_vs_prior_run: {delta:+.4f}")
                if delta > WATCH_MARKET_ECE_DELTAS[market]:
                    alerts.append(
                        f"{market.upper()} ECE worsened by {delta:.4f} vs prior run "
                        f"(threshold {WATCH_MARKET_ECE_DELTAS[market]:.3f})."
                    )

    prior_reports = _prior_automation_reports(reports_dir, limit=5)
    tiers: list[str] = []
    for path in prior_reports:
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        tier = parse_api_tier_from_report_markdown(text)
        if tier:
            tiers.append(tier)
    if len(tiers) >= 2:
        old, new = tiers[-2], tiers[-1]
        lines.append(f"- api_coverage_tier (last two saved reports): {old} -> {new}")
        rank = {"A": 0, "B": 1, "C": 2}
        if rank.get(new, 0) > rank.get(old, 0):
            alerts.append(f"API coverage tier deteriorated across saved reports ({old} -> {new}).")
    elif not prior_reports:
        lines.append("- api_coverage_tier_trend: no prior automation reports on disk")
    else:
        lines.append("- api_coverage_tier_trend: could not parse tier from prior reports")

    return AutomationTrendSnapshot(lines=lines, alerts=alerts)

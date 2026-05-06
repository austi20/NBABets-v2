from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.services.local_autonomy.contracts import OverfitSignal


@dataclass(frozen=True)
class OverfitIntelSnapshot:
    risk_score: float
    signals: tuple[OverfitSignal, ...]
    mitigations: tuple[str, ...]


def build_overfit_intel_snapshot(
    *,
    latest_model_metrics: dict[str, Any],
    latest_backtest_metrics: dict[str, Any],
    trend_alerts: list[str],
) -> OverfitIntelSnapshot:
    diagnostics = latest_model_metrics.get("calibration_diagnostics")
    signals: list[OverfitSignal] = []
    if isinstance(diagnostics, dict):
        for market, payload in diagnostics.items():
            if not isinstance(payload, dict):
                continue
            raw_ece = payload.get("ece")
            if not isinstance(raw_ece, (int, float, str)):
                continue
            try:
                ece = float(raw_ece)
            except (TypeError, ValueError):
                continue
            score = min(1.0, max(0.0, ece / 0.12))
            note = f"ECE={ece:.3f}"
            if ece > 0.10:
                note += " (high)"
            elif ece > 0.08:
                note += " (elevated)"
            signals.append(OverfitSignal(market=str(market), score=score, note=note))

    sufficiency_ratio = _extract_backtest_sufficiency_ratio(latest_backtest_metrics)
    if sufficiency_ratio is not None and sufficiency_ratio < 0.40:
        deficit = 1.0 - max(0.0, min(sufficiency_ratio / 0.40, 1.0))
        signals.append(
            OverfitSignal(
                market="sample_sufficiency",
                score=deficit,
                note=f"sufficiency_ratio={sufficiency_ratio:.2f} (<0.40)",
            )
        )

    for alert in trend_alerts:
        if "ECE" in alert.upper():
            signals.append(OverfitSignal(market="trend_alert", score=0.75, note=alert))

    if not signals:
        return OverfitIntelSnapshot(
            risk_score=0.0,
            signals=tuple(),
            mitigations=(
                "No overfit regression signal detected from latest diagnostics.",
            ),
        )

    ranked = sorted(signals, key=lambda item: item.score, reverse=True)
    risk_score = sum(item.score for item in ranked[:5]) / min(5, len(ranked))
    mitigations = _mitigation_actions(ranked)
    return OverfitIntelSnapshot(risk_score=risk_score, signals=tuple(ranked), mitigations=tuple(mitigations))


def _mitigation_actions(signals: list[OverfitSignal]) -> list[str]:
    actions: list[str] = []
    markets = {signal.market for signal in signals}
    if any(signal.market in {"pra", "turnovers"} and signal.score >= 0.8 for signal in signals):
        actions.append("Tighten watch-market calibration thresholds and block release escalation until stabilized.")
    if "sample_sufficiency" in markets:
        actions.append("Increase backtest sample density before accepting calibration deltas.")
    if "trend_alert" in markets:
        actions.append("Run bounded deep-eval and compare watch-market drift vs prior 3 runs.")
    if not actions:
        actions.append("Keep current calibration policy and monitor next run for trend continuation.")
    return actions


def build_feature_attribution_review(
    session: Any,  # sqlalchemy Session — local import avoids circular dep
    orchestrator: Any,  # AIOrchestrator — local import avoids circular dep
    *,
    limit: int = 50,
) -> dict[str, Any]:
    """Aggregate feature weights across recent Predictions and ask AI to review for overfitting."""
    import json
    from collections import defaultdict

    from sqlalchemy import select
    from sqlalchemy.orm import Session as _Session  # noqa: F401 — type reference only

    from app.models.all import Prediction
    from app.services.local_autonomy.contracts import extract_json_object

    rows = (
        session.execute(
            select(Prediction.feature_attribution_summary)
            .where(Prediction.feature_attribution_summary.is_not(None))
            .order_by(Prediction.predicted_at.desc())
            .limit(limit)
        )
        .scalars()
        .all()
    )
    non_empty = [r for r in rows if isinstance(r, dict) and r]
    if not non_empty:
        return {}

    aggregated: dict[str, list[float]] = defaultdict(list)
    for attribution in non_empty:
        for feature, weight in attribution.items():
            if isinstance(weight, (int, float)):
                aggregated[feature].append(float(weight))

    feature_summary = {
        f: {
            "avg_weight": round(sum(ws) / len(ws), 4),
            "max_weight": round(max(ws), 4),
            "count": len(ws),
        }
        for f, ws in aggregated.items()
    }
    prompt = (
        "Review these NBA prop model feature weights for signs of overfitting. "
        "Flag any feature with avg_weight > 0.30. "
        "Return STRICT JSON only: "
        '{"suspicious_features": [{"name": str, "avg_weight": float, "concern": str}], '
        '"overall_risk": "low"|"medium"|"high", "recommendation": str}\n'
        f"Features:\n{json.dumps(feature_summary, indent=2)}"
    )
    ai_result = orchestrator.summarize(task_name="feature_attribution_review", prompt=prompt)
    parsed = extract_json_object(
        ai_result.text,
        required_keys=frozenset({"suspicious_features", "overall_risk", "recommendation"}),
    )
    return parsed if parsed is not None else {}


def _extract_backtest_sufficiency_ratio(metrics: dict[str, Any]) -> float | None:
    rows = metrics.get("summary_rows")
    if not isinstance(rows, list):
        return None
    values: list[float] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        raw = row.get("sample_sufficient")
        if not isinstance(raw, (int, float, str)):
            continue
        try:
            values.append(float(raw))
        except (TypeError, ValueError):
            continue
    if not values:
        return None
    return sum(values) / len(values)

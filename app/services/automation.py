from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from pathlib import Path
from typing import cast

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from app.config.settings import Settings, get_settings
from app.models.all import BacktestResult, ModelRun, Prediction, RawPayload
from app.services.agents.control_plane import AgentControlPlane, render_agent_markdown
from app.services.ai_orchestrator import AIOrchestrator
from app.services.automation_preflight import verify_agent_run_events_table
from app.services.automation_trends import build_automation_trend_snapshot
from app.services.local_autonomy.engine import LocalAutonomyEngine, render_local_autonomy_markdown
from app.services.model_quality.api_coverage import assess_api_coverage
from app.services.model_quality.release_compare import compare_latest_model_runs

logger = logging.getLogger(__name__)


def generate_daily_automation_report(
    session: Session,
    target_date: date | None = None,
    *,
    agent_mode: str | None = None,
    dry_run: bool | None = None,
) -> Path:
    settings = get_settings()
    orchestrator = AIOrchestrator(session)
    report_date = target_date or date.today()
    day_start = datetime.combine(report_date, datetime.min.time(), tzinfo=UTC)

    prediction_count = session.scalar(
        select(func.count(Prediction.prediction_id)).where(Prediction.predicted_at >= day_start)
    ) or 0
    payload_count = session.scalar(
        select(func.count(RawPayload.payload_id)).where(RawPayload.fetched_at >= day_start)
    ) or 0
    model_run_count = session.scalar(
        select(func.count(ModelRun.model_run_id)).where(ModelRun.completed_at >= day_start)
    ) or 0
    latest_model_run = session.execute(
        select(ModelRun.model_run_id, ModelRun.completed_at, ModelRun.metrics)
        .where(ModelRun.model_version.not_like("%_backtest"))
        .order_by(ModelRun.completed_at.desc())
        .limit(1)
    ).first()
    latest_backtest = session.execute(
        select(BacktestResult.computed_at, BacktestResult.metrics)
        .order_by(BacktestResult.computed_at.desc())
        .limit(1)
    ).first()
    provider_error_rows = session.execute(
        text(
            """
            SELECT provider_name, COUNT(*) as failures
            FROM ai_provider_events
            WHERE status = 'error' AND created_at >= :day_start
            GROUP BY provider_name
            ORDER BY failures DESC
            """
        ),
        {"day_start": day_start},
    ).all()

    # --- Data Quality Sentinel: detect extreme predictions and line divergences ---
    extreme_prediction_count = session.scalar(
        select(func.count(Prediction.prediction_id)).where(
            Prediction.predicted_at >= day_start,
            (Prediction.over_probability > 0.97) | (Prediction.over_probability < 0.03),
        )
    ) or 0
    projection_line_divergence_count = int(
        session.execute(
            text(
                """
                SELECT COUNT(*) FROM predictions p
                JOIN line_snapshots ls ON p.line_snapshot_id = ls.snapshot_id
                WHERE p.predicted_at >= :day_start
                  AND ls.line_value > 0
                  AND ABS(p.projected_mean - ls.line_value) / ls.line_value > 0.40
                """
            ),
            {"day_start": day_start},
        ).scalar()
        or 0
    )
    sentinel_status = "ALERT" if (extreme_prediction_count > 0 or projection_line_divergence_count > 0) else "CLEAN"

    backtest_summary = "No backtest row available."
    if latest_backtest is not None:
        backtest_summary = f"computed_at={latest_backtest.computed_at}, metrics={latest_backtest.metrics}"
    latest_training_data_quality = "unavailable"
    if latest_model_run is not None and isinstance(latest_model_run.metrics, dict):
        dq = latest_model_run.metrics.get("training_data_quality")
        if isinstance(dq, dict):
            latest_training_data_quality = str(dq)
    provider_error_summary = (
        ", ".join(f"{row.provider_name}:{row.failures}" for row in provider_error_rows) or "none"
    )

    model_prompt = (
        "Summarize model health in 4 bullet points using these metrics:\n"
        f"- predictions_today={prediction_count}\n"
        f"- model_runs_today={model_run_count}\n"
        f"- latest_backtest={backtest_summary}\n"
        "Include one confidence score (0-100) and one next action."
    )
    provider_prompt = (
        "Summarize provider/ingestion health in 4 bullet points using:\n"
        f"- raw_payloads_today={payload_count}\n"
        f"- ai_provider_errors_today={provider_error_summary}\n"
        "Call out likely bottlenecks and one concrete mitigation."
    )
    retrain_prompt = (
        "Based on these signals, should retraining be triggered today?\n"
        f"- predictions_today={prediction_count}\n"
        f"- model_runs_today={model_run_count}\n"
        f"- latest_backtest={backtest_summary}\n"
        "Return decision as: Trigger=YES/NO, Reason, Confidence."
    )

    model_section = orchestrator.summarize(task_name="model_health", prompt=model_prompt)
    provider_section = orchestrator.summarize(task_name="provider_health", prompt=provider_prompt)
    retrain_section = orchestrator.summarize(task_name="retrain_decision", prompt=retrain_prompt)
    api_coverage = assess_api_coverage(session, report_date=report_date)
    release_comparison = compare_latest_model_runs(session)
    latest_model_metrics = latest_model_run.metrics if latest_model_run and isinstance(latest_model_run.metrics, dict) else {}
    latest_backtest_metrics = latest_backtest.metrics if latest_backtest and isinstance(latest_backtest.metrics, dict) else {}
    quality_guardrail = _build_quality_guardrail_status(
        latest_model_metrics=latest_model_metrics,
        latest_backtest_metrics=latest_backtest_metrics,
    )
    release_recommendation = apply_release_policy_override(
        _build_release_recommendation(
            api_tier=api_coverage.tier,
            prediction_count=int(prediction_count),
            model_run_count=int(model_run_count),
            quality_guardrail=quality_guardrail,
        ),
        settings=settings,
    )
    trend_snapshot = build_automation_trend_snapshot(
        session, settings.reports_dir, report_date=report_date
    )
    mode = (agent_mode or settings.agent_mode).strip().lower()
    if mode not in {"off", "recommend", "auto"}:
        mode = "off"
    # Non-auto modes are advisory-only: never honor caller dry_run=False (CLI already enforced;
    # this closes programmatic bypass for DataQualityAgent and keeps report/local autonomy consistent).
    if mode == "auto":
        effective_dry_run = settings.agent_default_dry_run if dry_run is None else dry_run
    else:
        effective_dry_run = True
    local_autonomy_markdown = "Local autonomy disabled."
    if mode != "off" and settings.local_autonomy_enabled:
        autonomy_result = LocalAutonomyEngine(session).run(
            report_date=report_date,
            mode=mode,
            dry_run=effective_dry_run,
            latest_model_metrics=latest_model_metrics,
            latest_backtest_metrics=latest_backtest_metrics,
            release_status=release_recommendation["status"],
            trend_alerts=trend_snapshot.alerts,
        )
        release_recommendation = _apply_local_autonomy_recommendation(
            base_recommendation=release_recommendation,
            autonomy_status=autonomy_result.decision.status,
            autonomy_confidence=autonomy_result.decision.confidence,
            autonomy_summary=autonomy_result.decision.summary,
            confidence_floor=settings.local_autonomy_min_confidence,
        )
        local_autonomy_markdown = render_local_autonomy_markdown(autonomy_result)
    agent_report = "Agent mode disabled."
    agent_preflight_ok = True
    agent_preflight_detail = "not_applicable"
    if mode != "off":
        agent_preflight_ok, agent_preflight_detail = verify_agent_run_events_table(session)
        if agent_preflight_ok:
            agent_results = AgentControlPlane(session).run(
                mode=mode,
                dry_run=effective_dry_run,
                report_date=report_date,
                examiner_context={
                    "latest_model_metrics": latest_model_metrics,
                    "latest_backtest_metrics": latest_backtest_metrics,
                    "trend_alerts": trend_snapshot.alerts,
                },
            )
            agent_report = render_agent_markdown(agent_results)
            if settings.examiner_enabled:
                try:
                    from app.services.examiner.csv_loader import load_examiner_dataset
                    from app.services.examiner.feedback import capture_daily_feedback

                    ds = None
                    try:
                        ds = load_examiner_dataset(
                            settings.examiner_csv_path,
                            real_only=settings.examiner_real_only_default,
                        )
                    except OSError:
                        pass
                    capture_daily_feedback(
                        session,
                        settings=settings,
                        report_date=report_date,
                        csv_dataset=ds,
                    )
                except Exception:
                    logger.warning("examiner feedback hook failed", exc_info=True)
        else:
            agent_report = (
                f"**Agent preflight failed:** {agent_preflight_detail}\n\n"
                "Control plane skipped; apply the agent-mode release checklist (DB schema) before "
                "running recommend/auto."
            )

    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    report_path = settings.reports_dir / f"automation_daily_{timestamp}.md"
    report_path.write_text(
        "\n".join(
            [
                f"# Daily Automation Report ({report_date.isoformat()})",
                "",
                "## Inputs",
                f"- predictions_today: {prediction_count}",
                f"- model_runs_today: {model_run_count}",
                f"- raw_payloads_today: {payload_count}",
                f"- latest_backtest: {backtest_summary}",
                f"- latest_training_data_quality: {latest_training_data_quality}",
                f"- ai_provider_errors_today: {provider_error_summary}",
                "",
                "## Data Quality Sentinel",
                f"- extreme_predictions_today (>97% or <3%): {extreme_prediction_count}",
                f"- projection_line_divergences (>40% off line): {projection_line_divergence_count}",
                f"- sentinel_status: {sentinel_status}",
                "",
                "## Model Health",
                f"_provider: {model_section.provider} | model: {model_section.model}_",
                "",
                model_section.text,
                "",
                "## Provider Health",
                f"_provider: {provider_section.provider} | model: {provider_section.model}_",
                "",
                provider_section.text,
                "",
                "## Retrain Recommendation",
                f"_provider: {retrain_section.provider} | model: {retrain_section.model}_",
                "",
                retrain_section.text,
                "",
                "## Agent Control Plane",
                f"- mode: {mode}",
                f"- dry_run: {effective_dry_run}",
                f"- preflight_ok: {agent_preflight_ok}",
                f"- preflight_detail: {agent_preflight_detail}",
                "",
                agent_report,
                "",
                "## Local Autonomy",
                local_autonomy_markdown,
                "",
                "## API Coverage Tier",
                f"- tier: {api_coverage.tier}",
                f"- summary: {api_coverage.summary}",
                f"- payload_counts: {api_coverage.payload_counts}",
                f"- provider_counts: {api_coverage.provider_counts}",
                f"- network_observer_ok: {api_coverage.network_ok_count}",
                f"- network_observer_error: {api_coverage.network_error_count}",
                *([f"- reason: {reason}" for reason in api_coverage.reasons] or ["- reason: none"]),
                "",
                "## Automation Health Trends",
                *trend_snapshot.lines,
                "",
                "### Deterioration alerts",
                *(
                    [f"- {a}" for a in trend_snapshot.alerts]
                    if trend_snapshot.alerts
                    else ["- none"]
                ),
                "",
                "## Release Recommendation",
                f"- status: {release_recommendation['status']}",
                f"- confidence: {release_recommendation['confidence']}",
                f"- rationale: {release_recommendation['rationale']}",
                f"- quality_guardrail_status: {quality_guardrail['status']}",
                f"- quality_guardrail_summary: {quality_guardrail['summary']}",
                "",
                "## Champion-Challenger Snapshot",
                f"- status: {release_comparison.status}",
                f"- summary: {release_comparison.summary}",
                f"- candidate_run_id: {release_comparison.candidate_run_id}",
                f"- champion_run_id: {release_comparison.champion_run_id}",
                f"- candidate_avg_ece: {release_comparison.candidate_avg_ece}",
                f"- champion_avg_ece: {release_comparison.champion_avg_ece}",
                f"- candidate_data_quality: {release_comparison.candidate_data_quality_status}",
                f"- champion_data_quality: {release_comparison.champion_data_quality_status}",
                f"- rationale: {release_comparison.rationale}",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return cast(Path, report_path)


def _apply_local_autonomy_recommendation(
    *,
    base_recommendation: dict[str, str],
    autonomy_status: str,
    autonomy_confidence: float,
    autonomy_summary: str,
    confidence_floor: float,
) -> dict[str, str]:
    rank = {"GO": 0, "CAUTION": 1, "HOLD": 2, "BLOCKED": 3}
    if autonomy_status == "execute" and autonomy_confidence >= confidence_floor:
        return base_recommendation
    if autonomy_status in {"hold", "error"} and autonomy_confidence >= confidence_floor:
        target_status = "HOLD"
    elif autonomy_status == "disabled":
        return base_recommendation
    else:
        target_status = "CAUTION"
    if rank[target_status] <= rank.get(base_recommendation["status"], 0):
        return base_recommendation
    return {
        "status": target_status,
        "confidence": "medium" if target_status == "CAUTION" else "low",
        "rationale": (
            f"Local autonomy recommendation tightened release posture ({autonomy_status}, "
            f"confidence={autonomy_confidence:.2f}): {autonomy_summary}. "
            f"Base recommendation was {base_recommendation['status']}: {base_recommendation['rationale']}"
        ),
    }


def apply_release_policy_override(
    recommendation: dict[str, str],
    *,
    settings: Settings,
) -> dict[str, str]:
    """Apply explicit release override from settings (audit reason + optional expiry).

    Overrides never downgrade a GO recommendation; they only annotate blocked/caution states.
    """
    if not settings.release_policy_override_enabled:
        return recommendation
    reason = (settings.release_policy_override_reason or "").strip()
    if not reason:
        return recommendation
    until = settings.release_policy_override_until
    if until is not None:
        until_aware = until if until.tzinfo is not None else until.replace(tzinfo=UTC)
        if datetime.now(UTC) > until_aware:
            return recommendation
    base_status = recommendation["status"]
    if base_status == "GO":
        return recommendation
    until_clause = f" until {until.isoformat()}" if until is not None else ""
    return {
        "status": "POLICY_OVERRIDE",
        "confidence": "low",
        "rationale": (
            f"Manual release policy override active{until_clause}: {reason}. "
            f"Base recommendation was {base_status}: {recommendation['rationale']}"
        ),
    }


def _build_release_recommendation(
    *,
    api_tier: str,
    prediction_count: int,
    model_run_count: int,
    quality_guardrail: dict[str, str] | None = None,
) -> dict[str, str]:
    if api_tier == "C":
        recommendation = {
            "status": "BLOCKED",
            "confidence": "low",
            "rationale": "API coverage is degraded (Tier C). Hold model release until provider coverage recovers.",
        }
    elif prediction_count <= 0 or model_run_count <= 0:
        recommendation = {
            "status": "HOLD",
            "confidence": "medium",
            "rationale": "Model run or prediction counts are incomplete for the report day.",
        }
    elif api_tier == "B":
        recommendation = {
            "status": "CAUTION",
            "confidence": "medium",
            "rationale": "Partial provider fidelity (Tier B). Release only with explicit override.",
        }
    else:
        recommendation = {
            "status": "GO",
            "confidence": "high",
            "rationale": "Coverage and core model execution health checks passed.",
        }
    if quality_guardrail:
        recommendation = _apply_quality_guardrail(recommendation, quality_guardrail)
    return recommendation


def _apply_quality_guardrail(
    base_recommendation: dict[str, str],
    quality_guardrail: dict[str, str],
) -> dict[str, str]:
    rank = {"GO": 0, "CAUTION": 1, "HOLD": 2, "BLOCKED": 3}
    guardrail_status = quality_guardrail.get("status", "GO")
    if rank.get(guardrail_status, 0) <= rank.get(base_recommendation["status"], 0):
        return base_recommendation
    return {
        "status": guardrail_status,
        "confidence": "medium" if guardrail_status in {"CAUTION", "HOLD"} else "low",
        "rationale": (
            f"Quality guardrail escalated recommendation: {quality_guardrail.get('summary', 'guardrail triggered')}. "
            f"Base recommendation was {base_recommendation['status']}: {base_recommendation['rationale']}"
        ),
    }


def _build_quality_guardrail_status(
    *,
    latest_model_metrics: dict[str, object],
    latest_backtest_metrics: dict[str, object],
) -> dict[str, str]:
    diagnostics = latest_model_metrics.get("calibration_diagnostics")
    if not isinstance(diagnostics, dict) or not diagnostics:
        return {"status": "HOLD", "summary": "Missing calibration diagnostics in latest model run."}

    ece_values: list[float] = []
    watch_breaches: list[str] = []
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
        ece_values.append(ece)
        if market in {"pra", "turnovers"} and ece > 0.10:
            watch_breaches.append(f"{market.upper()} ECE={ece:.3f}")

    if not ece_values:
        return {"status": "HOLD", "summary": "Calibration diagnostics present but ECE values are missing."}
    avg_ece = sum(ece_values) / len(ece_values)
    if avg_ece > 0.09:
        return {"status": "BLOCKED", "summary": f"Average ECE {avg_ece:.3f} exceeds release ceiling 0.090."}
    if watch_breaches:
        return {"status": "BLOCKED", "summary": "Watch-market calibration breach: " + ", ".join(watch_breaches)}

    sufficiency_ratio = _extract_backtest_sufficiency_ratio(latest_backtest_metrics)
    if sufficiency_ratio is not None and sufficiency_ratio < 0.40:
        return {
            "status": "HOLD",
            "summary": (
                f"Backtest sample sufficiency ratio {sufficiency_ratio:.2f} is below minimum 0.40."
            ),
        }
    watch_elevated = False
    for market in ("pra", "turnovers"):
        payload = diagnostics.get(market)
        if not isinstance(payload, dict):
            continue
        raw_ece = payload.get("ece")
        if not isinstance(raw_ece, (int, float, str)):
            continue
        try:
            ece_value = float(raw_ece)
        except (TypeError, ValueError):
            continue
        if ece_value > 0.08:
            watch_elevated = True
            break
    if watch_elevated:
        return {"status": "CAUTION", "summary": "Watch-market ECE is elevated (>0.08)."}
    return {"status": "GO", "summary": "Calibration and backtest sufficiency guardrails passed."}


def _extract_backtest_sufficiency_ratio(metrics: dict[str, object]) -> float | None:
    summary_rows = metrics.get("summary_rows")
    if not isinstance(summary_rows, list):
        return None
    values: list[float] = []
    for row in summary_rows:
        if not isinstance(row, dict):
            continue
        raw_value = row.get("sample_sufficient")
        if raw_value is None:
            continue
        if not isinstance(raw_value, (int, float, str)):
            continue
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            continue
        values.append(value)
    if not values:
        return None
    return float(sum(values) / len(values))

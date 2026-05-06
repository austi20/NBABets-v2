"""Local Qwen accuracy examiner agent (advisory; retrain stays guarded_write).

Retrain recommendations are emitted as ``AgentAction`` with ``safe_to_auto_execute=False``.
``AgentControlPlane._execute_workflow_actions`` intentionally does not execute examiner
retrain actions — humans or a future autonomy gate must approve.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

from sqlalchemy.orm import Session

from app.config.settings import get_settings
from app.services.agents.contracts import AgentAction, AgentResult, AgentTask
from app.services.ai_orchestrator import AIOrchestrator
from app.services.brain.brain import Brain
from app.services.brain.contracts import CorrectionRecord, SignalType
from app.services.examiner.contracts import (
    EXAMINER_SIGNAL_TAXONOMY,
    ExaminerPromptContext,
    bucket_confidence,
)
from app.services.examiner.csv_loader import load_examiner_dataset, recent_real_examples
from app.services.examiner.prompt_builder import build_examiner_prompt
from app.services.examiner.retrieval import ExaminerRetriever
from app.services.examiner.store import ExaminerStore
from app.services.local_autonomy.contracts import extract_json_object
from app.services.local_autonomy.debug_copilot import build_debug_hints
from app.services.local_autonomy.overfit_intel import (
    build_feature_attribution_review,
    build_overfit_intel_snapshot,
)

logger = logging.getLogger(__name__)

_DEFAULT_MARKETS = ("points", "rebounds", "assists", "threes", "turnovers", "pra")


def _parse_report_date(raw: Any) -> date:
    if isinstance(raw, date) and not isinstance(raw, datetime):
        return raw
    if isinstance(raw, str):
        return date.fromisoformat(raw[:10])
    return date.today()


def examiner_signal_to_brain_signal(raw: str) -> SignalType:
    """Map examiner taxonomy strings onto Brain ``SignalType`` literals."""

    normalized = (raw or "").strip().lower()
    allowed: set[str] = {
        "dnp_contamination",
        "overfit",
        "calibration_drift",
        "projection_divergence",
        "extreme_probability",
        "empty_backtest",
        "data_quality_degraded",
    }
    if normalized in allowed:
        return normalized  # type: ignore[return-value]
    if normalized in {"synthetic_leakage", "calculation_error"}:
        return "data_quality_degraded"
    return "data_quality_degraded"


class AccuracyExaminerAgent:
    role = "accuracy_examiner"

    def __init__(self, session: Session) -> None:
        self._session = session
        self._settings = get_settings()
        self._orchestrator = AIOrchestrator(session)

    def handle(self, task: AgentTask) -> AgentResult:
        pl = task.input_payload
        report_date = _parse_report_date(pl.get("report_date"))
        markets = tuple(pl.get("markets") or _DEFAULT_MARKETS)
        if isinstance(markets, list):
            markets = tuple(str(m) for m in markets)
        latest_model = pl.get("latest_model_metrics") or {}
        latest_bt = pl.get("latest_backtest_metrics") or {}
        alerts = pl.get("trend_alerts") or ()
        if isinstance(alerts, list):
            alerts = tuple(str(a) for a in alerts)

        try:
            dataset = load_examiner_dataset(
                self._settings.examiner_csv_path,
                real_only=self._settings.examiner_real_only_default,
            )
        except FileNotFoundError as exc:
            return AgentResult(
                task_id=task.task_id,
                role=self.role,
                status="skipped",
                summary=f"Examiner CSV missing: {exc}",
                error_category="missing_resource",
            )
        except (OSError, ValueError) as exc:
            return AgentResult(
                task_id=task.task_id,
                role=self.role,
                status="error",
                summary=f"Failed to load examiner CSV: {exc}",
                error_category="data_error",
            )

        brain = Brain()
        store: ExaminerStore | None = None
        try:
            store = ExaminerStore(self._settings.brain_db_path)
            recent = recent_real_examples(dataset, n_days=14, reference=report_date)
            to_seed = recent if recent else dataset.examples[:500]
            store.reseed_labeled_from_dataset(to_seed)

            hints = build_debug_hints(self._session, report_date=report_date)
            hint_lines = tuple(
                f"{h.category}: {h.summary}" for h in hints[:20]
            )

            overfit_snap = build_overfit_intel_snapshot(
                latest_model_metrics=latest_model if isinstance(latest_model, dict) else {},
                latest_backtest_metrics=latest_bt if isinstance(latest_bt, dict) else {},
                trend_alerts=list(alerts),
            )
            attr = build_feature_attribution_review(
                self._session, self._orchestrator, limit=40
            )
            overfit_block: dict[str, Any] = {
                "risk_score": overfit_snap.risk_score,
                "signals": [{"market": s.market, "score": s.score, "note": s.note} for s in overfit_snap.signals],
                "mitigations": list(overfit_snap.mitigations),
                "feature_attribution": attr,
            }

            ctx = ExaminerPromptContext(
                report_date=report_date,
                markets=markets,
                line_bucket=None,
                confidence_bucket=None,
                latest_model_metrics=latest_model if isinstance(latest_model, dict) else {},
                latest_backtest_metrics=latest_bt if isinstance(latest_bt, dict) else {},
                trend_alerts=alerts,
            )
            retriever = ExaminerRetriever(store)
            retrieved = retriever.retrieve(
                ctx,
                dataset,
                top_k=self._settings.examiner_top_k,
                debug_hint_lines=hint_lines,
            )
            mix_warning = None
            if dataset.mix_ratio_real_vs_synthetic < 0.05:
                mix_warning = (
                    "The graded CSV is overwhelmingly synthetic; treat empirical claims "
                    "with extreme caution and lean on real rows only."
                )
            prompt = build_examiner_prompt(
                ctx,
                retrieved,
                csv_mix_warning=mix_warning,
                overfit_block=overfit_block,
            )
            ai = self._orchestrator.summarize(task_name="accuracy_examiner", prompt=prompt)
            parsed = extract_json_object(
                ai.text,
                required_keys=frozenset({"errors", "data_filters", "calculation_checks"}),
            )
            if parsed is None:
                return AgentResult(
                    task_id=task.task_id,
                    role=self.role,
                    status="error",
                    summary="Examiner model returned no parseable JSON.",
                    error_category="parse_error",
                    details={"raw_preview": ai.text[:500]},
                )

            retr = parsed.get("retrain_recommendation")
            actions: list[AgentAction] = []
            findings: list[dict[str, Any]] = []
            errors = parsed.get("errors") or []
            if isinstance(errors, list):
                for item in errors:
                    if not isinstance(item, dict):
                        continue
                    sig = str(item.get("signal") or "data_quality_degraded")
                    if sig not in EXAMINER_SIGNAL_TAXONOMY:
                        sig = "data_quality_degraded"
                    headline = str(item.get("headline") or "finding")
                    detail = str(item.get("detail") or "")
                    market = item.get("market")
                    conf = float(item.get("confidence") or 0.5)
                    findings.append(
                        {
                            "signal": sig,
                            "headline": headline,
                            "market": market,
                            "line_bucket": None,
                            "confidence_bucket": str(bucket_confidence(conf) or "mid"),
                            "confidence": conf,
                        }
                    )
                    actions.append(
                        AgentAction(
                            action_type="flag_examiner_finding",
                            reason=f"[{sig}] {headline}: {detail[:400]}",
                            payload={"signal": sig, "market": market, "confidence": conf},
                            safe_to_auto_execute=False,
                        )
                    )
                    rec = CorrectionRecord(
                        signal_type=examiner_signal_to_brain_signal(sig),
                        action_type="selective_retrain",
                        market=str(market) if market else None,
                        params_before={},
                        params_after={"examiner": item},
                        ece_before=None,
                        outcome="pending",
                        confidence=conf,
                        notes=f"accuracy_examiner:{headline}"[:500],
                    )
                    brain.record_correction(rec)

            if isinstance(retr, dict):
                trig = bool(retr.get("trigger"))
                rconf = float(retr.get("confidence") or 0.0)
                reason = str(retr.get("reason") or "")
                if trig and rconf >= self._settings.examiner_min_confidence_for_retrain:
                    actions.append(
                        AgentAction(
                            action_type="retrain_and_predict",
                            reason=f"Examiner recommends retrain (confidence={rconf:.2f}): {reason[:400]}",
                            payload={"confidence": rconf, "reason": reason},
                            safe_to_auto_execute=False,
                        )
                    )
                    brain.record_correction(
                        CorrectionRecord(
                            signal_type="calibration_drift",
                            action_type="selective_retrain",
                            market=None,
                            params_before={},
                            params_after={"retrain_recommendation": retr},
                            confidence=rconf,
                            notes="accuracy_examiner:retrain_and_predict",
                        )
                    )

            status = "recommendation" if actions else "ok"
            summary = (
                f"Examiner emitted {len(actions)} advisory actions."
                if actions
                else "Examiner found no actionable issues in structured output."
            )
            retr_conf: float | None = (
                float(retr["confidence"]) if isinstance(retr, dict) and "confidence" in retr else None
            )
            return AgentResult(
                task_id=task.task_id,
                role=self.role,
                status=status,
                summary=summary,
                actions=actions,
                confidence=retr_conf,
                details={
                    "examiner_findings": findings,
                    "mix_ratio_real_vs_synthetic": dataset.mix_ratio_real_vs_synthetic,
                    "data_filters": parsed.get("data_filters"),
                    "calculation_checks": parsed.get("calculation_checks"),
                    "retrain_recommendation": retr if isinstance(retr, dict) else None,
                },
            )
        finally:
            brain.close()
            if store is not None:
                try:
                    store.close()
                except OSError:
                    logger.debug("examiner store close failed", exc_info=True)

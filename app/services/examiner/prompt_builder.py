"""Assemble the local-Qwen accuracy examiner system prompt."""

from __future__ import annotations

import json
from typing import Any

from app.services.examiner.contracts import ExaminerPromptContext, ExaminerRetrievalResult

_MAX_EXAMPLES = 6
_MAX_EXAMPLE_CHARS = 150
_MAX_LESSONS = 3
_MAX_LESSON_CHARS = 200
_MAX_DEBUG_HINTS = 5


def _clip(text: str, max_len: int) -> str:
    t = text.replace("\n", " ").strip()
    if len(t) <= max_len:
        return t
    return t[: max_len - 1] + "…"


def build_examiner_prompt(
    context: ExaminerPromptContext,
    retrieved: ExaminerRetrievalResult,
    *,
    csv_mix_warning: str | None,
    overfit_block: dict[str, Any],
) -> str:
    """Return instructions plus context; model must answer with strict JSON."""

    lines: list[str] = [
        "You are the NBA prop accuracy examiner. Prefer source=real graded examples; "
        "treat synthetic rows as structural hints only (line mechanics), not empirical truth.",
        "",
        "Respond with STRICT JSON only (no markdown fences) using exactly these keys:",
        '{"errors":[...],"data_filters":[...],"calculation_checks":[...],"retrain_recommendation":'
        '{"trigger":false,"confidence":0.0,"reason":""}}',
        "",
        "- errors: list of {signal, headline, detail, market|null, confidence}",
        "- data_filters: list of strings describing rows or feeds to quarantine or review",
        "- calculation_checks: list of strings for deterministic QA steps",
        "- retrain_recommendation: trigger bool, confidence 0-1, short reason",
        "",
        f"Report date: {context.report_date.isoformat()}",
        f"Focus markets: {', '.join(context.markets) or 'all'}",
    ]

    if csv_mix_warning:
        lines.append("")
        lines.append(f"DATA_QUALITY_WARNING: {csv_mix_warning}")

    lines.append("")
    lines.append(f"Mix ratio (real/total in CSV file): {retrieved.mix_ratio_real_vs_synthetic:.4f}")

    if overfit_block:
        lines.append("")
        lines.append("Overfit / attribution context (JSON):")
        lines.append(_clip(json.dumps(overfit_block, ensure_ascii=False), 4000))

    lines.append("")
    lines.append("Recent automation / model metrics (JSON):")
    lines.append(_clip(json.dumps(context.latest_model_metrics, ensure_ascii=False), 2500))
    lines.append("")
    lines.append("Backtest metrics (JSON):")
    lines.append(_clip(json.dumps(context.latest_backtest_metrics, ensure_ascii=False), 2500))

    if context.trend_alerts:
        lines.append("")
        lines.append("Trend alerts:")
        for a in context.trend_alerts[:8]:
            lines.append(f"- {_clip(a, 240)}")

    dbg = list(retrieved.debug_hints[:_MAX_DEBUG_HINTS])
    lines.append("")
    lines.append("Debug hints:")
    for h in dbg:
        lines.append(f"- {_clip(h, 220)}")

    lessons = list(retrieved.lesson_cards[:_MAX_LESSONS])
    exs = list(retrieved.examples[:_MAX_EXAMPLES])

    lines.append("")
    lines.append("Lesson cards (graded feedback from prior runs):")
    if not lessons:
        lines.append("- none")
    for lc in lessons:
        block = f"{lc.signal}: {lc.headline} — {lc.body} (outcome={lc.outcome})"
        lines.append(f"- {_clip(block, _MAX_LESSON_CHARS)}")

    lines.append("")
    lines.append("Graded prop examples (subset):")
    if not exs:
        lines.append("- none")
    for ex in exs:
        block = (
            f"{ex.game_date} {ex.player_name} {ex.market} L={ex.line_value} "
            f"src={ex.source} hit_over={ex.hit_over} hit_under={ex.hit_under}"
        )
        lines.append(f"- {_clip(block, _MAX_EXAMPLE_CHARS)}")

    return "\n".join(lines)

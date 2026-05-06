"""Keyword and bucket-based retrieval for the accuracy examiner."""

from __future__ import annotations

from app.services.examiner.contracts import (
    ExaminerPromptContext,
    ExaminerRetrievalResult,
    LabeledPropDataset,
    LabeledPropExample,
    bucket_line,
)
from app.services.examiner.store import ExaminerStore


class ExaminerRetriever:
    def __init__(self, store: ExaminerStore) -> None:
        self._store = store

    def retrieve(
        self,
        context: ExaminerPromptContext,
        dataset: LabeledPropDataset,
        *,
        top_k: int,
        debug_hint_lines: tuple[str, ...],
    ) -> ExaminerRetrievalResult:
        """Match CSV examples and persisted lesson cards to the prompt context."""

        markets = frozenset(m.lower() for m in context.markets) if context.markets else None
        lb = context.line_bucket or None
        cb = context.confidence_bucket

        scored: list[tuple[int, LabeledPropExample]] = []
        for ex in dataset.examples:
            if markets and ex.market.lower() not in markets:
                continue
            ex_lb = bucket_line(ex.line_value)
            score = 0
            if lb and ex_lb == lb:
                score += 3
            elif lb and ex_lb:
                try:
                    if abs(float(ex_lb) - float(lb)) <= 1.0:
                        score += 1
                except ValueError:
                    pass
            if ex.source == "real":
                score += 2
            if score > 0 or not markets:
                scored.append((score, ex))

        scored.sort(key=lambda t: (-t[0], t[1].game_date))
        picked: list[LabeledPropExample] = []
        seen: set[tuple[str, str, str, float]] = set()
        for _, ex in scored:
            key = (ex.player_name, ex.market, ex.game_date.isoformat(), ex.line_value)
            if key in seen:
                continue
            seen.add(key)
            picked.append(ex)
            if len(picked) >= top_k:
                break

        if len(picked) < top_k:
            for ex in self._store.fetch_labeled_matching(
                markets=markets,
                line_bucket=lb,
                confidence_bucket=cb,
                limit=top_k - len(picked),
            ):
                key = (ex.player_name, ex.market, ex.game_date.isoformat(), ex.line_value)
                if key in seen:
                    continue
                seen.add(key)
                picked.append(ex)
                if len(picked) >= top_k:
                    break

        if len(picked) < top_k:
            for ex in sorted(dataset.examples, key=lambda e: e.game_date, reverse=True):
                if markets and ex.market.lower() not in markets:
                    continue
                key = (ex.player_name, ex.market, ex.game_date.isoformat(), ex.line_value)
                if key in seen:
                    continue
                seen.add(key)
                picked.append(ex)
                if len(picked) >= top_k:
                    break

        lessons = self._store.fetch_lesson_cards(
            markets=markets,
            line_bucket=lb,
            confidence_bucket=cb,
            limit=12,
        )

        return ExaminerRetrievalResult(
            examples=tuple(picked),
            lesson_cards=tuple(lessons),
            debug_hints=debug_hint_lines,
            mix_ratio_real_vs_synthetic=dataset.mix_ratio_real_vs_synthetic,
        )

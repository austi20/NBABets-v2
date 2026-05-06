"""SQLite persistence for examiner labeled examples and lesson cards (brain.sqlite)."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from app.services.examiner.contracts import (
    ConfidenceBucket,
    LabeledPropExample,
    LessonCard,
    bucket_line,
)

EXAMINER_DDL = """
CREATE TABLE IF NOT EXISTS examiner_labeled_example (
    example_id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_date TEXT NOT NULL,
    player_name TEXT NOT NULL,
    market TEXT NOT NULL,
    line_bucket TEXT,
    confidence_bucket TEXT,
    source TEXT NOT NULL DEFAULT 'synthetic',
    payload TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_examiner_labeled_mlc
    ON examiner_labeled_example(market, line_bucket, confidence_bucket);

CREATE TABLE IF NOT EXISTS examiner_lesson_card (
    lesson_id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    market TEXT,
    line_bucket TEXT,
    confidence_bucket TEXT,
    signal TEXT NOT NULL,
    headline TEXT NOT NULL,
    body TEXT NOT NULL DEFAULT '',
    ece_before REAL,
    ece_after REAL,
    outcome TEXT NOT NULL DEFAULT 'pending'
);

CREATE INDEX IF NOT EXISTS idx_examiner_lesson_mlc
    ON examiner_lesson_card(market, line_bucket, confidence_bucket);
"""


def _example_payload(ex: LabeledPropExample) -> str:
    return json.dumps(
        {
            "game_date": ex.game_date.isoformat(),
            "player_name": ex.player_name,
            "market": ex.market,
            "line_value": ex.line_value,
            "hit_over": ex.hit_over,
            "hit_under": ex.hit_under,
            "push": ex.push,
            "source": ex.source,
        },
        ensure_ascii=False,
    )


class ExaminerStore:
    """Read/write examiner tables co-located in ``brain.sqlite``."""

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(EXAMINER_DDL)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def clear_labeled_examples(self) -> None:
        self._conn.execute("DELETE FROM examiner_labeled_example")
        self._conn.commit()

    def insert_labeled_example(
        self,
        ex: LabeledPropExample,
        *,
        line_bucket: str | None,
        confidence_bucket: ConfidenceBucket | None,
    ) -> int:
        cur = self._conn.execute(
            """INSERT INTO examiner_labeled_example
               (game_date, player_name, market, line_bucket, confidence_bucket, source, payload)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                ex.game_date.isoformat(),
                ex.player_name,
                ex.market,
                line_bucket,
                confidence_bucket,
                ex.source,
                _example_payload(ex),
            ),
        )
        self._conn.commit()
        return int(cur.lastrowid or 0)

    def reseed_labeled_from_dataset(self, examples: Sequence[LabeledPropExample]) -> int:
        """Replace all labeled rows with ``examples`` (used to sync CSV snapshot into SQLite)."""

        self.clear_labeled_examples()
        for ex in examples:
            lb = bucket_line(ex.line_value)
            self._conn.execute(
                """INSERT INTO examiner_labeled_example
                   (game_date, player_name, market, line_bucket, confidence_bucket, source, payload)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    ex.game_date.isoformat(),
                    ex.player_name,
                    ex.market,
                    lb,
                    None,
                    ex.source,
                    _example_payload(ex),
                ),
            )
        self._conn.commit()
        return len(examples)

    def fetch_labeled_matching(
        self,
        *,
        markets: frozenset[str] | None,
        line_bucket: str | None,
        confidence_bucket: ConfidenceBucket | None,
        limit: int,
    ) -> list[LabeledPropExample]:
        clauses: list[str] = []
        params: list[Any] = []
        if markets:
            clauses.append(f"market IN ({','.join('?' for _ in markets)})")
            params.extend(sorted(markets))
        if line_bucket:
            clauses.append("(line_bucket = ? OR line_bucket IS NULL)")
            params.append(line_bucket)
        if confidence_bucket:
            clauses.append("(confidence_bucket = ? OR confidence_bucket IS NULL)")
            params.append(confidence_bucket)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self._conn.execute(
            f"SELECT * FROM examiner_labeled_example {where} ORDER BY game_date DESC LIMIT ?",
            (*params, limit),
        ).fetchall()
        return [_row_to_example(row) for row in rows]

    def insert_lesson_card(
        self,
        *,
        market: str | None,
        line_bucket: str | None,
        confidence_bucket: ConfidenceBucket | None,
        signal: str,
        headline: str,
        body: str,
        ece_before: float | None,
        ece_after: float | None,
        outcome: str,
    ) -> int:
        cur = self._conn.execute(
            """INSERT INTO examiner_lesson_card
               (created_at, market, line_bucket, confidence_bucket, signal, headline, body,
                ece_before, ece_after, outcome)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                datetime.now(UTC).isoformat(),
                market,
                line_bucket,
                confidence_bucket,
                signal,
                headline,
                body,
                ece_before,
                ece_after,
                outcome,
            ),
        )
        self._conn.commit()
        return int(cur.lastrowid or 0)

    def fetch_lesson_cards(
        self,
        *,
        markets: frozenset[str] | None,
        line_bucket: str | None,
        confidence_bucket: ConfidenceBucket | None,
        limit: int,
    ) -> list[LessonCard]:
        clauses: list[str] = []
        params: list[Any] = []
        if markets:
            clauses.append(f"(market IN ({','.join('?' for _ in markets)}) OR market IS NULL)")
            params.extend(sorted(markets))
        if line_bucket:
            clauses.append("(line_bucket = ? OR line_bucket IS NULL)")
            params.append(line_bucket)
        if confidence_bucket:
            clauses.append("(confidence_bucket = ? OR confidence_bucket IS NULL)")
            params.append(confidence_bucket)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self._conn.execute(
            f"SELECT * FROM examiner_lesson_card {where} ORDER BY created_at DESC LIMIT ?",
            (*params, limit),
        ).fetchall()
        return [_row_to_lesson(row) for row in rows]


def _row_to_example(row: sqlite3.Row) -> LabeledPropExample:
    payload = json.loads(row["payload"] or "{}")
    gd = date.fromisoformat(str(payload.get("game_date") or row["game_date"]))
    src = str(payload.get("source") or row["source"] or "synthetic")
    if src not in ("real", "synthetic"):
        src = "synthetic"
    return LabeledPropExample(
        game_date=gd,
        player_name=str(payload.get("player_name") or row["player_name"] or ""),
        team="",
        opponent="",
        market=str(payload.get("market") or row["market"] or ""),
        sportsbook="",
        line_value=float(payload.get("line_value", 0.0)),
        over_odds=None,
        under_odds=None,
        actual=None,
        hit_over=payload.get("hit_over"),
        hit_under=payload.get("hit_under"),
        push=bool(payload.get("push", False)),
        minutes=None,
        source=src,  # type: ignore[arg-type]
    )


def _row_to_lesson(row: sqlite3.Row) -> LessonCard:
    cb = row["confidence_bucket"]
    if cb not in ("low", "mid", "high", "extreme", None):
        cb = None
    oc = row["outcome"]
    if oc not in ("improved", "worsened", "neutral", "pending"):
        oc = "pending"
    return LessonCard(
        lesson_id=int(row["lesson_id"]),
        created_at=datetime.fromisoformat(row["created_at"]),
        market=row["market"],
        line_bucket=row["line_bucket"],
        confidence_bucket=cb,  # type: ignore[arg-type]
        signal=row["signal"],
        headline=row["headline"],
        body=row["body"] or "",
        ece_before=row["ece_before"],
        ece_after=row["ece_after"],
        outcome=oc,  # type: ignore[arg-type]
    )

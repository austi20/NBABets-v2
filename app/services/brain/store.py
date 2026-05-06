"""SQLite-backed persistent storage for brain learning data."""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.services.brain.contracts import (
    CorrectionOutcome,
    CorrectionRecord,
    MarketProfile,
    PredictionOutcome,
    StrategyMemory,
)
from app.services.examiner.store import EXAMINER_DDL

_SCHEMA_VERSION = 2

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS correction_log (
    correction_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_type     TEXT NOT NULL,
    action_type     TEXT NOT NULL,
    market          TEXT,
    params_before   TEXT NOT NULL DEFAULT '{}',
    params_after    TEXT NOT NULL DEFAULT '{}',
    ece_before      REAL,
    ece_after       REAL,
    outcome         TEXT NOT NULL DEFAULT 'pending',
    confidence      REAL NOT NULL DEFAULT 0.0,
    created_at      TEXT NOT NULL,
    resolved_at     TEXT,
    notes           TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS market_profile (
    market              TEXT PRIMARY KEY,
    optimal_weights     TEXT NOT NULL DEFAULT '{}',
    calibration_strategy TEXT NOT NULL DEFAULT 'auto',
    failure_modes       TEXT NOT NULL DEFAULT '[]',
    ece_history         TEXT NOT NULL DEFAULT '[]',
    correction_count    INTEGER NOT NULL DEFAULT 0,
    avg_improvement     REAL NOT NULL DEFAULT 0.0,
    updated_at          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS strategy_memory (
    strategy_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    problem_type        TEXT NOT NULL,
    action_template     TEXT NOT NULL,
    market              TEXT,
    parameters          TEXT NOT NULL DEFAULT '{}',
    success_rate        REAL NOT NULL DEFAULT 0.0,
    avg_ece_improvement REAL NOT NULL DEFAULT 0.0,
    sample_count        INTEGER NOT NULL DEFAULT 0,
    last_used_at        TEXT
);

CREATE TABLE IF NOT EXISTS prediction_outcome (
    outcome_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    prediction_id           INTEGER,
    player_name             TEXT NOT NULL DEFAULT '',
    market                  TEXT NOT NULL DEFAULT '',
    line_value              REAL NOT NULL DEFAULT 0.0,
    predicted_probability   REAL NOT NULL DEFAULT 0.0,
    calibrated_probability  REAL NOT NULL DEFAULT 0.0,
    actual_value            REAL,
    hit                     INTEGER,
    game_date               TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS feature_weight_overrides (
    market          TEXT NOT NULL,
    feature_name    TEXT NOT NULL,
    scale_factor    REAL NOT NULL DEFAULT 1.0,
    reason          TEXT NOT NULL DEFAULT '',
    correction_id   INTEGER,
    created_at      TEXT NOT NULL,
    active          INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (market, feature_name)
);

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY
);

CREATE INDEX IF NOT EXISTS idx_correction_market ON correction_log(market);
CREATE INDEX IF NOT EXISTS idx_correction_outcome ON correction_log(outcome);
CREATE INDEX IF NOT EXISTS idx_strategy_problem ON strategy_memory(problem_type, market);
CREATE INDEX IF NOT EXISTS idx_outcome_game_date ON prediction_outcome(game_date);
CREATE INDEX IF NOT EXISTS idx_outcome_market ON prediction_outcome(market);
"""


class BrainStore:
    """Low-level SQLite persistence for brain learning data."""

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        cursor = self._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'"
        )
        if cursor.fetchone() is None:
            self._conn.executescript(_SCHEMA_SQL)
            self._conn.executescript(EXAMINER_DDL)
            self._conn.execute(
                "INSERT OR REPLACE INTO schema_version (version) VALUES (?)",
                (_SCHEMA_VERSION,),
            )
            self._conn.commit()
            return
        row = self._conn.execute("SELECT version FROM schema_version LIMIT 1").fetchone()
        version = int(row[0]) if row and row[0] is not None else 1
        if version < _SCHEMA_VERSION:
            self._conn.executescript(EXAMINER_DDL)
            self._conn.execute(
                "UPDATE schema_version SET version = ?",
                (_SCHEMA_VERSION,),
            )
            self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    # -- Correction Log -------------------------------------------------------

    def store_correction(self, record: CorrectionRecord) -> int:
        cursor = self._conn.execute(
            """INSERT INTO correction_log
               (signal_type, action_type, market, params_before, params_after,
                ece_before, ece_after, outcome, confidence, created_at, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                record.signal_type,
                record.action_type,
                record.market,
                json.dumps(record.params_before),
                json.dumps(record.params_after),
                record.ece_before,
                record.ece_after,
                record.outcome,
                record.confidence,
                (record.created_at or datetime.now(UTC)).isoformat(),
                record.notes,
            ),
        )
        self._conn.commit()
        return cursor.lastrowid or 0

    def resolve_correction(
        self,
        correction_id: int,
        outcome: CorrectionOutcome,
        ece_after: float | None = None,
    ) -> None:
        self._conn.execute(
            """UPDATE correction_log
               SET outcome = ?, ece_after = ?, resolved_at = ?
               WHERE correction_id = ?""",
            (outcome, ece_after, datetime.now(UTC).isoformat(), correction_id),
        )
        self._conn.commit()

    def recall_corrections(
        self,
        signal_type: str | None = None,
        market: str | None = None,
        limit: int = 20,
    ) -> list[CorrectionRecord]:
        conditions: list[str] = []
        params: list[Any] = []
        if signal_type:
            conditions.append("signal_type = ?")
            params.append(signal_type)
        if market:
            conditions.append("market = ?")
            params.append(market)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        rows = self._conn.execute(
            f"SELECT * FROM correction_log {where} ORDER BY created_at DESC LIMIT ?",
            [*params, limit],
        ).fetchall()
        return [_row_to_correction(row) for row in rows]

    def pending_corrections(self) -> list[CorrectionRecord]:
        rows = self._conn.execute(
            "SELECT * FROM correction_log WHERE outcome = 'pending' ORDER BY created_at"
        ).fetchall()
        return [_row_to_correction(row) for row in rows]

    # -- Market Profile -------------------------------------------------------

    def recall_market(self, market: str) -> MarketProfile | None:
        row = self._conn.execute(
            "SELECT * FROM market_profile WHERE market = ?", (market,)
        ).fetchone()
        if row is None:
            return None
        return MarketProfile(
            market=row["market"],
            optimal_weights=json.loads(row["optimal_weights"]),
            calibration_strategy=row["calibration_strategy"],
            failure_modes=tuple(json.loads(row["failure_modes"])),
            ece_history=tuple(tuple(e) for e in json.loads(row["ece_history"])),
            correction_count=row["correction_count"],
            avg_improvement=row["avg_improvement"],
        )

    def save_market_profile(self, profile: MarketProfile) -> None:
        self._conn.execute(
            """INSERT OR REPLACE INTO market_profile
               (market, optimal_weights, calibration_strategy, failure_modes,
                ece_history, correction_count, avg_improvement, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                profile.market,
                json.dumps(profile.optimal_weights),
                profile.calibration_strategy,
                json.dumps(list(profile.failure_modes)),
                json.dumps([list(e) for e in profile.ece_history]),
                profile.correction_count,
                profile.avg_improvement,
                datetime.now(UTC).isoformat(),
            ),
        )
        self._conn.commit()

    # -- Strategy Memory ------------------------------------------------------

    def recall_strategies(
        self,
        problem_type: str | None = None,
        market: str | None = None,
        min_success_rate: float = 0.0,
    ) -> list[StrategyMemory]:
        conditions = ["success_rate >= ?"]
        params: list[Any] = [min_success_rate]
        if problem_type:
            conditions.append("problem_type = ?")
            params.append(problem_type)
        if market:
            conditions.append("(market = ? OR market IS NULL)")
            params.append(market)
        where = f"WHERE {' AND '.join(conditions)}"
        rows = self._conn.execute(
            f"SELECT * FROM strategy_memory {where} ORDER BY success_rate DESC, sample_count DESC",
            params,
        ).fetchall()
        return [_row_to_strategy(row) for row in rows]

    def upsert_strategy(self, strategy: StrategyMemory) -> int:
        # Try to find existing strategy for same problem+action+market
        existing = self._conn.execute(
            """SELECT strategy_id FROM strategy_memory
               WHERE problem_type = ? AND action_template = ? AND market IS ?""",
            (strategy.problem_type, strategy.action_template, strategy.market),
        ).fetchone()
        if existing:
            self._conn.execute(
                """UPDATE strategy_memory
                   SET parameters = ?, success_rate = ?, avg_ece_improvement = ?,
                       sample_count = ?, last_used_at = ?
                   WHERE strategy_id = ?""",
                (
                    json.dumps(strategy.parameters),
                    strategy.success_rate,
                    strategy.avg_ece_improvement,
                    strategy.sample_count,
                    (strategy.last_used_at or datetime.now(UTC)).isoformat(),
                    existing["strategy_id"],
                ),
            )
            self._conn.commit()
            return existing["strategy_id"]
        cursor = self._conn.execute(
            """INSERT INTO strategy_memory
               (problem_type, action_template, market, parameters,
                success_rate, avg_ece_improvement, sample_count, last_used_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                strategy.problem_type,
                strategy.action_template,
                strategy.market,
                json.dumps(strategy.parameters),
                strategy.success_rate,
                strategy.avg_ece_improvement,
                strategy.sample_count,
                (strategy.last_used_at or datetime.now(UTC)).isoformat(),
            ),
        )
        self._conn.commit()
        return cursor.lastrowid or 0

    def prune_weak_strategies(self, min_samples: int = 5, max_success_rate: float = 0.30) -> int:
        cursor = self._conn.execute(
            "DELETE FROM strategy_memory WHERE sample_count >= ? AND success_rate < ?",
            (min_samples, max_success_rate),
        )
        self._conn.commit()
        return cursor.rowcount

    # -- Prediction Outcomes --------------------------------------------------

    def store_outcome(self, outcome: PredictionOutcome) -> int:
        cursor = self._conn.execute(
            """INSERT INTO prediction_outcome
               (prediction_id, player_name, market, line_value,
                predicted_probability, calibrated_probability, actual_value, hit, game_date)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                outcome.prediction_id,
                outcome.player_name,
                outcome.market,
                outcome.line_value,
                outcome.predicted_probability,
                outcome.calibrated_probability,
                outcome.actual_value,
                1 if outcome.hit else (0 if outcome.hit is not None else None),
                outcome.game_date,
            ),
        )
        self._conn.commit()
        return cursor.lastrowid or 0

    def recall_outcomes(
        self,
        market: str | None = None,
        game_date: str | None = None,
        limit: int = 500,
    ) -> list[PredictionOutcome]:
        conditions: list[str] = []
        params: list[Any] = []
        if market:
            conditions.append("market = ?")
            params.append(market)
        if game_date:
            conditions.append("game_date = ?")
            params.append(game_date)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        rows = self._conn.execute(
            f"SELECT * FROM prediction_outcome {where} ORDER BY game_date DESC LIMIT ?",
            [*params, limit],
        ).fetchall()
        return [_row_to_outcome(row) for row in rows]

    def unresolved_outcomes(self, limit: int = 1000) -> list[PredictionOutcome]:
        rows = self._conn.execute(
            "SELECT * FROM prediction_outcome WHERE hit IS NULL ORDER BY game_date LIMIT ?",
            (limit,),
        ).fetchall()
        return [_row_to_outcome(row) for row in rows]

    # -- Feature Weight Overrides ---------------------------------------------

    def get_weight_overrides(self, market: str | None = None) -> dict[str, dict[str, float]]:
        """Return {market: {feature: scale_factor}} for active overrides."""
        if market:
            rows = self._conn.execute(
                "SELECT * FROM feature_weight_overrides WHERE active = 1 AND market = ?",
                (market,),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM feature_weight_overrides WHERE active = 1"
            ).fetchall()
        result: dict[str, dict[str, float]] = {}
        for row in rows:
            result.setdefault(row["market"], {})[row["feature_name"]] = row["scale_factor"]
        return result

    def set_weight_override(
        self,
        market: str,
        feature_name: str,
        scale_factor: float,
        reason: str = "",
        correction_id: int | None = None,
    ) -> None:
        self._conn.execute(
            """INSERT OR REPLACE INTO feature_weight_overrides
               (market, feature_name, scale_factor, reason, correction_id, created_at, active)
               VALUES (?, ?, ?, ?, ?, ?, 1)""",
            (market, feature_name, scale_factor, reason, correction_id, datetime.now(UTC).isoformat()),
        )
        self._conn.commit()

    def deactivate_weight_override(self, market: str, feature_name: str) -> None:
        self._conn.execute(
            "UPDATE feature_weight_overrides SET active = 0 WHERE market = ? AND feature_name = ?",
            (market, feature_name),
        )
        self._conn.commit()

    # -- Stats ----------------------------------------------------------------

    def correction_stats(self) -> dict[str, Any]:
        total = self._conn.execute("SELECT COUNT(*) FROM correction_log").fetchone()[0]
        improved = self._conn.execute(
            "SELECT COUNT(*) FROM correction_log WHERE outcome = 'improved'"
        ).fetchone()[0]
        worsened = self._conn.execute(
            "SELECT COUNT(*) FROM correction_log WHERE outcome = 'worsened'"
        ).fetchone()[0]
        pending = self._conn.execute(
            "SELECT COUNT(*) FROM correction_log WHERE outcome = 'pending'"
        ).fetchone()[0]
        return {
            "total": total,
            "improved": improved,
            "worsened": worsened,
            "pending": pending,
            "success_rate": improved / max(total - pending, 1),
        }


def _row_to_correction(row: sqlite3.Row) -> CorrectionRecord:
    return CorrectionRecord(
        correction_id=row["correction_id"],
        signal_type=row["signal_type"],
        action_type=row["action_type"],
        market=row["market"],
        params_before=json.loads(row["params_before"]),
        params_after=json.loads(row["params_after"]),
        ece_before=row["ece_before"],
        ece_after=row["ece_after"],
        outcome=row["outcome"],
        confidence=row["confidence"],
        created_at=datetime.fromisoformat(row["created_at"]) if row["created_at"] else None,
        resolved_at=datetime.fromisoformat(row["resolved_at"]) if row["resolved_at"] else None,
        notes=row["notes"],
    )


def _row_to_strategy(row: sqlite3.Row) -> StrategyMemory:
    return StrategyMemory(
        strategy_id=row["strategy_id"],
        problem_type=row["problem_type"],
        action_template=row["action_template"],
        market=row["market"],
        parameters=json.loads(row["parameters"]),
        success_rate=row["success_rate"],
        avg_ece_improvement=row["avg_ece_improvement"],
        sample_count=row["sample_count"],
        last_used_at=datetime.fromisoformat(row["last_used_at"]) if row["last_used_at"] else None,
    )


def _row_to_outcome(row: sqlite3.Row) -> PredictionOutcome:
    hit_raw = row["hit"]
    return PredictionOutcome(
        outcome_id=row["outcome_id"],
        prediction_id=row["prediction_id"],
        player_name=row["player_name"],
        market=row["market"],
        line_value=row["line_value"],
        predicted_probability=row["predicted_probability"],
        calibrated_probability=row["calibrated_probability"],
        actual_value=row["actual_value"],
        hit=bool(hit_raw) if hit_raw is not None else None,
        game_date=row["game_date"],
    )

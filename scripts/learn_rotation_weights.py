from __future__ import annotations

# ruff: noqa: E402
import argparse
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.db.session import session_scope
from app.training.data import DEFAULT_PREGAME_BUFFER_MINUTES, DatasetLoader
from app.training.rotation import classify_archetype, status_to_play_probability
from app.training.rotation_weights import aggregate_rotation_weights, summarize_weight_learning

DEFAULT_OUTPUT_PATH = Path("data/artifacts/rotation_weights.parquet")
DEFAULT_REPORT_DIR = Path("reports/rotation_weights")
OBSERVATION_COLUMNS = [
    "game_id",
    "team_id",
    "season",
    "absent_player_id",
    "candidate_player_id",
    "absent_archetype",
    "candidate_archetype",
    "minute_delta",
    "usage_delta",
    "absence_source",
    "rotation_shock_confidence",
]
BASELINE_MINUTES_THRESHOLD = 12.0
POST_HOC_MAX_DAYS_SINCE_LAST_GAME = 21
EPSILON = 1e-9


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Learn rotation redistribution weights from historical data.")
    parser.add_argument("--as-of-date", type=str, default=None, help="Optional YYYY-MM-DD cutoff.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH, help="Output parquet path.")
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR, help="Sanity report directory.")
    parser.add_argument(
        "--prediction-buffer-minutes",
        type=int,
        default=DEFAULT_PREGAME_BUFFER_MINUTES,
        help="Pregame cutoff for injury-report snapshots.",
    )
    parser.add_argument(
        "--disable-post-hoc",
        action="store_true",
        help="Use only explicit injury/official inactive rows; skip inferred missing-rotation absences.",
    )
    return parser.parse_args()


def _empty_observations() -> pd.DataFrame:
    return pd.DataFrame(columns=OBSERVATION_COLUMNS)


def _safe_numeric(frame: pd.DataFrame, column: str, default: float = 0.0) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(default, index=frame.index, dtype=float)
    return pd.to_numeric(frame[column], errors="coerce").fillna(default)


def _nba_season(game_date: pd.Series) -> pd.Series:
    dates = pd.to_datetime(game_date, errors="coerce")
    return (dates.dt.year - (dates.dt.month < 7).astype(int)).astype("Int64")


def _baseline_series(values: pd.Series, *, shifted: bool) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    source = numeric.shift(1) if shifted else numeric
    trailing = source.rolling(15, min_periods=10).mean()
    season_to_date = source.expanding(min_periods=1).mean()
    return trailing.combine_first(season_to_date)


def _position_group(value: object) -> str:
    return str(value or "").upper()[:1] or "UNK"


def _classify_from_values(
    *,
    position: object,
    usage_proxy: float,
    assists: float,
    rebounds: float,
    threes: float,
    field_goal_attempts: float,
    starter_score: float,
) -> str:
    usage_share = max(float(usage_proxy), 0.0) / 30.0
    assist_share = max(float(assists), 0.0) / 10.0
    rebound_share = max(float(rebounds), 0.0) / 15.0
    three_point_rate = max(float(threes), 0.0) / max(float(field_goal_attempts), 1.0)
    return classify_archetype(
        position_group=_position_group(position),
        usage_share=usage_share,
        assist_share=assist_share,
        rebound_share=rebound_share,
        three_point_rate=three_point_rate,
        starter_score=max(float(starter_score), 0.0),
    )


def _prepare_historical_frame(historical: pd.DataFrame) -> pd.DataFrame:
    if historical.empty:
        return historical.copy()

    required = {"player_id", "game_id", "game_date", "minutes"}
    missing = required.difference(historical.columns)
    if missing:
        missing_cols = ", ".join(sorted(missing))
        raise ValueError(f"Historical frame missing required columns: {missing_cols}")

    frame = historical.copy()
    if "player_team_id" in frame.columns:
        player_team_id = pd.to_numeric(frame["player_team_id"], errors="coerce")
        current_team_id = pd.to_numeric(
            frame["team_id"] if "team_id" in frame.columns else pd.Series(np.nan, index=frame.index),
            errors="coerce",
        )
        frame["team_id"] = player_team_id.combine_first(current_team_id)
    elif "team_id" in frame.columns:
        frame["team_id"] = pd.to_numeric(frame["team_id"], errors="coerce")
    else:
        raise ValueError("Historical frame must contain team_id or player_team_id.")

    frame["game_date"] = pd.to_datetime(frame["game_date"], errors="coerce")
    frame["start_time"] = pd.to_datetime(frame.get("start_time", frame["game_date"]), errors="coerce")
    frame["season"] = _nba_season(frame["game_date"])
    frame["player_id"] = pd.to_numeric(frame["player_id"], errors="coerce")
    frame["game_id"] = pd.to_numeric(frame["game_id"], errors="coerce")
    frame = frame.dropna(subset=["player_id", "game_id", "team_id", "season", "game_date", "start_time"]).copy()
    if frame.empty:
        return frame

    frame["player_id"] = frame["player_id"].astype(int)
    frame["game_id"] = frame["game_id"].astype(int)
    frame["team_id"] = frame["team_id"].astype(int)
    frame["season"] = frame["season"].astype(int)
    frame["sort_time"] = frame["start_time"].fillna(frame["game_date"])
    frame["minutes"] = _safe_numeric(frame, "minutes")
    frame["field_goal_attempts"] = _safe_numeric(frame, "field_goal_attempts")
    frame["free_throw_attempts"] = _safe_numeric(frame, "free_throw_attempts")
    frame["usage_proxy"] = frame["field_goal_attempts"] + 0.44 * frame["free_throw_attempts"]
    frame["assists"] = _safe_numeric(frame, "assists")
    frame["rebounds"] = _safe_numeric(frame, "rebounds")
    frame["threes"] = _safe_numeric(frame, "threes")
    frame["starter_flag"] = _safe_numeric(frame, "starter_flag")

    sort_columns = ["player_id", "team_id", "season", "sort_time", "game_id"]
    frame = frame.sort_values(sort_columns, kind="mergesort").reset_index(drop=True)
    group_keys = ["player_id", "team_id", "season"]
    metric_columns = [
        "minutes",
        "usage_proxy",
        "assists",
        "rebounds",
        "threes",
        "field_goal_attempts",
        "starter_flag",
    ]
    grouped = frame.groupby(group_keys, dropna=False)
    for column in metric_columns:
        frame[f"baseline_{column}"] = grouped[column].transform(lambda s: _baseline_series(s, shifted=True))
        frame[f"snapshot_{column}"] = grouped[column].transform(lambda s: _baseline_series(s, shifted=False))

    frame["candidate_archetype"] = [
        _classify_from_values(
            position=position,
            usage_proxy=usage_proxy,
            assists=assists,
            rebounds=rebounds,
            threes=threes,
            field_goal_attempts=field_goal_attempts,
            starter_score=starter_score,
        )
        for position, usage_proxy, assists, rebounds, threes, field_goal_attempts, starter_score in zip(
            frame.get("position", pd.Series("", index=frame.index)),
            frame["baseline_usage_proxy"],
            frame["baseline_assists"],
            frame["baseline_rebounds"],
            frame["baseline_threes"],
            frame["baseline_field_goal_attempts"],
            frame["baseline_starter_flag"],
            strict=False,
        )
    ]
    frame["snapshot_archetype"] = [
        _classify_from_values(
            position=position,
            usage_proxy=usage_proxy,
            assists=assists,
            rebounds=rebounds,
            threes=threes,
            field_goal_attempts=field_goal_attempts,
            starter_score=starter_score,
        )
        for position, usage_proxy, assists, rebounds, threes, field_goal_attempts, starter_score in zip(
            frame.get("position", pd.Series("", index=frame.index)),
            frame["snapshot_usage_proxy"],
            frame["snapshot_assists"],
            frame["snapshot_rebounds"],
            frame["snapshot_threes"],
            frame["snapshot_field_goal_attempts"],
            frame["snapshot_starter_flag"],
            strict=False,
        )
    ]

    extended_return = _safe_numeric(frame, "days_since_extended_absence") > 0.0
    previous_extended_return = extended_return.groupby(frame["player_id"]).shift(1).fillna(False).astype(bool)
    frame["_return_exclusion"] = extended_return | previous_extended_return
    return frame


def _normalize_timestamp_column(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns]")
    return pd.to_datetime(frame[column], errors="coerce", utc=True).dt.tz_convert(None)


def _load_absence_events(
    session: Session,
    *,
    as_of_date: date | None,
    prediction_buffer_minutes: int,
) -> pd.DataFrame:
    params: dict[str, object] = {}
    game_date_filter = ""
    if as_of_date is not None:
        game_date_filter = "AND g.game_date <= :as_of_date"
        params["as_of_date"] = as_of_date

    injuries = pd.read_sql_query(
        text(
            f"""
            SELECT
                ir.game_id,
                ir.team_id,
                ir.player_id,
                p.full_name AS player_name,
                p.position,
                ir.report_timestamp,
                ir.status,
                ir.expected_availability_flag,
                g.game_date,
                g.start_time
            FROM injury_reports ir
            JOIN games g ON g.game_id = ir.game_id
            JOIN players p ON p.player_id = ir.player_id
            WHERE ir.game_id IS NOT NULL
              AND g.status != 'superseded'
              {game_date_filter}
            """
        ),
        session.bind,
        params=params,
        parse_dates=["game_date", "start_time", "report_timestamp"],
    )
    injury_absences = _filter_injury_absences(injuries, prediction_buffer_minutes=prediction_buffer_minutes)

    official = pd.read_sql_query(
        text(
            f"""
            SELECT
                gpa.game_id,
                COALESCE(t.team_id, p.team_id) AS team_id,
                gpa.player_id,
                COALESCE(gpa.player_name, p.full_name) AS player_name,
                p.position,
                gpa.fetched_at AS report_timestamp,
                'inactive' AS status,
                g.game_date,
                g.start_time
            FROM game_player_availability gpa
            JOIN games g ON g.game_id = gpa.game_id
            LEFT JOIN players p ON p.player_id = gpa.player_id
            LEFT JOIN teams t ON t.abbreviation = gpa.team_abbreviation
            WHERE gpa.is_active = 0
              AND gpa.player_id IS NOT NULL
              AND g.status != 'superseded'
              {game_date_filter}
            """
        ),
        session.bind,
        params=params,
        parse_dates=["game_date", "start_time", "report_timestamp"],
    )
    official_absences = _normalize_absence_frame(
        official,
        source="official_inactive",
        rotation_shock_confidence=1.0,
    )

    combined = pd.concat([injury_absences, official_absences], ignore_index=True, sort=False)
    if combined.empty:
        return pd.DataFrame(
            columns=[
                "game_id",
                "team_id",
                "player_id",
                "player_name",
                "position",
                "report_timestamp",
                "status",
                "source",
                "rotation_shock_confidence",
            ]
        )

    combined["_source_priority"] = combined["source"].map({"injury_report": 1, "official_inactive": 2}).fillna(0)
    combined = combined.sort_values(
        ["game_id", "team_id", "player_id", "_source_priority", "report_timestamp"],
        kind="mergesort",
    )
    combined = combined.drop_duplicates(subset=["game_id", "team_id", "player_id"], keep="last")
    return combined.drop(columns=["_source_priority"], errors="ignore").reset_index(drop=True)


def _filter_injury_absences(injuries: pd.DataFrame, *, prediction_buffer_minutes: int) -> pd.DataFrame:
    if injuries.empty:
        return _normalize_absence_frame(injuries, source="injury_report", rotation_shock_confidence=1.0)

    frame = injuries.copy()
    frame["report_timestamp"] = _normalize_timestamp_column(frame, "report_timestamp")
    frame["start_time"] = _normalize_timestamp_column(frame, "start_time")
    cutoff = frame["start_time"] - pd.to_timedelta(prediction_buffer_minutes, unit="m")
    frame = frame[frame["report_timestamp"].notna() & (frame["report_timestamp"] <= cutoff)].copy()
    if frame.empty:
        return _normalize_absence_frame(frame, source="injury_report", rotation_shock_confidence=1.0)

    frame = frame.sort_values(["game_id", "team_id", "player_id", "report_timestamp"], kind="mergesort")
    frame = frame.drop_duplicates(subset=["game_id", "team_id", "player_id"], keep="last")
    expected_flags = frame["expected_availability_flag"] if "expected_availability_flag" in frame.columns else pd.Series(
        None,
        index=frame.index,
    )
    play_probability = [
        status_to_play_probability(
            status,
            expected_availability_flag=None if pd.isna(expected_flag) else bool(expected_flag),
        )
        for status, expected_flag in zip(frame["status"], expected_flags, strict=False)
    ]
    frame = frame[np.asarray(play_probability) <= EPSILON].copy()
    return _normalize_absence_frame(frame, source="injury_report", rotation_shock_confidence=1.0)


def _normalize_absence_frame(
    frame: pd.DataFrame,
    *,
    source: str,
    rotation_shock_confidence: float,
) -> pd.DataFrame:
    columns = [
        "game_id",
        "team_id",
        "player_id",
        "player_name",
        "position",
        "report_timestamp",
        "status",
        "source",
        "rotation_shock_confidence",
    ]
    if frame.empty:
        return pd.DataFrame(columns=columns)

    result = frame.copy()
    for column in ["game_id", "team_id", "player_id"]:
        result[column] = pd.to_numeric(result[column], errors="coerce")
    result = result.dropna(subset=["game_id", "team_id", "player_id"]).copy()
    result["game_id"] = result["game_id"].astype(int)
    result["team_id"] = result["team_id"].astype(int)
    result["player_id"] = result["player_id"].astype(int)
    result["player_name"] = result.get("player_name", pd.Series("", index=result.index)).fillna("").astype(str)
    result["position"] = result.get("position", pd.Series("", index=result.index)).fillna("").astype(str)
    result["report_timestamp"] = _normalize_timestamp_column(result, "report_timestamp")
    result["status"] = result.get("status", pd.Series("out", index=result.index)).fillna("out").astype(str)
    result["source"] = source
    result["rotation_shock_confidence"] = float(rotation_shock_confidence)
    return result[columns].reset_index(drop=True)


def _normalize_provided_absences(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    if "status" not in result.columns:
        result["status"] = "out"
    if "player_name" not in result.columns:
        result["player_name"] = ""
    if "position" not in result.columns:
        result["position"] = ""
    if "report_timestamp" not in result.columns:
        result["report_timestamp"] = pd.NaT
    for column in ["game_id", "team_id", "player_id"]:
        result[column] = pd.to_numeric(result[column], errors="coerce")
    result = result.dropna(subset=["game_id", "team_id", "player_id"]).copy()
    result["game_id"] = result["game_id"].astype(int)
    result["team_id"] = result["team_id"].astype(int)
    result["player_id"] = result["player_id"].astype(int)
    result["player_name"] = result["player_name"].fillna("").astype(str)
    result["position"] = result["position"].fillna("").astype(str)
    result["report_timestamp"] = _normalize_timestamp_column(result, "report_timestamp")
    result["status"] = result["status"].fillna("out").astype(str)
    result["source"] = result["source"].fillna("injury_report").astype(str)
    result["rotation_shock_confidence"] = pd.to_numeric(
        result["rotation_shock_confidence"],
        errors="coerce",
    ).fillna(1.0)
    return result[
        [
            "game_id",
            "team_id",
            "player_id",
            "player_name",
            "position",
            "report_timestamp",
            "status",
            "source",
            "rotation_shock_confidence",
        ]
    ].reset_index(drop=True)


def _attach_explicit_absence_baselines(prepared: pd.DataFrame, absences: pd.DataFrame) -> pd.DataFrame:
    if absences.empty or prepared.empty:
        return pd.DataFrame()

    game_lookup = prepared[["game_id", "team_id", "season", "game_date", "start_time", "sort_time"]].drop_duplicates()
    explicit = absences.merge(game_lookup, on=["game_id", "team_id"], how="inner")
    if explicit.empty:
        return explicit

    active_keys = set(zip(prepared["game_id"], prepared["team_id"], prepared["player_id"], strict=False))
    history = prepared[
        [
            "player_id",
            "player_name",
            "team_id",
            "season",
            "game_date",
            "sort_time",
            "position",
            "snapshot_minutes",
            "snapshot_usage_proxy",
            "snapshot_assists",
            "snapshot_rebounds",
            "snapshot_threes",
            "snapshot_field_goal_attempts",
            "snapshot_starter_flag",
            "snapshot_archetype",
        ]
    ].dropna(subset=["snapshot_minutes", "snapshot_usage_proxy"])
    histories = {
        key: group.sort_values("sort_time", kind="mergesort")
        for key, group in history.groupby(["player_id", "team_id", "season"], dropna=False)
    }

    rows: list[dict[str, object]] = []
    for row in explicit.itertuples(index=False):
        if (row.game_id, row.team_id, row.player_id) in active_keys:
            continue
        prior = histories.get((row.player_id, row.team_id, row.season))
        if prior is None:
            continue
        prior = prior[prior["sort_time"] < row.sort_time]
        if prior.empty:
            continue
        snapshot = prior.iloc[-1]
        if float(snapshot["snapshot_minutes"]) < BASELINE_MINUTES_THRESHOLD:
            continue
        rows.append(
            {
                "game_id": int(row.game_id),
                "team_id": int(row.team_id),
                "season": int(row.season),
                "player_id": int(row.player_id),
                "player_name": row.player_name or snapshot.get("player_name", ""),
                "position": row.position or snapshot.get("position", ""),
                "status": row.status,
                "source": row.source,
                "rotation_shock_confidence": float(row.rotation_shock_confidence),
                "baseline_minutes": float(snapshot["snapshot_minutes"]),
                "baseline_usage_proxy": float(snapshot["snapshot_usage_proxy"]),
                "absent_archetype": str(snapshot["snapshot_archetype"]),
            }
        )
    return pd.DataFrame(rows)


def _derive_post_hoc_absences(prepared: pd.DataFrame, explicit_absences: pd.DataFrame) -> pd.DataFrame:
    if prepared.empty:
        return pd.DataFrame()

    snapshot_columns = [
        "player_id",
        "player_name",
        "team_id",
        "season",
        "game_date",
        "sort_time",
        "position",
        "snapshot_minutes",
        "snapshot_usage_proxy",
        "snapshot_archetype",
    ]
    snapshots = prepared[snapshot_columns].dropna(subset=["snapshot_minutes", "snapshot_usage_proxy"]).copy()
    snapshots = snapshots[snapshots["snapshot_minutes"] >= BASELINE_MINUTES_THRESHOLD].copy()
    if snapshots.empty:
        return pd.DataFrame()

    team_games = prepared[["game_id", "team_id", "season", "game_date", "sort_time"]].drop_duplicates()
    active_by_game_team = prepared.groupby(["game_id", "team_id"])["player_id"].apply(lambda values: set(values)).to_dict()
    explicit_keys = set()
    if not explicit_absences.empty:
        explicit_keys = set(
            zip(
                explicit_absences["game_id"],
                explicit_absences["team_id"],
                explicit_absences["player_id"],
                strict=False,
            )
        )
    player_histories = {
        int(player_id): group.sort_values("sort_time", kind="mergesort")
        for player_id, group in prepared.groupby("player_id", dropna=False)
    }

    rows: list[dict[str, object]] = []
    for game in team_games.itertuples(index=False):
        prior = snapshots[
            (snapshots["team_id"] == game.team_id)
            & (snapshots["season"] == game.season)
            & (snapshots["sort_time"] < game.sort_time)
        ]
        if prior.empty:
            continue
        latest = prior.sort_values("sort_time", kind="mergesort").groupby("player_id", dropna=False).tail(1)
        days_since_last = (pd.Timestamp(game.game_date) - pd.to_datetime(latest["game_date"])).dt.days
        latest = latest[(days_since_last >= 0) & (days_since_last <= POST_HOC_MAX_DAYS_SINCE_LAST_GAME)]
        if latest.empty:
            continue
        active_ids = active_by_game_team.get((game.game_id, game.team_id), set())
        for snapshot in latest.itertuples(index=False):
            if snapshot.player_id in active_ids:
                continue
            if (game.game_id, game.team_id, snapshot.player_id) in explicit_keys:
                continue
            player_history = player_histories.get(int(snapshot.player_id), pd.DataFrame())
            if not player_history.empty:
                other_team_rows = player_history[
                    (player_history["sort_time"] > snapshot.sort_time)
                    & (player_history["sort_time"] < game.sort_time)
                    & (player_history["team_id"] != game.team_id)
                ]
                if not other_team_rows.empty:
                    continue
            rows.append(
                {
                    "game_id": int(game.game_id),
                    "team_id": int(game.team_id),
                    "season": int(game.season),
                    "player_id": int(snapshot.player_id),
                    "player_name": snapshot.player_name,
                    "position": snapshot.position,
                    "status": "post_hoc_absent",
                    "source": "post_hoc",
                    "rotation_shock_confidence": 0.5,
                    "baseline_minutes": float(snapshot.snapshot_minutes),
                    "baseline_usage_proxy": float(snapshot.snapshot_usage_proxy),
                    "absent_archetype": str(snapshot.snapshot_archetype),
                }
            )
    return pd.DataFrame(rows)


def _build_observations(
    historical: pd.DataFrame,
    absences: pd.DataFrame | None = None,
    *,
    include_post_hoc: bool = True,
) -> pd.DataFrame:
    if historical.empty:
        return _empty_observations()

    prepared = _prepare_historical_frame(historical)
    if prepared.empty:
        return _empty_observations()

    raw_absences = pd.DataFrame() if absences is None else absences
    if raw_absences.empty:
        explicit_absences = _normalize_absence_frame(
            raw_absences,
            source="injury_report",
            rotation_shock_confidence=1.0,
        )
    elif {"source", "rotation_shock_confidence"}.issubset(raw_absences.columns):
        explicit_absences = _normalize_provided_absences(raw_absences)
    else:
        explicit_absences = _normalize_absence_frame(
            raw_absences,
            source="injury_report",
            rotation_shock_confidence=1.0,
        )
    explicit_with_baselines = _attach_explicit_absence_baselines(prepared, explicit_absences)
    post_hoc_absences = (
        _derive_post_hoc_absences(prepared, explicit_with_baselines) if include_post_hoc else pd.DataFrame()
    )
    learned_absences = pd.concat([explicit_with_baselines, post_hoc_absences], ignore_index=True, sort=False)
    if learned_absences.empty:
        return _empty_observations()

    candidate_rows = prepared[
        prepared["baseline_minutes"].notna()
        & prepared["baseline_usage_proxy"].notna()
        & (~prepared["_return_exclusion"])
    ].copy()
    if candidate_rows.empty:
        return _empty_observations()

    rows: list[dict[str, object]] = []
    candidate_groups = {
        key: group
        for key, group in candidate_rows.groupby(["game_id", "team_id"], dropna=False)
    }
    for (game_id, team_id), absence_group in learned_absences.groupby(["game_id", "team_id"], dropna=False):
        candidates = candidate_groups.get((game_id, team_id), pd.DataFrame())
        if candidates.empty:
            continue
        absent_ids = set(absence_group["player_id"].astype(int))
        candidates = candidates[~candidates["player_id"].isin(absent_ids)].copy()
        if candidates.empty:
            continue

        removed_basis = pd.to_numeric(absence_group["baseline_usage_proxy"], errors="coerce").clip(lower=0.0)
        if float(removed_basis.sum()) <= EPSILON:
            removed_basis = pd.to_numeric(absence_group["baseline_minutes"], errors="coerce").clip(lower=0.0)
        total_removed_basis = float(removed_basis.sum())
        if total_removed_basis <= EPSILON:
            continue

        candidate_minute_delta = (candidates["minutes"] - candidates["baseline_minutes"]).clip(lower=0.0)
        candidate_usage_delta = (candidates["usage_proxy"] - candidates["baseline_usage_proxy"]).clip(lower=0.0)
        positive_candidates = candidates[(candidate_minute_delta > EPSILON) | (candidate_usage_delta > EPSILON)].copy()
        if positive_candidates.empty:
            continue

        for absence_index, absence in absence_group.reset_index(drop=True).iterrows():
            attribution_share = float(removed_basis.iloc[absence_index]) / total_removed_basis
            for candidate in positive_candidates.itertuples(index=False):
                minute_delta = max(float(candidate.minutes) - float(candidate.baseline_minutes), 0.0) * attribution_share
                usage_delta = max(float(candidate.usage_proxy) - float(candidate.baseline_usage_proxy), 0.0) * attribution_share
                if minute_delta <= EPSILON and usage_delta <= EPSILON:
                    continue
                rows.append(
                    {
                        "game_id": int(game_id),
                        "team_id": int(team_id),
                        "season": int(absence["season"]),
                        "absent_player_id": int(absence["player_id"]),
                        "candidate_player_id": int(candidate.player_id),
                        "absent_archetype": str(absence["absent_archetype"]),
                        "candidate_archetype": str(candidate.candidate_archetype),
                        "minute_delta": minute_delta,
                        "usage_delta": usage_delta,
                        "absence_source": str(absence["source"]),
                        "rotation_shock_confidence": float(absence["rotation_shock_confidence"]),
                    }
                )

    if not rows:
        return _empty_observations()
    return pd.DataFrame(rows, columns=OBSERVATION_COLUMNS)


def _write_sanity_report(report_dir: Path, weights: pd.DataFrame) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    report_path = report_dir / f"rotation_weights_sanity_{timestamp}.md"
    source_counts = weights["weight_source"].value_counts(dropna=False).to_dict() if not weights.empty else {}
    lines = [
        "# Rotation Weights Sanity Report",
        "",
        f"- Generated: {datetime.now(UTC).isoformat()}",
        f"- Total rows: {len(weights)}",
        f"- Team rows: {int(source_counts.get('team', 0))}",
        f"- League rows: {int(source_counts.get('league', 0))}",
        f"- Fallback diagnostic rows: {int(source_counts.get('fallback', 0))}",
        "",
        "## Top Cells By Sample Size",
        "",
    ]
    top = weights.sort_values("sample_size", ascending=False).head(20) if not weights.empty else pd.DataFrame()
    for row in top.to_dict("records"):
        lines.append(_format_weight_row(row))

    lines.extend(["", "## Extreme Minute Transfer Cells", ""])
    extreme = weights.sort_values("minute_delta_mean", ascending=False).head(20) if not weights.empty else pd.DataFrame()
    for row in extreme.to_dict("records"):
        lines.append(_format_weight_row(row))

    lines.extend(["", "## Suspicious Bench Depth Absences", ""])
    bench = weights[
        (weights["absent_archetype"] == "bench_depth")
        & ((weights["minute_gain_weight"] >= 0.50) | (weights["minute_delta_mean"] >= 6.0))
    ] if not weights.empty else pd.DataFrame()
    if bench.empty:
        lines.append("- None")
    else:
        for row in bench.sort_values("minute_delta_mean", ascending=False).head(20).to_dict("records"):
            lines.append(_format_weight_row(row))

    lines.extend(["", "## Usage Normalization Checks", ""])
    if weights.empty:
        lines.append("- No rows")
    else:
        grouped = weights.groupby(["team_id", "season", "absent_archetype", "weight_source"], dropna=False)
        usage_sums = grouped["usage_gain_weight"].sum().reset_index(name="usage_weight_sum")
        bad_usage = usage_sums[(usage_sums["usage_weight_sum"] - 1.0).abs() > 1e-6]
        if bad_usage.empty:
            lines.append("- All cells normalize within tolerance")
        else:
            for row in bad_usage.head(20).to_dict("records"):
                lines.append(
                    f"- {row['weight_source']} team={row['team_id']} season={row['season']} "
                    f"absent={row['absent_archetype']} usage_sum={float(row['usage_weight_sum']):.6f}"
                )

    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report_path


def _format_weight_row(row: dict[str, object]) -> str:
    return (
        f"- {row['weight_source']} team={row['team_id']} season={row['season']} "
        f"{row['absent_archetype']} -> {row['candidate_archetype']} n={row['sample_size']} "
        f"minute_mean={float(row['minute_delta_mean']):.2f} minute_w={float(row['minute_gain_weight']):.3f} "
        f"usage_mean={float(row['usage_delta_mean']):.2f} usage_w={float(row['usage_gain_weight']):.3f}"
    )


def main() -> None:
    args = _parse_args()
    as_of_date = None
    if args.as_of_date:
        as_of_date = datetime.strptime(args.as_of_date, "%Y-%m-%d").date()

    with session_scope() as session:
        historical = DatasetLoader(session).load_historical_player_games(as_of_date=as_of_date)
        absences = _load_absence_events(
            session,
            as_of_date=as_of_date,
            prediction_buffer_minutes=args.prediction_buffer_minutes,
        )

    observations = _build_observations(
        historical,
        absences,
        include_post_hoc=not args.disable_post_hoc,
    )
    weights = aggregate_rotation_weights(observations)
    summary = summarize_weight_learning(observations, weights)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    weights.to_parquet(args.output, index=False)
    report_path = _write_sanity_report(args.report_dir, weights)

    print(
        "Rotation weight learning complete: "
        f"observations={summary.observation_count} "
        f"team_cells={summary.team_cell_count} "
        f"league_cells={summary.league_cell_count} "
        f"fallback_cells={summary.fallback_cell_count}"
    )
    print(f"Wrote weights parquet: {args.output}")
    print(f"Wrote sanity report: {report_path}")


if __name__ == "__main__":
    main()

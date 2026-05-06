from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.providers.canonical_schema import (
    ADVANCED_CANONICAL_FIELDS,
    SCORING_CANONICAL_FIELDS,
    TRACKING_CANONICAL_FIELDS,
    capability_rows,
    log_missing_canonical_fields,
)
from app.services.board_date import matches_board_date

PLAYER_META_FIELDS = [
    *ADVANCED_CANONICAL_FIELDS,
    *TRACKING_CANONICAL_FIELDS,
    *SCORING_CANONICAL_FIELDS,
]
ODDS_CONTEXT_FIELDS = [
    "line_value",
    "raw_implied_over_probability",
    "raw_implied_under_probability",
    "no_vig_over_probability",
    "no_vig_under_probability",
    "consensus_line_mean",
    "consensus_line_median",
    "consensus_line_std",
    "consensus_prob_mean",
    "consensus_prob_std",
    "best_over_price",
    "best_under_price",
    "book_count",
    "market_count",
    "line_movement_1h",
    "line_movement_6h",
    "line_movement_24h",
]


def _deduplicate_columns(frame: pd.DataFrame, *, keep: str = "last") -> pd.DataFrame:
    if frame.columns.is_unique:
        return frame.copy()
    return frame.loc[:, ~frame.columns.duplicated(keep=keep)].copy()
AVAILABILITY_CONTEXT_FIELDS = [
    "team_injuries",
    "team_out_count",
    "team_doubtful_count",
    "team_questionable_count",
    "same_position_out_count",
    "same_position_doubtful_count",
    "projected_starter_count",
    "missing_starter_count",
    "projected_rotation_players",
    "projected_rotation_minutes",
    "projected_minutes_share",
    "projected_starter_flag",
    "lineup_report_count",
    "lineup_instability_score",
    "teammate_absence_pressure",
    # v1.2.3 A2: production-weighted teammate absence signals
    "missing_teammate_usage_sum",  # sum of out-teammates' career avg (FGA + 0.44*FTA)
    "star_absent_flag",            # 1 if any out teammate exceeds the star usage threshold
]
PLAYER_INJURY_CONTEXT_FIELDS = [
    "player_injury_return_flag",
    "player_days_since_last_game",
    "player_games_since_return",
    "days_since_extended_absence",
    "player_on_inactive_list",
]
TRADE_CONTEXT_FIELDS = [
    "_team_changed",
    "team_changed_recently",
]
DEFAULT_PREGAME_BUFFER_MINUTES = 90

# P0 CHANGE 2: Minimum qualifying minutes filter - games under 5 minutes represent
# injury exits, foul-outs, or coach's-decision DNPs where the player briefly appeared.
# These do not reflect true production and should be excluded from training.
MINIMUM_QUALIFYING_MINUTES = 5


@dataclass(frozen=True)
class DatasetBundle:
    historical: pd.DataFrame
    upcoming: pd.DataFrame


class DatasetLoader:
    def __init__(self, session: Session) -> None:
        self._engine: Engine = session.bind  # type: ignore[assignment]

    def load_historical_player_games(
        self,
        as_of_date: date | None = None,
        prediction_buffer_minutes: int = DEFAULT_PREGAME_BUFFER_MINUTES,
    ) -> pd.DataFrame:
        filters = ["g.status != 'superseded'", f"pgl.minutes >= {MINIMUM_QUALIFYING_MINUTES}"]
        params: dict[str, object] = {}
        if as_of_date is not None:
            filters.append("g.game_date <= :as_of_date")
            params["as_of_date"] = as_of_date
        filter_sql = f"WHERE {' AND '.join(filters)}"
        query = f"""
            SELECT
                pgl.player_id,
                p.full_name AS player_name,
                p.position,
                p.team_id,
                g.game_id,
                g.game_date,
                g.start_time,
                g.home_team_id,
                g.away_team_id,
                CASE WHEN pgl.team_id = g.home_team_id THEN 1 ELSE 0 END AS is_home,
                ht.abbreviation AS home_team_abbreviation,
                g.spread,
                g.total,
                pgl.team_id AS player_team_id,
                pgl.opponent_team_id,
                pgl.minutes,
                pgl.points,
                pgl.rebounds,
                pgl.assists,
                pgl.threes,
                pgl.turnovers,
                pgl.steals,
                pgl.blocks,
                pgl.field_goal_attempts,
                pgl.field_goals_made,
                pgl.free_throw_attempts,
                pgl.free_throws_made,
                pgl.offensive_rebounds,
                pgl.defensive_rebounds,
                pgl.plus_minus,
                pgl.fouls,
                pgl.starter_flag,
                pgl.meta AS player_meta
            FROM player_game_logs pgl
            JOIN players p ON p.player_id = pgl.player_id
            JOIN games g ON g.game_id = pgl.game_id
            JOIN teams ht ON ht.team_id = g.home_team_id
            {filter_sql}
            ORDER BY g.game_date, g.start_time, pgl.player_id
        """
        frame = pd.read_sql_query(
            text(query),
            self._engine,
            params=params,
            parse_dates=["game_date", "start_time"],
        )
        if frame.empty:
            return frame
        frame = _coerce_numeric_columns(
            frame,
            [
                "spread",
                "total",
                "minutes",
                "points",
                "rebounds",
                "assists",
                "threes",
                "turnovers",
                "steals",
                "blocks",
                "field_goal_attempts",
                "field_goals_made",
                "free_throw_attempts",
                "free_throws_made",
                "offensive_rebounds",
                "defensive_rebounds",
                "plus_minus",
                "fouls",
            ],
        )
        frame = self._expand_player_meta(frame, "player_meta")
        frame["availability_cutoff"] = frame["start_time"] - pd.to_timedelta(prediction_buffer_minutes, unit="m")
        frame = self._attach_availability_context(frame, team_column="player_team_id", cutoff_column="availability_cutoff")
        frame = self._attach_historical_odds_context(frame, as_of_date, prediction_buffer_minutes)
        frame = self._attach_player_injury_context(
            frame,
            cutoff_column="availability_cutoff",
            history_source=frame,
            is_upcoming=False,
        )
        frame = self._attach_trade_context(
            frame,
            current_team_column="player_team_id",
            history_source=frame,
            is_upcoming=False,
        )
        frame["pra"] = frame["points"] + frame["rebounds"] + frame["assists"]
        return frame.drop(columns=["availability_cutoff"], errors="ignore")

    @property
    def use_parquet(self) -> bool:
        return os.getenv("NBA_USE_PARQUET", "0") == "1"

    def load_historical_player_games_from_parquet(
        self,
        parquet_root: Path,
        as_of_date: date | None = None,
    ) -> pd.DataFrame:
        """Load historical player game logs from parquet partitions.

        Produces the same column schema as load_historical_player_games.
        Context columns (availability, injury, odds) are filled with 0.0 / NaN
        defaults since the parquet source contains box scores only.
        """
        parquet_files = sorted(parquet_root.rglob("*.parquet"))
        if not parquet_files:
            return pd.DataFrame()
        frame = pd.concat(
            [pd.read_parquet(f, engine="pyarrow") for f in parquet_files],
            ignore_index=True,
        )

        if frame.empty:
            return frame

        frame["game_date"] = pd.to_datetime(frame["game_date"], errors="coerce")
        frame["start_time"] = frame["game_date"]

        frame = _coerce_numeric_columns(frame, ["minutes"])
        frame = frame[frame["minutes"] >= MINIMUM_QUALIFYING_MINUTES].copy()

        if as_of_date is not None:
            frame = frame[frame["game_date"].dt.date <= as_of_date].copy()

        if frame.empty:
            return frame

        nan_cols = [
            "position",
            "team_id",
            "player_team_id",
            "opponent_team_id",
            "home_team_id",
            "away_team_id",
            "is_home",
            "home_team_abbreviation",
            "spread",
            "total",
            *PLAYER_META_FIELDS,
        ]
        zero_cols = [*AVAILABILITY_CONTEXT_FIELDS, *PLAYER_INJURY_CONTEXT_FIELDS, *TRADE_CONTEXT_FIELDS]
        defaults = pd.DataFrame(
            {
                "player_id": pd.to_numeric(frame["provider_player_id"], errors="coerce"),
                "game_id": pd.to_numeric(frame["provider_game_id"], errors="coerce"),
                **{col: np.nan for col in nan_cols},
                **{col: 0.0 for col in zero_cols},
            },
            index=frame.index,
        )
        frame = pd.concat([frame, defaults], axis=1)

        stat_cols = [
            "points",
            "rebounds",
            "assists",
            "threes",
            "turnovers",
            "steals",
            "blocks",
            "field_goal_attempts",
            "field_goals_made",
            "free_throw_attempts",
            "free_throws_made",
            "offensive_rebounds",
            "defensive_rebounds",
            "plus_minus",
            "fouls",
        ]
        frame = _coerce_numeric_columns(frame, stat_cols)
        frame["starter_flag"] = pd.to_numeric(frame["starter_flag"], errors="coerce")
        frame["pra"] = frame["points"] + frame["rebounds"] + frame["assists"]

        frame = frame.drop(
            columns=["provider_game_id", "provider_player_id", "overtime_flag", "season_type"],
            errors="ignore",
        )

        return frame.sort_values(["game_date", "player_id"]).reset_index(drop=True)

    def load_upcoming_player_lines(self, target_date: date | None = None) -> pd.DataFrame:
        filter_sql = "WHERE g.status = 'scheduled'"
        params: dict[str, object] = {}
        query = f"""
            SELECT
                g.game_id,
                g.game_date,
                g.start_time,
                p.player_id,
                p.full_name AS player_name,
                p.team_id,
                p.position,
                CASE WHEN p.team_id = g.home_team_id THEN 1 ELSE 0 END AS is_home,
                g.home_team_id,
                g.away_team_id,
                CASE WHEN p.team_id = g.home_team_id THEN g.away_team_id ELSE g.home_team_id END AS opponent_team_id,
                ht.abbreviation AS home_team_abbreviation,
                g.spread,
                g.total,
                pm.key AS market_key,
                ls.line_value,
                ls.over_odds,
                ls.under_odds,
                ls.snapshot_id,
                ls.timestamp,
                ls.sportsbook_id,
                ls.meta
            FROM line_snapshots ls
            JOIN games g ON g.game_id = ls.game_id
            JOIN players p ON p.player_id = ls.player_id
            JOIN prop_markets pm ON pm.market_id = ls.market_id
            JOIN teams ht ON ht.team_id = g.home_team_id
            {filter_sql}
            ORDER BY ls.timestamp DESC, ls.snapshot_id DESC
        """
        frame = pd.read_sql_query(
            text(query),
            self._engine,
            params=params,
            parse_dates=["game_date", "start_time", "timestamp"],
        )
        if frame.empty:
            return frame
        if target_date is not None:
            frame = _filter_frame_to_board_date(frame, target_date)
        if frame.empty:
            return frame.drop(columns=["meta"], errors="ignore")
        frame = frame[frame["meta"].apply(_is_fully_verified_meta)].copy()
        if frame.empty:
            return frame.drop(columns=["meta"], errors="ignore")
        frame = _coerce_numeric_columns(frame, ["spread", "total", "line_value", "over_odds", "under_odds"])
        frame["availability_cutoff"] = frame["timestamp"]
        frame = self._attach_availability_context(frame, team_column="team_id", cutoff_column="availability_cutoff")
        latest_per_book = _attach_quote_probabilities(_latest_snapshot_per_book(frame))
        aggregated = _aggregate_odds_frame(frame)
        consensus_columns = [
            "game_id",
            "player_id",
            "market_key",
            "consensus_line_mean",
            "consensus_line_median",
            "consensus_line_std",
            "consensus_prob_mean",
            "consensus_prob_std",
            "best_over_price",
            "best_under_price",
            "book_count",
            "market_count",
            "line_movement_1h",
            "line_movement_6h",
            "line_movement_24h",
        ]
        upcoming = latest_per_book.merge(
            aggregated[consensus_columns],
            on=["game_id", "player_id", "market_key"],
            how="left",
        )
        upcoming = self._attach_player_injury_context(
            upcoming,
            cutoff_column="availability_cutoff",
            history_source=None,
            is_upcoming=True,
        )
        upcoming = self._attach_trade_context(
            upcoming,
            current_team_column="team_id",
            history_source=None,
            is_upcoming=True,
        )
        static_columns = [
            "game_id",
            "game_date",
            "start_time",
            "player_id",
            "player_name",
            "team_id",
            "position",
            "is_home",
            "home_team_id",
            "away_team_id",
            "opponent_team_id",
            "home_team_abbreviation",
            "spread",
            "total",
            "market_key",
        ]
        ordered_columns = [
            *static_columns,
            "sportsbook_id",
            "snapshot_id",
            "timestamp",
            "line_value",
            "over_odds",
            "under_odds",
            *PLAYER_INJURY_CONTEXT_FIELDS,
            *TRADE_CONTEXT_FIELDS,
            *AVAILABILITY_CONTEXT_FIELDS,
            *[column for column in ODDS_CONTEXT_FIELDS if column != "line_value"],
        ]
        return upcoming[ordered_columns].sort_values(
            ["game_date", "start_time", "player_name", "market_key"]
        ).reset_index(drop=True)

    def load_historical_bet_quotes(
        self,
        as_of_date: date | None = None,
        prediction_buffer_minutes: int = DEFAULT_PREGAME_BUFFER_MINUTES,
    ) -> pd.DataFrame:
        odds = self._load_historical_odds_snapshots(as_of_date)
        if odds.empty:
            return odds
        # game.start_time is stored as midnight (date only) for historical games.
        # Override with the accurate event_start_time from snapshot meta when available.
        event_starts = _extract_meta_event_start_times(odds["meta"])
        midnight_mask = (
            (odds["start_time"].dt.hour == 0)
            & (odds["start_time"].dt.minute == 0)
            & event_starts.notna()
        )
        if midnight_mask.any():
            odds.loc[midnight_mask, "start_time"] = event_starts[midnight_mask]
        odds["quote_cutoff"] = odds["start_time"] - pd.to_timedelta(prediction_buffer_minutes, unit="m")
        selected = _select_latest_available_snapshots(
            odds,
            cutoff_column="quote_cutoff",
            fallback_column="start_time",
            group_columns=["game_id", "player_id", "market_key", "sportsbook_id"],
        )
        if selected.empty:
            return selected
        closing = _select_latest_available_snapshots(
            odds,
            cutoff_column="start_time",
            fallback_column="start_time",
            group_columns=["game_id", "player_id", "market_key", "sportsbook_id"],
        )
        selected = selected.merge(
            closing[
                [
                    "game_id",
                    "player_id",
                    "market_key",
                    "sportsbook_id",
                    "line_value",
                    "over_odds",
                    "under_odds",
                    "no_vig_over_probability",
                    "no_vig_under_probability",
                    "timestamp",
                ]
            ].rename(
                columns={
                    "line_value": "closing_line_value",
                    "over_odds": "closing_over_odds",
                    "under_odds": "closing_under_odds",
                    "no_vig_over_probability": "closing_no_vig_over_probability",
                    "no_vig_under_probability": "closing_no_vig_under_probability",
                    "timestamp": "closing_timestamp",
                }
            ),
            on=["game_id", "player_id", "market_key", "sportsbook_id"],
            how="left",
        )
        selected["sportsbook_key"] = selected["sportsbook_key"].fillna("unknown")
        selected["sportsbook_name"] = selected["sportsbook_name"].fillna(selected["sportsbook_key"])
        meta_payload = selected["meta"].apply(_coerce_json_dict)
        selected["odds_source_provider"] = meta_payload.apply(
            lambda payload: str(payload.get("odds_source_provider") or "unknown")
        )
        selected["odds_verification_status"] = meta_payload.apply(
            lambda payload: str(payload.get("odds_verification_status") or "unknown")
        )
        selected["is_live_quote"] = meta_payload.apply(lambda payload: int(bool(payload.get("is_live_quote", False))))
        return selected.reset_index(drop=True)

    def load_provider_capabilities(self) -> pd.DataFrame:
        return pd.DataFrame(capability_rows())

    def _expand_player_meta(self, frame: pd.DataFrame, meta_column: str) -> pd.DataFrame:
        parsed_meta = frame[meta_column].apply(_coerce_json_dict)
        available_fields = sorted(
            {
                key
                for payload in parsed_meta
                for key in payload
                if key in PLAYER_META_FIELDS
            }
        )
        log_missing_canonical_fields("historical_player_meta", PLAYER_META_FIELDS, available_fields)
        for field_name in PLAYER_META_FIELDS:
            values = parsed_meta.apply(lambda payload, key=field_name: payload.get(key))
            frame[field_name] = pd.to_numeric(values, errors="coerce")
            if field_name.startswith("percentage_") or field_name.endswith("_percentage"):
                frame[field_name] = frame[field_name].apply(_normalize_percentage_scalar)
        player_name = parsed_meta.apply(lambda payload: payload.get("player_name"))
        position = parsed_meta.apply(lambda payload: payload.get("position"))
        frame["player_name"] = frame["player_name"].fillna(player_name)
        frame["position"] = frame["position"].fillna(position)
        return frame.drop(columns=[meta_column])

    def _attach_historical_odds_context(
        self,
        frame: pd.DataFrame,
        as_of_date: date | None,
        prediction_buffer_minutes: int,
    ) -> pd.DataFrame:
        odds = self._load_historical_odds_snapshots(as_of_date)
        if odds.empty:
            for market_key in ("points", "rebounds", "assists", "threes", "turnovers", "pra"):
                frame[f"line_{market_key}"] = frame.get(f"line_{market_key}", 0.0)
            return frame
        odds["quote_cutoff"] = odds["start_time"] - pd.to_timedelta(prediction_buffer_minutes, unit="m")
        aggregated = _aggregate_point_in_time_odds_frame(odds)
        if aggregated.empty:
            return frame
        wide_parts: list[pd.DataFrame] = []
        for value_column in [
            "line_value",
            *[column for column in ODDS_CONTEXT_FIELDS if column != "line_value"],
        ]:
            pivot = (
                aggregated.pivot_table(
                    index=["game_id", "player_id"],
                    columns="market_key",
                    values=value_column,
                    aggfunc="first",
                )
                .rename(columns=lambda market_key, value_column=value_column: _historical_odds_column_name(market_key, value_column))
                .reset_index()
            )
            pivot.columns.name = None
            wide_parts.append(pivot)
        wide = wide_parts[0]
        for part in wide_parts[1:]:
            wide = wide.merge(part, on=["game_id", "player_id"], how="outer")
        frame = frame.merge(wide, on=["game_id", "player_id"], how="left")
        return frame

    def _load_historical_odds_snapshots(self, as_of_date: date | None) -> pd.DataFrame:
        odds_query = """
            SELECT
                ls.game_id,
                g.game_date,
                g.start_time,
                ls.player_id,
                pm.key AS market_key,
                ls.line_value,
                ls.over_odds,
                ls.under_odds,
                ls.snapshot_id,
                ls.timestamp,
                ls.sportsbook_id,
                sb.key AS sportsbook_key,
                sb.display_name AS sportsbook_name,
                ls.meta
            FROM line_snapshots ls
            JOIN games g ON g.game_id = ls.game_id
            JOIN prop_markets pm ON pm.market_id = ls.market_id
            JOIN sportsbooks sb ON sb.sportsbook_id = ls.sportsbook_id
            WHERE g.status != 'superseded'
        """
        params: dict[str, object] = {}
        if as_of_date is not None:
            odds_query += " AND g.game_date <= :as_of_date"
            params["as_of_date"] = as_of_date
        odds = pd.read_sql_query(
            text(odds_query),
            self._engine,
            params=params,
            parse_dates=["game_date", "start_time", "timestamp"],
        )
        if odds.empty:
            return odds
        odds = odds[odds["meta"].apply(_is_training_usable_meta)].copy()
        if odds.empty:
            return odds
        odds = _coerce_numeric_columns(odds, ["line_value", "over_odds", "under_odds"])
        return _attach_quote_probabilities(odds)

    def _attach_availability_context(
        self,
        frame: pd.DataFrame,
        *,
        team_column: str,
        cutoff_column: str,
    ) -> pd.DataFrame:
        result = frame.copy()
        for column in AVAILABILITY_CONTEXT_FIELDS:
            if column not in result.columns:
                result[column] = 0.0
        if result.empty:
            return result

        injuries = pd.read_sql_query(
            text(
                """
                SELECT
                    ir.game_id,
                    ir.team_id,
                    ir.player_id,
                    ir.report_timestamp,
                    ir.status,
                    ir.expected_availability_flag,
                    p.position
                FROM injury_reports ir
                JOIN players p ON p.player_id = ir.player_id
                WHERE ir.game_id IS NOT NULL
                """
            ),
            self._engine,
            parse_dates=["report_timestamp"],
        )
        lineups = pd.read_sql_query(
            text(
                """
                SELECT
                    pl.game_id,
                    pl.team_id,
                    pl.player_id,
                    pl.lineup_status,
                    pl.expected_minutes,
                    pl.projected_starter,
                    pl.report_timestamp,
                    p.position
                FROM projected_lineups pl
                JOIN players p ON p.player_id = pl.player_id
                """
            ),
            self._engine,
            parse_dates=["report_timestamp"],
        )
        # Official NBA inactive list — supersedes injury-report uncertainty.
        # Keyed by (game_id, team_id) so it can be passed per-row like injury_groups.
        try:
            official_inactives_raw = pd.read_sql_query(
                text(
                    """
                    SELECT
                        gpa.game_id,
                        gpa.player_id,
                        p.team_id,
                        p.position
                    FROM game_player_availability gpa
                    JOIN players p ON p.player_id = gpa.player_id
                    WHERE gpa.is_active = 0
                    """
                ),
                self._engine,
            )
        except Exception:
            official_inactives_raw = pd.DataFrame()

        injury_groups = {
            key: value.sort_values("report_timestamp")
            for key, value in injuries.groupby(["game_id", "team_id"], dropna=False)
        } if not injuries.empty else {}
        lineup_groups = {
            key: value.sort_values("report_timestamp")
            for key, value in lineups.groupby(["game_id", "team_id"], dropna=False)
        } if not lineups.empty else {}
        official_inactive_groups: dict[tuple, pd.DataFrame] = {
            key: group
            for key, group in official_inactives_raw.groupby(["game_id", "team_id"], dropna=False)
        } if not official_inactives_raw.empty else {}

        # v1.2.3 A2: Build per-player usage proxy lookup for production-weighted
        # teammate-absence signals.  Usage proxy = FGA + 0.44*FTA (same formula
        # as features.py usage_proxy).  We use the career/season average across
        # all rows in this frame — not a per-game value — so there is no leakage
        # risk: we're measuring "how impactful is this player in general", not
        # "how did they perform in this specific game."
        # The lookup is empty when the frame lacks game-log columns (upcoming
        # inference path); _availability_context_row defaults to 0.0 in that case.
        _fga = pd.to_numeric(result.get("field_goal_attempts", pd.Series(dtype=float)), errors="coerce").fillna(0.0)
        _fta = pd.to_numeric(result.get("free_throw_attempts", pd.Series(dtype=float)), errors="coerce").fillna(0.0)
        if _fga.sum() > 0 or _fta.sum() > 0:
            _raw_usage = _fga + 0.44 * _fta
            player_usage_lookup: dict[int, float] = (
                _raw_usage.groupby(result["player_id"]).mean().to_dict()
            )
        else:
            player_usage_lookup = {}

        availability_rows: list[dict[str, float]] = []
        for row in result[
            ["game_id", "player_id", team_column, "position", cutoff_column]
        ].itertuples(index=False, name="AvailabilityRow"):
            team_id = getattr(row, team_column)
            cutoff = pd.Timestamp(getattr(row, cutoff_column))
            position_group = str(row.position or "").upper()[:1] or "UNK"
            injury_group = injury_groups.get((row.game_id, team_id), pd.DataFrame())
            lineup_group = lineup_groups.get((row.game_id, team_id), pd.DataFrame())
            official_inactive_group = official_inactive_groups.get((row.game_id, team_id), pd.DataFrame())
            availability_rows.append(
                _availability_context_row(
                    injury_group=injury_group,
                    lineup_group=lineup_group,
                    official_inactive_group=official_inactive_group,
                    cutoff=cutoff,
                    player_id=int(row.player_id),
                    position_group=position_group,
                    player_usage_lookup=player_usage_lookup,
                )
            )
        availability_frame = pd.DataFrame(availability_rows, index=result.index)
        for column in AVAILABILITY_CONTEXT_FIELDS:
            if column not in availability_frame.columns:
                availability_frame[column] = 0.0
        result = _deduplicate_columns(result, keep="last").drop(columns=AVAILABILITY_CONTEXT_FIELDS, errors="ignore")
        availability_frame = _deduplicate_columns(availability_frame[AVAILABILITY_CONTEXT_FIELDS], keep="last")
        return pd.concat([result, availability_frame], axis=1).copy()

    def _attach_player_injury_context(
        self,
        frame: pd.DataFrame,
        *,
        cutoff_column: str,
        history_source: pd.DataFrame | None,
        is_upcoming: bool,
    ) -> pd.DataFrame:
        result = frame.copy()
        for column in PLAYER_INJURY_CONTEXT_FIELDS:
            if column not in result.columns:
                result[column] = 0.0
        if result.empty or "player_id" not in result.columns:
            return result

        history = self._load_player_history_context(history_source)
        if not history.empty:
            history = history[history["player_id"].isin(result["player_id"].astype(int))].copy()

        if history.empty:
            result["player_days_since_last_game"] = 4.0
            result["days_since_extended_absence"] = 0.0
            result["player_games_since_return"] = 0.0
        elif is_upcoming:
            latest_history = history.groupby("player_id").tail(1)[
                ["player_id", "game_date", "player_games_since_return"]
            ].rename(columns={"game_date": "_last_game_date"})
            result = result.drop(columns=["player_games_since_return"], errors="ignore")
            result = result.merge(latest_history, on="player_id", how="left")
            fallback_days = 4.0
            result["player_days_since_last_game"] = (
                (pd.to_datetime(result["game_date"]) - pd.to_datetime(result["_last_game_date"])).dt.days
                .fillna(fallback_days)
                .clip(lower=0)
            )
            result["days_since_extended_absence"] = (result["player_days_since_last_game"] - 10.0).clip(lower=0.0)
            latest_games_since_return = pd.to_numeric(
                result["player_games_since_return"],
                errors="coerce",
            ).fillna(0.0)
            result["player_games_since_return"] = np.where(
                result["player_days_since_last_game"] > 10.0,
                0.0,
                latest_games_since_return,
            )
            result = result.drop(columns=["_last_game_date"], errors="ignore")
        else:
            join_columns = [
                "player_id",
                "game_id",
                "player_days_since_last_game",
                "player_games_since_return",
                "days_since_extended_absence",
            ]
            result = result.drop(columns=PLAYER_INJURY_CONTEXT_FIELDS, errors="ignore").merge(
                history[join_columns],
                on=["player_id", "game_id"],
                how="left",
            )
            result["player_days_since_last_game"] = pd.to_numeric(
                result["player_days_since_last_game"],
                errors="coerce",
            ).fillna(4.0)
            result["days_since_extended_absence"] = pd.to_numeric(
                result["days_since_extended_absence"],
                errors="coerce",
            ).fillna(0.0)
            result["player_games_since_return"] = pd.to_numeric(
                result["player_games_since_return"],
                errors="coerce",
            ).fillna(0.0)
            result["player_on_inactive_list"] = 0.0

        injury_reports = pd.read_sql_query(
            text(
                """
                SELECT
                    player_id,
                    report_timestamp,
                    status,
                    designation
                FROM injury_reports
                """
            ),
            self._engine,
            parse_dates=["report_timestamp"],
        )
        injury_reports = injury_reports[injury_reports["player_id"].isin(result["player_id"].astype(int))].copy()
        injury_groups = {
            int(player_id): group.sort_values("report_timestamp")
            for player_id, group in injury_reports.groupby("player_id", dropna=False)
        } if not injury_reports.empty else {}

        injury_return_flags: list[float] = []
        for row in result[["player_id", cutoff_column]].itertuples(index=False, name="InjuryRow"):
            cutoff = pd.Timestamp(getattr(row, cutoff_column))
            player_reports = injury_groups.get(int(row.player_id), pd.DataFrame())
            if player_reports.empty:
                injury_return_flags.append(0.0)
                continue
            eligible = player_reports[player_reports["report_timestamp"] <= cutoff]
            if eligible.empty:
                injury_return_flags.append(0.0)
                continue
            latest = eligible.iloc[-1]
            within_window = (cutoff - pd.Timestamp(latest["report_timestamp"])) <= pd.Timedelta(days=14)
            status_bucket = _injury_status_bucket(latest.get("designation") or latest.get("status"))
            injury_return_flags.append(float(within_window and status_bucket in {"out", "doubtful"}))
        result["player_injury_return_flag"] = injury_return_flags

        if is_upcoming:
            try:
                inactive_rows = pd.read_sql_query(
                    text(
                        """
                        SELECT
                            game_id,
                            player_id
                        FROM game_player_availability
                        WHERE is_active = 0
                        """
                    ),
                    self._engine,
                )
            except Exception:
                inactive_rows = pd.DataFrame()
            inactive_lookup = {
                (int(game_id), int(player_id))
                for game_id, player_id in inactive_rows[["game_id", "player_id"]].itertuples(index=False, name=None)
            } if not inactive_rows.empty else set()
            result["player_on_inactive_list"] = [
                float((int(game_id), int(player_id)) in inactive_lookup)
                for game_id, player_id in result[["game_id", "player_id"]].itertuples(index=False, name=None)
            ]
        else:
            result["player_on_inactive_list"] = 0.0
        return result

    def _attach_trade_context(
        self,
        frame: pd.DataFrame,
        *,
        current_team_column: str,
        history_source: pd.DataFrame | None,
        is_upcoming: bool,
    ) -> pd.DataFrame:
        result = frame.copy()
        for column in TRADE_CONTEXT_FIELDS:
            if column not in result.columns:
                result[column] = 0.0
        if result.empty or "player_id" not in result.columns:
            return result

        history = self._load_player_history_context(history_source)
        if history.empty:
            result["_team_changed"] = 0.0
            result["team_changed_recently"] = 0.0
            return result
        history = history[history["player_id"].isin(result["player_id"].astype(int))].copy()
        history["_previous_team_id"] = history.groupby("player_id")["player_team_id"].shift(1)
        history["_team_change_event"] = (
            history["_previous_team_id"].notna()
            & (history["player_team_id"] != history["_previous_team_id"])
        )
        history["_team_change_segment"] = history.groupby("player_id")["_team_change_event"].cumsum()
        history["_games_since_team_change"] = history.groupby(
            ["player_id", "_team_change_segment"]
        ).cumcount()
        history["team_changed_recently"] = (
            (history["_team_change_segment"] > 0) & (history["_games_since_team_change"] < 5)
        ).astype(float)

        if is_upcoming:
            latest_history = history.groupby("player_id").tail(1)[
                ["player_id", "player_team_id", "team_changed_recently"]
            ].rename(columns={"player_team_id": "_last_team_id"})
            result = result.drop(columns=TRADE_CONTEXT_FIELDS, errors="ignore")
            result = result.merge(latest_history, on="player_id", how="left")
            result["_team_changed"] = (
                pd.to_numeric(result[current_team_column], errors="coerce")
                != pd.to_numeric(result["_last_team_id"], errors="coerce")
            ).astype(float)
            result["_team_changed"] = result["_team_changed"].where(
                pd.to_numeric(result["_last_team_id"], errors="coerce").notna(),
                0.0,
            )
            result["team_changed_recently"] = np.maximum(
                pd.to_numeric(result["team_changed_recently"], errors="coerce").fillna(0.0),
                pd.to_numeric(result["_team_changed"], errors="coerce").fillna(0.0),
            )
            result = result.drop(columns=["_last_team_id"], errors="ignore")
        else:
            join_columns = ["player_id", "game_id", "team_changed_recently", "_team_change_event"]
            result = result.drop(columns=TRADE_CONTEXT_FIELDS, errors="ignore").merge(
                history[join_columns],
                on=["player_id", "game_id"],
                how="left",
            )
            result["team_changed_recently"] = pd.to_numeric(
                result["team_changed_recently"],
                errors="coerce",
            ).fillna(0.0)
            result["_team_changed"] = pd.to_numeric(
                result["_team_change_event"],
                errors="coerce",
            ).fillna(0.0)
            result = result.drop(columns=["_team_change_event"], errors="ignore")
        return result

    def _load_player_history_context(self, history_source: pd.DataFrame | None) -> pd.DataFrame:
        if history_source is not None and not history_source.empty:
            history = history_source.copy()
            if "player_team_id" not in history.columns and "team_id" in history.columns:
                history["player_team_id"] = history["team_id"]
            required = {"player_id", "game_id", "game_date", "start_time", "player_team_id"}
            if required.issubset(history.columns):
                history = history[list(required)].copy()
            else:
                history = pd.DataFrame()
        else:
            history = pd.read_sql_query(
                text(
                    f"""
                    SELECT
                        pgl.player_id,
                        pgl.game_id,
                        g.game_date,
                        g.start_time,
                        pgl.team_id AS player_team_id
                    FROM player_game_logs pgl
                    JOIN games g ON g.game_id = pgl.game_id
                    WHERE g.status != 'superseded'
                      AND pgl.minutes >= {MINIMUM_QUALIFYING_MINUTES}
                    """
                ),
                self._engine,
                parse_dates=["game_date", "start_time"],
            )
        if history.empty:
            return history
        history = history.sort_values(["player_id", "game_date", "start_time", "game_id"]).copy()
        previous_game_date = history.groupby("player_id")["game_date"].shift(1)
        history["player_days_since_last_game"] = (
            (history["game_date"] - previous_game_date).dt.days.fillna(4.0).clip(lower=0)
        )
        return_segments = (history["player_days_since_last_game"] > 10.0).groupby(history["player_id"]).cumsum()
        history["player_games_since_return"] = history.groupby(
            [history["player_id"], return_segments]
        ).cumcount() + 1
        history["days_since_extended_absence"] = (history["player_days_since_last_game"] - 10.0).clip(lower=0.0)
        return history


def _aggregate_odds_frame(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for group_values, group in frame.groupby(["game_id", "player_id", "market_key"], dropna=False):
        game_id, player_id, market_key = group_values
        latest_per_book = _representative_snapshot_per_book(group)
        current_timestamp = latest_per_book["timestamp"].max()
        base_row = latest_per_book.sort_values(["timestamp", "snapshot_id"], ascending=[False, False]).iloc[0]
        row = {
            "game_id": game_id,
            "player_id": player_id,
            "market_key": market_key,
            "snapshot_id": int(base_row["snapshot_id"]),
            "timestamp": current_timestamp,
            "line_value": float(latest_per_book["line_value"].median()),
            "over_odds": float(latest_per_book["over_odds"].median()) if latest_per_book["over_odds"].notna().any() else np.nan,
            "under_odds": float(latest_per_book["under_odds"].median()) if latest_per_book["under_odds"].notna().any() else np.nan,
            "raw_implied_over_probability": float(latest_per_book["raw_implied_over_probability"].median())
            if latest_per_book["raw_implied_over_probability"].notna().any()
            else np.nan,
            "raw_implied_under_probability": float(latest_per_book["raw_implied_under_probability"].median())
            if latest_per_book["raw_implied_under_probability"].notna().any()
            else np.nan,
            "no_vig_over_probability": float(latest_per_book["no_vig_over_probability"].mean())
            if latest_per_book["no_vig_over_probability"].notna().any()
            else np.nan,
            "no_vig_under_probability": float(latest_per_book["no_vig_under_probability"].mean())
            if latest_per_book["no_vig_under_probability"].notna().any()
            else np.nan,
            "consensus_line_mean": float(latest_per_book["line_value"].mean()),
            "consensus_line_median": float(latest_per_book["line_value"].median()),
            "consensus_line_std": float(latest_per_book["line_value"].std(ddof=0) or 0.0),
            "consensus_prob_mean": float(latest_per_book["no_vig_over_probability"].mean())
            if latest_per_book["no_vig_over_probability"].notna().any()
            else np.nan,
            "consensus_prob_std": float(latest_per_book["no_vig_over_probability"].std(ddof=0) or 0.0)
            if latest_per_book["no_vig_over_probability"].notna().any()
            else np.nan,
            "best_over_price": float(latest_per_book["over_odds"].max()) if latest_per_book["over_odds"].notna().any() else np.nan,
            "best_under_price": float(latest_per_book["under_odds"].max()) if latest_per_book["under_odds"].notna().any() else np.nan,
            "book_count": int(latest_per_book["sportsbook_id"].nunique()),
            "market_count": int(len(latest_per_book)),
            "line_movement_1h": _line_movement(group, current_timestamp, timedelta(hours=1)),
            "line_movement_6h": _line_movement(group, current_timestamp, timedelta(hours=6)),
            "line_movement_24h": _line_movement(group, current_timestamp, timedelta(hours=24)),
        }
        static_columns = [
            "game_date",
            "start_time",
            "player_name",
            "team_id",
            "position",
            "is_home",
            "home_team_id",
            "away_team_id",
            "opponent_team_id",
            "spread",
            "total",
        ]
        for column in static_columns:
            if column in group.columns:
                row[column] = base_row[column]
        rows.append(row)
    aggregated = pd.DataFrame(rows)
    for column in [*ODDS_CONTEXT_FIELDS, "over_odds", "under_odds", "line_value"]:
        if column in aggregated.columns:
            aggregated[column] = pd.to_numeric(aggregated[column], errors="coerce")
    return aggregated


def _aggregate_point_in_time_odds_frame(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, group in frame.groupby(["game_id", "player_id", "market_key"], dropna=False):
        eligible = _eligible_snapshot_group(group, cutoff_column="quote_cutoff", fallback_column="start_time")
        if eligible.empty:
            continue
        aggregated = _aggregate_odds_frame(eligible)
        if not aggregated.empty:
            rows.extend(aggregated.to_dict("records"))
    return pd.DataFrame(rows)


def _select_latest_available_snapshots(
    frame: pd.DataFrame,
    *,
    cutoff_column: str,
    fallback_column: str,
    group_columns: list[str],
) -> pd.DataFrame:
    rows: list[pd.Series] = []
    for _, group in frame.groupby(group_columns, dropna=False):
        eligible = _eligible_snapshot_group(group, cutoff_column=cutoff_column, fallback_column=fallback_column)
        if eligible.empty:
            continue
        rows.append(eligible.sort_values(["timestamp", "snapshot_id"], ascending=[False, False]).iloc[0])
    if not rows:
        return pd.DataFrame(columns=frame.columns)
    return pd.DataFrame(rows).reset_index(drop=True)


def _eligible_snapshot_group(group: pd.DataFrame, *, cutoff_column: str, fallback_column: str) -> pd.DataFrame:
    cutoff = pd.Timestamp(group[cutoff_column].iloc[0])
    fallback = pd.Timestamp(group[fallback_column].iloc[0])
    eligible = group[group["timestamp"] <= cutoff]
    if eligible.empty:
        eligible = group[group["timestamp"] <= fallback]
    return eligible.copy()


def _filter_frame_to_board_date(frame: pd.DataFrame, target_date: date) -> pd.DataFrame:
    mask = [
        matches_board_date(game_date, start_time, target_date)
        for game_date, start_time in zip(frame["game_date"], frame["start_time"], strict=False)
    ]
    return frame.loc[mask].copy()


_STAR_USAGE_THRESHOLD = 10.0  # FGA + 0.44*FTA ≥ 10 ≈ a secondary-star workload (~15 shot attempts/game)


def _availability_context_row(
    *,
    injury_group: pd.DataFrame,
    lineup_group: pd.DataFrame,
    official_inactive_group: pd.DataFrame,
    cutoff: pd.Timestamp,
    player_id: int,
    position_group: str,
    player_usage_lookup: dict[int, float] | None = None,
) -> dict[str, float]:
    """Compute per-row availability context.

    player_usage_lookup: optional {player_id → career-avg usage proxy} mapping used
    to compute production-weighted absence signals (missing_teammate_usage_sum,
    star_absent_flag).  Populated from game-log FGA/FTA for historical rows;
    empty dict / None for upcoming-game rows (inference fallback to count signals).
    """
    injuries = injury_group[injury_group["report_timestamp"] <= cutoff].copy() if not injury_group.empty else pd.DataFrame()
    if not injuries.empty:
        injuries = injuries[injuries["player_id"] != player_id].copy()
        injuries["status_bucket"] = injuries["status"].map(_injury_status_bucket)
        injuries["position_group"] = injuries["position"].fillna("").astype(str).str.upper().str[:1].replace("", "UNK")

    # Overlay official NBA inactive list — these are confirmed outs, so they
    # override injury-report uncertainty (questionable/doubtful → certain out).
    # Players on the list who weren't in any injury report are added as "out".
    if not official_inactive_group.empty:
        confirmed_out = official_inactive_group[official_inactive_group["player_id"] != player_id].copy()
        confirmed_out["status_bucket"] = "out"
        confirmed_out["position_group"] = (
            confirmed_out["position"].fillna("").astype(str).str.upper().str[:1].replace("", "UNK")
        )
        if injuries.empty:
            injuries = confirmed_out[["player_id", "position_group", "status_bucket"]].copy()
        else:
            # Upgrade any existing entry to "out"; append newly seen players
            injuries.loc[injuries["player_id"].isin(confirmed_out["player_id"]), "status_bucket"] = "out"
            new_ids = confirmed_out[~confirmed_out["player_id"].isin(injuries["player_id"])]
            if not new_ids.empty:
                injuries = pd.concat(
                    [injuries, new_ids[["player_id", "position_group", "status_bucket"]]],
                    ignore_index=True,
                )
    lineups = lineup_group[lineup_group["report_timestamp"] <= cutoff].copy() if not lineup_group.empty else pd.DataFrame()
    if not lineups.empty:
        lineups = (
            lineups.sort_values(["report_timestamp", "player_id"])
            .drop_duplicates(subset=["player_id"], keep="last")
            .copy()
        )
        lineups["position_group"] = lineups["position"].fillna("").astype(str).str.upper().str[:1].replace("", "UNK")

    out_count = int((injuries.get("status_bucket") == "out").sum()) if not injuries.empty else 0
    doubtful_count = int((injuries.get("status_bucket") == "doubtful").sum()) if not injuries.empty else 0
    questionable_count = int((injuries.get("status_bucket") == "questionable").sum()) if not injuries.empty else 0
    same_position_out = int(
        ((injuries.get("status_bucket") == "out") & (injuries.get("position_group") == position_group)).sum()
    ) if not injuries.empty else 0
    same_position_doubtful = int(
        ((injuries.get("status_bucket") == "doubtful") & (injuries.get("position_group") == position_group)).sum()
    ) if not injuries.empty else 0

    projected_starter_count = int(lineups["projected_starter"].fillna(False).astype(bool).sum()) if not lineups.empty else 0
    projected_rotation_players = int(lineups["player_id"].nunique()) if not lineups.empty else 0
    projected_rotation_minutes = float(pd.to_numeric(lineups.get("expected_minutes"), errors="coerce").fillna(0.0).sum()) if not lineups.empty else 0.0
    player_lineup = lineups[lineups["player_id"] == player_id] if not lineups.empty else pd.DataFrame()
    player_expected_minutes = (
        float(pd.to_numeric(player_lineup["expected_minutes"], errors="coerce").fillna(0.0).iloc[-1])
        if not player_lineup.empty
        else 0.0
    )
    projected_minutes_share = player_expected_minutes / max(projected_rotation_minutes, 1.0)
    projected_starter_flag = float(player_lineup["projected_starter"].fillna(False).astype(bool).iloc[-1]) if not player_lineup.empty else 0.0
    lineup_report_count = int(lineups["player_id"].count()) if not lineups.empty else 0
    missing_starter_count = max(0, 5 - projected_starter_count) if projected_starter_count else min(5, out_count + doubtful_count)
    rotation_shortfall = max(0.0, 240.0 - projected_rotation_minutes) / 240.0
    report_coverage_gap = 1.0 - min(projected_rotation_players / 8.0, 1.0) if projected_rotation_players else 1.0
    teammate_absence_pressure = min(out_count + 0.6 * doubtful_count + 0.25 * questionable_count + 0.75 * missing_starter_count, 8.0)
    lineup_instability = np.clip(
        0.35 * min(teammate_absence_pressure / 6.0, 1.0)
        + 0.25 * min(missing_starter_count / 5.0, 1.0)
        + 0.20 * rotation_shortfall
        + 0.20 * report_coverage_gap,
        0.0,
        1.0,
    )
    # v1.2.3 A2: production-weighted teammate absence signals.
    # Collect player_ids of confirmed-out teammates, then look up their
    # career-average usage proxy (FGA + 0.44*FTA) from the pre-built lookup.
    # When the lookup is empty (inference / no game-log columns in frame),
    # both values default to 0.0 — the model falls back to count-based signals.
    _lookup: dict[int, float] = player_usage_lookup or {}
    out_player_ids: list[int] = (
        [int(pid) for pid in injuries[injuries["status_bucket"] == "out"]["player_id"].tolist()]
        if not injuries.empty else []
    )
    missing_teammate_usage_sum = float(sum(_lookup.get(pid, 0.0) for pid in out_player_ids))
    star_absent_flag = float(any(_lookup.get(pid, 0.0) >= _STAR_USAGE_THRESHOLD for pid in out_player_ids))

    return {
        "team_injuries": float(out_count + doubtful_count + questionable_count),
        "team_out_count": float(out_count),
        "team_doubtful_count": float(doubtful_count),
        "team_questionable_count": float(questionable_count),
        "same_position_out_count": float(same_position_out),
        "same_position_doubtful_count": float(same_position_doubtful),
        "projected_starter_count": float(projected_starter_count),
        "missing_starter_count": float(missing_starter_count),
        "projected_rotation_players": float(projected_rotation_players),
        "projected_rotation_minutes": float(projected_rotation_minutes),
        "projected_minutes_share": float(projected_minutes_share),
        "projected_starter_flag": float(projected_starter_flag),
        "lineup_report_count": float(lineup_report_count),
        "lineup_instability_score": float(lineup_instability),
        "teammate_absence_pressure": float(teammate_absence_pressure),
        "missing_teammate_usage_sum": missing_teammate_usage_sum,
        "star_absent_flag": star_absent_flag,
    }


def _injury_status_bucket(status: object) -> str:
    lowered = str(status or "").strip().lower()
    if lowered in {"out", "inactive", "suspended"}:
        return "out"
    if lowered == "doubtful":
        return "doubtful"
    if lowered == "questionable":
        return "questionable"
    return "available"


def _line_movement(group: pd.DataFrame, current_timestamp: pd.Timestamp, window: timedelta) -> float:
    current_latest = _representative_snapshot_per_book(group)
    current_line = float(current_latest["line_value"].median())
    cutoff = current_timestamp - pd.Timedelta(window)
    previous = group[group["timestamp"] <= cutoff]
    if previous.empty:
        return 0.0
    previous_latest = _representative_snapshot_per_book(previous)
    return float(current_line - previous_latest["line_value"].median())


def _latest_snapshot_per_book(frame: pd.DataFrame) -> pd.DataFrame:
    dedupe_columns = [
        column
        for column in ("game_id", "player_id", "market_key", "sportsbook_id", "line_value")
        if column in frame.columns
    ]
    return (
        frame.sort_values(["timestamp", "snapshot_id"], ascending=[False, False])
        .drop_duplicates(subset=dedupe_columns, keep="first")
        .copy()
    )


def _representative_snapshot_per_book(frame: pd.DataFrame) -> pd.DataFrame:
    latest = _attach_quote_probabilities(_latest_snapshot_per_book(frame))
    if latest.empty:
        return latest
    dedupe_columns = [
        column
        for column in ("game_id", "player_id", "market_key", "sportsbook_id")
        if column in latest.columns
    ]
    representative = latest.copy()
    representative["prob_distance_from_even"] = (
        representative["no_vig_over_probability"].fillna(0.5) - 0.5
    ).abs()
    representative["line_distance_from_group_median"] = (
        representative["line_value"]
        - representative.groupby(["game_id", "player_id", "market_key"])["line_value"].transform("median")
    ).abs()
    return (
        representative.sort_values(
            [
                "prob_distance_from_even",
                "line_distance_from_group_median",
                "timestamp",
                "snapshot_id",
            ],
            ascending=[True, True, False, False],
        )
        .drop_duplicates(subset=dedupe_columns, keep="first")
        .drop(columns=["prob_distance_from_even", "line_distance_from_group_median"])
        .copy()
    )


def _attach_quote_probabilities(frame: pd.DataFrame) -> pd.DataFrame:
    enriched = frame.copy()
    enriched["raw_implied_over_probability"] = enriched["over_odds"].apply(_american_to_probability)
    enriched["raw_implied_under_probability"] = enriched["under_odds"].apply(_american_to_probability)
    no_vig = enriched.apply(
        lambda row: _no_vig_probabilities(
            row.get("raw_implied_over_probability"),
            row.get("raw_implied_under_probability"),
        ),
        axis=1,
    )
    enriched["no_vig_over_probability"] = [pair[0] for pair in no_vig]
    enriched["no_vig_under_probability"] = [pair[1] for pair in no_vig]
    return enriched


def _historical_odds_column_name(market_key: str, value_column: str) -> str:
    if value_column == "line_value":
        return f"line_{market_key}"
    return f"{market_key}_{value_column}"


def _coerce_numeric_columns(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column not in frame.columns:
            continue
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def _coerce_json_dict(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            loaded = json.loads(value)
        except Exception:
            return {}
        return loaded if isinstance(loaded, dict) else {}
    return {}


def _normalize_percentage_scalar(value: float | int | None) -> float | None:
    if value is None or pd.isna(value):
        return None
    numeric = float(value)
    if numeric > 1.0:
        return numeric / 100.0
    if numeric < 0.0:
        return None
    return numeric


def _american_to_probability(value: object) -> float | None:
    if value in (None, "", 0):
        return None
    odds = float(value)
    if odds > 0:
        return 100.0 / (odds + 100.0)
    return abs(odds) / (abs(odds) + 100.0)


def _no_vig_probabilities(over_probability: float | None, under_probability: float | None) -> tuple[float | None, float | None]:
    if over_probability is None or under_probability is None:
        return None, None
    total = over_probability + under_probability
    if total <= 0:
        return None, None
    return over_probability / total, under_probability / total


def _extract_meta_event_start_times(metas: pd.Series) -> pd.Series:
    """Vectorised extraction of event_start_time from snapshot meta dicts.

    Returns a Series of tz-naive Timestamps (NaT where the field is absent
    or unparseable).  Used to override midnight-only start_time values that
    the NBA API stores for completed games.
    """
    def _get(meta: object) -> pd.Timestamp:
        payload = _coerce_json_dict(meta)
        est = payload.get("event_start_time")
        if not est:
            return pd.NaT  # type: ignore[return-value]
        try:
            ts = pd.Timestamp(est)
            if ts.tzinfo is not None:
                ts = ts.tz_localize(None)
            return ts
        except Exception:
            return pd.NaT  # type: ignore[return-value]

    return metas.map(_get)


def _is_fully_verified_meta(meta: object) -> bool:
    payload = _coerce_json_dict(meta)
    return (
        bool(payload.get("is_live_quote", False))
        and str(payload.get("odds_verification_status", "")).lower() == "provider_live"
    )


def _is_training_usable_meta(meta: object) -> bool:
    payload = _coerce_json_dict(meta)
    # Accept snapshots explicitly stamped as historical training quotes by
    # IngestionService.mark_historical_training_quotes (C4 fix).
    if bool(payload.get("is_historical_training_quote", False)):
        return True
    # Also accept live quotes that were verified by the provider at ingestion time.
    return _is_fully_verified_meta(meta)

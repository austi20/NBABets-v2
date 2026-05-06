from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from pandas.errors import EmptyDataError

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db.session import session_scope  # noqa: E402
from app.evaluation.backtest import RollingOriginBacktester  # noqa: E402
from app.training.data import DatasetLoader  # noqa: E402

REPORT_PATH = ROOT / "reports" / "surrogate_backtest_v1.md"
BOX_SCORE_ROOT = ROOT / "data" / "parquet" / "box_scores"
SURROGATE_ROOT = ROOT / "data" / "parquet" / "odds_synthetic"
EDGE_THRESHOLDS = (0.00, 0.02, 0.05, 0.08)


class _SurrogateQuoteLoader:
    def __init__(self, quote_inventory: pd.DataFrame) -> None:
        self._quote_inventory = quote_inventory.copy()

    def load_historical_bet_quotes(self, *_: object, **__: object) -> pd.DataFrame:
        return self._quote_inventory.copy()


def _parse_seasons(raw: str) -> list[int]:
    return [int(part.strip()) for part in raw.split(",") if part.strip()]


def _season_start_year_from_date(values: pd.Series) -> pd.Series:
    dates = pd.to_datetime(values, errors="coerce")
    return pd.Series(
        np.where(dates.dt.month >= 10, dates.dt.year, dates.dt.year - 1),
        index=values.index,
        dtype="Int64",
    )


def _season_start_year_from_frame(frame: pd.DataFrame) -> pd.Series:
    if "season" in frame.columns:
        season = frame["season"].astype(str).str.strip()
        extracted = pd.to_numeric(season.str.extract(r"^(\d{4})", expand=False), errors="coerce").astype("Int64")
        fallback_mask = extracted.isna()
        if fallback_mask.any():
            extracted.loc[fallback_mask] = _season_start_year_from_date(frame.loc[fallback_mask, "game_date"])
        return extracted
    return _season_start_year_from_date(frame["game_date"])


def _load_parquet_partitions(root: Path, seasons: list[int]) -> pd.DataFrame:
    files: list[Path] = []
    for season in seasons:
        files.extend(sorted((root / f"season={season}").glob("*.parquet")))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(path) for path in files], ignore_index=True)


def _american_to_prob(odds: float) -> float:
    if odds > 0:
        return 100.0 / (odds + 100.0)
    return abs(odds) / (abs(odds) + 100.0)


def _build_quote_inventory(surrogate_lines: pd.DataFrame) -> pd.DataFrame:
    over_prob = _american_to_prob(-110.0)
    under_prob = _american_to_prob(-110.0)
    no_vig_over = over_prob / (over_prob + under_prob)
    no_vig_under = under_prob / (over_prob + under_prob)

    frame = surrogate_lines.copy()
    frame["game_id"] = pd.to_numeric(frame["game_id"], errors="coerce").astype("Int64")
    frame["player_id"] = pd.to_numeric(frame["player_id"], errors="coerce").astype("Int64")
    frame["game_date"] = pd.to_datetime(frame["game_date"], errors="coerce")
    frame["timestamp"] = frame["game_date"]
    frame["over_odds"] = -110
    frame["under_odds"] = -110
    frame["raw_implied_over_probability"] = over_prob
    frame["raw_implied_under_probability"] = under_prob
    frame["no_vig_over_probability"] = no_vig_over
    frame["no_vig_under_probability"] = no_vig_under
    frame["closing_line_value"] = frame["line_value"]
    frame["closing_over_odds"] = frame["over_odds"]
    frame["closing_under_odds"] = frame["under_odds"]
    frame["closing_no_vig_over_probability"] = frame["no_vig_over_probability"]
    frame["closing_no_vig_under_probability"] = frame["no_vig_under_probability"]
    frame["closing_timestamp"] = frame["timestamp"]
    frame["sportsbook_key"] = "synthetic"
    frame["sportsbook_name"] = "Synthetic Surrogate"
    frame["odds_source_provider"] = "synthetic_surrogate_v1"
    frame["odds_verification_status"] = "synthetic"
    frame["is_live_quote"] = 0
    return frame[
        [
            "game_id",
            "player_id",
            "game_date",
            "market_key",
            "line_value",
            "over_odds",
            "under_odds",
            "raw_implied_over_probability",
            "raw_implied_under_probability",
            "no_vig_over_probability",
            "no_vig_under_probability",
            "closing_line_value",
            "closing_over_odds",
            "closing_under_odds",
            "closing_no_vig_over_probability",
            "closing_no_vig_under_probability",
            "closing_timestamp",
            "timestamp",
            "sportsbook_key",
            "sportsbook_name",
            "odds_source_provider",
            "odds_verification_status",
            "is_live_quote",
        ]
    ].dropna(subset=["game_date", "game_id", "player_id", "line_value"])


def _attach_surrogate_lines_to_historical(
    historical: pd.DataFrame,
    surrogate_lines: pd.DataFrame,
) -> pd.DataFrame:
    if historical.empty or surrogate_lines.empty:
        return historical
    pivot = (
        surrogate_lines.pivot_table(
            index=["game_id", "player_id"],
            columns="market_key",
            values="line_value",
            aggfunc="mean",
        )
        .rename(columns=lambda market_key: f"line_{market_key}")
        .reset_index()
    )
    merged = historical.merge(pivot, on=["game_id", "player_id"], how="left")
    return merged


def _compute_threshold_roi(detail: pd.DataFrame, threshold: float) -> tuple[int, float]:
    if detail.empty:
        return 0, 0.0
    candidate = detail[pd.to_numeric(detail["edge_vs_no_vig"], errors="coerce").fillna(-1.0) >= threshold].copy()
    if candidate.empty:
        return 0, 0.0
    over_hit = pd.to_numeric(candidate["actual_total"], errors="coerce") > pd.to_numeric(candidate["line_value"], errors="coerce")
    under_hit = pd.to_numeric(candidate["actual_total"], errors="coerce") < pd.to_numeric(candidate["line_value"], errors="coerce")
    push = np.isclose(
        pd.to_numeric(candidate["actual_total"], errors="coerce").to_numpy(dtype=float),
        pd.to_numeric(candidate["line_value"], errors="coerce").to_numpy(dtype=float),
    )
    win_mask = ((candidate["recommended_side"] == "OVER") & over_hit) | ((candidate["recommended_side"] == "UNDER") & under_hit)
    decimal_odds = pd.to_numeric(candidate["decimal_odds"], errors="coerce").fillna(1.0)
    win_profit = decimal_odds - 1.0
    profits = np.where(push, 0.0, np.where(win_mask, win_profit, -1.0))
    return int(len(candidate)), float(np.mean(profits))


def _metric_means_from_folds(fold_df: pd.DataFrame) -> tuple[float, float, float]:
    if fold_df.empty:
        return float("nan"), float("nan"), float("nan")
    log_loss = pd.to_numeric(fold_df.get("log_loss"), errors="coerce")
    brier = pd.to_numeric(fold_df.get("brier_score"), errors="coerce")
    ece = pd.to_numeric(fold_df.get("expected_calibration_error"), errors="coerce")
    return float(log_loss.mean()), float(brier.mean()), float(ece.mean())


def _write_report(
    *,
    seasons: list[int],
    mean_log_loss: float,
    mean_brier: float,
    mean_ece: float,
    threshold_rows: list[dict[str, object]],
) -> None:
    lines = [
        "# Surrogate Backtest v1",
        "",
        "## Scope",
        "",
        f"- Seasons: {', '.join(str(season) for season in seasons)}",
        "- Backtest engine: `RollingOriginBacktester`",
        "- Line source: `synthetic_surrogate_v1` (trailing average)",
        "",
        "## Aggregate Calibration Metrics",
        "",
        f"- Mean log_loss: {mean_log_loss:.4f}",
        f"- Mean Brier score: {mean_brier:.4f}",
        f"- Mean ECE: {mean_ece:.4f}",
        "",
        "## Hypothetical ROI by Edge Threshold",
        "",
        "| Edge threshold | Candidate bets | Hypothetical ROI |",
        "|---:|---:|---:|",
    ]
    for row in threshold_rows:
        lines.append(
            f"| {float(row['edge_threshold']):.2f} | {int(row['candidate_bets'])} | {float(row['roi']):.4f} |"
        )
    lines.extend(
        [
            "",
            "## Honesty Note",
            "",
            "**Surrogate results are an upper-bound estimate, not a real-money expectation.**",
            "",
            "Synthetic lines are generated from trailing player performance and cannot capture real market dynamics",
            "(bookmaker shading, injury news timing, liquidity, and line movement), so this report should be treated",
            "as a calibration sanity check only.",
            "",
        ]
    )
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run rolling-origin backtest against synthetic surrogate lines.")
    parser.add_argument("--seasons", default="2023,2024", help="Comma-separated seasons, e.g. 2023,2024")
    parser.add_argument("--box-score-root", default=str(BOX_SCORE_ROOT))
    parser.add_argument("--surrogate-root", default=str(SURROGATE_ROOT))
    parser.add_argument("--train-days", type=int, default=45, help="Training window size in days.")
    parser.add_argument("--validation-days", type=int, default=14, help="Validation window size in days.")
    parser.add_argument("--step-days", type=int, default=7, help="Step size in days.")
    args = parser.parse_args()

    seasons = _parse_seasons(args.seasons)
    if not seasons:
        raise SystemExit("No seasons provided.")

    surrogate_lines = _load_parquet_partitions(Path(args.surrogate_root), seasons)
    if surrogate_lines.empty:
        raise SystemExit(f"No surrogate parquet found under {args.surrogate_root} for seasons {seasons}.")
    quote_inventory = _build_quote_inventory(surrogate_lines)

    with session_scope() as session:
        loader = DatasetLoader(session)
        historical = loader.load_historical_player_games_from_parquet(Path(args.box_score_root))
        season_start_year = _season_start_year_from_frame(historical)
        historical = historical[season_start_year.isin(seasons)].copy()
        if historical.empty:
            raise SystemExit("Historical parquet load produced no rows for selected seasons.")
        historical["game_id"] = pd.to_numeric(historical["game_id"], errors="coerce").astype("Int64")
        historical["player_id"] = pd.to_numeric(historical["player_id"], errors="coerce").astype("Int64")
        historical = _attach_surrogate_lines_to_historical(historical, surrogate_lines)

        backtester = RollingOriginBacktester(session)
        backtester._loader = _SurrogateQuoteLoader(quote_inventory)  # type: ignore[attr-defined]
        result = backtester.run(
            historical=historical,
            train_days=max(14, int(args.train_days)),
            validation_days=max(7, int(args.validation_days)),
            step_days=max(1, int(args.step_days)),
        )

    detail_csv = result.get("artifacts", {}).get("detail_csv")
    fold_csv = result.get("artifacts", {}).get("fold_csv")
    detail_df = pd.DataFrame()
    fold_df = pd.DataFrame()
    if isinstance(detail_csv, str) and Path(detail_csv).exists():
        try:
            detail_df = pd.read_csv(detail_csv)
        except EmptyDataError:
            detail_df = pd.DataFrame()
    if isinstance(fold_csv, str) and Path(fold_csv).exists():
        try:
            fold_df = pd.read_csv(fold_csv)
        except EmptyDataError:
            fold_df = pd.DataFrame()
    mean_log_loss, mean_brier, mean_ece = _metric_means_from_folds(fold_df)
    threshold_rows: list[dict[str, object]] = []
    for threshold in EDGE_THRESHOLDS:
        count, roi = _compute_threshold_roi(detail_df, threshold)
        threshold_rows.append({"edge_threshold": threshold, "candidate_bets": count, "roi": roi})

    _write_report(
        seasons=seasons,
        mean_log_loss=mean_log_loss,
        mean_brier=mean_brier,
        mean_ece=mean_ece,
        threshold_rows=threshold_rows,
    )
    print(f"Wrote {REPORT_PATH}")


if __name__ == "__main__":
    main()

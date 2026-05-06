"""
deep_model_eval.py — v1.2.1 comprehensive model accuracy analysis.

Runs rolling-origin backtests across multiple train-window configurations,
then applies 7 analytic lenses to the bet detail data.  Writes a full
markdown report to reports/ and prints a progress log to stdout.

Usage:
    python scripts/deep_model_eval.py

No external API calls are made — all data comes from the local DB.
"""
from __future__ import annotations

import io
import sys
import time
from datetime import date
from pathlib import Path

# Force UTF-8 on Windows consoles that default to cp1252
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if sys.stderr.encoding and sys.stderr.encoding.lower() != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Bootstrap path so imports resolve from project root
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.config.settings import get_settings  # noqa: E402
from app.db.bootstrap import create_all  # noqa: E402
from app.db.session import configure_engine, session_scope  # noqa: E402
from app.evaluation.backtest import RollingOriginBacktester  # noqa: E402
from app.evaluation.metrics import expected_calibration_error  # noqa: E402
from app.training.data import DatasetLoader  # noqa: E402

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
WINDOW_CONFIGS = [
    {"label": "short_window",   "train_days": 60,  "validation_days": 14, "step_days": 7},
    {"label": "standard_window","train_days": 120, "validation_days": 14, "step_days": 14},
    {"label": "long_window",    "train_days": 180, "validation_days": 14, "step_days": 14},
]
RELIABILITY_BINS = 10
CONFIDENCE_THRESHOLDS = [0.50, 0.55, 0.58, 0.60, 0.63, 0.65, 0.68, 0.70]
LINE_TIERS = {
    "points":    [(0, 15, "low"), (15, 25, "mid"), (25, 99, "high")],
    "rebounds":  [(0,  6, "low"), ( 6, 10, "mid"), (10, 99, "high")],
    "assists":   [(0,  4, "low"), ( 4,  7, "mid"), ( 7, 99, "high")],
    "threes":    [(0,  1, "low"), ( 1,  3, "mid"), ( 3, 99, "high")],
    "turnovers": [(0,  1, "low"), ( 1,  3, "mid"), ( 3, 99, "high")],
    "pra":       [(0, 20, "low"), (20, 35, "mid"), (35, 99, "high")],
}
PLAYER_MIN_TIERS = [(0, 20, "bench_<20min"), (20, 30, "mid_20-30min"), (30, 99, "starter_>30min")]


def _ts() -> str:
    return time.strftime("%H:%M:%S")


def _pct(n: float) -> str:
    return f"{n * 100:.1f}%"


def _num(n: float, dp: int = 3) -> str:
    return f"{n:.{dp}f}"


# ---------------------------------------------------------------------------
# Analytic lenses applied to a bet_detail DataFrame
# ---------------------------------------------------------------------------

def reliability_diagram_data(df: pd.DataFrame, col: str = "calibrated_over_probability", bins: int = 10) -> pd.DataFrame:
    """Per-bin: predicted mean prob vs actual hit rate."""
    probs = pd.to_numeric(df[col], errors="coerce")
    labels = pd.to_numeric(df["label"], errors="coerce")
    mask = probs.notna() & labels.notna()
    probs, labels = probs[mask].to_numpy(), labels[mask].to_numpy().astype(int)
    edges = np.linspace(0.0, 1.0, bins + 1)
    rows = []
    for lo, hi in zip(edges[:-1], edges[1:], strict=False):
        sel = (probs >= lo) & (probs < hi if hi < 1.0 else probs <= hi)
        n = int(sel.sum())
        if n == 0:
            continue
        rows.append({
            "bin_lo": round(float(lo), 2),
            "bin_hi": round(float(hi), 2),
            "predicted_mean": round(float(probs[sel].mean()), 4),
            "actual_freq": round(float(labels[sel].mean()), 4),
            "count": n,
            "gap": round(float(labels[sel].mean() - probs[sel].mean()), 4),
        })
    return pd.DataFrame(rows)


def confidence_threshold_table(df: pd.DataFrame, thresholds: list[float]) -> pd.DataFrame:
    """For each threshold: bet count, hit rate, and ECE on subset."""
    rows = []
    probs = pd.to_numeric(df["calibrated_over_probability"], errors="coerce")
    labels = pd.to_numeric(df["label"], errors="coerce")
    for t in thresholds:
        high_conf = (probs >= t) | (probs <= 1 - t)
        sub_p = probs[high_conf].dropna().to_numpy()
        sub_l = labels[high_conf].dropna().astype(int).to_numpy()
        over_mask = probs >= t
        sub_p_over = probs[over_mask].dropna().to_numpy()
        sub_l_over = labels[over_mask].dropna().astype(int).to_numpy()
        n_over = int(over_mask.sum())
        hit_rate_over = float(sub_l_over.mean()) if n_over > 0 else float("nan")
        rows.append({
            "threshold": t,
            "bets_over": n_over,
            "hit_rate_over": round(hit_rate_over, 4),
            "expected_hit_rate": round(float(sub_p_over.mean()), 4) if n_over > 0 else float("nan"),
            "total_filtered": int(high_conf.sum()),
            "ece_filtered": round(expected_calibration_error(sub_l, sub_p, bins=RELIABILITY_BINS), 4) if len(sub_l) > 10 else float("nan"),
        })
    return pd.DataFrame(rows)


def line_tier_breakdown(df: pd.DataFrame) -> pd.DataFrame:
    """Accuracy by line-value tier per market."""
    rows = []
    for market, tiers in LINE_TIERS.items():
        mdf = df[df["market_key"] == market].copy()
        if mdf.empty:
            continue
        lines = pd.to_numeric(mdf["line_value"], errors="coerce")
        probs = pd.to_numeric(mdf["calibrated_over_probability"], errors="coerce")
        labels = pd.to_numeric(mdf["label"], errors="coerce")
        for lo, hi, tier_name in tiers:
            sel = (lines >= lo) & (lines < hi)
            n = int(sel.sum())
            if n == 0:
                continue
            sp, sl = probs[sel].dropna(), labels[sel].dropna().astype(int)
            rows.append({
                "market": market,
                "line_tier": tier_name,
                "line_range": f"{lo}–{hi}",
                "count": n,
                "hit_rate": round(float(sl.mean()), 4) if len(sl) else float("nan"),
                "avg_predicted_prob": round(float(sp.mean()), 4) if len(sp) else float("nan"),
                "brier": round(float(np.mean((sp.to_numpy() - sl.to_numpy()) ** 2)), 4) if len(sp) else float("nan"),
            })
    return pd.DataFrame(rows)


def home_away_breakdown(df: pd.DataFrame) -> pd.DataFrame:
    """Accuracy split by home vs away if column present."""
    if "is_home" not in df.columns:
        return pd.DataFrame()
    rows = []
    for is_home, label in [(1, "home"), (0, "away")]:
        sub = df[df["is_home"] == is_home]
        if sub.empty:
            continue
        for market in sub["market_key"].unique():
            msub = sub[sub["market_key"] == market]
            probs = pd.to_numeric(msub["calibrated_over_probability"], errors="coerce").dropna()
            labels = pd.to_numeric(msub["label"], errors="coerce").dropna().astype(int)
            if len(probs) < 5:
                continue
            rows.append({
                "market": market,
                "side": label,
                "count": len(probs),
                "hit_rate": round(float(labels.mean()), 4),
                "avg_predicted_prob": round(float(probs.mean()), 4),
                "ece": round(expected_calibration_error(labels.to_numpy(), probs.to_numpy()), 4),
            })
    return pd.DataFrame(rows)


def model_vs_consensus(df: pd.DataFrame) -> pd.DataFrame:
    """When model agrees vs disagrees with sportsbook consensus."""
    if "no_vig_over_probability" not in df.columns:
        return pd.DataFrame()
    probs = pd.to_numeric(df["calibrated_over_probability"], errors="coerce")
    consensus = pd.to_numeric(df["no_vig_over_probability"], errors="coerce")
    labels = pd.to_numeric(df["label"], errors="coerce")
    agrees = ((probs > 0.5) & (consensus > 0.5)) | ((probs < 0.5) & (consensus < 0.5))
    rows = []
    for align, name in [(True, "agrees_with_book"), (False, "disagrees_with_book")]:
        sub_p = probs[agrees == align].dropna()
        sub_l = labels[agrees == align].dropna().astype(int)
        if len(sub_p) < 5:
            continue
        rows.append({
            "alignment": name,
            "count": len(sub_p),
            "hit_rate": round(float(sub_l.mean()), 4),
            "avg_model_prob": round(float(sub_p.mean()), 4),
            "avg_consensus_prob": round(float(consensus[agrees == align].dropna().mean()), 4),
            "ece": round(expected_calibration_error(sub_l.to_numpy(), sub_p.to_numpy()), 4),
        })
    return pd.DataFrame(rows)


def temporal_decay_analysis(df: pd.DataFrame, eval_start: date | None = None) -> pd.DataFrame:
    """Does accuracy fall off further into the validation window?"""
    if "game_date" not in df.columns:
        return pd.DataFrame()
    dates = pd.to_datetime(df["game_date"]).dt.date
    probs = pd.to_numeric(df["calibrated_over_probability"], errors="coerce")
    labels = pd.to_numeric(df["label"], errors="coerce")
    if eval_start is None:
        eval_start = dates.min()
    df2 = df.copy()
    df2["days_from_start"] = (dates - eval_start).apply(lambda d: d.days if hasattr(d, "days") else int(d))
    rows = []
    for day_bucket in range(0, 22, 7):  # weeks 0, 1, 2, 3
        sel = (df2["days_from_start"] >= day_bucket) & (df2["days_from_start"] < day_bucket + 7)
        sub_p = probs[sel].dropna()
        sub_l = labels[sel].dropna().astype(int)
        if len(sub_p) < 5:
            continue
        rows.append({
            "days_offset": f"{day_bucket}–{day_bucket + 6}",
            "count": len(sub_p),
            "hit_rate": round(float(sub_l.mean()), 4),
            "avg_predicted_prob": round(float(sub_p.mean()), 4),
            "brier": round(float(np.mean((sub_p.to_numpy() - sub_l.to_numpy()) ** 2)), 4),
        })
    return pd.DataFrame(rows)


def train_vs_val_overfit_check(fold_results: pd.DataFrame) -> pd.DataFrame:
    """Per-market: compare metrics across time to spot performance drift."""
    if fold_results.empty or "market_key" not in fold_results.columns:
        return pd.DataFrame()
    rows = []
    for market in fold_results["market_key"].unique():
        mf = fold_results[fold_results["market_key"] == market].copy()
        if len(mf) < 3:
            continue
        mf["fold_start"] = pd.to_datetime(mf["fold_start"])
        mf_sorted = mf.sort_values("fold_start")
        # Split into first-half and second-half folds
        mid = len(mf_sorted) // 2
        early = mf_sorted.iloc[:mid]
        late = mf_sorted.iloc[mid:]
        rows.append({
            "market": market,
            "early_mae": round(float(early["mae"].mean()), 4),
            "late_mae": round(float(late["mae"].mean()), 4),
            "mae_drift": round(float(late["mae"].mean() - early["mae"].mean()), 4),
            "early_brier": round(float(early["brier_score"].mean()), 4),
            "late_brier": round(float(late["brier_score"].mean()), 4),
            "brier_drift": round(float(late["brier_score"].mean() - early["brier_score"].mean()), 4),
            "early_ece": round(float(early["expected_calibration_error"].mean()), 4),
            "late_ece": round(float(late["expected_calibration_error"].mean()), 4),
            "n_folds_early": int(len(early)),
            "n_folds_late": int(len(late)),
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Report writer
# ---------------------------------------------------------------------------

def _md_table(df: pd.DataFrame, title: str) -> list[str]:
    if df is None or df.empty:
        return [f"### {title}", "", "_No data available._", ""]
    cols = list(df.columns)
    lines = [f"### {title}", "", "| " + " | ".join(cols) + " |", "|" + "|".join(["---"] * len(cols)) + "|"]
    for _, row in df.iterrows():
        cells = []
        for c in cols:
            v = row[c]
            if isinstance(v, float):
                cells.append(f"{v:.4f}" if abs(v) < 100 else f"{v:.1f}")
            else:
                cells.append(str(v))
        lines.append("| " + " | ".join(cells) + " |")
    lines.append("")
    return lines


def write_report(
    report_path: Path,
    window_label: str,
    fold_df: pd.DataFrame,
    bet_detail: pd.DataFrame,
) -> None:
    lines: list[str] = [
        f"# Model Accuracy Deep-Dive — {window_label}",
        f"_Generated {time.strftime('%Y-%m-%d %H:%M:%S')}_",
        "",
        "## 1. Overall Fold Summary",
        "",
    ]

    # -- overall summary per market
    if not fold_df.empty:
        agg = fold_df.groupby("market_key").agg(
            folds=("mae", "count"),
            mae=("mae", "mean"),
            rmse=("rmse", "mean"),
            brier=("brier_score", "mean"),
            ece=("expected_calibration_error", "mean"),
            interval_80_coverage=("interval_80_coverage", "mean"),
            avg_edge_implied=("average_edge_implied", "mean"),
            avg_ev=("average_expected_value", "mean"),
            roi=("realized_roi", "mean"),
        ).reset_index().round(4)
        lines += _md_table(agg, "Per-Market Fold Averages (MAE, Brier, ECE, ROI)")
        if "sample_sufficient" in fold_df.columns:
            suff = (
                fold_df.groupby("market_key")
                .agg(
                    sufficient_ratio=("sample_sufficient", "mean"),
                    avg_train_quotes=("train_quote_count_raw", "mean"),
                    avg_validation_quotes=("validation_quote_count_raw", "mean"),
                )
                .reset_index()
                .round(4)
            )
            lines += _md_table(suff, "Data Sufficiency by Market")
        if "sufficiency_flag" in fold_df.columns:
            flags = (
                fold_df.groupby(["market_key", "sufficiency_flag"], dropna=False)
                .size()
                .reset_index(name="fold_count")
                .sort_values(["market_key", "fold_count"], ascending=[True, False])
            )
            lines += _md_table(flags, "Fold Sufficiency Flags")

    if bet_detail.empty:
        lines.append("_No bet detail data available for further analytics._")
        report_path.write_text("\n".join(lines), encoding="utf-8")
        return

    # -- calibration reliability
    lines.append("## 2. Calibration Reliability (10-bin)")
    lines.append("")
    lines.append("_Predicted probability bucket vs actual outcome frequency._")
    lines.append("_A well-calibrated model has `predicted_mean ≈ actual_freq`._")
    lines.append("")
    for market in sorted(bet_detail["market_key"].unique()):
        mdf = bet_detail[bet_detail["market_key"] == market]
        rd = reliability_diagram_data(mdf)
        lines += _md_table(rd, f"Reliability — {market.upper()}")

    # -- confidence threshold hit rate
    lines.append("## 3. Confidence Threshold Hit Rate")
    lines.append("")
    lines.append("_Does the model hit its predicted frequency when it is most confident?_")
    lines.append("")
    ct = confidence_threshold_table(bet_detail, CONFIDENCE_THRESHOLDS)
    lines += _md_table(ct, "Hit Rate at Confidence Thresholds (all markets)")
    for market in sorted(bet_detail["market_key"].unique()):
        ct_m = confidence_threshold_table(bet_detail[bet_detail["market_key"] == market], CONFIDENCE_THRESHOLDS)
        lines += _md_table(ct_m, f"Confidence Hit Rate — {market.upper()}")

    # -- line tier breakdown
    lines.append("## 4. Line-Tier Accuracy")
    lines.append("")
    lines.append("_Does model accuracy vary by how big/small the prop line is?_")
    lines.append("")
    lt = line_tier_breakdown(bet_detail)
    lines += _md_table(lt, "Accuracy by Line Tier and Market")

    # -- home / away
    lines.append("## 5. Home vs Away")
    lines.append("")
    ha = home_away_breakdown(bet_detail)
    lines += _md_table(ha, "Accuracy by Home/Away")

    # -- model vs consensus
    lines.append("## 6. Model vs Sportsbook Consensus Alignment")
    lines.append("")
    lines.append("_When model disagrees with the book: is it right more or less often?_")
    lines.append("")
    mvc = model_vs_consensus(bet_detail)
    lines += _md_table(mvc, "Alignment with Book Consensus")

    # -- temporal decay
    lines.append("## 7. Temporal Decay — Accuracy by Days from Fold Start")
    lines.append("")
    lines.append("_Does the model stay accurate deeper into the future (7, 14, 21 days out)?_")
    lines.append("")
    td = temporal_decay_analysis(bet_detail)
    lines += _md_table(td, "Accuracy by Days Offset from Training Cutoff")

    # -- overfit / drift check
    lines.append("## 8. Performance Drift (Early vs Late Folds)")
    lines.append("")
    lines.append("_Is the model's accuracy improving, degrading, or stable across historical time?_")
    lines.append("")
    od = train_vs_val_overfit_check(fold_df)
    lines += _md_table(od, "MAE & Brier Drift (early folds vs late folds)")

    report_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"[{_ts()}] Report written → {report_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print(f"[{_ts()}] Starting deep model evaluation — v1.2.1 branch")
    configure_engine()
    create_all()

    reports_dir = get_settings().reports_dir
    reports_dir.mkdir(parents=True, exist_ok=True)

    all_window_summaries: list[dict] = []

    # Load historical data once — reuse across all window configs
    with session_scope() as session:
        print(f"[{_ts()}] Loading historical data...")
        historical = DatasetLoader(session).load_historical_player_games()
        print(f"[{_ts()}] Loaded {len(historical):,} rows spanning "
              f"{historical['game_date'].min()} → {historical['game_date'].max()}")

    for cfg in WINDOW_CONFIGS:
        label = cfg["label"]
        train_d = cfg["train_days"]
        val_d = cfg["validation_days"]
        step_d = cfg["step_days"]
        print(f"\n[{_ts()}] ── Window config: {label} (train={train_d}d, val={val_d}d, step={step_d}d) ──")

        def _progress(current, total, message, _lbl=label):
            if current is not None and total:
                pct = int(current / total * 100)
                print(f"[{_ts()}] [{_lbl}] {pct:3d}% {message}", end="\r")

        with session_scope() as session:
            backtester = RollingOriginBacktester(session)
            result = backtester.run(
                train_days=train_d,
                validation_days=val_d,
                step_days=step_d,
                progress_callback=_progress,
                historical=historical,
            )

        print(f"\n[{_ts()}] [{label}] Backtest done. Loading artifacts...")

        # Load the detail CSV the backtester just wrote
        detail_csv_path = result.get("artifacts", {}).get("detail_csv")
        fold_summary = result.get("summary", [])

        if detail_csv_path and Path(detail_csv_path).exists() and Path(detail_csv_path).stat().st_size > 0:
            try:
                bet_detail = pd.read_csv(detail_csv_path)
                print(f"[{_ts()}] [{label}] Bet detail: {len(bet_detail):,} rows")
            except Exception as exc:
                print(f"[{_ts()}] [{label}] WARNING: could not read detail CSV ({exc}), skipping deep analytics")
                bet_detail = pd.DataFrame()
        else:
            print(f"[{_ts()}] [{label}] WARNING: no detail CSV or empty file — no quote data matched; skipping deep analytics")
            bet_detail = pd.DataFrame()

        # Load fold-level CSV for drift and sufficiency analysis.
        fold_df = pd.DataFrame()
        fold_csv_path = result.get("artifacts", {}).get("fold_csv")
        if fold_csv_path and Path(fold_csv_path).exists() and Path(fold_csv_path).stat().st_size > 0:
            try:
                fold_df = pd.read_csv(fold_csv_path)
            except Exception:
                fold_df = pd.DataFrame()

        # Combine fold metrics from summary
        if fold_summary:
            for row in fold_summary:
                row["window"] = label
            all_window_summaries.extend(fold_summary)

        # Write per-window deep report
        ts = time.strftime("%Y%m%dT%H%M%S")
        report_path = reports_dir / f"model_eval_{label}_{ts}.md"
        write_report(report_path, label, fold_df, bet_detail)

    # Write cross-window comparison
    print(f"\n[{_ts()}] Writing cross-window comparison...")
    ts = time.strftime("%Y%m%dT%H%M%S")
    comp_path = reports_dir / f"model_eval_comparison_{ts}.md"
    comp_lines = [
        "# Cross-Window Model Comparison",
        f"_Generated {time.strftime('%Y-%m-%d %H:%M:%S')}_",
        "",
        "Compares short (60d), standard (120d), and long (180d) training windows.",
        "",
        "| Window | Market | MAE | RMSE | Brier | ECE | ROI | Avg EV |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in all_window_summaries:
        comp_lines.append(
            f"| {row.get('window','?')} | {row.get('market_key','?')} "
            f"| {row.get('mae', float('nan')):.4f} | {row.get('rmse', float('nan')):.4f} "
            f"| {row.get('brier_score', float('nan')):.4f} | {row.get('expected_calibration_error', float('nan')):.4f} "
            f"| {row.get('realized_roi', float('nan')):.4f} | {row.get('average_expected_value', float('nan')):.4f} |"
        )
    comp_path.write_text("\n".join(comp_lines), encoding="utf-8")
    print(f"[{_ts()}] Cross-window comparison → {comp_path}")
    print(f"[{_ts()}] Deep model evaluation complete.")


if __name__ == "__main__":
    main()

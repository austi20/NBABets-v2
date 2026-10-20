"""Overfitting and generalization tables from extract_walkforward.py output.

Mirrors the NFL project's diagnostics:
  1. in sample vs out of sample error, per stage
  2. out of sample error of every stage and of the naive anchors
  3. calibration slope: regress actual on predicted, out of sample
       slope < 1  predictions too spread out (overfit)
       slope > 1  predictions too compressed (over regularized)
  4. least squares trust weight on the model against the anchor, and how much
     that weight moves when one season is left out (unstable means noise)
  5. do the hand set caps bind, and what do they cost
  6. out of sample error against tree count

Usage:
    python scripts/diag/generalization_report.py [--out docs/diag/nba_generalization_tables.md]
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DIAG = ROOT / "data" / "artifacts" / "diag"
MARKETS = ["points", "rebounds", "assists", "threes", "turnovers", "pra"]
STAGES = ["direct", "rate", "stacked", "pooled", "final"]
ANCHORS = ["avg5", "avg10", "season_avg", "history_anchor", "recency"]
MIN_HISTORY = 5  # rows with real history only


def md_table(headers: list[str], rows: list[list[str]]) -> list[str]:
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] + ["---:"] * (len(headers) - 1)) + "|"]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    lines.append("")
    return lines


def gap_section(insample: pd.DataFrame) -> list[str]:
    lines = ["## 1. In sample vs out of sample MAE", ""]
    lines.append("Gap = (OOS - in sample) / in sample. A big gap on `direct` with a small one on `final` means the caps hide it.")
    lines.append("")
    rows = []
    for market in MARKETS:
        row = [market]
        for stage in ("direct", "rate", "stacked", "final"):
            part = insample[(insample["market"] == market) & (insample["stage"] == stage)]
            gap = (part["oos_mae"] - part["in_sample_mae"]).sum() / part["in_sample_mae"].sum()
            row.append(f"{part['in_sample_mae'].mean():.3f} / {part['oos_mae'].mean():.3f} ({gap:+.0%})")
        rows.append(row)
    lines += md_table(["market", "direct in/oos", "rate in/oos", "stacked in/oos", "final in/oos"], rows)
    return lines


def stage_section(data: pd.DataFrame) -> list[str]:
    lines = ["## 2. Out of sample MAE by stage, against naive anchors", ""]
    lines.append("Rows with at least 5 prior games. Last column is final vs the best anchor.")
    lines.append("")
    rows = []
    for market in MARKETS:
        part = data[(data["market"] == market) & (data["history_games"] >= MIN_HISTORY)]
        actual = part["actual"].to_numpy(float)
        maes = {name: float(np.mean(np.abs(actual - part[name].to_numpy(float)))) for name in STAGES + ANCHORS}
        best_anchor = min(ANCHORS, key=lambda n: maes[n])
        gap = (maes[best_anchor] - maes["final"]) / maes[best_anchor]
        rows.append(
            [market]
            + [f"{maes[n]:.3f}" for n in STAGES + ANCHORS]
            + [f"{best_anchor} {gap:+.1%}"]
        )
    lines += md_table(["market"] + STAGES + ANCHORS + ["final vs best anchor"], rows)
    return lines


def slope_of(actual: np.ndarray, pred: np.ndarray) -> tuple[float, float]:
    if len(actual) < 100 or pred.std() < 1e-9:
        return float("nan"), float("nan")
    slope, intercept = np.polyfit(pred, actual, 1)
    return float(slope), float(intercept)


def calibration_section(data: pd.DataFrame) -> list[str]:
    lines = ["## 3. Calibration slope (actual regressed on predicted), out of sample", ""]
    lines.append("Per eval season. Near 1 is right. Bias by predicted quintile is final minus actual, as % of the actual mean.")
    lines.append("")
    seasons = sorted(data["eval_season"].unique())
    rows = []
    quint_rows = []
    for market in MARKETS:
        part = data[(data["market"] == market) & (data["history_games"] >= MIN_HISTORY)]
        row = [market]
        for season in seasons:
            sub = part[part["eval_season"] == season]
            slope, _ = slope_of(sub["actual"].to_numpy(float), sub["final"].to_numpy(float))
            row.append(f"{slope:.3f}")
        pooled_slope, _ = slope_of(part["actual"].to_numpy(float), part["final"].to_numpy(float))
        stacked_slope, _ = slope_of(part["actual"].to_numpy(float), part["stacked"].to_numpy(float))
        direct_slope, _ = slope_of(part["actual"].to_numpy(float), part["direct"].to_numpy(float))
        row += [f"{pooled_slope:.3f}", f"{stacked_slope:.3f}", f"{direct_slope:.3f}"]
        rows.append(row)
        quint = pd.qcut(part["final"], 5, labels=False, duplicates="drop")
        mean_actual = part["actual"].mean()
        biases = []
        for q in sorted(quint.dropna().unique()):
            mask = quint == q
            biases.append(f"{(part.loc[mask, 'final'].mean() - part.loc[mask, 'actual'].mean()) / mean_actual:+.1%}")
        quint_rows.append([market] + biases)
    lines += md_table(["market"] + [str(s) for s in seasons] + ["pooled final", "pooled stacked", "pooled direct"], rows)
    lines.append("Bias by predicted quintile of `final` (Q1 lowest, Q5 highest):")
    lines.append("")
    lines += md_table(["market", "Q1", "Q2", "Q3", "Q4", "Q5"], quint_rows)
    return lines


def trust_weight(frame: pd.DataFrame, model_col: str, anchor_col: str) -> float:
    x = (frame[model_col] - frame[anchor_col]).to_numpy(float)
    y = (frame["actual"] - frame[anchor_col]).to_numpy(float)
    denominator = float((x * x).sum())
    return float((x * y).sum() / denominator) if denominator > 1e-12 else float("nan")


def weight_section(data: pd.DataFrame) -> list[str]:
    lines = ["## 4. How much should the model be trusted against the anchor?", ""]
    lines.append(
        "Least squares through the origin on `(actual - anchor) = w * (model - anchor)`, anchor = the project's "
        "`history_anchor`. w = 1 means the stage is already weighted right, w < 1 means it should be pulled "
        "toward the anchor (over trusted), w > 1 means it is under trusted. The leave one season out columns show "
        "how much w moves; a coefficient of variation above 25% is noise."
    )
    lines.append("")
    seasons = sorted(data["eval_season"].unique())
    rows = []
    for market in MARKETS:
        part = data[(data["market"] == market) & (data["history_games"] >= MIN_HISTORY)]
        for stage in ("direct", "stacked", "final"):
            full = trust_weight(part, stage, "history_anchor")
            loo = np.array([trust_weight(part[part["eval_season"] != s], stage, "history_anchor") for s in seasons])
            cv = loo.std() / abs(loo.mean()) if abs(loo.mean()) > 1e-9 else float("nan")
            flag = "unstable" if cv >= 0.25 else ("noisy" if cv >= 0.10 else "ok")
            per_season = " ".join(
                f"{trust_weight(part[part['eval_season'] == s], stage, 'history_anchor'):.2f}" for s in seasons
            )
            rows.append([market, stage, f"{full:.3f}", per_season, f"{cv:.0%}", flag])
    lines += md_table(["market", "stage", "w (all)", "w by season (" + " ".join(map(str, seasons)) + ")", "loo cv", "verdict"], rows)
    return lines


def cap_section(data: pd.DataFrame) -> list[str]:
    lines = ["## 5. Do the hand set caps bind?", ""]
    lines.append(
        "`pooled` is the mean before the stabilize caps, `final` after. Rows capped = final below pooled by "
        "more than 0.01. Bias is predicted minus actual. If the capped rows are biased low, the caps cost accuracy."
    )
    lines.append("")
    rows = []
    for market in MARKETS:
        part = data[(data["market"] == market) & (data["history_games"] >= MIN_HISTORY)]
        capped = (part["pooled"] - part["final"]) > 0.01
        if capped.sum() < 30:
            rows.append([market, f"{capped.mean():.1%}", "n/a", "n/a", "n/a"])
            continue
        c = part[capped]
        bias_pooled = (c["pooled"] - c["actual"]).mean()
        bias_final = (c["final"] - c["actual"]).mean()
        mae_pooled = (c["pooled"] - c["actual"]).abs().mean()
        mae_final = (c["final"] - c["actual"]).abs().mean()
        rows.append(
            [market, f"{capped.mean():.1%}", f"{bias_pooled:+.2f} -> {bias_final:+.2f}", f"{mae_pooled:.3f}", f"{mae_final:.3f}"]
        )
    lines += md_table(["market", "rows capped", "bias on capped rows (pooled -> final)", "MAE pooled", "MAE final"], rows)
    return lines


def curve_section(curve: pd.DataFrame) -> list[str]:
    lines = ["## 6. Out of sample RMSE of the direct XGBoost model by tree count", ""]
    lines.append("Production uses 220 trees. If val RMSE bottoms out well before that while train RMSE keeps falling, it is overfitting.")
    lines.append("")
    rows = []
    for market in MARKETS:
        part = curve[curve["market"] == market].groupby("trees")[["train_rmse", "val_rmse"]].mean()
        best = int(part["val_rmse"].idxmin())
        rows.append(
            [market]
            + [f"{part.loc[t, 'val_rmse']:.3f}" for t in part.index]
            + [f"{part.loc[220, 'train_rmse']:.3f}", str(best)]
        )
    trees = sorted(curve["trees"].unique())
    lines += md_table(["market"] + [f"val@{t}" for t in trees] + ["train@220", "best trees"], rows)
    return lines


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()

    data = pd.read_parquet(DIAG / "nba_stage_predictions.parquet")
    insample = pd.read_csv(DIAG / "nba_insample_vs_oos.csv")
    curve = pd.read_csv(DIAG / "nba_tree_curve.csv")
    seasons = sorted(data["eval_season"].unique())
    header = [
        "# NBA generalization tables",
        "",
        f"Eval seasons {seasons}, each fit on all earlier seasons. Rows per market: "
        f"{len(data) // len(MARKETS):,}. Generated by scripts/diag/generalization_report.py.",
        "",
    ]
    lines = (
        header
        + gap_section(insample)
        + stage_section(data)
        + calibration_section(data)
        + weight_section(data)
        + cap_section(data)
        + curve_section(curve)
    )
    text = "\n".join(lines)
    print(text)
    if args.out is not None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(text + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()

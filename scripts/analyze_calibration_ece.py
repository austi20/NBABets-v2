"""Offline analysis of a captured calibration OOF dump.

Usage:
    .venv/Scripts/python.exe scripts/analyze_calibration_ece.py \
        --dump data/artifacts/calibration_oof_dump.json \
        --out docs/plans/reports/calibration-ece-findings.md
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.training.calibration_ece import load_calibration_oof, summarize_fold_subsets  # noqa: E402

FOLD_COUNTS = [2, 4, 6, 8, 10, 12, 15, 20]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dump", default="data/artifacts/calibration_oof_dump.json")
    parser.add_argument("--out", default="docs/plans/reports/calibration-ece-findings.md")
    parser.add_argument("--percentile", type=float, default=99.0)
    parser.add_argument("--n-boot", type=int, default=2000)
    args = parser.parse_args()

    data = load_calibration_oof(args.dump)
    rng = np.random.default_rng(42)
    lines: list[str] = ["# Calibration ECE Findings", ""]
    lines.append(f"Source: `{args.dump}` | null percentile: p{args.percentile:.0f} | n_boot: {args.n_boot}")
    lines.append("")
    for market, payload in data["markets"].items():
        fold_probs = [np.asarray(probs, dtype=float) for probs in payload["fold_probs"]]
        fold_labels = [np.asarray(labels, dtype=float) for labels in payload["fold_labels"]]
        rows = summarize_fold_subsets(
            fold_probs,
            fold_labels,
            fold_counts=FOLD_COUNTS,
            percentile=args.percentile,
            n_boot=args.n_boot,
            rng=rng,
        )
        lines.append(f"## {market}")
        lines.append("")
        lines.append("| folds | n | ECE | null_floor_p99 | null_floor_p95 | verdict |")
        lines.append("|---|---:|---:|---:|---:|---|")
        for row in rows:
            verdict = "OK" if row["passed"] else "MISCALIBRATED"
            lines.append(
                f"| {row['fold_count']} | {row['sample_count']} | {row['ece']:.4f} | "
                f"{row['null_floor_p99']:.4f} | {row['null_floor_p95']:.4f} | {verdict} |"
            )
        lines.append("")

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines), encoding="utf-8")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()

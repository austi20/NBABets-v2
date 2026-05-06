from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path


def _run_build_dataset(
    csv: Path,
    out_dir: Path,
    *,
    val_from: str,
    curricula: str,
    max_rows: int | None,
    seed_dir: Path,
) -> None:
    cmd = [
        sys.executable,
        "-m",
        "llm_train.dataset.build_dataset",
        "--csv",
        str(csv),
        "--out-dir",
        str(out_dir),
        "--val-from",
        val_from,
        "--curricula",
        curricula,
        "--seed-dir",
        str(seed_dir),
    ]
    if max_rows is not None:
        cmd.extend(["--max-rows", str(max_rows)])
    subprocess.run(cmd, check=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="If CSV is newer than state file, rebuild JSONL dataset and print suggested train command.",
    )
    parser.add_argument("--csv", type=Path, required=True)
    parser.add_argument("--state-file", type=Path, default=Path("llm_train/outputs/.continuous_state.json"))
    parser.add_argument("--out-dir", type=Path, default=Path("llm_train/outputs/dataset_latest"))
    parser.add_argument("--val-from", type=str, default="2026-03-01")
    parser.add_argument("--curricula", type=str, default="csv_qa,local_autonomy,automation")
    parser.add_argument("--max-rows", type=int, default=None)
    parser.add_argument(
        "--seed-dir",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "curricula",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Run dataset build when CSV is newer (default only prints plan).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore mtime check and rebuild when --execute is set.",
    )
    args = parser.parse_args(argv)

    if not args.csv.is_file():
        print(f"CSV not found: {args.csv}", file=sys.stderr)
        return 1

    csv_mtime = datetime.fromtimestamp(args.csv.stat().st_mtime, tz=UTC)
    should_run = args.force
    last_mtime: str | None = None
    if args.state_file.is_file():
        try:
            state = json.loads(args.state_file.read_text(encoding="utf-8"))
            last_mtime = state.get("csv_mtime_utc")
        except json.JSONDecodeError:
            last_mtime = None
        if last_mtime != csv_mtime.isoformat():
            should_run = True
    else:
        should_run = True

    plan = {
        "csv": str(args.csv),
        "csv_mtime_utc": csv_mtime.isoformat(),
        "should_rebuild_dataset": should_run,
        "suggested_train_cmd": (
            f"{sys.executable} -m llm_train.train.sft_lora "
            f"--train-jsonl {args.out_dir / 'train.jsonl'} "
            f"--output-dir llm_train/outputs/lora_run_{csv_mtime.strftime('%Y%m%d')}"
        ),
    }
    print(json.dumps(plan, indent=2))

    if args.execute and should_run:
        args.state_file.parent.mkdir(parents=True, exist_ok=True)
        _run_build_dataset(
            args.csv,
            args.out_dir,
            val_from=args.val_from,
            curricula=args.curricula,
            max_rows=args.max_rows,
            seed_dir=args.seed_dir,
        )
        args.state_file.write_text(
            json.dumps({"csv_mtime_utc": csv_mtime.isoformat(), "updated_at": datetime.now(UTC).isoformat()}, indent=2),
            encoding="utf-8",
        )
        print("Dataset rebuild completed.", file=sys.stderr)
    elif args.execute and not should_run:
        print("Skipping rebuild: CSV mtime matches state (use --force to rebuild).", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

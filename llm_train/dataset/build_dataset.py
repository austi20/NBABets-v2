from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from collections.abc import Iterator
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from llm_train.dataset.projectors import csv_row_to_messages

DEFAULT_VAL_FROM = "2026-03-01"


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _sha256_file(path: Path, *, chunk_size: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def _iter_seed_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def _git_sha() -> str | None:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=Path(__file__).resolve().parents[2],
            check=False,
            capture_output=True,
            text=True,
        )
        if out.returncode == 0:
            return out.stdout.strip()
    except OSError:
        return None
    return None


def build_from_csv(
    csv_path: Path,
    *,
    val_from: date,
    curricula: set[str],
    max_rows: int | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    train: list[dict[str, Any]] = []
    val: list[dict[str, Any]] = []
    if "csv_qa" not in curricula:
        return train, val

    usecols = [
        "game_date",
        "game_id",
        "player_name",
        "player_team",
        "opponent",
        "home_team",
        "away_team",
        "market",
        "sportsbook",
        "line_value",
        "over_odds",
        "under_odds",
        "actual",
        "hit_over",
        "hit_under",
        "push",
        "minutes",
        "source",
    ]
    reader = pd.read_csv(csv_path, usecols=lambda c: c in usecols, chunksize=10_000)
    count = 0
    for chunk in reader:
        chunk["game_date"] = pd.to_datetime(chunk["game_date"], errors="coerce").dt.date
        for _, row in chunk.iterrows():
            gd = row["game_date"]
            if not isinstance(gd, date):
                continue
            record = csv_row_to_messages(row, row_index=count)
            if gd >= val_from:
                val.append(record)
            else:
                train.append(record)
            count += 1
            if max_rows is not None and count >= max_rows:
                return train, val
    return train, val


def append_seeds(train: list[dict[str, Any]], seed_dir: Path, curricula: set[str]) -> None:
    if "local_autonomy" in curricula:
        path = seed_dir / "seed_autonomy.jsonl"
        if path.is_file():
            train.extend(_iter_seed_jsonl(path))
    if "automation" in curricula:
        path = seed_dir / "seed_automation.jsonl"
        if path.is_file():
            train.extend(_iter_seed_jsonl(path))


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build JSONL SFT dataset for local Qwen accuracy examiner.")
    parser.add_argument("--csv", type=Path, help="Path to nba_props CSV (required if csv_qa in curricula).")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("llm_train/outputs/dataset"),
        help="Output directory for train.jsonl, val.jsonl, manifest.json",
    )
    parser.add_argument(
        "--val-from",
        type=str,
        default=DEFAULT_VAL_FROM,
        help="Rows with game_date >= this (ISO) go to val (csv_qa only).",
    )
    parser.add_argument(
        "--curricula",
        type=str,
        default="csv_qa,local_autonomy,automation",
        help="Comma list: csv_qa, local_autonomy, automation",
    )
    parser.add_argument("--max-rows", type=int, default=None, help="Max CSV rows to scan (csv_qa).")
    parser.add_argument(
        "--seed-dir",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "curricula",
        help="Directory containing seed_autonomy.jsonl and seed_automation.jsonl",
    )
    parser.add_argument("--schema-version", type=str, default="1")
    args = parser.parse_args(argv)

    curricula = {c.strip() for c in args.curricula.split(",") if c.strip()}
    if "csv_qa" in curricula and args.csv is None:
        parser.error("--csv is required when csv_qa is included in --curricula")

    val_from = _parse_date(args.val_from)
    train: list[dict[str, Any]] = []
    val: list[dict[str, Any]] = []

    if "csv_qa" in curricula:
        assert args.csv is not None
        t, v = build_from_csv(args.csv, val_from=val_from, curricula=curricula, max_rows=args.max_rows)
        train.extend(t)
        val.extend(v)

    append_seeds(train, args.seed_dir, curricula)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    write_jsonl(args.out_dir / "train.jsonl", train)
    write_jsonl(args.out_dir / "val.jsonl", val)

    csv_sha: str | None = None
    if args.csv is not None and args.csv.is_file():
        csv_sha = _sha256_file(args.csv)

    manifest: dict[str, Any] = {
        "schema_version": args.schema_version,
        "curricula": sorted(curricula),
        "val_from": val_from.isoformat(),
        "train_rows": len(train),
        "val_rows": len(val),
        "csv_path": str(args.csv) if args.csv else None,
        "csv_sha256": csv_sha,
        "seed_dir": str(args.seed_dir),
        "git_sha": _git_sha(),
    }
    (args.out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Phase 8: legacy-vs-rotation inference comparison for promotion safety."""

from __future__ import annotations

import csv
import os
from collections.abc import Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
from sqlalchemy.orm import Session

from app.config.settings import get_settings
from app.schemas.domain import PropPrediction
from app.training.pipeline import TrainingPipeline


@dataclass(frozen=True)
class RotationShadowCompareSummary:
    report_date: date
    run_dir: Path
    overlap_count: int
    legacy_only: int
    shadow_only: int
    mean_abs_delta_calibrated: float | None
    mean_abs_delta_projected_mean: float | None
    markdown_lines: tuple[str, ...]


@contextmanager
def _legacy_vs_full_env(*, legacy: bool):
    keys = ("ROTATION_SHOCK_ENABLED", "ROTATION_SHOCK_ABLATION_MODE")
    preserved = {k: os.environ.get(k) for k in keys}
    try:
        if legacy:
            os.environ["ROTATION_SHOCK_ENABLED"] = "false"
            os.environ["ROTATION_SHOCK_ABLATION_MODE"] = "off"
        else:
            os.environ["ROTATION_SHOCK_ENABLED"] = "true"
            os.environ["ROTATION_SHOCK_ABLATION_MODE"] = "full"
        yield
    finally:
        for k in keys:
            v = preserved[k]
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def build_comparison_frame(
    legacy: Sequence[PropPrediction],
    shadow: Sequence[PropPrediction],
) -> pd.DataFrame:
    lk = {(p.game_id, p.player_id, p.market_key, float(p.sportsbook_line)): p for p in legacy}
    rows: list[dict[str, object]] = []
    for s in shadow:
        key = (s.game_id, s.player_id, s.market_key, float(s.sportsbook_line))
        lrow = lk.get(key)
        if lrow is None:
            continue
        rows.append(
            {
                "game_id": s.game_id,
                "player_id": s.player_id,
                "market_key": s.market_key,
                "line": s.sportsbook_line,
                "legacy_proj_mean": lrow.projected_mean,
                "shadow_proj_mean": s.projected_mean,
                "delta_proj_mean": s.projected_mean - lrow.projected_mean,
                "legacy_calibrated_over": lrow.calibrated_over_probability,
                "shadow_calibrated_over": s.calibrated_over_probability,
                "delta_calibrated_over": s.calibrated_over_probability - lrow.calibrated_over_probability,
                "shadow_dnp_risk": s.dnp_risk,
            }
        )
    return pd.DataFrame(rows)


def _summarize_overlap(df: pd.DataFrame) -> tuple[float | None, float | None]:
    if df.empty:
        return None, None
    return (
        float(df["delta_calibrated_over"].abs().mean()),
        float(df["delta_proj_mean"].abs().mean()),
    )


def _append_rolling(summary: RotationShadowCompareSummary, rolling_path: Path) -> None:
    rolling_path.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(UTC).isoformat()
    row = [
        ts,
        summary.report_date.isoformat(),
        str(summary.overlap_count),
        str(summary.legacy_only),
        str(summary.shadow_only),
        "" if summary.mean_abs_delta_calibrated is None else f"{summary.mean_abs_delta_calibrated:.6g}",
        "" if summary.mean_abs_delta_projected_mean is None else f"{summary.mean_abs_delta_projected_mean:.6g}",
        summary.run_dir.as_posix(),
    ]
    headers = (
        "generated_at_iso",
        "report_date",
        "overlap_predictions",
        "legacy_only_predictions",
        "shadow_only_predictions",
        "mean_abs_delta_calibrated_over",
        "mean_abs_delta_projected_mean",
        "run_dir",
    )
    write_header = not rolling_path.exists()
    with rolling_path.open("a", newline="", encoding="utf-8") as handle:
        w = csv.writer(handle)
        if write_header:
            w.writerow(headers)
        w.writerow(row)


def compare_legacy_and_rotation(
    *,
    pipeline: TrainingPipeline,
    report_date: date,
    persist_authoritative_legacy: bool = False,
    game_ids: set[int] | None = None,
    append_rolling_summary: bool = True,
    reports_root: Path | None = None,
) -> RotationShadowCompareSummary:
    reports_root = reports_root if reports_root is not None else get_settings().reports_dir
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    run_dir = reports_root / "rotation_shadow" / stamp
    run_dir.mkdir(parents=True, exist_ok=True)

    with _legacy_vs_full_env(legacy=True):
        legacy_preds = pipeline.predict_upcoming(
            target_date=report_date,
            game_ids=game_ids,
            persist_predictions=persist_authoritative_legacy,
        )

    with _legacy_vs_full_env(legacy=False):
        shadow_preds = pipeline.predict_upcoming(
            target_date=report_date,
            game_ids=game_ids,
            persist_predictions=False,
        )

    lk = {(p.game_id, p.player_id, p.market_key, float(p.sportsbook_line)) for p in legacy_preds}
    sk = {(p.game_id, p.player_id, p.market_key, float(p.sportsbook_line)) for p in shadow_preds}
    overlap_keys = lk & sk
    legacy_only = len(lk - sk)
    shadow_only = len(sk - lk)

    frame = build_comparison_frame(legacy_preds, shadow_preds)
    detail_path = run_dir / "rotation_shadow_overlap.csv"
    frame.to_csv(detail_path, index=False)

    dac, damp = _summarize_overlap(frame)

    try:
        detail_rel = detail_path.relative_to(reports_root)
    except ValueError:
        detail_rel = detail_path

    lines = (
        "## Rotation Shock Shadow Compare",
        "",
        f"- Report date (slate target): `{report_date.isoformat()}`",
        f"- Overlap predictions (paired): **{len(overlap_keys)}**",
        f"- Legacy-only tuples (game/player/market/line): {legacy_only}",
        f"- Shadow-only triples: {shadow_only}",
        (
            f"- Mean abs Δ calibrated-over: `{dac:.4f}`"
            if dac is not None
            else "- Mean abs Δ calibrated-over: `n/a`"
        ),
        (
            f"- Mean abs Δ projected mean: `{damp:.4f}`"
            if damp is not None
            else "- Mean abs Δ projected mean: `n/a`"
        ),
        "",
        "**Artifacts:**",
        f"- Detail: `{detail_rel}`",
        "",
    )

    summary = RotationShadowCompareSummary(
        report_date=report_date,
        run_dir=run_dir,
        overlap_count=len(overlap_keys),
        legacy_only=legacy_only,
        shadow_only=shadow_only,
        mean_abs_delta_calibrated=dac,
        mean_abs_delta_projected_mean=damp,
        markdown_lines=lines,
    )
    if append_rolling_summary:
        _append_rolling(summary, reports_root / "rotation_shadow" / "rolling_overlap_summary.csv")

    promo = run_dir / "promotion_placeholder.md"
    promo.write_text(
        "\n".join(
            [
                "# Rotation shadow — promotion checklist (manual)",
                "",
                f"- Generated: {datetime.now(UTC).isoformat()}",
                f"- Slate date: {report_date.isoformat()}",
                "",
                "## Operator steps",
                "",
                "1. Spot-check overlap CSV for unreasonable Δ on clean-roster games.",
                "2. Focus review on elevated `shadow_dnp_risk` rows and high magnitude `delta_proj_mean`.",
                "3. After ≥2 basketball weeks plus outcome logs, reconcile against live board before promotion.",
                "",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    return summary


def run_shadow_compare_for_report(
    session: Session,
    report_date: date,
    *,
    reports_root: Path | None = None,
    persist_authoritative_legacy: bool = False,
) -> RotationShadowCompareSummary:
    pipeline = TrainingPipeline(session)
    return compare_legacy_and_rotation(
        pipeline=pipeline,
        report_date=report_date,
        persist_authoritative_legacy=persist_authoritative_legacy,
        append_rolling_summary=True,
        reports_root=reports_root,
    )

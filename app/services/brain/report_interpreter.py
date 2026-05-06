"""Deterministic parser that extracts structured signals from automation reports.

No AI calls — pure regex/structure matching on the known markdown report format.
"""

from __future__ import annotations

import re
from pathlib import Path

from app.services.brain.contracts import DiagnosticSignal


def interpret_report(report_text: str) -> list[DiagnosticSignal]:
    """Parse an automation report markdown string into diagnostic signals."""
    signals: list[DiagnosticSignal] = []
    signals.extend(_parse_data_quality_sentinel(report_text))
    signals.extend(_parse_overfit_signals(report_text))
    signals.extend(_parse_backtest_status(report_text))
    signals.extend(_parse_data_quality_status(report_text))
    signals.extend(_parse_prediction_validator(report_text))
    signals.extend(_parse_release_status(report_text))
    return signals


def interpret_report_file(report_path: str | Path) -> list[DiagnosticSignal]:
    """Read a report file and parse it."""
    text = Path(report_path).read_text(encoding="utf-8")
    return interpret_report(text)


# -- Section Parsers ----------------------------------------------------------


def _parse_data_quality_sentinel(text: str) -> list[DiagnosticSignal]:
    signals: list[DiagnosticSignal] = []

    extreme_match = re.search(
        r"extreme_predictions_today.*?:\s*(\d+)", text, re.IGNORECASE
    )
    divergence_match = re.search(
        r"projection_line_divergences.*?:\s*(\d+)", text, re.IGNORECASE
    )
    sentinel_match = re.search(r"sentinel_status:\s*(\w+)", text, re.IGNORECASE)

    extreme_count = int(extreme_match.group(1)) if extreme_match else 0
    divergence_count = int(divergence_match.group(1)) if divergence_match else 0
    sentinel_status = sentinel_match.group(1).upper() if sentinel_match else "OK"

    if extreme_count > 50:
        signals.append(
            DiagnosticSignal(
                signal_type="extreme_probability",
                severity="critical" if extreme_count > 200 else "high",
                metrics={"extreme_count": extreme_count},
                raw_text=f"sentinel: {extreme_count} extreme predictions",
            )
        )

    if divergence_count > 100:
        signals.append(
            DiagnosticSignal(
                signal_type="projection_divergence",
                severity="critical" if divergence_count > 500 else "high",
                metrics={"divergence_count": divergence_count},
                raw_text=f"sentinel: {divergence_count} projection-line divergences",
            )
        )

    if sentinel_status == "ALERT" and extreme_count > 50:
        # Extract affected players from prediction_validator section
        players = _extract_flagged_players(text)
        if players:
            signals.append(
                DiagnosticSignal(
                    signal_type="dnp_contamination",
                    severity="critical",
                    metrics={
                        "extreme_count": extreme_count,
                        "divergence_count": divergence_count,
                    },
                    affected_players=tuple(players),
                    raw_text="DNP contamination detected via extreme probabilities",
                )
            )

    return signals


def _parse_overfit_signals(text: str) -> list[DiagnosticSignal]:
    signals: list[DiagnosticSignal] = []

    overfit_section = re.search(
        r"### Overfit Signals\s*\n(.*?)(?=\n###|\n## |\Z)",
        text,
        re.DOTALL,
    )
    if not overfit_section:
        return signals

    block = overfit_section.group(1)
    for match in re.finditer(
        r"-\s+(\w+):\s+score=([\d.]+)\s*\(ECE=([\d.]+)",
        block,
    ):
        market = match.group(1)
        score = float(match.group(2))
        ece = float(match.group(3))

        if score >= 0.75:
            signals.append(
                DiagnosticSignal(
                    signal_type="overfit",
                    severity="critical" if score >= 0.90 else "high",
                    market=market,
                    metrics={"overfit_score": score, "ece": ece},
                    raw_text=f"{market}: overfit score={score}, ECE={ece}",
                )
            )
        elif score >= 0.40:
            signals.append(
                DiagnosticSignal(
                    signal_type="calibration_drift",
                    severity="medium",
                    market=market,
                    metrics={"overfit_score": score, "ece": ece},
                    raw_text=f"{market}: calibration drift, ECE={ece}",
                )
            )

    return signals


def _parse_backtest_status(text: str) -> list[DiagnosticSignal]:
    signals: list[DiagnosticSignal] = []

    summary_match = re.search(r"summary_rows.*?\[\s*\]", text)
    if summary_match:
        signals.append(
            DiagnosticSignal(
                signal_type="empty_backtest",
                severity="high",
                metrics={},
                raw_text="Backtest returned empty summary_rows",
            )
        )

    return signals


def _parse_data_quality_status(text: str) -> list[DiagnosticSignal]:
    signals: list[DiagnosticSignal] = []

    dq_match = re.search(r"'status':\s*'(\w+)'", text)
    if dq_match and dq_match.group(1) == "degraded":
        null_match = re.search(r"'numeric_null_fraction':\s*([\d.]+)", text)
        finite_match = re.search(r"'numeric_finite_ratio':\s*([\d.]+)", text)
        null_frac = float(null_match.group(1)) if null_match else 0.0
        finite_ratio = float(finite_match.group(1)) if finite_match else 1.0

        signals.append(
            DiagnosticSignal(
                signal_type="data_quality_degraded",
                severity="high" if null_frac > 0.05 else "medium",
                metrics={
                    "null_fraction": null_frac,
                    "finite_ratio": finite_ratio,
                },
                raw_text=f"Data quality degraded: null_fraction={null_frac}",
            )
        )

    return signals


def _parse_prediction_validator(text: str) -> list[DiagnosticSignal]:
    """Extract per-market DNP contamination signals from validator flags."""
    signals: list[DiagnosticSignal] = []
    players_by_market: dict[str, set[str]] = {}

    for match in re.finditer(
        r"\[critical\]\s+([\w\s]+?)\s+(points|rebounds|assists|threes|turnovers|pra):\s+probability\s+([\d.]+)",
        text,
    ):
        player = match.group(1).strip()
        market = match.group(2)
        players_by_market.setdefault(market, set()).add(player)

    for market, players in players_by_market.items():
        signals.append(
            DiagnosticSignal(
                signal_type="dnp_contamination",
                severity="critical",
                market=market,
                metrics={"affected_player_count": len(players)},
                affected_players=tuple(sorted(players)),
                raw_text=f"DNP contamination in {market}: {len(players)} players",
            )
        )

    return signals


def _parse_release_status(text: str) -> list[DiagnosticSignal]:
    signals: list[DiagnosticSignal] = []

    ece_match = re.search(r"Average ECE\s+(\d+\.\d+)\s+exceeds release ceiling\s+(\d+\.\d+)", text)
    if ece_match:
        avg_ece = float(ece_match.group(1))
        ceiling = float(ece_match.group(2))
        signals.append(
            DiagnosticSignal(
                signal_type="calibration_drift",
                severity="critical",
                metrics={"avg_ece": avg_ece, "ceiling": ceiling},
                raw_text=f"Avg ECE {avg_ece} exceeds ceiling {ceiling}",
            )
        )

    return signals


def _extract_flagged_players(text: str) -> list[str]:
    """Pull unique player names from prediction_validator critical flags."""
    players: set[str] = set()
    for match in re.finditer(
        r"\[critical\]\s+([\w\s]+?)\s+(?:points|rebounds|assists|threes|turnovers|pra):",
        text,
    ):
        players.add(match.group(1).strip())
    return sorted(players)

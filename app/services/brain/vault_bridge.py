"""Bridge between the Brain learning system and the Obsidian vault.

Writes human-readable learning notes and market profiles into the
Obsidian vault at ``E:/AI Brain/ClaudeBrain/`` so persistent knowledge
is available across Claude sessions and tools.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_DEFAULT_VAULT_ROOT = Path("E:/AI Brain/ClaudeBrain")
_PROJECT_AREA = "05 Knowledge and Skills/Data Analysis"
_LEARNING_FOLDER = "NBA Prop Engine Learning"


def _vault_learning_dir(vault_root: Path | None = None) -> Path:
    root = vault_root or _DEFAULT_VAULT_ROOT
    return root / _PROJECT_AREA / _LEARNING_FOLDER


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_correction_note(
    correction_id: int,
    signal_type: str,
    action_type: str,
    market: str | None,
    params_before: dict[str, Any],
    params_after: dict[str, Any],
    ece_before: float | None,
    outcome: str = "pending",
    vault_root: Path | None = None,
) -> Path:
    """Write a single correction event as an Obsidian note."""
    dest = _vault_learning_dir(vault_root) / "Corrections"
    _ensure_dir(dest)
    ts = datetime.now(UTC).strftime("%Y-%m-%d %H%M")
    slug = f"{ts} {signal_type} {market or 'global'}"
    filepath = dest / f"{slug}.md"
    market_display = market or "all"
    ece_str = f"{ece_before:.4f}" if ece_before is not None else "unknown"

    content = f"""---
note_type: "correction"
status: "{outcome}"
memory_priority: "medium"
area: "knowledge-and-skills"
topics:
  - "nba-prop-engine"
  - "brain-learning"
  - "{signal_type}"
llm_ready: true
---
# Correction #{correction_id}: {signal_type} ({market_display})

- **Signal**: {signal_type}
- **Action**: {action_type}
- **Market**: {market_display}
- **ECE before**: {ece_str}
- **Outcome**: {outcome}
- **Date**: {ts}

## Parameters Changed
- **Before**: {params_before}
- **After**: {params_after}

Related: [[NBA Prop Engine Brain]] · [[NBA Prop Engine Local AI Integration]]
"""
    try:
        filepath.write_text(content, encoding="utf-8")
        logger.info("Wrote correction note: %s", filepath)
    except OSError:
        logger.warning("Failed to write correction note to vault: %s", filepath)
    return filepath


def write_market_profile_note(
    market: str,
    ece_history: list[tuple[str, float]],
    optimal_weights: dict[str, float],
    calibration_strategy: str,
    failure_modes: list[str],
    correction_count: int,
    avg_improvement: float,
    vault_root: Path | None = None,
) -> Path:
    """Write or overwrite a per-market learning profile note."""
    dest = _vault_learning_dir(vault_root) / "Market Profiles"
    _ensure_dir(dest)
    filepath = dest / f"{market.title()} Profile.md"

    ece_lines = "\n".join(f"  - {d}: {v:.4f}" for d, v in ece_history[-20:])
    weight_lines = "\n".join(f"  - {k}: {v:.3f}" for k, v in sorted(optimal_weights.items()))
    failure_lines = "\n".join(f"  - {f}" for f in failure_modes) if failure_modes else "  - none observed"

    content = f"""---
note_type: "market-profile"
status: "active"
memory_priority: "high"
area: "knowledge-and-skills"
topics:
  - "nba-prop-engine"
  - "brain-learning"
  - "{market}"
llm_ready: true
---
# {market.title()} Market Profile

- **Calibration strategy**: {calibration_strategy}
- **Corrections applied**: {correction_count}
- **Average ECE improvement**: {avg_improvement:.4f}

## ECE History (recent)
{ece_lines or "  - no data yet"}

## Optimal Feature Weights
{weight_lines or "  - defaults"}

## Known Failure Modes
{failure_lines}

Related: [[NBA Prop Engine Brain]] · [[NBA Prop Engine Local AI Integration]]
"""
    try:
        filepath.write_text(content, encoding="utf-8")
        logger.info("Wrote market profile note: %s", filepath)
    except OSError:
        logger.warning("Failed to write market profile note to vault: %s", filepath)
    return filepath


def write_strategy_note(
    problem_type: str,
    action_template: str,
    market: str | None,
    success_rate: float,
    avg_improvement: float,
    sample_count: int,
    parameters: dict[str, Any],
    vault_root: Path | None = None,
) -> Path:
    """Write or overwrite a strategy lesson note."""
    dest = _vault_learning_dir(vault_root) / "Strategies"
    _ensure_dir(dest)
    market_slug = market or "global"
    filepath = dest / f"{problem_type} {action_template} {market_slug}.md"

    param_lines = "\n".join(f"  - {k}: {v}" for k, v in parameters.items())

    content = f"""---
note_type: "strategy"
status: "active"
memory_priority: "high"
area: "knowledge-and-skills"
topics:
  - "nba-prop-engine"
  - "brain-learning"
  - "{problem_type}"
llm_ready: true
---
# Strategy: {problem_type} -> {action_template} ({market_slug})

- **Success rate**: {success_rate:.0%} ({sample_count} samples)
- **Avg ECE improvement**: {avg_improvement:.4f}

## Parameters
{param_lines or "  - defaults"}

## When to Apply
When the automation report signals **{problem_type}** for market **{market_slug}**,
apply **{action_template}** with the above parameters.

Related: [[NBA Prop Engine Brain]] · [[NBA Prop Engine Local AI Integration]]
"""
    try:
        filepath.write_text(content, encoding="utf-8")
        logger.info("Wrote strategy note: %s", filepath)
    except OSError:
        logger.warning("Failed to write strategy note to vault: %s", filepath)
    return filepath


def write_daily_learning_summary(
    report_date: str,
    signals_found: int,
    corrections_planned: int,
    corrections_executed: int,
    dry_run: bool,
    notes: str = "",
    vault_root: Path | None = None,
) -> Path:
    """Write a daily summary of brain activity."""
    dest = _vault_learning_dir(vault_root) / "Daily Summaries"
    _ensure_dir(dest)
    filepath = dest / f"{report_date} Learning Summary.md"
    mode_label = "DRY RUN" if dry_run else "LIVE"

    content = f"""---
note_type: "daily-summary"
status: "active"
memory_priority: "low"
area: "knowledge-and-skills"
topics:
  - "nba-prop-engine"
  - "brain-learning"
llm_ready: true
---
# Brain Learning Summary: {report_date} ({mode_label})

- **Signals detected**: {signals_found}
- **Corrections planned**: {corrections_planned}
- **Corrections executed**: {corrections_executed}
- **Mode**: {mode_label}

{notes}

Related: [[NBA Prop Engine Brain]] · [[NBA Prop Engine Local AI Integration]]
"""
    try:
        filepath.write_text(content, encoding="utf-8")
    except OSError:
        logger.warning("Failed to write daily summary to vault: %s", filepath)
    return filepath

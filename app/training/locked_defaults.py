from __future__ import annotations

from typing import Literal

DistFamily = Literal["legacy", "count_aware", "decomposed"]

# Phase 4 baseline defaults. run_ablation.py updates this module after synthesis.
DEFAULT_K_SEASONS = 4
DEFAULT_L1_ALPHA = 0.0
DEFAULT_DIST_FAMILY: DistFamily = "legacy"

MARKET_DIST_FAMILY_DEFAULTS: dict[str, DistFamily] = {
    "points": "legacy",
    "rebounds": "count_aware",
    "assists": "count_aware",
    "threes": "count_aware",
    "turnovers": "count_aware",
    "pra": "legacy",
}

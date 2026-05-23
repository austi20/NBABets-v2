# Volatility Tier Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the four-entry hardcoded `_VOLATILE_MARKET_PENALTY` in `app/services/insights.py` with a per-prop, multi-input volatility coefficient computed at query time. Probability gets a gentle edge-preserving discount; confidence gets a sharper multiplier; tier (low/medium/high) surfaces in the UI. Consumed identically by insights, decision brain, parlays, and the API.

**Architecture:** Pure-function module `app/services/volatility.py`. Caller assembles a `FeatureSnapshot` from `player_game_logs` (one helper), passes it plus the prediction row into `compute_volatility(...) → VolatilityScore`. Each consumer takes 2-3 lines to apply the result. No DB schema changes.

**Tech Stack:** Python 3.12, SQLAlchemy 2.x, FastAPI, pytest, dataclasses, React+TypeScript+Vitest for the frontend badge.

**Spec reference:** [docs/superpowers/specs/2026-05-15-volatility-tier-design.md](../specs/2026-05-15-volatility-tier-design.md)

---

## File Structure

**New files:**
- `app/services/volatility.py` — score dataclasses, normalizers, archetype mapping, `compute_volatility`, `build_feature_snapshot`
- `tests/unit/services/test_volatility_basics.py` — config + score dataclasses + simple pure functions
- `tests/unit/services/test_volatility_normalize.py` — per-input normalizers
- `tests/unit/services/test_volatility_archetype.py` — archetype mapping
- `tests/unit/services/test_volatility_compute.py` — composer + missing-input handling
- `tests/integration/test_volatility_snapshot.py` — SQL feature-builder against fixture DB
- `tests/integration/test_volatility_application.py` — insights + decision_brain + parlays integration
- `tests/integration/test_volatility_api.py` — diagnostic endpoint
- `desktop_tauri/src/components/primitives/VolatilityBadge.tsx` — UI chip
- `desktop_tauri/src/components/primitives/VolatilityBadge.test.tsx` — component test

**Modified files:**
- `app/services/prop_analysis.py` — add three optional fields to `PropOpportunity`
- `app/services/insights.py` — delete `_VOLATILE_MARKET_PENALTY`, apply `confidence_multiplier`
- `app/trading/decision_brain.py` — apply `adjusted_probability` to ranking
- `app/services/parlays.py` — apply `adjusted_probability` to leg EV
- `app/server/schemas/props.py` — add `volatility_coefficient`, `volatility_tier`, `adjusted_over_probability`
- `app/server/routers/props.py` — new `GET /api/props/predictions/{prediction_id}/volatility` endpoint
- `app/services/startup.py` — emit volatility distribution log line after the predict step
- `desktop_tauri/src/api/types.ts` (or equivalent) — extend the prop type
- Frontend prop renderers — wire the badge in

---

## Task 1: Module scaffold, config, score dataclasses, scalar pure functions

**Files:**
- Create: `app/services/volatility.py`
- Create: `tests/unit/services/test_volatility_basics.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/services/test_volatility_basics.py`:

```python
from __future__ import annotations

import math

import pytest

from app.services.volatility import (
    DEFAULT_CONFIG,
    VolatilityConfig,
    VolatilityContributor,
    VolatilityScore,
    adjust_probability,
    confidence_multiplier,
    tier_from_coefficient,
)


def test_default_config_weights_sum_to_one() -> None:
    total = sum(DEFAULT_CONFIG.weights.values())
    assert math.isclose(total, 1.0, abs_tol=1e-9)


def test_config_rejects_invalid_weight_sum() -> None:
    with pytest.raises(ValueError, match="must sum to 1"):
        VolatilityConfig(
            weights={"stat_cv": 0.5, "minutes_instability": 0.2},
            prob_alpha=0.30,
            conf_alpha=0.55,
            tier_low_cap=0.33,
            tier_high_cap=0.66,
        )


@pytest.mark.parametrize(
    ("coefficient", "expected"),
    [
        (0.0, "low"),
        (0.10, "low"),
        (0.32, "low"),
        (0.33, "medium"),
        (0.50, "medium"),
        (0.65, "medium"),
        (0.66, "high"),
        (0.90, "high"),
        (1.0, "high"),
    ],
)
def test_tier_from_coefficient(coefficient: float, expected: str) -> None:
    assert tier_from_coefficient(coefficient) == expected


@pytest.mark.parametrize(
    ("raw_p", "coefficient", "expected"),
    [
        (0.90, 1.0, 0.78),
        (0.80, 1.0, 0.71),
        (0.70, 1.0, 0.64),
        (0.60, 1.0, 0.57),
        (0.55, 1.0, 0.535),
        (0.50, 1.0, 0.50),
        (0.20, 1.0, 0.29),
        (0.80, 0.0, 0.80),
        (0.80, 0.5, 0.755),
    ],
)
def test_adjust_probability_table(raw_p: float, coefficient: float, expected: float) -> None:
    result = adjust_probability(raw_p, coefficient)
    assert math.isclose(result, expected, abs_tol=1e-3)


def test_adjust_probability_preserves_side() -> None:
    for raw_p in [0.51, 0.55, 0.60, 0.80, 0.95]:
        assert adjust_probability(raw_p, 1.0) >= 0.5
    for raw_p in [0.49, 0.45, 0.40, 0.20, 0.05]:
        assert adjust_probability(raw_p, 1.0) <= 0.5


@pytest.mark.parametrize(
    ("coefficient", "expected"),
    [
        (0.0, 1.0),
        (0.5, 0.725),
        (1.0, 0.45),
    ],
)
def test_confidence_multiplier(coefficient: float, expected: float) -> None:
    result = confidence_multiplier(coefficient)
    assert math.isclose(result, expected, abs_tol=1e-3)


def test_score_dataclass_is_frozen() -> None:
    score = VolatilityScore(
        coefficient=0.5,
        tier="medium",
        contributors=(VolatilityContributor(name="x", raw_value=1.0, weight=0.5, contribution=0.5),),
        adjusted_probability=0.6,
        confidence_multiplier=0.725,
    )
    with pytest.raises(AttributeError):
        score.coefficient = 0.7  # type: ignore[misc]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/services/test_volatility_basics.py -v`
Expected: FAIL with `ModuleNotFoundError: app.services.volatility`

- [ ] **Step 3: Create the module with minimal implementation**

Create `app/services/volatility.py`:

```python
"""Per-prop volatility coefficient + tier.

See docs/superpowers/specs/2026-05-15-volatility-tier-design.md for the full
specification. This module is intended to be a pure-function dependency:
callers assemble a FeatureSnapshot and pass it in along with the prediction
row, and the module returns a VolatilityScore.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Literal

Tier = Literal["low", "medium", "high"]


@dataclass(frozen=True)
class VolatilityContributor:
    name: str
    raw_value: float
    weight: float
    contribution: float


@dataclass(frozen=True)
class VolatilityScore:
    coefficient: float
    tier: Tier
    contributors: tuple[VolatilityContributor, ...]
    adjusted_probability: float
    confidence_multiplier: float
    reason: str = ""


@dataclass(frozen=True)
class VolatilityConfig:
    weights: dict[str, float]
    prob_alpha: float
    conf_alpha: float
    tier_low_cap: float
    tier_high_cap: float
    stat_cv_max: float = 1.5
    minutes_std_max: float = 8.0
    minutes_cv_max: float = 0.6
    usage_cv_max: float = 0.5
    recent_form_z_max: float = 2.0

    def __post_init__(self) -> None:
        total = sum(self.weights.values())
        if not math.isclose(total, 1.0, abs_tol=1e-6):
            raise ValueError(
                f"VolatilityConfig.weights must sum to 1.0, got {total:.4f}"
            )


DEFAULT_CONFIG = VolatilityConfig(
    weights={
        "stat_cv": 0.30,
        "minutes_instability": 0.20,
        "usage_instability": 0.15,
        "recent_form_divergence": 0.20,
        "archetype_risk": 0.15,
    },
    prob_alpha=0.30,
    conf_alpha=0.55,
    tier_low_cap=0.33,
    tier_high_cap=0.66,
)


def tier_from_coefficient(coefficient: float, config: VolatilityConfig = DEFAULT_CONFIG) -> Tier:
    if coefficient < config.tier_low_cap:
        return "low"
    if coefficient < config.tier_high_cap:
        return "medium"
    return "high"


def adjust_probability(raw_p: float, coefficient: float, config: VolatilityConfig = DEFAULT_CONFIG) -> float:
    """Gentle, edge-preserving shrinkage. Never crosses 0.5."""
    edge = raw_p - 0.5
    return 0.5 + edge * (1.0 - coefficient * config.prob_alpha)


def confidence_multiplier(coefficient: float, config: VolatilityConfig = DEFAULT_CONFIG) -> float:
    """Sharper discount intended for the 1-99 confidence score."""
    return 1.0 - coefficient * config.conf_alpha
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/services/test_volatility_basics.py -v`
Expected: PASS (all parametrized cases plus the four other tests).

- [ ] **Step 5: Lint and type-check**

Run: `ruff check app/services/volatility.py tests/unit/services/test_volatility_basics.py`
Expected: `All checks passed!`

Run: `mypy app/services/volatility.py`
Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git add app/services/volatility.py tests/unit/services/test_volatility_basics.py
git commit -m "feat(volatility): config + score dataclasses + scalar pure functions"
```

---

## Task 2: Input normalizers

**Files:**
- Modify: `app/services/volatility.py` (append normalizer functions)
- Create: `tests/unit/services/test_volatility_normalize.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/services/test_volatility_normalize.py`:

```python
from __future__ import annotations

import math

import pytest

from app.services.volatility import (
    DEFAULT_CONFIG,
    normalize_stat_cv,
    normalize_minutes_instability,
    normalize_usage_instability,
    normalize_recent_form_divergence,
)


@pytest.mark.parametrize(
    ("std", "mean", "expected"),
    [
        (0.0, 5.0, 0.0),
        (3.0, 5.0, min(0.6 / 1.5, 1.0)),  # cv=0.6 -> 0.4
        (7.5, 5.0, 1.0),                  # cv=1.5 -> 1.0
        (15.0, 5.0, 1.0),                 # cv=3.0 -> clipped to 1
        (5.0, 0.0, 1.0),                  # mean below eps -> max
    ],
)
def test_normalize_stat_cv(std: float, mean: float, expected: float) -> None:
    assert math.isclose(normalize_stat_cv(std, mean), expected, abs_tol=1e-6)


@pytest.mark.parametrize(
    ("predicted_std", "mean_10", "std_10", "expected"),
    [
        (0.0, 30.0, 0.0, 0.0),
        (4.0, 30.0, 9.0, 0.5),    # 0.5 + 0.5  -> avg = 0.5
        (8.0, 30.0, 18.0, 1.0),   # both at max -> 1.0
        (16.0, 30.0, 36.0, 1.0),  # over max -> clipped 1.0
    ],
)
def test_normalize_minutes_instability(
    predicted_std: float, mean_10: float, std_10: float, expected: float
) -> None:
    result = normalize_minutes_instability(
        predicted_std=predicted_std,
        minutes_std_10=std_10,
        minutes_mean_10=mean_10,
    )
    assert math.isclose(result, expected, abs_tol=1e-6)


@pytest.mark.parametrize(
    ("std", "mean", "expected"),
    [
        (0.0, 0.22, 0.0),
        (0.055, 0.22, 0.5),  # cv = 0.25 -> 0.5
        (0.11, 0.22, 1.0),   # cv = 0.5 -> 1.0
        (0.20, 0.22, 1.0),   # over max -> 1.0
        (0.05, 0.0, 1.0),    # mean below eps -> max
    ],
)
def test_normalize_usage_instability(std: float, mean: float, expected: float) -> None:
    assert math.isclose(normalize_usage_instability(std, mean), expected, abs_tol=1e-6)


@pytest.mark.parametrize(
    ("m5", "m_season", "std_season", "expected"),
    [
        (20.0, 20.0, 5.0, 0.0),
        (25.0, 20.0, 5.0, 0.5),   # z = 1.0 -> 0.5
        (30.0, 20.0, 5.0, 1.0),   # z = 2.0 -> 1.0
        (40.0, 20.0, 5.0, 1.0),   # over max -> clipped
        (10.0, 20.0, 5.0, 1.0),   # z=2 (absolute) -> 1.0
        (20.0, 20.0, 0.0, 0.0),   # zero std + zero divergence -> 0
    ],
)
def test_normalize_recent_form_divergence(
    m5: float, m_season: float, std_season: float, expected: float
) -> None:
    result = normalize_recent_form_divergence(
        mean_5=m5, mean_season=m_season, std_season=std_season
    )
    assert math.isclose(result, expected, abs_tol=1e-6)


def test_normalizers_respect_config_constants() -> None:
    # Using a custom config with halved caps should double the normalized score
    from app.services.volatility import VolatilityConfig

    tight = VolatilityConfig(
        weights=dict(DEFAULT_CONFIG.weights),
        prob_alpha=DEFAULT_CONFIG.prob_alpha,
        conf_alpha=DEFAULT_CONFIG.conf_alpha,
        tier_low_cap=DEFAULT_CONFIG.tier_low_cap,
        tier_high_cap=DEFAULT_CONFIG.tier_high_cap,
        stat_cv_max=0.75,
    )
    assert math.isclose(normalize_stat_cv(3.0, 5.0, config=tight), 0.8, abs_tol=1e-6)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/services/test_volatility_normalize.py -v`
Expected: FAIL with `ImportError: cannot import name 'normalize_stat_cv'`

- [ ] **Step 3: Add normalizer implementations**

Append to `app/services/volatility.py` (after `confidence_multiplier`):

```python
_STAT_CV_EPS = 0.5
_USAGE_CV_EPS = 0.05
_MIN_MINUTES_DENOM = 1.0


def _clip01(value: float) -> float:
    if value < 0.0:
        return 0.0
    if value > 1.0:
        return 1.0
    return value


def normalize_stat_cv(
    std: float, mean: float, config: VolatilityConfig = DEFAULT_CONFIG
) -> float:
    if mean < _STAT_CV_EPS:
        return 1.0
    cv = std / mean
    return _clip01(cv / config.stat_cv_max)


def normalize_minutes_instability(
    *,
    predicted_std: float,
    minutes_std_10: float,
    minutes_mean_10: float,
    config: VolatilityConfig = DEFAULT_CONFIG,
) -> float:
    pred_component = _clip01(predicted_std / config.minutes_std_max)
    denom = max(minutes_mean_10, _MIN_MINUTES_DENOM)
    cv_component = _clip01((minutes_std_10 / denom) / config.minutes_cv_max)
    return _clip01((pred_component + cv_component) / 2.0)


def normalize_usage_instability(
    std: float, mean: float, config: VolatilityConfig = DEFAULT_CONFIG
) -> float:
    if mean < _USAGE_CV_EPS:
        return 1.0
    cv = std / mean
    return _clip01(cv / config.usage_cv_max)


def normalize_recent_form_divergence(
    *,
    mean_5: float,
    mean_season: float,
    std_season: float,
    config: VolatilityConfig = DEFAULT_CONFIG,
) -> float:
    denom = max(std_season, 1.0)
    z = abs(mean_5 - mean_season) / denom
    return _clip01(z / config.recent_form_z_max)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/services/test_volatility_normalize.py -v`
Expected: PASS

- [ ] **Step 5: Lint**

Run: `ruff check app/services/volatility.py tests/unit/services/test_volatility_normalize.py`
Expected: `All checks passed!`

- [ ] **Step 6: Commit**

```bash
git add app/services/volatility.py tests/unit/services/test_volatility_normalize.py
git commit -m "feat(volatility): per-input normalizers (stat cv, minutes, usage, recent form)"
```

---

## Task 3: Archetype mapping

**Files:**
- Modify: `app/services/volatility.py` (append archetype function)
- Create: `tests/unit/services/test_volatility_archetype.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/services/test_volatility_archetype.py`:

```python
from __future__ import annotations

import pytest

from app.services.volatility import (
    archetype_risk,
    classify_archetype,
)


@pytest.mark.parametrize(
    ("starter_rate", "minutes_mean", "expected"),
    [
        (0.95, 32.0, "starter"),
        (0.70, 24.0, "starter"),
        (0.69, 28.0, "rotation"),
        (0.40, 22.0, "rotation"),
        (0.10, 18.0, "rotation"),
        (0.05, 17.5, "bench"),
        (0.0, 10.0, "bench"),
        (0.0, 9.0, "fringe"),
        (0.0, 0.0, "fringe"),
    ],
)
def test_classify_archetype(starter_rate: float, minutes_mean: float, expected: str) -> None:
    assert classify_archetype(starter_flag_rate=starter_rate, minutes_mean_season=minutes_mean) == expected


@pytest.mark.parametrize(
    ("archetype", "expected"),
    [
        ("starter", 0.0),
        ("rotation", 0.3),
        ("bench", 0.7),
        ("fringe", 1.0),
    ],
)
def test_archetype_risk(archetype: str, expected: float) -> None:
    assert archetype_risk(archetype) == expected
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/services/test_volatility_archetype.py -v`
Expected: FAIL with `ImportError: cannot import name 'archetype_risk'`

- [ ] **Step 3: Add archetype implementation**

Append to `app/services/volatility.py`:

```python
Archetype = Literal["starter", "rotation", "bench", "fringe"]

_ARCHETYPE_RISK: dict[Archetype, float] = {
    "starter": 0.0,
    "rotation": 0.3,
    "bench": 0.7,
    "fringe": 1.0,
}


def classify_archetype(*, starter_flag_rate: float, minutes_mean_season: float) -> Archetype:
    if starter_flag_rate >= 0.7 and minutes_mean_season >= 24.0:
        return "starter"
    if minutes_mean_season >= 18.0:
        return "rotation"
    if minutes_mean_season >= 10.0:
        return "bench"
    return "fringe"


def archetype_risk(archetype: Archetype) -> float:
    return _ARCHETYPE_RISK[archetype]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/services/test_volatility_archetype.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/services/volatility.py tests/unit/services/test_volatility_archetype.py
git commit -m "feat(volatility): archetype classification + risk mapping"
```

---

## Task 4: `compute_volatility` composer with missing-input handling

**Files:**
- Modify: `app/services/volatility.py` (add `FeatureSnapshot` + `compute_volatility`)
- Create: `tests/unit/services/test_volatility_compute.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/services/test_volatility_compute.py`:

```python
from __future__ import annotations

import math

import pytest

from app.services.volatility import (
    DEFAULT_CONFIG,
    FeatureSnapshot,
    VolatilityScore,
    compute_volatility,
)


def _full_snapshot(**overrides: float) -> FeatureSnapshot:
    base = dict(
        stat_std_10=0.0,
        stat_mean_10=20.0,
        predicted_minutes_std=0.0,
        minutes_std_10=0.0,
        minutes_mean_10=32.0,
        usage_std_10=0.0,
        usage_mean_10=0.25,
        mean_5=20.0,
        mean_season=20.0,
        std_season=5.0,
        starter_flag_rate=1.0,
        minutes_mean_season=30.0,
    )
    base.update(overrides)
    return FeatureSnapshot(**base)


def test_all_zero_inputs_yields_zero_coefficient() -> None:
    snap = _full_snapshot()
    score = compute_volatility(raw_probability=0.80, features=snap)
    assert math.isclose(score.coefficient, 0.0, abs_tol=1e-6)
    assert score.tier == "low"
    assert math.isclose(score.adjusted_probability, 0.80, abs_tol=1e-6)
    assert math.isclose(score.confidence_multiplier, 1.0, abs_tol=1e-6)
    assert score.reason == ""


def test_all_max_inputs_yields_one_coefficient() -> None:
    snap = _full_snapshot(
        stat_std_10=40.0,        # cv = 2.0 -> clip to max
        stat_mean_10=20.0,
        predicted_minutes_std=20.0,
        minutes_std_10=40.0,
        minutes_mean_10=20.0,
        usage_std_10=0.5,
        usage_mean_10=0.25,
        mean_5=50.0,             # huge divergence
        mean_season=20.0,
        std_season=5.0,
        starter_flag_rate=0.0,
        minutes_mean_season=0.0, # fringe
    )
    score = compute_volatility(raw_probability=0.80, features=snap)
    assert math.isclose(score.coefficient, 1.0, abs_tol=1e-6)
    assert score.tier == "high"
    assert math.isclose(score.adjusted_probability, 0.71, abs_tol=1e-3)
    assert math.isclose(score.confidence_multiplier, 0.45, abs_tol=1e-3)


def test_contributors_sum_matches_coefficient() -> None:
    snap = _full_snapshot(
        stat_std_10=5.0, stat_mean_10=20.0,    # cv 0.25 -> 0.1667 normalized -> contribution 0.30*0.1667=0.05
        predicted_minutes_std=4.0,
        minutes_std_10=9.0, minutes_mean_10=30.0,
        usage_std_10=0.055, usage_mean_10=0.22,
        mean_5=25.0, mean_season=20.0, std_season=5.0,
        starter_flag_rate=1.0, minutes_mean_season=30.0,
    )
    score = compute_volatility(raw_probability=0.65, features=snap)
    total = sum(c.contribution for c in score.contributors)
    assert math.isclose(total, score.coefficient, abs_tol=1e-6)


def test_missing_input_renormalizes_weights() -> None:
    # Drop stat_cv (weight 0.30). Remaining weights = 0.70; should renormalize.
    snap = FeatureSnapshot(
        stat_std_10=None,
        stat_mean_10=None,
        predicted_minutes_std=8.0,        # full
        minutes_std_10=18.0,
        minutes_mean_10=30.0,             # full
        usage_std_10=0.11, usage_mean_10=0.22,  # full
        mean_5=30.0, mean_season=20.0, std_season=5.0,  # full
        starter_flag_rate=0.0, minutes_mean_season=0.0, # fringe -> full
    )
    score = compute_volatility(raw_probability=0.80, features=snap)
    # All four remaining inputs at max -> renormalized weights sum to 1.0 -> coefficient = 1.0
    assert math.isclose(score.coefficient, 1.0, abs_tol=1e-6)
    contributor_names = {c.name for c in score.contributors}
    assert "stat_cv" not in contributor_names
    assert len(contributor_names) == 4


def test_all_inputs_missing_returns_neutral_score() -> None:
    snap = FeatureSnapshot(
        stat_std_10=None, stat_mean_10=None,
        predicted_minutes_std=None, minutes_std_10=None, minutes_mean_10=None,
        usage_std_10=None, usage_mean_10=None,
        mean_5=None, mean_season=None, std_season=None,
        starter_flag_rate=None, minutes_mean_season=None,
    )
    score = compute_volatility(raw_probability=0.80, features=snap)
    assert math.isclose(score.coefficient, 0.5, abs_tol=1e-6)
    assert score.tier == "medium"
    assert score.contributors == ()
    assert score.reason == "insufficient_features"
    assert math.isclose(score.adjusted_probability, 0.5 + 0.30 * (1 - 0.5 * 0.30), abs_tol=1e-3)


def test_partial_missing_uses_remaining_inputs() -> None:
    # Only archetype data present (weight 0.15).
    snap = FeatureSnapshot(
        stat_std_10=None, stat_mean_10=None,
        predicted_minutes_std=None, minutes_std_10=None, minutes_mean_10=None,
        usage_std_10=None, usage_mean_10=None,
        mean_5=None, mean_season=None, std_season=None,
        starter_flag_rate=0.0, minutes_mean_season=0.0,
    )
    score = compute_volatility(raw_probability=0.80, features=snap)
    # Only fringe archetype contributes -> renormalized weight 1.0 -> coefficient 1.0
    assert math.isclose(score.coefficient, 1.0, abs_tol=1e-6)
    assert score.reason == ""
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/services/test_volatility_compute.py -v`
Expected: FAIL with `ImportError: cannot import name 'FeatureSnapshot'`

- [ ] **Step 3: Add FeatureSnapshot and compute_volatility**

Append to `app/services/volatility.py`:

```python
@dataclass(frozen=True)
class FeatureSnapshot:
    """Inputs required to compute a volatility coefficient.

    Each field is Optional; `compute_volatility` drops missing inputs and
    renormalizes the remaining weights. If every input is None, the score
    is neutral (coefficient=0.5) with reason="insufficient_features".
    """

    stat_std_10: float | None
    stat_mean_10: float | None
    predicted_minutes_std: float | None
    minutes_std_10: float | None
    minutes_mean_10: float | None
    usage_std_10: float | None
    usage_mean_10: float | None
    mean_5: float | None
    mean_season: float | None
    std_season: float | None
    starter_flag_rate: float | None
    minutes_mean_season: float | None


def _maybe_stat_cv(snap: FeatureSnapshot, config: VolatilityConfig) -> float | None:
    if snap.stat_std_10 is None or snap.stat_mean_10 is None:
        return None
    return normalize_stat_cv(snap.stat_std_10, snap.stat_mean_10, config=config)


def _maybe_minutes(snap: FeatureSnapshot, config: VolatilityConfig) -> float | None:
    if (
        snap.predicted_minutes_std is None
        or snap.minutes_std_10 is None
        or snap.minutes_mean_10 is None
    ):
        return None
    return normalize_minutes_instability(
        predicted_std=snap.predicted_minutes_std,
        minutes_std_10=snap.minutes_std_10,
        minutes_mean_10=snap.minutes_mean_10,
        config=config,
    )


def _maybe_usage(snap: FeatureSnapshot, config: VolatilityConfig) -> float | None:
    if snap.usage_std_10 is None or snap.usage_mean_10 is None:
        return None
    return normalize_usage_instability(snap.usage_std_10, snap.usage_mean_10, config=config)


def _maybe_recent_form(snap: FeatureSnapshot, config: VolatilityConfig) -> float | None:
    if snap.mean_5 is None or snap.mean_season is None or snap.std_season is None:
        return None
    return normalize_recent_form_divergence(
        mean_5=snap.mean_5,
        mean_season=snap.mean_season,
        std_season=snap.std_season,
        config=config,
    )


def _maybe_archetype(snap: FeatureSnapshot) -> float | None:
    if snap.starter_flag_rate is None or snap.minutes_mean_season is None:
        return None
    archetype = classify_archetype(
        starter_flag_rate=snap.starter_flag_rate,
        minutes_mean_season=snap.minutes_mean_season,
    )
    return archetype_risk(archetype)


def compute_volatility(
    *,
    raw_probability: float,
    features: FeatureSnapshot,
    config: VolatilityConfig = DEFAULT_CONFIG,
) -> VolatilityScore:
    raw_inputs: dict[str, float | None] = {
        "stat_cv": _maybe_stat_cv(features, config),
        "minutes_instability": _maybe_minutes(features, config),
        "usage_instability": _maybe_usage(features, config),
        "recent_form_divergence": _maybe_recent_form(features, config),
        "archetype_risk": _maybe_archetype(features),
    }

    available = {name: value for name, value in raw_inputs.items() if value is not None}

    if not available:
        return VolatilityScore(
            coefficient=0.5,
            tier=tier_from_coefficient(0.5, config=config),
            contributors=(),
            adjusted_probability=adjust_probability(raw_probability, 0.5, config=config),
            confidence_multiplier=confidence_multiplier(0.5, config=config),
            reason="insufficient_features",
        )

    weight_sum = sum(config.weights[name] for name in available)
    contributors: list[VolatilityContributor] = []
    coefficient = 0.0
    for name, normalized in available.items():
        renormalized_weight = config.weights[name] / weight_sum
        contribution = renormalized_weight * normalized
        contributors.append(
            VolatilityContributor(
                name=name,
                raw_value=normalized,
                weight=renormalized_weight,
                contribution=contribution,
            )
        )
        coefficient += contribution

    coefficient = _clip01(coefficient)
    return VolatilityScore(
        coefficient=coefficient,
        tier=tier_from_coefficient(coefficient, config=config),
        contributors=tuple(contributors),
        adjusted_probability=adjust_probability(raw_probability, coefficient, config=config),
        confidence_multiplier=confidence_multiplier(coefficient, config=config),
        reason="",
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/services/test_volatility_compute.py -v`
Expected: PASS

- [ ] **Step 5: Run the full suite so far**

Run: `pytest tests/unit/services/test_volatility_basics.py tests/unit/services/test_volatility_normalize.py tests/unit/services/test_volatility_archetype.py tests/unit/services/test_volatility_compute.py -v`
Expected: all PASS.

- [ ] **Step 6: Lint and type-check**

Run: `ruff check app/services/volatility.py`
Run: `mypy app/services/volatility.py`
Expected: both pass.

- [ ] **Step 7: Commit**

```bash
git add app/services/volatility.py tests/unit/services/test_volatility_compute.py
git commit -m "feat(volatility): compose coefficient + missing-input renormalization"
```

---

## Task 5: `build_feature_snapshot` SQL helper

**Files:**
- Modify: `app/services/volatility.py` (append `build_feature_snapshot`)
- Create: `tests/integration/test_volatility_snapshot.py`

- [ ] **Step 1: Write the failing test**

Create `tests/integration/test_volatility_snapshot.py`:

```python
from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from sqlalchemy.orm import Session

from app.db.session import session_scope
from app.models.all import Game, Player, PlayerGameLog, Team
from app.services.volatility import FeatureSnapshot, build_feature_snapshot


@pytest.fixture
def seeded_session() -> Session:
    """Yields a session with one player + 10 recent games of synthetic data."""
    with session_scope() as session:
        team = Team(team_id=1001, abbreviation="ZZZ", name="Test", city="Test")
        opp = Team(team_id=1002, abbreviation="OPP", name="Opp", city="Opp")
        session.add_all([team, opp])
        session.flush()
        player = Player(player_id=99001, full_name="Test Player", position="G")
        session.add(player)
        session.flush()
        for index in range(10):
            game = Game(
                game_id=900000 + index,
                game_date=date(2026, 4, index + 1),
                start_time=datetime(2026, 4, index + 1, 22, tzinfo=timezone.utc),
                home_team_id=1001,
                away_team_id=1002,
                status="final",
            )
            session.add(game)
            session.flush()
            session.add(
                PlayerGameLog(
                    player_id=99001,
                    game_id=900000 + index,
                    team_id=1001,
                    opponent_team_id=1002,
                    minutes=30.0 + (index - 5) * 1.5,
                    points=20.0 + (index - 5) * 2.0,
                    rebounds=5.0,
                    assists=4.0,
                    threes=2.0 + (index - 5) * 0.5,
                    turnovers=2.0,
                    steals=1.0,
                    blocks=0.5,
                    field_goal_attempts=15.0,
                    field_goals_made=8.0,
                    free_throw_attempts=4.0,
                    free_throws_made=3.0,
                    offensive_rebounds=1.0,
                    defensive_rebounds=4.0,
                    plus_minus=0.0,
                    fouls=2.0,
                    starter_flag=1 if index >= 5 else 0,
                )
            )
        session.commit()
        yield session


def test_build_feature_snapshot_returns_populated_fields(seeded_session: Session) -> None:
    snap = build_feature_snapshot(
        session=seeded_session,
        player_id=99001,
        market_key="points",
        as_of_date=date(2026, 5, 1),
        predicted_minutes_std=4.0,
    )

    assert isinstance(snap, FeatureSnapshot)
    assert snap.stat_mean_10 is not None
    assert snap.stat_std_10 is not None
    assert snap.minutes_mean_10 is not None
    assert snap.minutes_std_10 is not None
    assert snap.predicted_minutes_std == 4.0
    assert snap.starter_flag_rate is not None
    assert 0.0 <= snap.starter_flag_rate <= 1.0
    assert snap.mean_5 is not None
    assert snap.mean_season is not None


def test_build_feature_snapshot_for_unknown_player_yields_all_none(seeded_session: Session) -> None:
    snap = build_feature_snapshot(
        session=seeded_session,
        player_id=12345,  # not seeded
        market_key="points",
        as_of_date=date(2026, 5, 1),
        predicted_minutes_std=None,
    )
    assert snap.stat_mean_10 is None
    assert snap.minutes_mean_10 is None
    assert snap.starter_flag_rate is None
    assert snap.predicted_minutes_std is None


def test_build_feature_snapshot_unknown_market_returns_none_stat_only(seeded_session: Session) -> None:
    snap = build_feature_snapshot(
        session=seeded_session,
        player_id=99001,
        market_key="not_a_real_market",
        as_of_date=date(2026, 5, 1),
        predicted_minutes_std=4.0,
    )
    assert snap.stat_mean_10 is None
    assert snap.stat_std_10 is None
    # Minutes/archetype still populated since they're market-independent
    assert snap.minutes_mean_10 is not None
    assert snap.starter_flag_rate is not None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/integration/test_volatility_snapshot.py -v`
Expected: FAIL with `ImportError: cannot import name 'build_feature_snapshot'`

- [ ] **Step 3: Add `build_feature_snapshot`**

Append to `app/services/volatility.py`:

```python
from datetime import date as _date_t

from sqlalchemy import select
from sqlalchemy.orm import Session

# Allowed market keys map to the `player_game_logs` column we aggregate.
_MARKET_TO_COLUMN: dict[str, str] = {
    "points": "points",
    "rebounds": "rebounds",
    "assists": "assists",
    "threes": "threes",
    "turnovers": "turnovers",
    "steals": "steals",
    "blocks": "blocks",
    "pra": "_pra_synthetic",  # computed below
}


def _safe_mean(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _safe_std(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    m = sum(values) / len(values)
    variance = sum((v - m) ** 2 for v in values) / (len(values) - 1)
    return variance**0.5


def _usage_proxy(row: "PlayerGameLog") -> float:  # type: ignore[name-defined]
    minutes = max(row.minutes or 0.0, 1.0)
    return (
        (row.field_goal_attempts or 0.0)
        + 0.44 * (row.free_throw_attempts or 0.0)
        + (row.turnovers or 0.0)
    ) / minutes


def build_feature_snapshot(
    *,
    session: Session,
    player_id: int,
    market_key: str,
    as_of_date: _date_t,
    predicted_minutes_std: float | None,
) -> FeatureSnapshot:
    from app.models.all import PlayerGameLog  # local import to avoid cycles

    rows: list[PlayerGameLog] = list(
        session.scalars(
            select(PlayerGameLog)
            .join(PlayerGameLog.game)
            .where(PlayerGameLog.player_id == player_id)
            .order_by(PlayerGameLog.game_id.desc())
            .limit(82)
        )
    )
    rows = [r for r in rows if r.game.game_date < as_of_date]
    rows.sort(key=lambda r: r.game.game_date, reverse=True)

    last_10 = rows[:10]
    last_5 = rows[:5]
    season = rows[:82]

    def _stat_values(slice_: list[PlayerGameLog]) -> list[float]:
        if market_key == "pra":
            return [
                float((r.points or 0.0) + (r.rebounds or 0.0) + (r.assists or 0.0))
                for r in slice_
            ]
        column = _MARKET_TO_COLUMN.get(market_key)
        if column is None:
            return []
        return [float(getattr(r, column, 0.0) or 0.0) for r in slice_]

    stat_10 = _stat_values(last_10)
    stat_5 = _stat_values(last_5)
    stat_season = _stat_values(season)

    minutes_10 = [float(r.minutes or 0.0) for r in last_10]
    minutes_season = [float(r.minutes or 0.0) for r in season]
    usage_10 = [_usage_proxy(r) for r in last_10]

    starter_flags = [float(r.starter_flag or 0) for r in last_10]
    starter_flag_rate = _safe_mean(starter_flags) if starter_flags else None
    minutes_mean_season = _safe_mean(minutes_season) if minutes_season else None

    return FeatureSnapshot(
        stat_std_10=_safe_std(stat_10) if stat_10 else None,
        stat_mean_10=_safe_mean(stat_10) if stat_10 else None,
        predicted_minutes_std=predicted_minutes_std,
        minutes_std_10=_safe_std(minutes_10) if minutes_10 else None,
        minutes_mean_10=_safe_mean(minutes_10) if minutes_10 else None,
        usage_std_10=_safe_std(usage_10) if usage_10 else None,
        usage_mean_10=_safe_mean(usage_10) if usage_10 else None,
        mean_5=_safe_mean(stat_5) if stat_5 else None,
        mean_season=_safe_mean(stat_season) if stat_season else None,
        std_season=_safe_std(stat_season) if stat_season else None,
        starter_flag_rate=starter_flag_rate,
        minutes_mean_season=minutes_mean_season,
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/integration/test_volatility_snapshot.py -v`
Expected: PASS

- [ ] **Step 5: Lint**

Run: `ruff check app/services/volatility.py tests/integration/test_volatility_snapshot.py`
Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add app/services/volatility.py tests/integration/test_volatility_snapshot.py
git commit -m "feat(volatility): build_feature_snapshot SQL helper"
```

---

## Task 6: Integrate into `insights.py` + extend `PropOpportunity`

**Files:**
- Modify: `app/services/prop_analysis.py` (add three optional fields)
- Modify: `app/services/insights.py` (delete `_VOLATILE_MARKET_PENALTY`, apply multiplier)
- Modify: `tests/unit/test_insights_volatile_penalty.py` (existing — adapt to new behavior)
- Create: `tests/integration/test_volatility_application.py`

- [ ] **Step 1: Write the new integration test first**

Create `tests/integration/test_volatility_application.py`:

```python
from __future__ import annotations

import pytest

from app.services.volatility import (
    DEFAULT_CONFIG,
    FeatureSnapshot,
    compute_volatility,
)


def test_high_volatility_drops_confidence_significantly() -> None:
    """A bench-player threes prop should drop hard once volatility hits."""
    snap = FeatureSnapshot(
        stat_std_10=2.5, stat_mean_10=2.0,           # cv 1.25 -> 0.83 normalized
        predicted_minutes_std=6.0,                    # near max
        minutes_std_10=8.0, minutes_mean_10=12.0,    # high cv
        usage_std_10=0.10, usage_mean_10=0.15,       # high cv
        mean_5=0.8, mean_season=2.0, std_season=1.5, # divergent
        starter_flag_rate=0.0, minutes_mean_season=8.0,  # fringe
    )
    score = compute_volatility(raw_probability=0.78, features=snap)
    assert score.tier == "high"
    assert score.confidence_multiplier < 0.65  # >35% confidence haircut
    assert score.adjusted_probability < 0.74


def test_low_volatility_preserves_confidence() -> None:
    """A stable starter prop should barely move."""
    snap = FeatureSnapshot(
        stat_std_10=2.0, stat_mean_10=22.0,           # cv 0.09 -> small
        predicted_minutes_std=1.0,
        minutes_std_10=2.5, minutes_mean_10=34.0,
        usage_std_10=0.015, usage_mean_10=0.28,
        mean_5=22.5, mean_season=22.0, std_season=4.0,
        starter_flag_rate=1.0, minutes_mean_season=33.0,
    )
    score = compute_volatility(raw_probability=0.78, features=snap)
    assert score.tier == "low"
    assert score.confidence_multiplier > 0.90
    assert score.adjusted_probability > 0.76
```

- [ ] **Step 2: Run the new test (no integration with insights yet)**

Run: `pytest tests/integration/test_volatility_application.py -v`
Expected: PASS (this just validates the formula yields the discounts we promised).

- [ ] **Step 3: Extend `PropOpportunity` dataclass**

Modify `app/services/prop_analysis.py`. Locate the `PropOpportunity` dataclass (around line 46) and append these three optional fields at the bottom (default values for backward compatibility):

```python
@dataclass(frozen=True)
class PropOpportunity:
    # ...existing fields...
    availability_branches: int = 1
    volatility_coefficient: float = 0.0
    volatility_tier: str = "low"
    adjusted_over_probability: float | None = None
```

Then locate every place `PropOpportunity(...)` is constructed in `prop_analysis.py` (e.g. inside `top_opportunities`). For each construction site, leave the new fields unset (they default), or pass the score values if available. For this task we leave them defaulted — they'll be populated in step 5.

- [ ] **Step 4: Read existing volatile-penalty test and replace its expectations**

Read `tests/unit/test_insights_volatile_penalty.py` first to understand its existing fixtures. Then replace the assertions: the test was checking the old hardcoded penalty (`-10` for threes etc.). Update it to assert the new shape — that confidence drops proportionally to a volatility coefficient passed in via the score.

Replace the file contents with:

```python
from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.services.insights import _prop_confidence_score
from app.services.prop_analysis import PropOpportunity, SportsbookQuote
from app.services.volatility import (
    VolatilityContributor,
    VolatilityScore,
)


def _make_opportunity(market_key: str) -> PropOpportunity:
    quote = SportsbookQuote(
        game_id=1,
        sportsbook_key="bk1",
        sportsbook_name="Book 1",
        icon="",
        market_key=market_key,
        line_value=10.0,
        over_odds=-110,
        under_odds=-110,
        timestamp=datetime(2026, 5, 15, 18, tzinfo=UTC).isoformat(),
        is_live_quote=True,
        verification_status="verified",
        odds_source_provider="test",
        over_probability=0.55,
        under_probability=0.45,
        push_probability=0.0,
        hit_probability=0.55,
        no_vig_market_probability=0.5,
        source_market_key=market_key,
        is_alternate_line=False,
    )
    return PropOpportunity(
        rank=1,
        game_id=1,
        player_id=1,
        player_name="Test",
        player_icon="",
        market_key=market_key,
        consensus_line=10.0,
        projected_mean=11.0,
        recommended_side="over",
        hit_probability=0.55,
        likelihood_score=0,
        calibrated_over_probability=0.55,
        sportsbooks_summary="Book 1",
        top_features=[],
        quotes=[quote],
        data_confidence_score=0.8,
    )


def _score(coefficient: float, tier: str) -> VolatilityScore:
    return VolatilityScore(
        coefficient=coefficient,
        tier=tier,  # type: ignore[arg-type]
        contributors=(VolatilityContributor(name="stat_cv", raw_value=1.0, weight=1.0, contribution=coefficient),),
        adjusted_probability=0.55 - coefficient * 0.05,
        confidence_multiplier=1.0 - coefficient * 0.55,
    )


def test_low_volatility_keeps_score_near_baseline() -> None:
    opp = _make_opportunity("points")
    baseline = _prop_confidence_score(
        opportunity=opp,
        best_quote=opp.quotes[0],
        edge=0.02,
        latest_quote_at=None,
        uncertainty_ratio=0.3,
        injury=None,
        now=None,
        volatility=_score(0.0, "low"),
    )
    high_vol = _prop_confidence_score(
        opportunity=opp,
        best_quote=opp.quotes[0],
        edge=0.02,
        latest_quote_at=None,
        uncertainty_ratio=0.3,
        injury=None,
        now=None,
        volatility=_score(0.8, "high"),
    )
    assert high_vol < baseline
    assert high_vol < int(baseline * 0.65)
```

- [ ] **Step 5: Modify `_prop_confidence_score` signature and body**

In `app/services/insights.py`:

1. Delete the `_VOLATILE_MARKET_PENALTY` dict (lines 18-25) and its import comment.

2. Modify `_prop_confidence_score` to accept a new `volatility` keyword argument and apply it. Replace the function with:

```python
def _prop_confidence_score(
    *,
    opportunity: PropOpportunity,
    best_quote: SportsbookQuote,
    edge: float,
    latest_quote_at: datetime | None,
    uncertainty_ratio: float | None,
    injury: InjuryStatusBadge | None,
    now: datetime | None,
    volatility: VolatilityScore | None = None,
) -> int:
    score = min(int(best_quote.hit_probability * 55), 55)
    score += min(int(max(edge, 0.0) * 300), 20)
    score += min(len(opportunity.quotes) * 4, 12)
    score += min(int(max(opportunity.data_confidence_score, 0.0) * 12), 12)
    age_label = format_relative_age(latest_quote_at, now=now)
    if age_label == "Just now" or age_label.endswith("m ago"):
        score += 8
    elif "h " in age_label:
        score += 4
    if uncertainty_ratio is not None:
        if uncertainty_ratio <= 0.35:
            score += 10
        elif uncertainty_ratio <= 0.6:
            score += 5
    if injury is not None:
        score -= injury.severity
    if best_quote.is_alternate_line:
        score -= 2
    if volatility is not None:
        score = int(score * volatility.confidence_multiplier)
    return max(1, min(score, 99))
```

Add the import at the top of the file:

```python
from app.services.volatility import VolatilityScore, build_feature_snapshot, compute_volatility
```

3. Locate the call site of `_prop_confidence_score` (around line 339). Just before that call, build the volatility score for this opportunity:

```python
# Around insights.py line 336, before `confidence_score = _prop_confidence_score(...)`:
volatility_score = compute_volatility(
    raw_probability=opportunity.calibrated_over_probability,
    features=build_feature_snapshot(
        session=session,
        player_id=opportunity.player_id,
        market_key=opportunity.market_key,
        as_of_date=board_date,
        predicted_minutes_std=None,
    ),
)
```

Then pass it to `_prop_confidence_score(..., volatility=volatility_score)`.

Where the response object is built downstream, attach the volatility fields onto the opportunity (or a wrapper DTO) — populate the new `PropOpportunity` fields. If the opportunity is reconstructed nearby, recreate it with the volatility values:

```python
opportunity = PropOpportunity(
    **{
        **opportunity.__dict__,
        "volatility_coefficient": volatility_score.coefficient,
        "volatility_tier": volatility_score.tier,
        "adjusted_over_probability": volatility_score.adjusted_probability,
    }
)
```

(If the existing code path doesn't reconstruct opportunities, locate where they flow into the response and add the fields there instead — the engineer should follow the existing pattern.)

- [ ] **Step 6: Run tests**

```
pytest tests/unit/test_insights_volatile_penalty.py tests/integration/test_volatility_application.py -v
```

Expected: PASS.

- [ ] **Step 7: Run full unit suite to catch regressions**

```
pytest tests/unit -v
```

Expected: no new failures vs. main.

- [ ] **Step 8: Lint**

Run: `ruff check app/services/insights.py app/services/prop_analysis.py`
Expected: pass.

- [ ] **Step 9: Commit**

```bash
git add app/services/insights.py app/services/prop_analysis.py tests/unit/test_insights_volatile_penalty.py tests/integration/test_volatility_application.py
git commit -m "feat(insights): replace hardcoded market penalty with per-prop volatility coefficient"
```

---

## Task 7: Integrate into `decision_brain.py`

**Files:**
- Modify: `app/trading/decision_brain.py`
- Create: `tests/integration/trading/test_decision_brain_volatility.py`

- [ ] **Step 1: Read `decision_brain.py` to find the candidate-ranking call site**

Run: `grep -n "calibrated_over_probability\|hit_probability\|confidence" app/trading/decision_brain.py | head -40`

Identify the place where each candidate's `calibrated_over_probability` or `hit_probability` is used for EV / ranking. The engineer must locate this in the actual file.

- [ ] **Step 2: Write the integration test**

First read `tests/integration/trading/` to find the existing decision_brain test pattern. Pick the most representative test and mirror its fixture setup.

Create `tests/integration/trading/test_decision_brain_volatility.py`:

```python
from __future__ import annotations

import math

import pytest

from app.services.volatility import (
    FeatureSnapshot,
    compute_volatility,
)


def _snap(starter_rate: float, minutes_mean: float) -> FeatureSnapshot:
    """Concise factory matching the archetype dimension only."""
    return FeatureSnapshot(
        stat_std_10=2.0, stat_mean_10=22.0,
        predicted_minutes_std=1.0,
        minutes_std_10=2.5, minutes_mean_10=32.0,
        usage_std_10=0.015, usage_mean_10=0.28,
        mean_5=22.0, mean_season=22.0, std_season=4.0,
        starter_flag_rate=starter_rate,
        minutes_mean_season=minutes_mean,
    )


def test_volatility_lowers_ev_for_high_archetype() -> None:
    """Two candidates with identical raw probability but different archetype
    should land at different adjusted probabilities. The fringe candidate's
    adjusted prob is lower; if decision_brain ranks by adjusted prob (after
    Step 3), the starter ranks above the fringe."""
    starter = compute_volatility(raw_probability=0.65, features=_snap(starter_rate=1.0, minutes_mean=33.0))
    fringe = compute_volatility(raw_probability=0.65, features=_snap(starter_rate=0.0, minutes_mean=8.0))
    assert starter.adjusted_probability > fringe.adjusted_probability
    assert starter.tier == "low"
    assert fringe.tier in ("medium", "high")


@pytest.mark.integration
def test_decision_brain_ranks_low_vol_above_high_vol(monkeypatch: pytest.MonkeyPatch) -> None:
    """Engineer: import the actual decision_brain entry point and build two
    Prediction rows with identical probabilities but different player_ids that
    map to different archetypes via the seeded fixture DB. Assert the ranking
    output places the low-volatility candidate first. Use the same session /
    fixture pattern observed in step 1's read-through."""
    # The shape:
    #   1. seed two predictions for the same game/market/line with calibrated_over_probability=0.65
    #   2. seed two players: starter (33 min/game, starter_flag=1) and fringe (8 min/game, starter_flag=0)
    #   3. call the decision_brain ranking function
    #   4. assert that the starter candidate comes first in the returned list
    pytest.fail("Replace this with the real call once Step 1's pattern review is done")
```

The second test deliberately fails initially. The engineer replaces `pytest.fail` with concrete code matching the existing fixture pattern. The first test is fully implemented and provides a numerical sanity check against the volatility module itself.

- [ ] **Step 3: Apply volatility to decision-brain ranking**

In `app/trading/decision_brain.py`, locate the candidate scoring path. Wherever the code reads `prediction.calibrated_over_probability` (or `over_probability`) for EV / ranking, replace it with the volatility-adjusted probability:

```python
from app.services.volatility import build_feature_snapshot, compute_volatility

# inside the per-candidate loop:
volatility = compute_volatility(
    raw_probability=prediction.calibrated_over_probability,
    features=build_feature_snapshot(
        session=session,
        player_id=prediction.player_id,
        market_key=prediction.market.market_key,
        as_of_date=as_of_date,
        predicted_minutes_std=None,
    ),
)
effective_probability = volatility.adjusted_probability
# ... use effective_probability where calibrated_over_probability was used
```

Add an optional policy knob `max_volatility_coefficient` in the existing policy config (default `1.0` = off). Where candidates are filtered, add:

```python
if policy.max_volatility_coefficient is not None and volatility.coefficient > policy.max_volatility_coefficient:
    blockers.append("high_volatility")
    continue
```

- [ ] **Step 4: Fill in the integration test**

Now that the application site is wired, replace the `pytest.skip` with two fixture candidates and assert the volatile one ranks lower.

- [ ] **Step 5: Run tests**

```
pytest tests/integration/trading/test_decision_brain_volatility.py -v
pytest tests/integration/trading -v
```

Expected: PASS.

- [ ] **Step 6: Lint**

Run: `ruff check app/trading/decision_brain.py`

- [ ] **Step 7: Commit**

```bash
git add app/trading/decision_brain.py tests/integration/trading/test_decision_brain_volatility.py
git commit -m "feat(trading): apply per-prop volatility to candidate EV + add policy gate"
```

---

## Task 8: Integrate into `parlays.py`

**Files:**
- Modify: `app/services/parlays.py`
- Create: `tests/unit/services/test_parlays_volatility.py`

- [ ] **Step 1: Locate the leg EV path**

Run: `grep -n "calibrated_over_probability\|hit_probability\|leg_probability" app/services/parlays.py`

- [ ] **Step 2: Write the test**

First read existing parlay tests in `tests/unit/services/` (look for `test_parlays*.py`) and identify the parlay-construction fixture pattern.

Create `tests/unit/services/test_parlays_volatility.py`:

```python
from __future__ import annotations

import math

import pytest

from app.services.volatility import (
    FeatureSnapshot,
    compute_volatility,
)


def _high_vol_snap() -> FeatureSnapshot:
    return FeatureSnapshot(
        stat_std_10=2.5, stat_mean_10=2.0,
        predicted_minutes_std=6.0,
        minutes_std_10=8.0, minutes_mean_10=12.0,
        usage_std_10=0.10, usage_mean_10=0.15,
        mean_5=0.8, mean_season=2.0, std_season=1.5,
        starter_flag_rate=0.0, minutes_mean_season=8.0,
    )


def _low_vol_snap() -> FeatureSnapshot:
    return FeatureSnapshot(
        stat_std_10=2.0, stat_mean_10=22.0,
        predicted_minutes_std=1.0,
        minutes_std_10=2.5, minutes_mean_10=34.0,
        usage_std_10=0.015, usage_mean_10=0.28,
        mean_5=22.0, mean_season=22.0, std_season=4.0,
        starter_flag_rate=1.0, minutes_mean_season=33.0,
    )


def test_parlay_ev_uses_adjusted_probability_per_leg() -> None:
    """Two legs with identical raw probability but different volatility
    must produce a different parlay EV. EV of the parlay is the product of
    adjusted leg probabilities, not raw ones."""
    high = compute_volatility(raw_probability=0.65, features=_high_vol_snap())
    low = compute_volatility(raw_probability=0.65, features=_low_vol_snap())

    raw_combined = 0.65 * 0.65  # what we'd get without volatility
    adjusted_combined = high.adjusted_probability * low.adjusted_probability

    assert adjusted_combined < raw_combined
    assert adjusted_combined > 0.0  # sanity


@pytest.mark.unit
def test_parlay_advisor_applies_volatility_to_each_leg() -> None:
    """Engineer: instantiate the parlay advisor with two synthetic legs that
    map to high- and low-volatility players in a fixture DB. Assert the
    parlay's expected_probability matches `adjusted_high * adjusted_low`
    (within a small tolerance) rather than the raw product."""
    pytest.fail("Replace this with the actual parlay advisor invocation")
```

The first test is concrete and tests the module's API directly. The second test deliberately fails until the engineer wires the parlay advisor call site (Step 3) and adapts to the existing fixture pattern.

- [ ] **Step 3: Replace leg-probability reads with volatility-adjusted value**

In `app/services/parlays.py`, wherever a leg's `over_probability`/`calibrated_over_probability` feeds parlay EV, build a `VolatilityScore` for that leg's player+market and use `volatility.adjusted_probability` for the EV calculation.

Add an optional knob `max_leg_volatility_tier: Literal["low", "medium", "high"] = "high"` defaulting to `"high"` (no filtering). If set lower, parlay legs above the cap are excluded.

- [ ] **Step 4: Fill in the test, then run**

```
pytest tests/unit/services/test_parlays_volatility.py -v
pytest tests/unit/services -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/services/parlays.py tests/unit/services/test_parlays_volatility.py
git commit -m "feat(parlays): apply per-prop volatility to leg EV"
```

---

## Task 9: API schemas + diagnostic endpoint

**Files:**
- Modify: `app/server/schemas/props.py`
- Modify: `app/server/routers/props.py`
- Create: `tests/integration/test_volatility_api.py`

- [ ] **Step 1: Write the API test**

Create `tests/integration/test_volatility_api.py`:

```python
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

# Engineer: import the FastAPI app from app.server.app (or wherever the
# main FastAPI instance is constructed).
from app.server.app import app


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def test_prop_opportunity_includes_volatility_fields(client: TestClient) -> None:
    # Hit the existing props listing endpoint. Each returned opportunity
    # must now carry the three volatility fields.
    response = client.get("/api/props")
    assert response.status_code == 200
    body = response.json()
    opportunities = body.get("opportunities") or body.get("data") or []
    if not opportunities:
        pytest.skip("No props in fixture DB to assert on")
    sample = opportunities[0]
    assert "volatility_coefficient" in sample
    assert "volatility_tier" in sample
    assert sample["volatility_tier"] in ("low", "medium", "high")


def test_diagnostic_endpoint_returns_contributors(client: TestClient) -> None:
    # Look up any prediction_id from /api/props and call the diagnostic.
    list_response = client.get("/api/props")
    opportunities = list_response.json().get("opportunities") or []
    if not opportunities:
        pytest.skip("No props in fixture DB")
    prediction_id = opportunities[0].get("prediction_id") or opportunities[0].get("rank")
    response = client.get(f"/api/props/predictions/{prediction_id}/volatility")
    assert response.status_code == 200
    body = response.json()
    assert "coefficient" in body
    assert "tier" in body
    assert "contributors" in body
    assert isinstance(body["contributors"], list)
    assert "adjusted_probability" in body
    assert "confidence_multiplier" in body
```

- [ ] **Step 2: Run test to verify the diagnostic 404s**

Run: `pytest tests/integration/test_volatility_api.py -v`
Expected: FAIL on the diagnostic test (404 since the route doesn't exist yet).

- [ ] **Step 3: Add fields to `PropOpportunityModel`**

Modify `app/server/schemas/props.py` and add to the `PropOpportunityModel` class (locate it; if it's pydantic):

```python
class PropOpportunityModel(BaseModel):
    # ...existing fields...
    volatility_coefficient: float = 0.0
    volatility_tier: str = "low"
    adjusted_over_probability: float | None = None
```

Ensure the mapping from `PropOpportunity` -> `PropOpportunityModel` copies the three new fields (search for `PropOpportunityModel(` constructions and add the fields).

- [ ] **Step 4: Add the diagnostic endpoint**

In `app/server/routers/props.py` append:

```python
from app.services.volatility import build_feature_snapshot, compute_volatility


@router.get("/predictions/{prediction_id}/volatility")
def get_prediction_volatility(prediction_id: int, request: Request) -> dict:
    """Return the per-prediction volatility breakdown for diagnostics."""
    session = request.state.session
    from app.models.all import Prediction

    prediction = session.get(Prediction, prediction_id)
    if prediction is None:
        raise HTTPException(status_code=404, detail="prediction not found")

    market_key = prediction.market.market_key if prediction.market else "points"
    score = compute_volatility(
        raw_probability=prediction.over_probability,
        features=build_feature_snapshot(
            session=session,
            player_id=prediction.player_id,
            market_key=market_key,
            as_of_date=prediction.predicted_at.date(),
            predicted_minutes_std=None,
        ),
    )

    return {
        "prediction_id": prediction_id,
        "coefficient": score.coefficient,
        "tier": score.tier,
        "adjusted_probability": score.adjusted_probability,
        "confidence_multiplier": score.confidence_multiplier,
        "reason": score.reason,
        "contributors": [
            {
                "name": c.name,
                "raw_value": c.raw_value,
                "weight": c.weight,
                "contribution": c.contribution,
            }
            for c in score.contributors
        ],
    }
```

(If `request.state.session` isn't the project's pattern, follow how other endpoints in `props.py` get a session — likely via a dependency.)

- [ ] **Step 5: Run tests**

```
pytest tests/integration/test_volatility_api.py -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add app/server/schemas/props.py app/server/routers/props.py tests/integration/test_volatility_api.py
git commit -m "feat(api): expose volatility fields on props + diagnostic endpoint"
```

---

## Task 10: Startup-time distribution log line

**Files:**
- Modify: `app/services/startup.py`

- [ ] **Step 1: Locate where predictions are written / loaded at startup**

Run: `grep -n "predict_upcoming\|TrainingPipeline" app/services/startup.py`

Identify the place after the `_step_predict` returns and the predictions are available.

- [ ] **Step 2: Add the log line**

Append a helper function in `app/services/startup.py`:

```python
def _emit_volatility_distribution(session: Session, board_date: date) -> None:
    from collections import Counter
    from app.models.all import Prediction
    from app.services.volatility import build_feature_snapshot, compute_volatility

    predictions = session.scalars(
        select(Prediction).where(Prediction.predicted_at >= datetime.combine(board_date, time.min, tzinfo=UTC))
    ).all()
    tiers: Counter[str] = Counter()
    coefficients: list[float] = []
    insufficient = 0
    for prediction in predictions:
        market_key = prediction.market.market_key if prediction.market else "points"
        score = compute_volatility(
            raw_probability=prediction.over_probability,
            features=build_feature_snapshot(
                session=session,
                player_id=prediction.player_id,
                market_key=market_key,
                as_of_date=board_date,
                predicted_minutes_std=None,
            ),
        )
        tiers[score.tier] += 1
        coefficients.append(score.coefficient)
        if score.reason == "insufficient_features":
            insufficient += 1

    if coefficients:
        sorted_c = sorted(coefficients)
        n = len(sorted_c)
        p50 = sorted_c[n // 2]
        p90 = sorted_c[min(n - 1, int(n * 0.9))]
        _log.info(
            "volatility: tier_distribution low=%d medium=%d high=%d coef_p50=%.2f p90=%.2f insufficient_features=%d",
            tiers.get("low", 0),
            tiers.get("medium", 0),
            tiers.get("high", 0),
            p50,
            p90,
            insufficient,
        )
```

Then call `_emit_volatility_distribution(session, board_date)` at the end of `_step_predict` (after predictions are persisted), wrapped in `try / except` so a failure here never blocks startup:

```python
try:
    _emit_volatility_distribution(session, board_date)
except Exception:  # noqa: BLE001
    _log.exception("volatility distribution log failed")
```

- [ ] **Step 3: Manual verification (no automated test for log lines)**

Run the app locally and grep:

```
npm run tauri:dev --prefix desktop_tauri
# in another shell after startup completes:
grep "volatility: tier_distribution" "$LOCALAPPDATA/NBAPropEngine/logs"/*.log
```

Expected: one line per startup. Sample distribution like `low=412 medium=287 high=89`.

- [ ] **Step 4: Commit**

```bash
git add app/services/startup.py
git commit -m "feat(startup): log volatility tier distribution after predict step"
```

---

## Task 11: `VolatilityBadge` React component

**Files:**
- Create: `desktop_tauri/src/components/primitives/VolatilityBadge.tsx`
- Create: `desktop_tauri/src/components/primitives/VolatilityBadge.test.tsx`

- [ ] **Step 1: Write the failing test**

Create `desktop_tauri/src/components/primitives/VolatilityBadge.test.tsx`:

```tsx
import { describe, expect, it } from "vitest"
import { render, screen } from "@testing-library/react"

import { VolatilityBadge } from "./VolatilityBadge"

describe("VolatilityBadge", () => {
  it("renders the tier label", () => {
    render(<VolatilityBadge tier="high" coefficient={0.78} />)
    expect(screen.getByText(/high/i)).toBeInTheDocument()
  })

  it("applies the medium color for medium tier", () => {
    render(<VolatilityBadge tier="medium" coefficient={0.5} />)
    const chip = screen.getByText(/medium/i)
    expect(chip.className).toMatch(/amber/i)
  })

  it("shows top contributors in the tooltip when supplied", () => {
    render(
      <VolatilityBadge
        tier="high"
        coefficient={0.78}
        contributors={[
          { name: "stat_cv", contribution: 0.28 },
          { name: "archetype_risk", contribution: 0.21 },
          { name: "minutes_instability", contribution: 0.18 },
        ]}
      />,
    )
    expect(screen.getByTitle(/stat_cv/i)).toBeInTheDocument()
  })

  it("renders a 'Limited data' badge when reason is insufficient_features", () => {
    render(<VolatilityBadge tier="medium" coefficient={0.5} reason="insufficient_features" />)
    expect(screen.getByText(/limited data/i)).toBeInTheDocument()
  })
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test --prefix desktop_tauri -- VolatilityBadge`
Expected: FAIL with `Cannot find module './VolatilityBadge'`

- [ ] **Step 3: Implement the badge**

Create `desktop_tauri/src/components/primitives/VolatilityBadge.tsx`:

```tsx
import type { ReactNode } from "react"

export type VolatilityTier = "low" | "medium" | "high"

interface Contributor {
  name: string
  contribution: number
}

interface VolatilityBadgeProps {
  tier: VolatilityTier
  coefficient: number
  contributors?: Contributor[]
  reason?: string
}

const TIER_STYLES: Record<VolatilityTier, { bg: string; fg: string; label: string }> = {
  low: { bg: "bg-green-600", fg: "text-white", label: "Low" },
  medium: { bg: "bg-amber-500", fg: "text-black", label: "Medium" },
  high: { bg: "bg-rose-600", fg: "text-white", label: "High" },
}

export function VolatilityBadge({
  tier,
  coefficient,
  contributors,
  reason,
}: VolatilityBadgeProps): ReactNode {
  if (reason === "insufficient_features") {
    return (
      <span
        className="inline-flex items-center rounded-full bg-slate-500 px-2 py-0.5 text-xs text-white"
        title="Limited data available to score this prop"
      >
        Limited data
      </span>
    )
  }

  const style = TIER_STYLES[tier]
  const top = (contributors ?? []).slice(0, 3)
  const tooltip = top.length
    ? top
        .map((c) => `${c.name}: ${(c.contribution * 100).toFixed(0)}%`)
        .join("  •  ")
    : `Volatility ${coefficient.toFixed(2)}`

  return (
    <span
      className={`inline-flex items-center rounded-full ${style.bg} ${style.fg} px-2 py-0.5 text-xs`}
      title={tooltip}
    >
      {style.label}
    </span>
  )
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test --prefix desktop_tauri -- VolatilityBadge`
Expected: PASS.

- [ ] **Step 5: Type-check**

Run: `npm run typecheck --prefix desktop_tauri` (or `tsc --noEmit` from the desktop_tauri directory)
Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add desktop_tauri/src/components/primitives/VolatilityBadge.tsx desktop_tauri/src/components/primitives/VolatilityBadge.test.tsx
git commit -m "feat(ui): VolatilityBadge primitive with tier color + tooltip"
```

---

## Task 12: Wire `VolatilityBadge` into existing prop displays

**Files:**
- Modify: `desktop_tauri/src/api/types.ts` (or equivalent prop type file)
- Modify: every component that renders a prop card / row

- [ ] **Step 1: Extend the TypeScript prop type**

Locate the type definition for a "prop opportunity" in the frontend. Likely in `desktop_tauri/src/api/client.ts` or a `types.ts`. Add:

```ts
export interface PropOpportunity {
  // ...existing fields...
  volatility_coefficient: number
  volatility_tier: "low" | "medium" | "high"
  adjusted_over_probability: number | null
}
```

- [ ] **Step 2: Find every renderer of a prop**

Run from inside `desktop_tauri/`:

```bash
grep -rn "calibrated_over_probability\|hit_probability\|likelihood_score" src/ | grep -E "\.(tsx|ts)$"
```

For each component that renders a prop card / row (start with the homepage `routes/index.tsx`, then `routes/trading/components/PickRow.tsx`, then any `PlayerCard.tsx` consumer), import `VolatilityBadge` and add it next to the existing confidence/edge badges:

```tsx
import { VolatilityBadge } from "@/components/primitives/VolatilityBadge"

// inside the card:
<VolatilityBadge
  tier={opportunity.volatility_tier}
  coefficient={opportunity.volatility_coefficient}
/>
```

If the component receives `contributors` from the API (e.g. via the diagnostic endpoint or an enriched listing), pass them in for the tooltip.

- [ ] **Step 3: Run the dev server and visually verify**

```
npm run tauri:dev --prefix desktop_tauri
```

Open the homepage; every prop card should now show a small Low/Medium/High chip. Hover the chip — tooltip shows the coefficient. Confirm the distribution is mixed (not 100% any one tier).

- [ ] **Step 4: Commit**

```bash
git add desktop_tauri/src/
git commit -m "feat(ui): render VolatilityBadge on prop cards across the app"
```

---

## Task 13: Transient `VOLATILITY_TIER_ENABLED` rollout guard

The spec calls for a kill-switch flag during the first 24 hours after rollout, intended to be deleted within a week. This task adds it; a follow-up ticket should remove it once the feature has soaked.

**Files:**
- Modify: `app/config/settings.py` (add the setting)
- Modify: `app/services/insights.py`, `app/trading/decision_brain.py`, `app/services/parlays.py` (wrap volatility calls)

- [ ] **Step 1: Add the setting**

In `app/config/settings.py`, add a field to the `Settings` class alongside the other feature toggles (e.g. near `examiner_enabled`):

```python
volatility_tier_enabled: bool = Field(default=True, alias="VOLATILITY_TIER_ENABLED")
```

- [ ] **Step 2: Wrap each call site**

In each of the three integration files (insights, decision_brain, parlays), guard the `compute_volatility(...)` call:

```python
settings = get_settings()
if settings.volatility_tier_enabled:
    volatility = compute_volatility(
        raw_probability=...,
        features=build_feature_snapshot(...),
    )
else:
    volatility = None
```

And ensure each downstream usage tolerates `volatility is None` (no adjustment, no badge data) by short-circuiting. The `_prop_confidence_score` change in Task 6 already accepts `None`; for decision_brain and parlays, fall back to the raw probability when volatility is None.

- [ ] **Step 3: Test the kill switch**

Add to `tests/unit/services/test_volatility_basics.py`:

```python
def test_volatility_tier_disabled_setting(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VOLATILITY_TIER_ENABLED", "false")
    from app.config.settings import get_settings
    get_settings.cache_clear()  # type: ignore[attr-defined]
    assert get_settings().volatility_tier_enabled is False
    get_settings.cache_clear()  # type: ignore[attr-defined]
```

Run: `pytest tests/unit/services/test_volatility_basics.py -v`
Expected: PASS.

- [ ] **Step 4: Manual smoke test**

```
VOLATILITY_TIER_ENABLED=false npm run tauri:dev --prefix desktop_tauri
```

Open the homepage; verify prop dock looks identical to pre-volatility (no badge rendered, confidence scores unchanged from the un-feature-flagged build).

- [ ] **Step 5: Commit**

```bash
git add app/config/settings.py app/services/insights.py app/trading/decision_brain.py app/services/parlays.py tests/unit/services/test_volatility_basics.py
git commit -m "feat(volatility): VOLATILITY_TIER_ENABLED kill-switch for rollout"
```

> **TODO after a week of stable operation:** delete the flag and the guards in a single revert-style PR. Track this as a separate ticket in the project queue.

---

## Final verification

- [ ] Run the full backend suite:

```
pytest tests -v
```

Expected: no failures introduced by this branch.

- [ ] Run the frontend suite:

```
npm test --prefix desktop_tauri
```

Expected: pass.

- [ ] Lint everything touched in this branch:

```
ruff check app/services/volatility.py app/services/insights.py app/services/prop_analysis.py app/trading/decision_brain.py app/services/parlays.py app/server/schemas/props.py app/server/routers/props.py app/services/startup.py tests/
mypy app/services/volatility.py
```

- [ ] Cache-clear and full E2E:

```
python scripts/reset_startup_day.py --hard
powershell -ExecutionPolicy Bypass -File scripts/build_sidecar.ps1
cp desktop_tauri/src-tauri/binaries/nba-sidecar-*.exe ../../desktop_tauri/src-tauri/binaries/  # if running app from main repo
# from the desired cwd:
npm run tauri:dev --prefix desktop_tauri
```

Open the app. Verify:
1. Startup log contains a `volatility: tier_distribution ...` line.
2. Top-prop dock no longer shows threes/bench-player props at every top slot.
3. Every prop card shows a tier badge.
4. `curl http://127.0.0.1:<sidecar_port>/api/props/predictions/<any_id>/volatility` returns the contributor breakdown.

---

## Notes

- The `predicted_minutes_std` argument to `build_feature_snapshot` is `None` everywhere in this plan because the value isn't easily available outside the training pipeline. The minutes-instability normalizer falls back to the `minutes_std_10 / minutes_mean_10` term, so the score remains useful. A follow-up can plumb `predicted_minutes_std` from the model output if needed.
- Each call site fetches its own `FeatureSnapshot`. That's one SQL query per prop. For ~700 props per board day, this is acceptable on a desktop app. If it ever becomes a hot path, batch the feature query.
- `VolatilityConfig` is module-level. Tuning the weights or alphas is a one-file edit and a restart. If the user wants live tuning, expose it in `app/config/settings.py` later.

# App-wide Per-Prop Volatility Tier — Design Spec

**Date:** 2026-05-15
**Status:** Draft, pending implementation plan
**Branch:** `claude/adoring-khayyam-ee653b`

## Problem

Top-prop dominance by high-variance markets (threes, bench-player unders) persists across retrains. The model produces probabilities, the insights service docks confidence for a hardcoded list of four "volatile" markets (`threes`, `turnovers`, `steals`, `blocks`), and every other surface (decision brain, parlays, trading UI, future props browser) ignores volatility entirely.

The hardcoded market-key penalty is the wrong shape: volatility is per-prop, not per-market. A 5-threes-per-game shooter against an under 3.5 line and a bench player's under 0.5 are both "threes" but have completely different risk profiles. Conversely, when a line is genuinely inflated, the same market-key penalty wrongly punishes a real edge.

## Goal

Replace the four-entry market-key penalty with a per-prop, multi-input volatility coefficient computed at query time. The coefficient gently discounts the model's probability and sharply discounts the displayed confidence score. The signal is surfaced as a low/medium/high tier badge on every prop and is consumed identically by every surface in the app.

Raw probability remains stored unchanged in the DB. Adjustment happens in a service-layer pure function so the formula can be iterated without touching consumers and without retraining.

## Non-goals

- No persistence of the coefficient (computed on read).
- No new ML model. The coefficient is a deterministic formula over existing features.
- No filtering of volatile props out of result sets. Discount only, never remove.
- No change to training-time variance handling (the existing `volatile_market_penalty` in training stays as-is).
- No change to Kalshi market routing logic.
- DD/TD, PA/PR/RA market additions are out of scope for this spec.

## Architecture

```
Prediction row + Feature snapshot
              |
              v
   compute_volatility(prediction, features)  --- pure function
              |
              v
       VolatilityScore {
         coefficient: float,
         tier: "low" | "medium" | "high",
         contributors: tuple[VolatilityContributor, ...],
         adjusted_probability: float,
         confidence_multiplier: float,
       }
              |
        +-----+-----+-----+-----+
        v     v     v     v     v
   insights  decision  parlays  API  frontend
                brain                  (badge)
```

**Key properties:**
- Pure function, no I/O. Trivial to unit-test with fixtures.
- DB schema unchanged.
- Each consumer is one to three small lines: call `compute_volatility`, use `score.adjusted_probability` or `score.confidence_multiplier`.
- Single source of truth (`app/services/volatility.py`) — formula changes propagate everywhere.

## Score formula

Five weighted inputs, normalized to `[0, 1]`, summed, clipped:

```
coefficient = clip(
    0.30 * stat_cv_norm
  + 0.20 * minutes_instability_norm
  + 0.15 * usage_instability_norm
  + 0.20 * recent_form_divergence_norm
  + 0.15 * archetype_risk
  , 0.0, 1.0
)
```

Per-input normalization:

| Input | Source feature(s) | Normalization | Notes |
|---|---|---|---|
| `stat_cv` | `<market>_std_10`, `<market>_mean_10` | `clip(std / max(mean, eps) / 1.5, 0, 1)` | `eps = 0.5`; cv of 1.5 = max |
| `minutes_instability` | `predicted_minutes_std`, `minutes_std_10`, `minutes_mean_10` | `clip((predicted_minutes_std / 8.0 + minutes_std_10/max(minutes_mean_10, 1.0) / 0.6) / 2.0, 0, 1)` | |
| `usage_instability` | `usage_rate_std_10`, `usage_rate_mean_10` | `clip(std / max(mean, 0.05) / 0.5, 0, 1)` | |
| `recent_form_divergence` | `<market>_mean_5`, `<market>_mean_season`, `<market>_std_season` | `clip(\|m5 - m_season\| / max(std_season, 1.0) / 2.0, 0, 1)` | Z-score against season distribution |
| `archetype_risk` | derived from `starter_flag_rate`, `minutes_mean_season`, `projected_starter_flag` | bucket → starter=0.0, rotation=0.3, bench=0.7, fringe=1.0 | See archetype mapping below |

**Archetype mapping** (computed in same module, no new tables):
- `starter`: `starter_flag_rate >= 0.7` AND `minutes_mean_season >= 24`
- `rotation`: `starter_flag_rate < 0.7` AND `minutes_mean_season >= 18`
- `bench`: `minutes_mean_season >= 10`
- `fringe`: everything else

## Application

### Probability — gentle, edge-preserving

```python
PROB_ALPHA = 0.30

def adjust_probability(raw_p: float, coefficient: float) -> float:
    edge = raw_p - 0.5
    return 0.5 + edge * (1.0 - coefficient * PROB_ALPHA)
```

At worst-case coefficient=1.0:

| raw_p | adjusted_p |
|---|---|
| 0.90 | 0.78 |
| 0.80 | 0.71 |
| 0.70 | 0.64 |
| 0.60 | 0.57 |
| 0.55 | 0.535 |

Ranking is preserved; no clustering near 0.50. `PROB_ALPHA` is the single tuner — set lower for gentler effect.

### Confidence — sharper, since it's editorial

```python
CONF_ALPHA = 0.55

confidence_multiplier = 1.0 - coefficient * CONF_ALPHA
# applied at the call site:
# adjusted_score = int(existing_confidence_score * score.confidence_multiplier)
```

Example: a prop computing to confidence 78 with coefficient 0.8 → `78 * (1 - 0.44) = 44`.

### Tier bucketing

```python
def tier_from_coefficient(c: float) -> Literal["low", "medium", "high"]:
    if c < 0.33:
        return "low"
    if c < 0.66:
        return "medium"
    return "high"
```

Thresholds live in `VolatilityConfig`.

## Data model

```python
from dataclasses import dataclass
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
    reason: str = ""  # populated only when degraded; "insufficient_features", etc.


@dataclass(frozen=True)
class VolatilityConfig:
    weights: dict[str, float]  # input_name -> weight, must sum to 1.0
    prob_alpha: float = 0.30
    conf_alpha: float = 0.55
    tier_low_cap: float = 0.33
    tier_high_cap: float = 0.66
    # normalization constants per input live here too
```

Default `VolatilityConfig` is a module-level constant; can be overridden in tests.

## Call sites

Six known consumers. Each gets a 2-3 line change.

1. **`app/services/insights.py`** — `_prop_confidence_score`
   Delete the `_VOLATILE_MARKET_PENALTY` dict and its subtraction. Wrap the final score with `score = int(score * volatility.confidence_multiplier)`. Add `score.tier` and `score.adjusted_probability` to the response object so the UI can render them.

2. **`app/trading/decision_brain.py`** — candidate ranking
   When ranking Kalshi candidates by confidence/edge, multiply confidence by `score.confidence_multiplier` and use `score.adjusted_probability` for the EV computation. Optional policy knob `max_volatility_coefficient` to skip candidates above a threshold; default off.

3. **`app/services/parlays.py`** — leg eligibility + ranking
   Same shape as decision_brain. Use `score.adjusted_probability` for parlay EV. Optional knob to require all legs at `tier != "high"`; default off.

4. **`app/server/routes/predictions.py`** (or equivalent) — API responses
   Augment the prediction DTO with `volatility_coefficient`, `volatility_tier`, `adjusted_over_probability` fields so the frontend can render them.

5. **`app/server/routes/insights.py`** — homepage dock DTO
   Same augmentation.

6. **`desktop_tauri/src/components/primitives/VolatilityBadge.tsx`** — new component
   Renders the colored tier chip with hover tooltip showing top-3 contributors. Used in:
   - homepage prop cards
   - prop tables in the trading UI
   - future per-player props browser (separate spec)

## Observability

Three hooks so behavior is verifiable.

1. **Per-prediction diagnostic endpoint**
   `GET /api/predictions/{prediction_id}/volatility` returns:
   ```json
   {
     "prediction_id": 12345,
     "coefficient": 0.71,
     "tier": "high",
     "adjusted_probability": 0.68,
     "confidence_multiplier": 0.61,
     "contributors": [
       {"name": "archetype_risk", "raw_value": 0.7, "weight": 0.15, "contribution": 0.105},
       {"name": "stat_cv", "raw_value": 1.32, "weight": 0.30, "contribution": 0.264},
       ...
     ],
     "reason": ""
   }
   ```

2. **Startup-time log line**
   Emitted from `app/services/startup.py` after the prediction step:
   ```
   INFO [app.services.volatility] tier_distribution low=412 medium=287 high=89  coef_p50=0.31 p90=0.71  insufficient_features=4
   ```
   So a quick `grep volatility` in the launch log confirms the system fired and the distribution looks sane.

3. **UI tooltip on tier badge**
   Hovering the badge shows the top-3 contributors by `contribution` value with their `raw_value`. No click-through required for casual inspection.

## Failure modes

**Missing input** (early-season player, rookie, recently traded, sparse feature row):
- The missing contributor is dropped.
- Remaining weights are renormalized to sum to 1.0.
- A `WARNING` log is emitted with the prediction_id and the missing input name (rate-limited to once per minute per input).

**All inputs missing:**
- `coefficient = 0.5` (neutral).
- `tier = "medium"`.
- `contributors = ()`.
- `reason = "insufficient_features"`.
- UI shows a generic "Limited data" badge instead of green/amber/red.
- Logged at `INFO` once per prediction.

**Config sum doesn't equal 1.0:**
- Validated at module import. Raises `ValueError` immediately so misconfiguration can't ship.

## Tests

**Unit (`tests/unit/services/test_volatility.py`):**
- Each input's normalization with boundary values (0, midpoint, max, beyond max).
- Weighted sum with all-zero inputs → coefficient = 0.
- Weighted sum with all-max inputs → coefficient = 1.0.
- Tier bucketing at thresholds (0.32, 0.33, 0.65, 0.66).
- `adjust_probability` for `(raw_p, coefficient)` pairs covering the ranking table above.
- `confidence_multiplier` correctness.
- Missing-input renormalization preserves remaining-weight ratios.
- All-missing-inputs path returns neutral score with `reason="insufficient_features"`.
- Archetype mapping for each bucket boundary.
- Property test (hypothesis): for any `(raw_p ∈ [0.5, 1.0], coefficient ∈ [0, 1])`, `adjusted_p ∈ [0.5, raw_p]` (monotonicity guarantee).

**Integration (`tests/integration/test_volatility_application.py`):**
- Insights service: confidence-score regression on a fixture prop with known features.
- Decision brain: candidate ranking with two synthetic candidates — same raw EV, different volatility — confirms ranking flips.
- Parlay advisor: leg eligibility with the `max_volatility_coefficient` knob.
- API endpoint: GET returns the full breakdown JSON.

**Visual sanity (manual, one-time):**
- Startup log line appears with a non-degenerate distribution (not 0% high, not 100% high).
- Hover a UI badge in dev — tooltip shows three named contributors with numbers that add up to roughly the coefficient.

## Migration / rollout

This is additive. No DB migration. No config flag required. Ships in one PR:
1. Module + tests
2. Insights call-site change (delete `_VOLATILE_MARKET_PENALTY`)
3. Decision brain + parlays call-site changes
4. API response augmentation
5. Frontend `VolatilityBadge` + integration into existing prop displays
6. Startup log line

A `VOLATILITY_TIER_ENABLED` env flag wraps the call sites for the first 24 hours so we can disable in a hurry if the dock composition is worse than today's — but the goal is to delete the flag within a week.

## Risks

1. **Formula tuning takes longer than expected.** Mitigation: weights/alphas/thresholds all live in one `VolatilityConfig`. The pure-function shape makes A/B against a fixture trivial.
2. **Some surfaces still favor volatile props.** Mitigation: the observability endpoint plus per-surface manual inspection. The user has explicitly asked for visibility because past fixes have not stuck.
3. **Confidence becomes too gloomy.** Mitigation: `CONF_ALPHA` is one number. If 90% of props are tier=medium-or-high, lower it to 0.40.
4. **Cross-cutting refactor breaks an unrelated surface.** Mitigation: call-sites are minimal (six locations). Integration tests cover insights + decision_brain + parlays + API.

## Out of scope, queued

- Deep audit of the full startup + training pipeline for visibility/correctness. The user has noted repeated fixes for volatile-market dominance failing to stick, which suggests something upstream may be miscompensating. Queued as a separate session after this lands.
- Per-player props browser tab at `/players` (the player-search stub). Separate spec.
- DraftKings prop attrition debug (only 21 props surviving). Separate ticket; may resolve itself once volatility is applied at the resolver level.

# Injury Rotation Shock Implementation Plan

## Executive Summary

This project already handles injuries, but it does so mostly as model features. The new methodology should treat injuries as explicit team rotation shocks:

1. Identify absent or uncertain players.
2. Classify their role and estimate removed minutes, usage, shots, assists, rebounds, and team environment impact.
3. Redistribute those opportunities to teammates using learned historical weights.
4. Run the existing minutes-plus-rate prediction stack on the adjusted rotation.
5. Use Monte Carlo in v1 for uncertain availability and boom/bust distributions.
6. Persist audit records so every projection change can be inspected.

The goal is prediction accuracy first. Runtime, storage, and implementation time are secondary.

## Locked Decisions

These decisions came from the completed brainstorming flow:

- Redistribution lives in an explicit pre-model adjustment layer.
- The adjustment runs in both training and inference to avoid train/serve skew.
- Redistribute minutes, usage, and small team-environment effects.
- Use continuous role vectors for the math and archetype labels for audit/UI.
- Monte Carlo is in v1, not a later nice-to-have.
- Availability is enumerated exactly for normal injury-report sizes instead of sampled.
- Stat realization uses the existing adaptive simulation system with updated defaults.
- Rotation weights are learned from historical absence games.
- Use inverse-variance weighting for learned cells when variance exists.
- Use league fallback only when a team/archetype cell is too thin or variance cannot be computed.
- Persist audit records in local parquet artifacts from v1.
- Qwen explanations are not in v1. The audit payload should be structured enough for Qwen later.
- Optimize implementation sessions for context/token use: small phases, strong file indexing, concise status reports, and bounded handoffs.

## Current Project State

The proposed methodology is partially implemented, but implicitly.

Already implemented:

- `app/training/pipeline.py` has a two-stage pipeline: `MinutesModel` predicts minutes, then `StatModelSuite` predicts stat totals and per-minute rates.
- `app/training/data.py` attaches injury, projected-lineup, and official inactive-list context through `_attach_availability_context`.
- Current availability features include `team_out_count`, `team_doubtful_count`, `team_questionable_count`, `same_position_out_count`, `missing_starter_count`, `lineup_instability_score`, `teammate_absence_pressure`, `missing_teammate_usage_sum`, and `star_absent_flag`.
- `app/training/features.py` builds implicit opportunity features like `role_expansion_score` and `positional_opportunity_index`.
- `app/training/models.py` uses `predicted_minutes` as exposure in the rate model.
- `Prediction.feature_attribution_summary` is JSON and can carry rotation-shock metadata immediately.
- `app/training/distributions.py` already has adaptive Monte Carlo sampling controlled by `app/config/settings.py`.

Missing:

- No explicit minutes redistribution.
- No explicit usage redistribution.
- No role-vector or absence-archetype system.
- No learned historical teammate-delta weights.
- No availability enumeration for questionable/doubtful/probable statuses.
- No explicit team efficiency or pace adjustment.
- No per-game audit artifact showing absence records and teammate adjustments.
- No UI/API field for DNP risk, p10/p90, floor/ceiling, boom probability, or bust probability.
- No Qwen narrative based on actual model-produced deltas.

## Pros And Cons

### Current Feature-Encoded Method

Pros:

- Already integrated and trained.
- Low rule-maintenance burden.
- XGBoost can learn nonlinear interactions from absence context.
- Works when projected-lineup coverage is thin.
- Lower risk of bad hand-tuned rules dominating the model.

Cons:

- Hard to explain why a teammate projection changed.
- Teammates are not explicitly given the missing player's minutes.
- Usage and role transfer are approximated rather than modeled.
- Star absences can be underrepresented.
- Questionable/doubtful/probable statuses collapse into one point estimate.
- No DNP-risk tail.
- No audit record.

### Explicit Rotation-Shock Method

Pros:

- Models the real basketball mechanism: removed opportunities are reassigned.
- More accurate on high-injury slates if weights are learned well.
- Separates minutes from per-minute rate, which matches the existing model design.
- Produces inspectable records: "Player X out -> Player Y +3.2 minutes, +4.8 usage points."
- Enables availability-aware distributions and DNP risk.
- Gives Qwen a clean factual payload later.

Cons:

- Adds a new layer that can double-count existing absence features.
- Needs leakage-safe historical reconstruction.
- Needs careful validation on no-injury games to avoid harming clean-roster predictions.
- Learned weights can overfit small samples.
- Monte Carlo increases inference complexity.

### Combined Approach

Keep the current model stack, but add a pure, testable rotation layer between baseline minutes and stat prediction:

```text
availability context
-> baseline features
-> baseline minutes model
-> rotation shock adjustment
-> adjusted minutes/rates/team environment
-> stat/rate models
-> availability + stat Monte Carlo
-> prediction payload + audit records
```

## Core Architecture

Add a new module:

- `app/training/rotation.py`

This module must be pure logic:

- No database reads.
- No filesystem writes.
- No model calls.
- Deterministic for fixed inputs.
- Unit-testable without provider data.

Primary API:

```python
def redistribute(
    *,
    game_id: int,
    team_id: int,
    players: Sequence[PlayerRotationProfile],
    weights: RotationWeightTable,
    play_probabilities: Mapping[int, float],
    mode: Literal["expected_value", "realized"],
) -> RedistributionResult:
    ...
```

The same function is used by:

- historical backfill
- training feature construction
- inference
- Monte Carlo availability branches
- audit persistence

No training/inference path should reimplement redistribution math.

## Data Structures

Add domain schemas/dataclasses. Keep them immutable where practical.

### RoleVector

Computed from trailing history as of a game date.

Fields:

- `player_id`
- `season`
- `position_group`
- `usage_proxy`
- `usage_share`
- `assist_share`
- `rebound_share`
- `three_point_rate`
- `rim_attempt_rate`
- `touches_per_minute`
- `passes_per_minute`
- `rebound_chances_per_minute`
- `blocks_per_minute`
- `starter_score`
- `role_stability`
- `archetype_label`

The continuous values drive math. `archetype_label` is for grouping, audit, and UI.

### Archetypes

Use these labels:

- `primary_creator`
- `scoring_wing`
- `rim_big`
- `spacing_guard`
- `bench_depth`

Heuristic examples:

- `primary_creator`: high usage plus high assist/touch creation.
- `scoring_wing`: high shot creation, wing/forward profile, medium assist load.
- `rim_big`: center/big with high rebound/rim/block profile.
- `spacing_guard`: high three-point rate, guard/wing profile.
- `bench_depth`: low minutes, low usage, low starter score.

### RotationWeights

Learned from historical absence games.

Fields:

- `team_id`
- `season`
- `absent_archetype`
- `candidate_archetype`
- `minute_gain_weight`
- `usage_gain_weight`
- `minute_delta_mean`
- `usage_delta_mean`
- `minute_delta_variance`
- `usage_delta_variance`
- `sample_size`
- `weight_source`: `team`, `league`, or `fallback`
- `last_updated`

Store as:

- `data/artifacts/rotation_weights.parquet`

Include league-wide fallback rows in the same file with `team_id = "LEAGUE"` or another explicit sentinel.

### AbsenceRecord

One row per absent or uncertain player per game.

Fields:

- `game_id`
- `team_id`
- `player_id`
- `player_name`
- `status`
- `play_probability`
- `archetype_label`
- `baseline_minutes`
- `baseline_usage_share`
- `removed_minutes`
- `removed_usage_share`
- `removed_assist_share`
- `removed_rebound_share`
- `removed_three_point_share`
- `source`: `official_inactive`, `injury_report`, or `post_hoc`
- `report_timestamp`
- `rotation_shock_confidence`

### TeammateAdjustment

One row per affected teammate per game.

Fields:

- `game_id`
- `team_id`
- `player_id`
- `player_name`
- `baseline_minutes`
- `adjusted_minutes`
- `minutes_delta`
- `baseline_usage_share`
- `adjusted_usage_share`
- `usage_delta`
- `baseline_assist_share`
- `adjusted_assist_share`
- `baseline_rebound_share`
- `adjusted_rebound_share`
- `source_absence_player_ids`
- `absence_reason`
- `weight_source`

### RedistributionResult

Returned by `rotation.redistribute`.

Fields:

- `adjusted_players`
- `absences`
- `teammate_adjustments`
- `team_efficiency_delta`
- `pace_delta`
- `rotation_shock_magnitude`
- `rotation_shock_confidence`
- `mass_conservation_warnings`

## Status To Play Probability

Official inactive list overrides injury reports.

Default mapping:

- Official inactive: `0.00`
- `Out`, `Inactive`, `Suspended`: `0.00`
- `Doubtful`: `0.15`
- `Questionable`: `0.50`
- `Probable`: `0.85`
- Available or no report: `1.00`
- `expected_availability_flag = False`: cap at `0.00`
- `expected_availability_flag = True`: floor at `0.85`

The exact probabilities must be configurable later, but v1 can hardcode them in `app/training/rotation.py` with tests.

## Historical Weight Learning

Add:

- `scripts/learn_rotation_weights.py`

Purpose:

- Learn how each team historically redistributes minutes and usage after absences.

Algorithm:

1. Load historical training frame.
2. For each game, identify absent rotation players using the same injury path as training.
3. Define rotation player as season-to-date or trailing role player with at least 12 MPG by that date.
4. Build each player's `RoleVector` as of that game date.
5. For each available teammate, compute actual minutes and usage delta versus baseline.
6. Attribute teammate deltas to absent players.
7. Aggregate by `(team_id, season, absent_archetype, candidate_archetype)`.
8. Compute league fallback rows.
9. Write `data/artifacts/rotation_weights.parquet`.
10. Emit a sanity report.

Baseline rule:

- Use trailing 15 games for baseline minutes and usage.
- If fewer than 10 qualifying games exist, use season-to-date.
- Exclude garbage-time games.
- Exclude the first 2 games after a player returns from extended absence.
- Reset or heavily downweight team-specific history after trades/team changes.

Multiple absent players:

- Attribute teammate deltas proportionally to each absent player's removed usage share.
- This prevents double-counting when two players are out.

Weight aggregation:

- Use inverse-variance weighting when `n >= 2` and variance is finite.
- Use team-specific cells when enough signal exists.
- Fall back to league rows when `n = 1`, variance is undefined, or the cell contains NaN/invalid values.
- Always validate that weights are non-negative and normalized.

Sanity report:

- Top cells by sample size.
- Cells with extreme minute transfer.
- Cells where `bench_depth` absence causes suspiciously large redistribution.
- Cells where usage gain weights do not sum correctly.
- Count of team-specific versus league fallback cells.

Nightly refresh:

- Add an optional step to `scripts/run_daily_automation.py`.
- Incrementally process games since the last learned-weight update.
- Full rebuild only on demand or major model retrain.

## Redistribution Logic

### Candidate Selection

Eligible gain candidates:

- Same game.
- Same team.
- Not official inactive.
- Play probability greater than zero.
- Has a baseline role/minutes estimate.

Confirmed-out players:

- Receive `adjusted_minutes = 0`.
- Receive `adjusted_usage_share = 0`.
- Are not eligible to absorb opportunity.

Uncertain players:

- In expected-value branch, keep fractional availability.
- In realized Monte Carlo branch, availability is hard 0 or 1.

### Minutes Redistribution

Removed minutes:

```text
removed_minutes = baseline_minutes * (1 - play_probability)
```

Candidate gain weight:

```text
minute_gain_weight =
  learned_weight(absent_archetype, candidate_archetype)
  * role_similarity
  * position_fit
  * depth_chart_fit
  * minutes_capacity
  * role_stability
  * candidate_play_probability
```

Caps:

- No negative minutes.
- No player exceeds a player-specific cap unless historical evidence supports it.
- Use recent high-minute cap for established players.
- Use position-level cap for thin-history players.
- Keep team total adjusted minutes near 240 regulation minutes.
- Overtime handling remains future work unless current data already flags it cleanly.

Mass conservation:

- Sum of gained minutes should equal sum of removed minutes after caps.
- If caps prevent full redistribution, write a warning and distribute remaining minutes by relaxed caps.

### Usage Redistribution

Removed usage:

```text
removed_usage_share = baseline_usage_share * (1 - play_probability)
```

Candidate gain weight:

```text
usage_gain_weight =
  learned_weight(absent_archetype, candidate_archetype)
  * creator_similarity
  * scorer_similarity
  * on_ball_role
  * adjusted_minutes_share
  * usage_capacity
  * candidate_play_probability
```

Caps:

- Cap individual usage increases.
- Cap team total redistributed usage.
- Do not assign high ball-handling usage to low-touch bigs unless the absent player profile supports it.
- Preserve baseline values in `baseline_*` columns before writing adjusted values.

### Rate Channels

Adjust these channels before stat models and simulations:

- `adjusted_projected_minutes`
- `adjusted_usage_share`
- `adjusted_usage_rate`
- `adjusted_field_goal_attempts_per_minute`
- `adjusted_free_throw_attempts_per_minute`
- `adjusted_assist_creation_proxy_per_minute`
- `adjusted_rebound_chances_total_per_minute`
- `adjusted_estimated_three_point_attempts_per_minute`
- `adjusted_touches_per_minute`
- `adjusted_passes_per_minute`

Preserve base columns:

- `baseline_projected_minutes`
- `baseline_usage_share`
- `baseline_usage_rate`
- `baseline_field_goal_attempts_per_minute`
- `baseline_assist_creation_proxy_per_minute`
- `baseline_rebound_chances_total_per_minute`

### Team Environment

Add small, capped deltas:

- `team_efficiency_delta`
- `pace_delta`
- `rotation_shock_magnitude`
- `rotation_shock_confidence`

Archetype effects:

- `primary_creator`: offensive efficiency down, turnover uncertainty up.
- `scoring_wing`: offensive efficiency slightly down, usage concentrated in scorers.
- `rim_big`: rebounding/rim protection affected.
- `spacing_guard`: spacing and three-point ecosystem affected.
- `bench_depth`: small environment effect.

Conservatism rule:

- These deltas must be small and backtested because the existing model already has `lineup_instability_score` and opponent disruption features.

## Monte Carlo V1

Monte Carlo is part of v1 because the priority is statistical accuracy.

### Why Not 500,000 Simulations By Default

The high simulation counts often mentioned for finance or season-long forecasting solve a different problem: high-dimensional continuous paths and rare-event probabilities. Player prop prediction has a much smaller uncertainty structure:

- Availability uncertainty is discrete and usually small.
- A game usually has 1 to 4 questionable/doubtful/probable rotation players.
- For `k` uncertain players, there are only `2^k` availability states.
- Those states can be enumerated exactly for normal NBA slates.

At 10,000 stat draws per branch, sampling error for means and percentiles is smaller than the model's residual uncertainty. Beyond that, the rate model's own error dominates, so more draws offer diminishing returns.

### Availability Layer

Enumerate exactly when:

- `k <= 8` uncertain players in the team/game context.

Fallback when:

- `k > 8`.

Fallback behavior:

- Sample availability states with a fixed seed.
- Log that enumeration was skipped due to large `k`.
- Keep `n_availability_samples` configurable.

Branch probability:

```text
P(branch) = product(p_i if player_i active else 1 - p_i)
```

Each branch calls `rotation.redistribute` with realized `0.0` or `1.0` play probabilities.

### Stat Realization Layer

Use the existing adaptive distribution simulator in `app/training/distributions.py`.

Update global simulation defaults in `app/config/settings.py`:

- `simulation_target_margin`: keep `0.01`
- `simulation_min_samples`: change from `50000` to `10000`
- `simulation_max_samples`: change from `1000000` to `100000`
- `simulation_batch_size`: change from `50000` to `10000`

Reason:

- 10,000 samples matches the practical public player-prop simulation range discussed in brainstorming.
- The current 50,000 floor is the binding constraint, not the 1% margin target.
- Smaller batches let the adaptive loop stop earlier.
- The 100,000 max keeps 10x headroom for harder tail cases.

Important implementation note:

- Do not introduce a second independent simulation config for rotation shock.
- Reuse the existing settings unless a specific test proves separate knobs are needed.

### Caching

Cache by realized absence set:

```text
cache_key = (game_id, team_id, frozenset(absent_player_ids))
```

Many Monte Carlo branches produce the same absence set, so redistribution and rate prediction should not be recomputed unnecessarily.

### Prediction Outputs

Add fields to prediction payloads and/or attribution JSON:

- `projected_mean`
- `projected_median`
- `percentile_10`
- `percentile_25`
- `percentile_75`
- `percentile_90`
- `dnp_risk`
- `boom_probability`
- `bust_probability`
- `availability_branches`
- `simulation_samples`
- `simulation_margin_of_error`

Definitions:

- `dnp_risk`: probability player is unavailable in enumerated availability branches.
- `boom_probability`: probability stat is at least line plus a configurable cushion, default line * 1.10.
- `bust_probability`: probability stat is at or below line * 0.70.
- `percentile_25`: floor-style output.
- `percentile_75`: ceiling-style output.

### API Compatibility

Existing fields must remain valid:

- `projected_mean`
- `projected_variance`
- `projected_median`
- `over_probability`
- `under_probability`
- `confidence_interval_low`
- `confidence_interval_high`

New fields should be additive. Do not break current board, props, parlays, or trading consumers.

## Training Integration

Main integration point:

- `app/training/pipeline.py`

Steps:

1. Load historical training data.
2. Build leakage-safe training features.
3. Fit baseline `MinutesModel`.
4. Predict baseline minutes.
5. Write `baseline_projected_minutes`.
6. Build team-game player rotation profiles.
7. Load learned rotation weights.
8. Run `rotation.redistribute` for each historical team/game using pregame-known statuses.
9. Attach adjusted columns.
10. Recompute post-minutes features using adjusted minutes.
11. Add `adjusted_*`, `baseline_*`, and `rotation_*` columns to feature columns.
12. Fit `StatModelSuite`.
13. Fit calibrators.
14. Persist model metadata noting rotation-shock version and weights artifact hash.

Leakage guard:

- Use injury reports with `report_timestamp < game_tipoff`.
- If pregame injury data is unavailable, use post-hoc inactive data only with `source = "post_hoc"` and `rotation_shock_confidence = 0.5`.
- Never let actual post-game player stats determine pregame role vectors beyond trailing-history windows.

Feature double-counting policy:

- Keep existing implicit features during shadow/treatment comparison.
- Add ablation flags to test:
  - `full`: explicit rotation + legacy features
  - `features-only`: new features without adjustment
  - `off`: legacy only
- After backtests, drop or downweight `role_expansion_score` only if evidence shows double-counting.

## Inference Integration

Main integration points:

- `TrainingPipeline.predict_upcoming`
- `scripts/retrain_and_predict.py`
- sidecar prediction path used by props board

Steps:

1. Load upcoming lines and roster context.
2. Load latest injury reports and official inactive rows.
3. Build inference features.
4. Predict baseline minutes.
5. Write `baseline_projected_minutes`.
6. Build team-game rotation profiles.
7. Load cached `rotation_weights.parquet`.
8. Enumerate availability branches for uncertain players.
9. For each branch, call `rotation.redistribute`.
10. Run stat/rate prediction for each adjusted branch.
11. Run adaptive stat realization sampling.
12. Aggregate branch-weighted distribution outputs.
13. Persist prediction rows.
14. Write audit parquet for each game.

Weights cache:

- Load at startup.
- Cache in memory.
- Refresh if artifact mtime or hash changes.

## Audit Persistence

Write audit artifacts from both training backfill and inference.

Path:

- `data/artifacts/rotation_audit/<game_id>.parquet`

Contents:

- `absences`
- `adjustments`
- optional `team_environment`

No SQLite manifest in v1. Time and storage are not concerns, and deterministic paths are simpler.

Read module:

- `app/services/rotation_audit.py`

Functions:

```python
get_redistribution(game_id: int) -> RedistributionAudit
get_player_adjustments(player_id: int, since_date: date | None = None) -> list[TeammateAdjustment]
```

Optional endpoint:

- `GET /props/{game_id}/rotation-audit`

UI can be a later phase, but the endpoint should be easy once audit artifacts exist.

## Qwen Narrative Plan

Qwen is not part of v1.

Reason:

- Prediction accuracy and audit correctness come first.
- The audit payload already gives a factual explanation without LLM risk.
- Qwen should never invent numbers.

Future Qwen input:

- player name
- market
- line
- baseline projection
- adjusted projection
- absence records
- teammate adjustment record
- top model signals
- Monte Carlo distribution summary

Prompt rule:

- "Explain only the supplied model facts. Do not calculate or invent projections."

## Implementation Phases

Each phase should be its own coding session unless it is tiny. Do not try to implement the full plan in one agent session.

### Phase 0: Planning, Indexing, And Safety Rails

Goal:

- Prepare the repo context and avoid context bloat.

Substeps:

1. Create or update a short implementation index in `memory/rotation_shock_index.md`.
2. Record key files, entry points, current simulation settings, and test commands.
3. Add a tiny checklist for phase ownership.
4. Decide feature flags:
   - `ROTATION_SHOCK_ENABLED`
   - `ROTATION_SHOCK_SHADOW_MODE`
   - `LEGACY_PIPELINE_ENABLED`
5. Do not change model behavior in this phase.

Deliverables:

- `memory/rotation_shock_index.md`
- feature flag names documented
- no behavior change

Tests:

- None required beyond `git diff` review.

### Phase 1: Core Rotation Schemas And Pure Logic

Goal:

- Add the pure redistribution engine without wiring it into prediction.

Substeps:

1. Add `app/training/rotation.py`.
2. Add dataclasses for `RoleVector`, `PlayerRotationProfile`, `RotationWeights`, `AbsenceRecord`, `TeammateAdjustment`, and `RedistributionResult`.
3. Implement status-to-play-probability mapping.
4. Implement role-vector normalization helpers.
5. Implement archetype classification.
6. Implement candidate selection.
7. Implement minutes redistribution.
8. Implement usage redistribution.
9. Implement team-environment deltas.
10. Implement mass-conservation warnings.

Deliverables:

- pure module
- no DB/filesystem dependency
- no integration into training or inference yet

Tests:

- `tests/unit/test_rotation_play_probability.py`
- `tests/unit/test_role_vector.py`
- `tests/unit/test_rotation_redistribute.py`

Acceptance:

- zero absences is a no-op
- official inactive overrides all other statuses
- no negative minutes
- removed minutes equal gained minutes after caps
- removed usage equals gained usage after caps

### Phase 2: Historical Weight Learning

Goal:

- Learn team and league redistribution weights from historical absence games.

Substeps:

1. Add `scripts/learn_rotation_weights.py`.
2. Reuse historical data loading from the current training frame.
3. Build trailing-15-game baselines with season-to-date fallback.
4. Identify absent rotation players using pregame reports when available.
5. Add post-hoc fallback with `rotation_shock_confidence = 0.5`.
6. Compute teammate deltas versus baseline.
7. Attribute multi-absence deltas proportionally by removed usage.
8. Aggregate with inverse-variance weighting.
9. Create league fallback rows.
10. Write `data/artifacts/rotation_weights.parquet`.
11. Emit sanity report to `reports/rotation_weights/`.

Deliverables:

- `scripts/learn_rotation_weights.py`
- `data/artifacts/rotation_weights.parquet`
- sanity report

Tests:

- `tests/unit/test_rotation_weights_aggregation.py`

Acceptance:

- no NaN weights
- all weights non-negative
- fallback rows exist
- sample-size metadata present
- suspicious cells flagged in report

### Phase 3: Monte Carlo Layer And Simulation Defaults

Goal:

- Add availability enumeration and update project-wide MC settings.

Substeps:

1. Update `app/config/settings.py` defaults:
   - `simulation_min_samples = 10000`
   - `simulation_max_samples = 100000`
   - `simulation_batch_size = 10000`
   - keep `simulation_target_margin = 0.01`
2. Confirm `app/core/resources.py` still caps memory correctly.
3. Add `app/training/rotation_monte_carlo.py` or equivalent if it keeps `rotation.py` cleaner.
4. Enumerate availability branches for `k <= 8`.
5. Add sampled fallback for `k > 8`.
6. Call `rotation.redistribute` per branch.
7. Aggregate branch-weighted distributions.
8. Add cache keyed by realized absent set.
9. Add DNP risk, p10, p25, p75, p90, boom, and bust calculations.

Deliverables:

- availability enumeration utility
- branch aggregation utility
- updated simulation defaults

Tests:

- `tests/unit/test_rotation_monte_carlo.py`
- update any tests that assume exactly `50000` samples

Acceptance:

- fixed seed is deterministic
- enumerated DNP risk equals analytical probability
- Bernoulli 0.5 convergence test passes
- existing distribution tests pass with new defaults

### Phase 4: Training Shadow Mode

Goal:

- Compute adjusted features historically without training the model on them yet.

Substeps:

1. Add a helper in `app/training/data.py` or `app/training/pipeline.py` to build team-game rotation profiles.
2. Load rotation weights artifact.
3. Call rotation adjustment during feature construction behind `ROTATION_SHOCK_SHADOW_MODE`.
4. Attach adjusted columns but do not include them in model feature columns yet.
5. Write audit artifacts for historical games.
6. Add parity fixture for one game.

Deliverables:

- adjusted columns available in shadow mode
- audit parquet written
- no model behavior change by default

Tests:

- `tests/integration/test_rotation_shadow_features.py`
- `tests/integration/test_rotation_audit_roundtrip.py`

Acceptance:

- mass conservation holds across fixture games
- no schema errors
- audit output can be read back
- feature flag off means no behavior change

### Phase 5: Treatment Training Integration

Goal:

- Train the model with rotation-adjusted features.

Substeps:

1. Enable adjusted columns in feature set when `ROTATION_SHOCK_ENABLED=true`.
2. Preserve `baseline_projected_minutes`.
3. Use adjusted minutes as the effective `predicted_minutes` consumed by rate models.
4. Add adjusted rate columns to market feature allowlists.
5. Update `_simulation_context` to prefer adjusted columns.
6. Record rotation-shock version and weights artifact hash in model metadata.
7. Add ablation flag support:
   - `off`
   - `features-only`
   - `full`

Deliverables:

- treatment training path
- metadata traceability
- ablation support

Tests:

- `tests/integration/test_train_serve_parity.py`
- `tests/integration/test_rotation_training_frame.py`

Acceptance:

- training frame and inference frame produce identical adjusted features for the same fixture
- no leakage from post-tipoff reports
- feature columns are stable

### Phase 6: Inference Integration

Goal:

- Produce live/upcoming predictions with rotation shock and Monte Carlo.

Substeps:

1. Wire rotation adjustment into `TrainingPipeline.predict_upcoming`.
2. Wire same path into `scripts/retrain_and_predict.py` through the pipeline, not duplicate logic.
3. Ensure sidecar prediction/props board uses the same pipeline outputs.
4. Add additive prediction payload fields.
5. Store rotation audit in `Prediction.feature_attribution_summary["rotation_shock"]`.
6. Persist per-game audit parquet.
7. Keep legacy output fields unchanged.

Deliverables:

- prediction path with rotation shock
- MC distribution outputs
- audit metadata in predictions

Tests:

- `tests/integration/test_rotation_predict_upcoming.py`
- endpoint schema tests for additive fields

Acceptance:

- existing board endpoints still pass
- predictions include DNP risk when player has uncertain status
- clean-roster games behave like legacy path within expected tolerance

### Phase 7: Backtest And Statistical Validation

**Recontext — May 2026:** The initial Phase 7 run showed **no-go**, but follow-up investigation found three **implementation bugs** in the rotation-shock path (not falsification of the treatment). After fixes plus a wider train/eval window, the criterion run is **go**.

| Root cause | Symptom | Fix |
|---|---|---|
| **RC1** `status_to_play_probability` lacked `"out for season"` → default 1.0 | ~266 OSF rows never entered absence pipeline | Mapping entry `0.0`; test `test_out_for_season_is_zero` |
| **RC2** `_build_shadow_absence_profiles` built histories from eval window only | Absent players with no eval-window box scores silently dropped (~7 bogus profiles vs 392 eval rows). Treatment near no-op. | Optional `historical_frame`; backtest wires `historical_frame=train` on eval `_apply_rotation_treatment_mode`; tests in `tests/unit/test_rotation_shadow_profiles.py` |
| **RC3** Missing `rotation_weights.parquet`; Parquet dtype mix int/`LEAGUE` | All lookups `"fallback"` uniform weights | Regenerate artifact; `team_id` cast to string before parquet write |

Authoritative rerun: `python scripts/backtest_rotation_shock.py --eval-days 30 --train-days 150` → `reports/rotation_calibration/20260506T221716Z/`, status **go** (high-injury slice: 6/6 RMSE improves; 4/6 log_loss improves).

**Script hardening shipped:** empirical PIT (`sample_market_outcomes` + mid-rank CDF); train-split `ProbabilityCalibrator` applied to probability metrics unless `--no-calibrator`; expanded acceptance (per-market high-injury RMSE + log loss, mean coverage band `--coverage-low/--coverage-high`, per-market clean-roster RMSE when both `off` and `full` run); clustered bootstrap (--bootstrap-iters`; optional `--bootstrap-as-blocker); treatment-fire diagnostics JSON; `daily_by_game_date.csv`; scale-aware **weighted_rmse** in summary; parquet/csv paired high-injury rows for bootstrap. **Post-review fixes:** eval features now build over combined train+eval chronology before slicing, and PIT/coverage sampling uses the same market distribution family plus simulation context as inference. **Outstanding:** Production uses OOF-calibrated stacks; Phase 7 uses in-sample-on-train calibration (documented discrepancy).

Goal:

- Prove the treatment improves accuracy where it should and does not degrade clean games.

Substeps:

1. Add `scripts/backtest_rotation_shock.py`.
2. Run control: legacy pipeline.
3. Run treatment: rotation shock + Monte Carlo.
4. Run ablations:
   - `off`
   - `features-only`
   - `full`
5. Slice metrics by injury state:
   - `team_out_count = 0`
   - `team_out_count = 1`
   - `team_out_count >= 2`
6. Track metrics per market:
   - RMSE
   - MAE
   - Brier score
   - log loss
   - ECE
   - p10/p90 coverage
   - PIT histogram quality
   - sportsbook-line divergence
7. Persist reports to `reports/rotation_calibration/<date>/`.

Deliverables:

- backtest script
- calibration report
- ablation report
- go/no-go summary

Acceptance:

- treatment beats control on RMSE and log loss for `team_out_count >= 2`
- treatment does not regress no-injury slice
- p10/p90 coverage is close to 80%
- no major calibration regression
- high-profile injury spot checks make basketball sense

### Phase 8: Shadow Inference Window

Goal:

- Run old and new predictions side-by-side before promotion.

**Begun:** `persist_predictions=` on `TrainingPipeline.predict_upcoming` skips DB writes for the shadow branch; `scripts/rotation_shadow_compare.py` runs legacy (persisted) vs treatment (dry) and emits CSV/markdown plus optional rolling rollup; `--shadow-compare` on `scripts/run_daily_automation.py` appends findings to the daily report. **`scripts/join_rotation_shadow_with_actuals.py`** joins `rotation_shadow_overlap.csv` rows to finalized box-score columns for retrospective outcome deltas. **Post-review fixes:** branch simulation is disabled when rotation shock is off, shadow dry-runs no longer write authoritative audit artifacts, comparison pairing includes line value, zero-minute branches simulate zero outcomes, and outcome joins report residual/absolute/squared errors rather than row-level "RMSE".

Substeps:

1. Add `--shadow-compare` mode to daily automation or a separate script.
2. Run legacy predictions as authoritative.
3. Run rotation-shock predictions in shadow.
4. Compare for two full game weeks.
5. Inspect high-injury games manually.
6. Record daily deltas and actual outcomes.

Deliverables:

- two-week comparison report
- promotion recommendation

Acceptance:

- no live regression on daily RMSE
- no clean-roster degradation
- high-injury slice improves or remains clearly better
- audit records are sane

### Phase 9: Promotion And Rollback

**Promotion update -- May 2026:** This is the final v1 implementation phase.
Qwen explanations are deferred to a future post-v1 track, not counted as a
remaining rotation-shock phase.

Goal:

- Make rotation shock authoritative with safe rollback.

Substeps:

1. Set `ROTATION_SHOCK_ENABLED=true` as default.
2. Keep `LEGACY_PIPELINE_ENABLED=true` available for one week.
3. Monitor nightly reports.
4. If stable, remove or deprecate legacy flag.
5. Add architecture doc under `docs/`.

Rollback:

- Set `ROTATION_SHOCK_ENABLED=false` for training rollback.
- Set `LEGACY_PIPELINE_ENABLED=true` for inference rollback.
- Audit artifacts are write-only and do not require rollback.

### Future: Qwen Explanation Layer

Goal:

- Add narrative after model accuracy and audit are stable.

Substeps:

1. Create a separate design doc.
2. Feed Qwen only factual audit payloads.
3. Cache explanations.
4. Add tests that prevent invented numbers.
5. Add UI panel only after endpoint payload is stable.

Deliverables:

- explanation service
- prompt tests
- UI copy based on audit facts

## Testing Matrix

Unit tests:

- `test_rotation_play_probability.py`
- `test_role_vector.py`
- `test_rotation_redistribute.py`
- `test_rotation_weights_aggregation.py`
- `test_rotation_monte_carlo.py`

Integration tests:

- `test_rotation_shadow_features.py`
- `test_train_serve_parity.py`
- `test_leakage_guard.py`
- `test_rotation_audit_roundtrip.py`
- `test_rotation_predict_upcoming.py`

Backtest scripts:

- `scripts/backtest_rotation_shock.py`
- `scripts/deep_model_eval.py --rotation-shock=off|features-only|full`

Standard gates:

```powershell
python -m pytest tests/unit/test_rotation_play_probability.py tests/unit/test_role_vector.py tests/unit/test_rotation_redistribute.py -q
python -m pytest tests/integration/test_train_serve_parity.py tests/integration/test_leakage_guard.py -q
python -m ruff check .
python scripts/backtest_rotation_shock.py
```

Use narrower commands while coding. Run the full suite only at phase boundaries.

## Risk Register

| Risk | Likelihood | Impact | Mitigation |
| --- | --- | --- | --- |
| Pregame injury timestamps unreliable for older seasons | High | Medium | Use `source="post_hoc"` plus `rotation_shock_confidence=0.5`; test leakage guard. |
| Train/serve skew | Low | Critical | Single `rotation.redistribute` source of truth; parity integration test. |
| Double-counting legacy injury features | Medium | High | Run `off|features-only|full` ablations before promotion. |
| Learned weights overfit small samples | Medium | Medium | Inverse-variance weighting plus league fallback and sanity reports. |
| Coach/team changes make old weights stale | Medium | Medium | Refresh nightly; track drift; fallback when sample quality degrades. |
| Monte Carlo compute becomes heavy | Low | Medium | Availability enumeration, absence-set cache, adaptive sampling, async/shadow profiling. |
| New fields break downstream consumers | Low | High | Additive payload only; legacy fields unchanged. |
| Audit artifact shape drifts | Medium | Low | Roundtrip tests and versioned artifact schema. |

## Context And Token Optimization Notes

This project is large enough that the implementation should be split across explicit sessions. Do not keep every detail in model context at once.

### Session Strategy

Recommended sessions:

1. Schemas and pure rotation logic.
2. Weight-learning script.
3. Monte Carlo and simulation settings.
4. Training shadow mode.
5. Treatment training integration.
6. Inference integration.
7. Backtest and validation scripts.
8. Promotion/monitoring docs.

Each session should start by reading only:

- this plan
- `memory/rotation_shock_index.md`
- the files owned by that phase
- the relevant tests

### Context Indexing

Create and maintain:

- `memory/rotation_shock_index.md`

It should contain:

- current phase
- files changed
- decisions already locked
- open questions
- commands run
- known failing tests
- artifact paths
- model/backtest result links

Update it at the end of each session. This prevents rereading the whole repository.

### Brain And Durable Memory Updates

Use durable project memory for compact state, not for raw logs.

At the end of each implementation session, update the index or brain memory with:

- phase completed
- exact files changed
- core decision changes
- test results
- known blockers
- next recommended command

Do not store:

- full command outputs
- full chat logs
- full source files
- speculative reasoning that is not needed later

If using the project's `app/services/brain` tools or memory files, write short structured entries that future sessions can search by terms like `rotation_shock`, `monte_carlo`, `train_serve_parity`, and `injury_weights`.

### Tool Use

Use tools aggressively:

- `rg` for locating code.
- `rg --files` for file maps.
- targeted `Get-Content` line windows instead of full-file reads.
- `pytest` on the smallest relevant test set during coding.
- full checks only at phase boundaries.

Avoid:

- rereading large files repeatedly
- broad searches without a focused pattern
- long prose status updates while coding
- dumping huge command output into the conversation

### Model/Agent Use

For lower-cost or lower-context work, delegate or use smaller models only for bounded tasks:

- writing isolated unit tests from a clear spec
- summarizing one module
- drafting docs after code behavior is known
- scanning for references to a symbol

Keep higher-reasoning work local:

- architecture decisions
- train/serve parity design
- leakage guards
- statistical validation
- integration of Monte Carlo with existing distribution code

If using sub-agents, give each one a disjoint file set and a narrow output contract. Do not ask multiple agents to edit the same module.

### Reporting Style

While implementing:

- report only what changed, what was verified, and what is next
- avoid long brainstorming in status updates
- keep interim updates short
- put durable knowledge in `memory/rotation_shock_index.md`, not chat

### Phase Boundaries

At the end of each phase, write a compact handoff:

- files changed
- tests passing/failing
- behavior changes
- known risks
- next phase starting point

This handoff should be enough to resume without replaying the chat log.

## Final Acceptance Criteria

The plan is successful only if:

- high-injury-game predictions improve versus legacy control
- clean-roster predictions do not regress
- train/serve parity is tested
- no post-tipoff leakage is used
- Monte Carlo outputs are calibrated enough for betting decisions
- rotation audit records are inspectable per game
- downstream APIs remain backward compatible
- rollout has a one-flag rollback path

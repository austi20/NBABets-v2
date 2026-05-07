# Rotation Shock Implementation Index

## Current Phase

- Phase 9: Promotion And Rollback
- Status: completed_with_targeted_validation
- Behavior changes in this phase: yes (rotation shock is authoritative by default; explicit rollback env flags remain available)

## Locked Decisions Snapshot

- Redistribution is an explicit pre-model adjustment layer.
- The adjustment runs in both training and inference.
- Minutes and usage are redistributed with small team-environment effects.
- Monte Carlo availability handling is part of v1.
- Audit records are persisted to local parquet artifacts from v1.

## Key Files

- `plan.md` - implementation plan and acceptance criteria.
- `app/config/settings.py` - simulation settings defaults.
- `app/training/pipeline.py` - training/inference pipeline entry.
- `app/training/data.py` - availability context and feature attachment.
- `app/training/features.py` - model feature construction.
- `app/training/models.py` - minutes/stat model stack.
- `app/training/distributions.py` - adaptive simulation system.
- `scripts/retrain_and_predict.py` - retrain + predict workflow.
- `scripts/run_daily_automation.py` - scheduled automation path.

## Entry Points

- Training pipeline: `app/training/pipeline.py`
- Upcoming inference: `TrainingPipeline.predict_upcoming` in `app/training/pipeline.py`
- Retrain script: `python scripts/retrain_and_predict.py`
- Daily automation: `python scripts/run_daily_automation.py`

## Original Simulation Settings (Before Phase 3)

From `app/config/settings.py`:

- `simulation_target_margin = 0.01`
- `simulation_min_samples = 50000`
- `simulation_max_samples = 1000000`
- `simulation_batch_size = 50000`

## Feature Flags (Phase 0 Decision)

- `ROTATION_SHOCK_ENABLED`
  - Purpose: enable treatment path that uses explicit rotation adjustments.
  - Phase 0 default intent: disabled.
- `ROTATION_SHOCK_SHADOW_MODE`
  - Purpose: compute/store adjusted artifacts without changing model behavior.
  - Phase 0 default intent: disabled.
- `LEGACY_PIPELINE_ENABLED`
  - Purpose: keep legacy path available for compare/rollback windows.
  - Phase 0 default intent: enabled when dual-run/rollback is needed.

## Phase Ownership Checklist

- [x] Create implementation index file.
- [x] Record key files and entry points.
- [x] Record current simulation defaults.
- [x] Document Phase 0 feature flag names and intent.

## Files Changed

- Added `memory/rotation_shock_index.md`.
- Added `app/training/rotation.py`.
- Added `tests/unit/test_rotation_play_probability.py`.
- Added `tests/unit/test_role_vector.py`.
- Added `tests/unit/test_rotation_redistribute.py`.
- Added `app/training/rotation_weights.py`.
- Added `scripts/learn_rotation_weights.py`.
- Added `tests/unit/test_rotation_weights_aggregation.py`.
- Added `app/training/rotation_monte_carlo.py`.
- Added `tests/unit/test_rotation_monte_carlo.py`.
- Added `app/services/rotation_audit.py`.
- Added `tests/integration/test_rotation_shadow_features.py`.
- Added `tests/integration/test_rotation_audit_roundtrip.py`.
- Added `tests/integration/test_train_serve_parity.py`.
- Added `tests/integration/test_rotation_training_frame.py`.
- Updated `app/config/settings.py` simulation defaults:
  - `simulation_min_samples = 10000`
  - `simulation_max_samples = 100000`
  - `simulation_batch_size = 10000`
- Updated Phase 2 after review:
  - explicit absence-event observation learning
  - league rows preferred over team fallback diagnostics
  - proportional multi-absence attribution
  - expanded sanity report diagnostics
  - direct CLI import bootstrap
- `plan.md` is present as the implementation plan.

## Test Commands

General developer checks:

- `pytest`
- `ruff check .`
- `mypy app`

Plan-defined phase-boundary checks:

- `python -m pytest tests/unit/test_rotation_play_probability.py tests/unit/test_role_vector.py tests/unit/test_rotation_redistribute.py -q`
- `python -m pytest tests/integration/test_train_serve_parity.py tests/integration/test_leakage_guard.py -q`
- `python -m ruff check .`
- `python scripts/backtest_rotation_shock.py`

## Commands Run In This Session

- Read `plan.md`
- Read `app/config/settings.py`
- Read `README.md`
- Created `memory/rotation_shock_index.md`
- Added `app/training/rotation.py`
- Added Phase 1 unit tests for play probability, role vector, and redistribution
- Ran `python -m pytest tests/unit/test_rotation_play_probability.py tests/unit/test_role_vector.py tests/unit/test_rotation_redistribute.py -q`
- Applied Phase 1 review fixes:
  - scoped redistribution to requested `game_id`/`team_id`
  - keyed weight lookup by team/season with league/fallback ordering
  - allocated gains per absence instead of blending multiple absences
  - prevented uncertain players from absorbing their own removed opportunity
  - added conservative minutes/usage caps with warnings when capacity is exhausted
  - preserved absence source/timestamp/confidence on audit records
- Added regression tests for team scoping, team-season weights, per-absence weighting, uncertain self-allocation, caps, and zero-absence adjustments
- Ran `python -m pytest tests/unit/test_rotation_play_probability.py tests/unit/test_role_vector.py tests/unit/test_rotation_redistribute.py -q`
- Ran `python -m ruff check app/training/rotation.py tests/unit/test_rotation_play_probability.py tests/unit/test_role_vector.py tests/unit/test_rotation_redistribute.py`
- Implemented Phase 2 weight learning utilities and CLI script.
- Ran `python -m pytest tests/unit/test_rotation_weights_aggregation.py -q`
- Applied Phase 2 review fixes:
  - loads explicit pregame injury absences and official inactive rows
  - derives optional post-hoc missing-rotation absences with reduced confidence
  - builds trailing-15 baselines with season-to-date fallback
  - attributes candidate gains proportionally by absent usage/minutes basis
  - uses game-team `player_team_id` instead of current player team where available
  - prevents learned team fallback rows from outranking `LEAGUE`
  - adds `last_updated` metadata and expanded sanity report sections
- Ran `python scripts/learn_rotation_weights.py --help`
- Ran `python -m pytest tests/unit/test_rotation_play_probability.py tests/unit/test_role_vector.py tests/unit/test_rotation_redistribute.py tests/unit/test_rotation_weights_aggregation.py -q --basetemp=.tmp_pytest_rotation_phase2c`
- Ran `python -m ruff check app/training/rotation.py app/training/rotation_weights.py scripts/learn_rotation_weights.py tests/unit/test_rotation_weights_aggregation.py`
- Attempted temp full learner smoke:
  - `python scripts/learn_rotation_weights.py --output .tmp_rotation_weights.parquet --report-dir .tmp_rotation_report --disable-post-hoc`
  - Result: timed out after 120s before writing temp outputs; stopped leftover Python processes.
- Implemented Phase 3 Monte Carlo utility module for:
  - exact branch enumeration (`k <= 8`)
  - sampled fallback (`k > 8`) with deterministic seed
  - branch-weighted sample aggregation
  - realized-absence cache keys and cache helper
- Applied Phase 3 review fixes:
  - exact/sample threshold now counts only true uncertain players (`0 < p < 1`)
  - deterministic active/inactive players are merged into every branch
  - branch active maps are immutable
  - positive-weight empty branch samples raise instead of silently shrinking output
  - added branch sample summary helper for p10/p25/p75/p90, boom, and bust metrics
- Ran `python -m pytest tests/unit/test_rotation_monte_carlo.py tests/unit/test_rotation_weights_aggregation.py tests/unit/test_rotation_play_probability.py tests/unit/test_role_vector.py tests/unit/test_rotation_redistribute.py -q --basetemp=.tmp_pytest_rotation_phase3`
- Ran `python -m ruff check app/config/settings.py app/training/rotation_monte_carlo.py tests/unit/test_rotation_monte_carlo.py`
- Ran `python -m pytest tests/unit/test_rotation_monte_carlo.py -q --basetemp=.tmp_pytest_rotation_phase3_fix`
- Ran `python -m pytest tests/unit/test_rotation_monte_carlo.py tests/unit/test_rotation_weights_aggregation.py tests/unit/test_rotation_play_probability.py tests/unit/test_role_vector.py tests/unit/test_rotation_redistribute.py tests/unit/test_distributions.py -q --basetemp=.tmp_pytest_rotation_phase3_fix_full`
- Ran `python -m ruff check app/config/settings.py app/training/rotation_monte_carlo.py tests/unit/test_rotation_monte_carlo.py`
- Began Phase 4 implementation:
  - wired shadow-mode adjustment in training pipeline behind `ROTATION_SHOCK_SHADOW_MODE`
  - added baseline/adjusted rotation columns in shadow path
  - added per-game audit parquet writer integration
  - added rotation-audit service read/write helpers
- Applied Phase 4 review fixes:
  - explicit injury/inactive absence profiles are loaded for shadow training games
  - absent-player baselines are built from prior same-team NBA-season history
  - active questionable/doubtful/probable players receive fractional play probability overlays
  - game audit writes are accumulated once per game, preserving both teams
  - audit reads drop irrelevant NaN fields and return all team-environment rows
  - shadow tests now isolate audit artifacts and assert real redistribution deltas
- Ran `python -m pytest tests/integration/test_rotation_shadow_features.py tests/integration/test_rotation_audit_roundtrip.py -q --basetemp=.tmp_pytest_rotation_phase4`
- Ran `python -m ruff check app/training/pipeline.py app/services/rotation_audit.py tests/integration/test_rotation_shadow_features.py tests/integration/test_rotation_audit_roundtrip.py`
- Ran `python -m pytest tests/integration/test_rotation_shadow_features.py tests/integration/test_rotation_audit_roundtrip.py -q --basetemp=.tmp_pytest_rotation_phase4_fix`
- Ran `python -m pytest tests/unit/test_rotation_play_probability.py tests/unit/test_role_vector.py tests/unit/test_rotation_redistribute.py tests/unit/test_rotation_weights_aggregation.py tests/unit/test_rotation_monte_carlo.py tests/integration/test_rotation_shadow_features.py tests/integration/test_rotation_audit_roundtrip.py -q --basetemp=.tmp_pytest_rotation_phase4_fix_full`
- Ran `python -m ruff check app/training/pipeline.py app/services/rotation_audit.py tests/integration/test_rotation_shadow_features.py tests/integration/test_rotation_audit_roundtrip.py`
- Removed generated temp pytest directories and old untracked audit artifact from the previous shallow test run.

## Tests Run

- `python -m pytest tests/unit/test_rotation_play_probability.py tests/unit/test_role_vector.py tests/unit/test_rotation_redistribute.py -q`
- Result: 13 passed
- `python -m ruff check app/training/rotation.py tests/unit/test_rotation_play_probability.py tests/unit/test_role_vector.py tests/unit/test_rotation_redistribute.py`
- Result: passed
- `python -m pytest tests/unit/test_rotation_weights_aggregation.py -q`
- Result before review fixes: 1 passed
- `python -m pytest tests/unit/test_rotation_play_probability.py tests/unit/test_role_vector.py tests/unit/test_rotation_redistribute.py tests/unit/test_rotation_weights_aggregation.py -q --basetemp=.tmp_pytest_rotation_phase2c`
- Result: 17 passed
- `python -m ruff check app/training/rotation.py app/training/rotation_weights.py scripts/learn_rotation_weights.py tests/unit/test_rotation_weights_aggregation.py`
- Result: passed
- `python scripts/learn_rotation_weights.py --help`
- Result: passed
- Temp full learner smoke with `--disable-post-hoc`
- Result: timed out after 120s; no temp output artifact/report found afterward.
- `python -m pytest tests/unit/test_rotation_monte_carlo.py tests/unit/test_rotation_weights_aggregation.py tests/unit/test_rotation_play_probability.py tests/unit/test_role_vector.py tests/unit/test_rotation_redistribute.py -q --basetemp=.tmp_pytest_rotation_phase3`
- Result: 21 passed
- `python -m ruff check app/config/settings.py app/training/rotation_monte_carlo.py tests/unit/test_rotation_monte_carlo.py`
- Result: passed
- `python -m pytest tests/integration/test_rotation_shadow_features.py tests/integration/test_rotation_audit_roundtrip.py -q --basetemp=.tmp_pytest_rotation_phase4`
- Result: 2 passed
- `python -m ruff check app/training/pipeline.py app/services/rotation_audit.py tests/integration/test_rotation_shadow_features.py tests/integration/test_rotation_audit_roundtrip.py`
- Result: passed
- `python -m pytest tests/integration/test_rotation_shadow_features.py tests/integration/test_rotation_audit_roundtrip.py -q --basetemp=.tmp_pytest_rotation_phase4_fix`
- Result: 5 passed
- `python -m pytest tests/unit/test_rotation_play_probability.py tests/unit/test_role_vector.py tests/unit/test_rotation_redistribute.py tests/unit/test_rotation_weights_aggregation.py tests/unit/test_rotation_monte_carlo.py tests/integration/test_rotation_shadow_features.py tests/integration/test_rotation_audit_roundtrip.py -q --basetemp=.tmp_pytest_rotation_phase4_fix_full`
- Result: 31 passed
- `python -m ruff check app/training/pipeline.py app/services/rotation_audit.py tests/integration/test_rotation_shadow_features.py tests/integration/test_rotation_audit_roundtrip.py`
- Result: passed
- `python -m pytest tests/unit/test_rotation_monte_carlo.py -q --basetemp=.tmp_pytest_rotation_phase3_fix`
- Result: 9 passed
- `python -m pytest tests/unit/test_rotation_monte_carlo.py tests/unit/test_rotation_weights_aggregation.py tests/unit/test_rotation_play_probability.py tests/unit/test_role_vector.py tests/unit/test_rotation_redistribute.py tests/unit/test_distributions.py -q --basetemp=.tmp_pytest_rotation_phase3_fix_full`
- Result: 30 passed
- `python -m ruff check app/config/settings.py app/training/rotation_monte_carlo.py tests/unit/test_rotation_monte_carlo.py`
- Result: passed

## Known Failing Tests

- None recorded.

## Phase 5 Session Update

- Updated `app/training/pipeline.py`:
  - added `ROTATION_SHOCK_ABLATION_MODE` handling (`off`, `features-only`, `full`) under `ROTATION_SHOCK_ENABLED`
  - wired shared treatment helper into train, calibration folds, and inference
  - preserved `baseline_projected_minutes` and promoted adjusted channels
  - `full` mode now sets effective downstream `predicted_minutes` from `adjusted_projected_minutes`
  - simulation context now prefers `adjusted_*` columns when available
  - metadata now records rotation-shock version and rotation-weights artifact hash
- Updated `app/training/features.py` to include `adjusted_*` feature columns in allowlist selection.
- Added integration tests:
  - `tests/integration/test_train_serve_parity.py`
  - `tests/integration/test_rotation_training_frame.py`
- Validation (Phase 5 targeted):
  - `python -m pytest tests/integration/test_train_serve_parity.py tests/integration/test_rotation_training_frame.py -q --basetemp=.tmp_pytest_rotation_phase5` (4 passed)
  - `python -m ruff check app/training/pipeline.py app/training/features.py tests/integration/test_train_serve_parity.py tests/integration/test_rotation_training_frame.py` (passed)
  - `python -m py_compile app/training/pipeline.py app/training/features.py` (passed)
  - known `.pytest_cache` warning remains non-fatal.

## Phase 5 Review Fixes

- Fixed adjusted treatment feature promotion:
  - minutes model keeps its pre-treatment feature set
  - stat model feature columns are rebuilt after rotation treatment columns exist
  - metadata now also records `minutes_feature_columns`
- Updated market model allowlists so adjusted usage/rate channels are actually selectable by market models.
- Recomputed post-minutes features after `full` mode swaps effective `predicted_minutes` to adjusted minutes.
- Attached rotation environment columns to the treatment frame:
  - `team_efficiency_delta`
  - `pace_delta`
  - `rotation_shock_magnitude`
  - `rotation_shock_confidence`
- Downgraded post-tip official inactive rows to `source="post_hoc"` with `rotation_shock_confidence=0.5`.
- Preserved legitimate zero adjusted values in simulation/calibration minute/context fallbacks.
- Added regression coverage for adjusted market feature selection, adjusted post-minutes recomputation, and post-tip inactive downgrade.
- Validation after fixes:
  - `python -m pytest tests/integration/test_train_serve_parity.py tests/integration/test_rotation_training_frame.py -q --basetemp=.tmp_pytest_rotation_phase5_fix` (6 passed)
  - `python -m ruff check app/training/pipeline.py app/training/features.py app/training/models.py tests/integration/test_train_serve_parity.py tests/integration/test_rotation_training_frame.py` (passed)
  - `python -m py_compile app/training/pipeline.py app/training/features.py app/training/models.py` (passed)
  - `python -m pytest tests/unit/test_rotation_play_probability.py tests/unit/test_role_vector.py tests/unit/test_rotation_redistribute.py tests/unit/test_rotation_weights_aggregation.py tests/unit/test_rotation_monte_carlo.py tests/integration/test_rotation_shadow_features.py tests/integration/test_rotation_audit_roundtrip.py tests/integration/test_train_serve_parity.py tests/integration/test_rotation_training_frame.py -q --basetemp=.tmp_pytest_rotation_phase5_fix_full` (37 passed)

## Phase 6 Session Update

- Began inference integration in `app/training/pipeline.py`:
  - added additive prediction payload fields for `dnp_risk`, `boom_probability`, `bust_probability`, and `availability_branches`
  - added additive percentile proxies `percentile_25` and `percentile_75`
  - attached `rotation_shock` context in `feature_attribution_summary` with metadata and per-row baseline/adjusted deltas
  - added availability branch context helpers keyed by `(game_id, team_id, player_id)` from explicit absence profiles
- Updated `app/schemas/domain.py` `PropPrediction` with additive fields (backward compatible defaults).
- Added Phase 6 regression coverage in `tests/integration/test_rotation_training_frame.py` for availability branch context.
- Added `tests/integration/test_rotation_predict_upcoming.py` for additive prediction schema compatibility and tail-probability checks.
- Finalized branch context derivation to use `app/training/rotation_monte_carlo.py` enumeration/sampling helpers directly.
- Validation:
  - `python -m pytest tests/integration/test_train_serve_parity.py tests/integration/test_rotation_training_frame.py -q --basetemp=.tmp_pytest_rotation_phase6` (7 passed)
  - `python -m ruff check app/training/pipeline.py app/schemas/domain.py tests/integration/test_rotation_training_frame.py tests/integration/test_train_serve_parity.py` (passed)
  - `python -m py_compile app/training/pipeline.py app/schemas/domain.py` (passed)
  - known `.pytest_cache` warning remains non-fatal.
  - `python -m pytest tests/integration/test_train_serve_parity.py tests/integration/test_rotation_training_frame.py tests/integration/test_rotation_predict_upcoming.py -q --basetemp=.tmp_pytest_rotation_phase6_finish` (9 passed)
  - `python -m ruff check app/training/pipeline.py app/schemas/domain.py tests/integration/test_train_serve_parity.py tests/integration/test_rotation_training_frame.py tests/integration/test_rotation_predict_upcoming.py` (passed)
  - `python -m py_compile app/training/pipeline.py app/schemas/domain.py` (passed)
  - `python -m pytest tests/unit/test_rotation_play_probability.py tests/unit/test_role_vector.py tests/unit/test_rotation_redistribute.py tests/unit/test_rotation_weights_aggregation.py tests/unit/test_rotation_monte_carlo.py tests/integration/test_rotation_shadow_features.py tests/integration/test_rotation_audit_roundtrip.py tests/integration/test_train_serve_parity.py tests/integration/test_rotation_training_frame.py tests/integration/test_rotation_predict_upcoming.py -q --basetemp=.tmp_pytest_rotation_phase6_full` (40 passed)

## Open Questions

- Confirm default rollout value for `ROTATION_SHOCK_ABLATION_MODE` (`features-only` transition vs direct `full`).
- Decide whether to expose ablation mode through settings model in addition to environment variables.

## Next Phase Starting Point

- Start Phase 7 backtest and statistical validation (`off|features-only|full` ablations).
- Add `scripts/backtest_rotation_shock.py` and slice metrics by injury state (`team_out_count` buckets).
- Verify treatment improves high-injury slices without regressing clean-roster games.

## Next Recommended Command

- `git status --short; git diff --stat`

## Phase 6 Post-Review + Phase 7 Pre-Work

- Recontext from Phase 6 post-review fix report confirmed in repo:
  - `app/training/distributions.py` now computes `p25/p75/boom/bust` from real samples.
  - `app/services/query.py` forwards additive Phase 6 fields from `feature_attribution_summary`.
  - `tests/integration/test_rotation_predict_upcoming.py` now validates behavioral outcomes (not just schema kwargs).
- Began Phase 7 implementation of the known gap (branch-weighted stat prediction):
  - Updated `app/training/pipeline.py` to build realized availability branch frames per `(game_id, team_id)` using `rotation.redistribute(..., mode="realized")`.
  - Updated market simulation path to run branch-specific stat simulation and combine branch outputs into a mixture summary for each player-market row.
  - Prediction payload now consumes branch-mixture `p25/p75/boom/bust` directly (instead of post-hoc DNP correction approximation).
- Validation:
  - `python -m ruff check app/training/pipeline.py tests/integration/test_rotation_predict_upcoming.py` (passed)
  - `python -m pytest tests/integration/test_rotation_predict_upcoming.py -q` (10 passed)

## Phase 7 Session Update

- Added `scripts/backtest_rotation_shock.py` for Phase 7 backtest/validation:
  - runs `off`, `features-only`, `full` ablations
  - computes per-market and injury-slice metrics (`team_out_count=0|1|>=2`)
  - reports RMSE, MAE, Brier, log loss, ECE, p10/p90 coverage, PIT KS, and sportsbook-line divergence
  - emits reports to `reports/rotation_calibration/<timestamp>/`
- Added automated phase artifacts:
  - `ablation_summary.csv`
  - `ablation_details.csv`
  - `ablation_report.md`
  - `phase_change_report.md`
  - `bug_report.md`
- Full Phase 7 medium run completed:
  - command: `python scripts/backtest_rotation_shock.py --eval-days 14 --train-days 120 --max-eval-rows 3000`
  - output root: `reports/rotation_calibration/20260506T210300Z/`
  - high-injury sample present (`team_out_count>=2` rows = 37 per market slice)
  - phase decision: **no-go** (full underperformed off on high-injury and clean-roster RMSE)

## Phase 9 Session Update

- Promoted rotation shock to the default authoritative path:
  - `ROTATION_SHOCK_ENABLED` defaults to true.
  - `ROTATION_SHOCK_ABLATION_MODE` defaults to `full`.
  - `LEGACY_PIPELINE_ENABLED` defaults to true for the rollback window.
- Added settings-model support for the rotation flags so `.env` files are honored in addition to process env vars.
- Updated shadow comparison after promotion:
  - daily `--shadow-compare` is dry-run by default.
  - `scripts/rotation_shadow_compare.py --persist-legacy` is available for rollback-only legacy persistence.
- Added architecture and rollback documentation:
  - `docs/rotation_shock_architecture.md`
- Updated `.env.example` with promoted rotation defaults and Phase 3 simulation defaults.
- Updated `plan.md` so Qwen explanation work is marked as future/post-v1 rather than a remaining numbered rotation-shock phase.

## Phase 9 Validation

- `python -m pytest tests/integration/test_rotation_training_frame.py tests/unit/test_rotation_shadow_inference_compare.py -q --basetemp=.tmp_pytest_rotation_phase9` (10 passed; known `.pytest_cache` warning)
- `python -m ruff check app/config/settings.py app/training/pipeline.py app/services/automation.py app/services/rotation_shadow_compare.py scripts/run_daily_automation.py scripts/rotation_shadow_compare.py tests/integration/test_rotation_training_frame.py` (passed)
- `python -m py_compile app/config/settings.py app/training/pipeline.py app/services/automation.py app/services/rotation_shadow_compare.py scripts/run_daily_automation.py scripts/rotation_shadow_compare.py` (passed)

## Phase 9 Next Recommended Command

- Monitor the first promotion-week run with `python scripts/run_daily_automation.py --shadow-compare`.

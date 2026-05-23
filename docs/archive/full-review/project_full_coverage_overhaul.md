## Current State (updated 2026-04-12)
- Last completed step: Phase 5 complete
- Files modified so far: `app/config/settings.py`, `app/training/data_sufficiency.py`, `app/training/pipeline.py`, `app/training/models.py`, `app/training/features.py`, `app/training/feature_builders/rolling_windows.py`, `app/training/artifacts.py`, `app/training/data.py`, `app/services/query.py`, `app/services/prop_analysis.py`, `app/desktop/insights.py`, `tests/unit/test_data_sufficiency.py`, `tests/unit/test_population_priors.py`, `memory/project_full_coverage_overhaul.md`
- Files still to modify: none for the full-coverage plan
- Tests passing: yes, `182/182` via `python -m pytest tests/ --basetemp .pytest_tmp_final`
- Key decisions made:
  - Tier thresholds are configurable and default to the plan values
  - Team changes downgrade by one full tier (`A -> B -> C -> D`)
  - Historical starter flags in this database were effectively all zero, so role-prior building falls back to `minutes >= 24` to recover usable `*_starter` buckets
  - Tier D prior fill restores current live market/odds columns after prior injection so sportsbook context stays real while player-history holes are filled
  - Optional post-trade rolling-window weighting was left unchanged because the builder already has strong recency weighting (`ewm_10`) and the plan marked this sub-item optional
- Next step: monitor live-board prediction quality, especially extreme rebound probabilities and any future Tier D boards
- Blockers: none

## Phase 1A
- Completed:
  - Added configurable thresholds in `app/config/settings.py`
  - Reworked `app/training/data_sufficiency.py` so `classify_data_sufficiency_tier()` now matches the plan inputs exactly
  - Added `tests/unit/test_data_sufficiency.py`
- Current state:
  - `classify_data_sufficiency_tier()` is at `app/training/data_sufficiency.py:25`
  - `annotate_tiers()` is at `app/training/data_sufficiency.py:70`
- Thresholds chosen:
  - Tier A: `>= 10` games, `>= 100` total minutes, `recent_avg_minutes >= 12`
  - Tier B: `5-9` games with `>= 50` total minutes, or `10+` games that miss the Tier A stability/minutes bar
  - Tier C: `1-4` games, or `5-9` games that still miss the `50` total-minute floor
  - Tier D: `0` games
- Test file location: `tests/unit/test_data_sufficiency.py`

## Phase 1B
- Completed:
  - `TrainingPipeline.predict_upcoming()` calls `_annotate_data_sufficiency()` at `app/training/pipeline.py:253`
  - `_annotate_data_sufficiency()` lives at `app/training/pipeline.py:415`
  - The old drop-and-filter path remains replaced by an annotator that preserves all rows
- Return-value change:
  - Old behavior: filter rows down to only sufficiently historical players
  - New behavior: return all rows plus `_data_sufficiency_tier`
- Edge cases:
  - Empty upcoming frame: returns early with an empty `_data_sufficiency_tier` column
  - Empty historical frame: all upcoming rows are annotated Tier `D`

## Phase 1C
- Completed:
  - `PropPrediction.data_sufficiency_tier` / `data_confidence_score` added at `app/schemas/domain.py:139`
  - Prediction emission stores `data_sufficiency_tier` / `data_confidence_score` in `feature_attribution_summary` at `app/training/pipeline.py:357`
  - Stored reads hydrate those fields in `app/services/query.py:317`
- JSON keys used in `feature_attribution_summary`:
  - `data_sufficiency_tier`
  - `data_confidence_score`
- Verification:
  - `python -m pytest tests/unit/test_data_sufficiency.py` -> `6/6` passed
  - Full suite after final implementation -> `182/182` passed
  - Full-board coverage after final implementation -> `511` active lines, `511` predictions generated

## Phase 2A
- Completed:
  - `FittedMarketModel` now carries `role_total_priors`, `role_rate_priors`, and `role_variance_priors` at `app/training/models.py:188`
  - Role priors are built during `StatModelSuite.fit()` at `app/training/models.py:788`
  - `ArtifactPaths.population_priors` added at `app/training/artifacts.py:20`
- Artifact path:
  - `data\\processed\\models\\v1\\sqlite_data_processed_nba_props_sqlite_b8408d4cbdd1\\population_priors.joblib`
- Role bucket naming convention:
  - `G_starter`, `G_bench`, `F_starter`, `F_bench`, `C_starter`, `C_bench`, plus `U_*` fallback buckets
- Sample live feature-prior values from the regenerated artifact:
  - `G_starter`: `points_avg_10=14.1036`, `rebounds_avg_10=3.7961`, `assists_avg_10=3.9272`, `starter_flag=1.0`
  - `G_bench`: `points_avg_10=7.8425`, `rebounds_avg_10=2.4942`, `assists_avg_10=2.2799`, `starter_flag=0.0`
  - `F_starter`: `points_avg_10=14.3336`, `rebounds_avg_10=5.3766`, `assists_avg_10=2.7598`, `starter_flag=1.0`
  - `C_starter`: `points_avg_10=13.2322`, `rebounds_avg_10=8.5116`, `assists_avg_10=2.3694`, `starter_flag=1.0`
- Sample live model role priors from `stat_models.joblib`:
  - `points.role_total_priors`: `G_starter=16.0143`, `G_bench=6.7629`, `F_starter=16.1991`, `C_starter=15.1626`
  - `rebounds.role_total_priors`: `G_starter=4.2302`, `G_bench=2.2698`, `F_starter=6.0403`, `C_starter=9.8916`
  - `assists.role_total_priors`: `G_starter=4.4316`, `G_bench=2.0324`, `F_starter=3.0538`, `C_starter=2.8266`

## Phase 2B
- Completed:
  - `RollingWindowBuilder.build_player_history_features(..., fill_terminal_nan=...)` added at `app/training/feature_builders/rolling_windows.py:65`
  - Training path still uses `fill_terminal_nan=0.0`
  - Inference path uses `fill_terminal_nan=None` via `FeatureEngineer.build_inference_frame()` at `app/training/features.py:438`
  - Tier-aware fill helper `_fill_with_population_priors()` added at `app/training/features.py:1065`
- Fill behavior:
  - Tier A/B: same cascade as before, final fallback still `0.0`
  - Tier C: remaining NaNs fill from position-group priors, then `0.0`
  - Tier D: feature columns fill from role/position priors, then current live market/odds columns are restored from the actual board
- Regression note:
  - The targeted training/feature subset passed during implementation
  - Final suite status is `182/182` passing

## Phase 2C
- Completed:
  - Tier D role-bucket selection now uses `line_value` thresholds inside `FeatureEngineer._population_role_bucket_for_row()` at `app/training/features.py:1102`
- Heuristic mapping:
  - `points >= 18.0 -> starter`
  - `rebounds >= 6.5 -> starter`
  - `assists >= 4.5 -> starter`
  - `threes >= 1.8 -> starter`
  - `turnovers >= 1.5 -> starter`
  - `pra >= 28.0 -> starter`
  - If `projected_starter_flag` exists it overrides the heuristic
- Live-board note:
  - The April 12, 2026 board had no Tier D rows after ingestion (`A=486`, `B=17`, `C=8`, `D=0`), so the Tier D path is implemented and unit-tested but had no live same-day examples to spot-check

## Phase 3A
- Completed:
  - `_apply_partial_pooling()` is at `app/training/models.py:600`
  - Tier-based prior strength mapping is implemented there
- Tier -> `prior_strength`:
  - `A: 10.0`
  - `B: 15.0`
  - `C: 25.0`
  - `D: 50.0`
- Fallback:
  - When `_data_sufficiency_tier` is missing, the model falls back to `self.prior_strength`

## Phase 3B
- Completed:
  - `_inflate_variance()` is at `app/training/models.py:677`
  - Tier multipliers are applied after the existing lineup/minutes inflation
- Tier variance multipliers:
  - `A: 1.0x`
  - `B: 1.3x`
  - `C: 2.0x`
  - `D: 3.0x`

## Phase 3C
- Completed:
  - `_compute_data_confidence()` is at `app/training/pipeline.py:49`
  - Prediction emission uses it at `app/training/pipeline.py:334`
  - Values are stored both in the schema object and in `feature_attribution_summary`
- Formula:
  - `base = {"A": 0.90, "B": 0.70, "C": 0.45, "D": 0.25}[tier]`
  - `uncertainty_penalty = min(variance / (mean + 1), 0.30)`
  - `history_bonus = min(games_played / 30, 0.10)`
  - `recency_bonus = 0.05 if days_since_last_game <= 3 else 0.0`
  - `injury_penalty = 0.15 if injury_return else 0.0`
  - `trade_penalty = 0.10 if team_changed else 0.0`
  - `score = clip(base - uncertainty_penalty + history_bonus + recency_bonus - injury_penalty - trade_penalty, 0.05, 0.95)`
- Live ranges observed on the April 12, 2026 board:
  - Tier A: `0.6500 -> 0.9128` (avg `0.7580`)
  - Tier B: `0.5500 -> 0.5500` (avg `0.5500`)
  - Tier C: `0.2333 -> 0.4176` (avg `0.3043`)
  - Tier D: none observed on this board

## Phase 4
- Completed:
  - `_attach_player_injury_context()` added at `app/training/data.py:637`
  - The helper is called from `load_historical_player_games()` at `app/training/data.py:190`
  - The helper is called from `load_upcoming_player_lines()` at `app/training/data.py:282`
  - `player_on_inactive_list` rows are skipped in `predict_upcoming()` at `app/training/pipeline.py:247`
- New feature names:
  - `player_injury_return_flag`
  - `player_days_since_last_game`
  - `player_games_since_return`
  - `days_since_extended_absence`
  - `player_on_inactive_list` (used as a valid exclusion, not a model feature)
- SQL queried:
  - `injury_reports`: `player_id`, `report_timestamp`, `status`, `designation`
  - `game_player_availability`: `game_id`, `player_id` where `is_active = 0`
  - historical game context comes from `player_game_logs` joined to `games`
- Schema assumptions:
  - `injury_reports.designation` exists and can contain values such as `Out` / `Doubtful`
  - `game_player_availability.is_active = 0` represents a confirmed inactive / unavailable player
- Valid exclusion behavior:
  - `player_on_inactive_list > 0.5` is treated as a confirmed DNP and the row is skipped before feature generation

## Phase 5
- Completed:
  - `_attach_trade_context()` added at `app/training/data.py:775`
  - `_load_player_history_context()` added at `app/training/data.py:846`
  - `team_changed_recently` is fed into training/inference features in `app/training/features.py:198` and `app/training/features.py:527`
  - Tier downgrade on trade is applied in `app/training/data_sufficiency.py:45`
- Trade detection logic:
  - Historical rows compare each `player_team_id` to the prior `player_team_id` within the player’s game history
  - `team_changed_recently` stays `1.0` for the first five games after a detected change
  - Upcoming rows set `_team_changed` when the current `team_id` differs from the most recent historical `player_team_id`
- Rolling-window weighting:
  - Left unchanged intentionally; the builder already emphasizes recency via `ewm_10`, and the plan marked extra post-trade weighting optional
- Final coverage stats:
  - `total_lines_available = 511`
  - `inactive_lines = 0`
  - `total_predictions_generated = 511`
  - Coverage is effectively `100%` of active lines on the April 12, 2026 board

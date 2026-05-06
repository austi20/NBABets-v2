# LLM fine-tuning curricula (accuracy examiner)

This spec aligns supervised data with how the NBA app calls local Qwen via `AIOrchestrator` (`task_name` values in code).

## Train / validation split

- **Split key**: `game_date` from the props CSV (Curriculum A). **Never** shuffle rows randomly for A; use a cutoff date.
- **Default cutoff** (CLI): training rows have `game_date < val_from`; validation rows have `game_date >= val_from`. Override with `--train-until` / `--val-from`.
- **Curricula B/C** (synthetic seeds): assign synthetic `game_date` in metadata as `1970-01-01` and include them **only in training** unless you duplicate a held-out seed file for regression tests.

## Curriculum A — `csv_qa` (tabular accuracy)

**Goal**: Teach the model to spot inconsistent labels, impossible combinations, and missing odds using the same columns as `nba_props_2025_26.csv`.

**User message**: JSON object describing one prop row (subset of columns), e.g. `game_date`, `game_id`, `player_name`, `market`, `line_value`, `over_odds`, `under_odds`, `actual`, `hit_over`, `hit_under`, `push`, `minutes`, `source`.

**Assistant message**: Strict JSON with schema (see `schemas/csv_qa_gold.schema.json`):

- `issues`: list of `{ "code", "severity", "detail" }` where `code` is from a fixed vocabulary (`hit_mismatch`, `missing_odds`, `suspicious_minutes`, `line_actual_inconsistent`, `none`).
- `recalculated`: `{ "hit_over", "hit_under", "push" }` — deterministic truth from `actual` vs `line_value` (half-point logic; float tolerance `1e-6`).
- `agrees_with_file`: boolean — whether file flags match `recalculated`.

Gold labels are **generated deterministically** in `llm_train/dataset/projectors.py` so the model learns repair behavior.

## Curriculum B — `local_autonomy` (strict JSON copilot)

**Goal**: Match [`_build_autonomy_prompt`](../../app/services/local_autonomy/engine.py): user text begins with the same instruction block, then `Snapshot:\n{...}`.

**Assistant**: JSON object parseable by `extract_json_object` with at least `status` and `confidence`; production also uses `summary` and `actions`.

- `status`: one of `advisory`, `hold`, `execute` (production clamps unknown values to `advisory`).
- `confidence`: number in `[0, 1]`.
- `summary`: short string.
- `actions`: list of `{ "action_type", "reason", "confidence", "payload" }` where `action_type` is preferably from `ALLOWED_ACTION_CLASS` keys in [`contracts.py`](../../app/services/local_autonomy/contracts.py): `run_refresh_all`, `run_backtest`, `retrain_and_predict`, `set_release_override`, `promote_model_candidate`, `patch_feature_logic`.

Seed examples live in `seed_autonomy.jsonl` (hand-authored; extend over time).

## Curriculum C — `automation` (health + retrain prose)

**Goal**: Match prompts shaped like [`automation.py`](../../app/services/automation.py) `model_prompt`, `provider_prompt`, `retrain_prompt`.

**User message**: One of those prompt templates with numeric slots filled.

**Assistant**: Markdown or plain text with:

- Exactly **four** bullet lines for health tasks (`model_health`, `provider_health`), or three bullets + decision line for `retrain_decision` (`Trigger=YES/NO`, `Reason`, `Confidence`).

Seed examples in `seed_automation.jsonl`.

## Versioning

Each dataset build writes `manifest.json` next to `train.jsonl` / `val.jsonl`:

- `schema_version`
- `curricula`
- `csv_path`, `csv_sha256` (optional), row counts, `val_from` date, git commit if available.

## JSONL record shape (all curricula)

One line per example:

```json
{
  "messages": [
    {"role": "user", "content": "..."},
    {"role": "assistant", "content": "..."}
  ],
  "meta": {
    "curriculum": "csv_qa",
    "game_date": "2026-01-03",
    "source_row": 12
  }
}
```

Training code expects a `messages` column (Hugging Face / TRL SFT).

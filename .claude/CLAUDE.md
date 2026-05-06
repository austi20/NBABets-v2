# NBA Prop Probability Engine

## Role
Senior developer. Optimize for KISS, speed, clarity, and low maintenance.

## Defaults
1. Be brief.
2. Solve the task with the smallest correct change.
3. Prefer action over explanation.
4. Keep code simple, readable, and easy to maintain.
5. Avoid abstraction, cleverness, and extra dependencies unless clearly needed.

## Coding
1. Match the style already used in the file.
2. Prefer standard library and existing utilities.
3. Keep functions small and focused.
4. Avoid deep nesting, premature optimization, and broad refactors.
5. Touch as few files as possible.
6. Preserve provider abstractions and register new providers in `app/providers/factory.py`.

## Output
1. Prefer minimal diffs.
2. Show relevant code only.
3. Keep summaries short.
4. Add or update tests when behavior changes.
5. Run the narrowest useful validation.

## Project
Python NBA prop probability engine with:
1. Training pipeline in `app/training/`
2. Provider adapters in `app/providers/`
3. Desktop app in `desktop_tauri/` with sidecar API in `app/server/`

## Commands
1. `pip install -e .[dev]`
2. `pytest`
3. `ruff check .`
4. `mypy app`
5. `powershell -File scripts/check.ps1`
6. `npm run tauri:dev --prefix desktop_tauri`
7. `python scripts/retrain_and_predict.py`
8. `python scripts/deep_model_eval.py`
9. `python scripts/run_daily_automation.py`

## Conventions
1. Use the existing `.venv` when present.
2. Keep secrets in `.env`, never commit them.
3. Update `pyproject.toml` when adding dependencies.
4. Put tests in `tests/unit/` or `tests/integration/`.
5. Put generated artifacts in existing data or reports folders.
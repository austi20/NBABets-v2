# Validation checklist

## Use targeted checks by default

### Ruff
- touched Python files
- new helper modules
- files with import changes

### Pytest
- touched unit tests first
- integration tests only when behavior crosses boundaries
- avoid full suite unless task scope justifies it

### Mypy
- useful on isolated modules
- avoid full repo mypy for local file-only changes unless requested

## Heavy commands

Run these only when the task directly changes them:
- `npm run tauri:dev --prefix desktop_tauri`
- `python scripts/run_server.py`
- `python scripts/retrain_and_predict.py`
- `python scripts/deep_model_eval.py`
- `python scripts/run_daily_automation.py`

## Report format
- checks run
- pass or fail
- remaining unverified surface
- environment assumptions

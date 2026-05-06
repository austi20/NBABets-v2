# Command quick reference

## General

```bash
pip install -e .[dev]
pytest
ruff check .
mypy app
powershell -File scripts/check.ps1
```

## App and workflows

```bash
npm run tauri:dev --prefix desktop_tauri
python scripts/run_server.py
python scripts/retrain_and_predict.py
python scripts/deep_model_eval.py
python scripts/run_daily_automation.py
```

## Packaging

```bash
powershell -File scripts/build_tauri.ps1
powershell -File scripts/smoke_tauri.ps1
```

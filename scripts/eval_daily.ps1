Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

python scripts/retrain_and_predict.py
python scripts/deep_model_eval.py
python scripts/run_daily_automation.py

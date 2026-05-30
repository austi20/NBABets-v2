Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

python scripts/retrain_and_predict.py
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
python scripts/deep_model_eval.py
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
python scripts/run_daily_automation.py
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

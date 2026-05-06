Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

python -m ruff check .
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

python -m mypy app
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

python -m pytest
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

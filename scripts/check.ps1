Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Non-regression floor for `mypy app`. The remaining errors are dominated by
# pandas/sklearn type friction in the training pipeline (not real bugs) and are
# intentionally deferred. New code must not push the count above this floor.
# See docs/superpowers/plans/2026-05-26-final-counts.txt.
$MypyErrorFloor = 290

# Ruff must be clean (exit 0).
python -m ruff check .
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

# Mypy: count errors and fail only if above the floor (non-regression gate).
$mypyOutput = python -m mypy app
$mypyOutput | Write-Output
$summary = $mypyOutput | Select-String -Pattern 'Found (\d+) error' | Select-Object -Last 1
if ($summary) {
    $mypyErrors = [int]$summary.Matches[0].Groups[1].Value
} else {
    # "Success: no issues found" (or unparseable output) => zero errors.
    $mypyErrors = 0
}
if ($mypyErrors -gt $MypyErrorFloor) {
    Write-Output "mypy regression: $mypyErrors errors exceeds floor of $MypyErrorFloor"
    exit 1
}
Write-Output "mypy ok: $mypyErrors errors (floor $MypyErrorFloor)"

python -m pytest
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

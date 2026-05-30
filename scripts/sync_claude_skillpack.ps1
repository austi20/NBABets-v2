param(
    [switch]$DryRun,
    [switch]$Prune,
    [switch]$NoPrune,
    [string]$Pack,
    [switch]$ForceRefetch,
    [string]$Config
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$syncScript  = Join-Path $PSScriptRoot "sync_claude_skillpack.py"

function Find-Python {
    $venvPython = Join-Path $projectRoot ".venv\Scripts\python.exe"
    if (Test-Path -LiteralPath $venvPython) { return $venvPython }
    $cmd = Get-Command python -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    throw "Python not found. Activate the repo .venv or install Python 3.12+."
}

if (-not (Test-Path -LiteralPath $syncScript)) {
    throw "Sync script not found at '$syncScript'."
}

$pythonExe = Find-Python

$pyArgs = @()
if ($DryRun)      { $pyArgs += "--dry-run" }
if ($NoPrune)     { $pyArgs += "--no-prune" }
elseif ($Prune)   { $pyArgs += "--prune" }
if ($Pack)        { $pyArgs += @("--pack", $Pack) }
if ($ForceRefetch){ $pyArgs += "--force-refetch" }
if ($Config)      { $pyArgs += @("--config", $Config) }

& $pythonExe $syncScript @pyArgs
exit $LASTEXITCODE

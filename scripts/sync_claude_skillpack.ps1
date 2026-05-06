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

$args = @()
if ($DryRun)      { $args += "--dry-run" }
if ($NoPrune)     { $args += "--no-prune" }
elseif ($Prune)   { $args += "--prune" }
if ($Pack)        { $args += @("--pack", $Pack) }
if ($ForceRefetch){ $args += "--force-refetch" }
if ($Config)      { $args += @("--config", $Config) }

& $pythonExe $syncScript @args
exit $LASTEXITCODE

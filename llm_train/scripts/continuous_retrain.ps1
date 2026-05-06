# Rebuild JSONL when the props CSV changes (mtime-based). Wire paths to your machine.
param(
    [string]$CsvPath = "E:\AI Brain\ClaudeBrain\nba-props-dataset\data\final\nba_props_2025_26.csv",
    [switch]$Execute,
    [switch]$Force
)

$ErrorActionPreference = "Stop"
$repo = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
Set-Location $repo

$pyargs = @(
    "-m", "llm_train.scripts.continuous_retrain",
    "--csv", $CsvPath
)
if ($Execute) { $pyargs += "--execute" }
if ($Force) { $pyargs += "--force" }

python @pyargs

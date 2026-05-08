$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$outputDir = Join-Path $root "desktop_tauri\src-tauri\binaries"
$binaryBase = "nba-sidecar"
$targetFile = Join-Path $outputDir "$binaryBase-x86_64-pc-windows-msvc.exe"

if (-not (Test-Path $outputDir)) {
    New-Item -ItemType Directory -Path $outputDir | Out-Null
}

Write-Host "Building Python sidecar with PyInstaller..."
& py -3 -m PyInstaller `
    --noconfirm `
    --clean `
    --onefile `
    --name $binaryBase `
    --collect-binaries xgboost `
    --collect-data xgboost `
    --hidden-import sklearn `
    --hidden-import scipy `
    --hidden-import nba_api `
    --distpath $outputDir `
    --workpath (Join-Path $root ".tmp_pyinstaller\work") `
    --specpath (Join-Path $root ".tmp_pyinstaller\spec") `
    "$root\app\server\main.py"

$rawOutput = Join-Path $outputDir "$binaryBase.exe"
if (-not (Test-Path $rawOutput)) {
    throw "Expected PyInstaller output not found: $rawOutput"
}

Move-Item -Force $rawOutput $targetFile
Write-Host "Sidecar ready: $targetFile"

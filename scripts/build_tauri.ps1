Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$DesktopRoot = Join-Path $RepoRoot "desktop_tauri"
$SidecarPath = Join-Path $DesktopRoot "src-tauri\binaries\nba-sidecar-x86_64-pc-windows-msvc.exe"

if (-not (Test-Path $DesktopRoot)) {
    throw "Missing desktop_tauri project at $DesktopRoot"
}

if (-not (Test-Path $SidecarPath)) {
    Write-Host "Sidecar binary missing; building it before Tauri packaging..."
    & (Join-Path $PSScriptRoot "build_sidecar.ps1")
}

Push-Location $DesktopRoot
try {
    npm run tauri:build
}
finally {
    Pop-Location
}

Write-Host "Tauri build complete. Bundles are under desktop_tauri/src-tauri/target/release/bundle/."

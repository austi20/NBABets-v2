Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$DesktopRoot = Join-Path $RepoRoot "desktop_tauri"

if (-not (Test-Path $DesktopRoot)) {
    throw "Missing desktop_tauri project at $DesktopRoot"
}

Push-Location $DesktopRoot
try {
    npm run tauri:build
}
finally {
    Pop-Location
}

Write-Host "Tauri build complete. Bundles are under desktop_tauri/src-tauri/target/release/bundle/."

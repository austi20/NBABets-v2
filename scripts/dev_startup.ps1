Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$DesktopRoot = Join-Path $RepoRoot "desktop_tauri"

Push-Location $DesktopRoot
try {
    npm run tauri:dev
}
finally {
    Pop-Location
}

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$BundleRoot = Join-Path $RepoRoot "desktop_tauri\src-tauri\target\release\bundle"
$MsiPattern = Join-Path $BundleRoot "msi\*.msi"
$NsisPattern = Join-Path $BundleRoot "nsis\*.exe"

$msi = Get-ChildItem -Path $MsiPattern -ErrorAction SilentlyContinue | Select-Object -First 1
$nsis = Get-ChildItem -Path $NsisPattern -ErrorAction SilentlyContinue | Select-Object -First 1

if (-not $msi -and -not $nsis) {
    throw "No Tauri bundles found in $BundleRoot. Run scripts/build_tauri.ps1 first."
}

if ($msi) {
    Write-Host "Found MSI bundle: $($msi.FullName)"
}
if ($nsis) {
    Write-Host "Found NSIS bundle: $($nsis.FullName)"
}

Write-Host "Tauri smoke check passed."

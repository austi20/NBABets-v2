Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# The Tauri shell finds the repo .env by walking up from its own exe, so the
# shortcut must point at the in-repo release build, not a copied install.
$RepoRoot = Split-Path -Parent $PSScriptRoot
$Exe = Join-Path $RepoRoot "desktop_tauri\src-tauri\target\release\nba_prop_engine_desktop.exe"
$Icon = Join-Path $RepoRoot "desktop_tauri\src-tauri\icons\icon.ico"

if (-not (Test-Path $Exe)) {
    throw "Release exe missing: $Exe. Run scripts/build_tauri.ps1 first."
}

$Desktop = [Environment]::GetFolderPath("Desktop")
$LinkPath = Join-Path $Desktop "NBA Prop Engine.lnk"

$shell = New-Object -ComObject WScript.Shell
$link = $shell.CreateShortcut($LinkPath)
$link.TargetPath = $Exe
$link.WorkingDirectory = $RepoRoot
$link.IconLocation = $Icon
$link.Description = "NBA Prop Engine"
$link.Save()

Write-Host "Shortcut ready: $LinkPath -> $Exe"

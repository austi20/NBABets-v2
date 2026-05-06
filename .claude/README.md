# `.claude` Folder

This folder now focuses on repo guardrails and repo-specific collaboration assets only.

## Included Here

- `settings.json`
  - shared Claude Code permissions, allowlists, and hooks for this repo
- `hooks/`
  - edit protection, targeted post-edit linting, and config change audit logging
- `agents/`
  - repo-specific domain agents such as `project-manager`, `provider-engineer`, `training-engineer`, `desktop-engineer`, `automation-engineer`, `code-reviewer`, and `test-runner`
- `skills/`
  - repo guardrails and subsystem checklists

## External Runtime

The Claude/Qwen hybrid runtime is no longer managed inside this project tree.

- Runtime home: `%LOCALAPPDATA%\ClaudeHybridQwen35`
- Control script: `%LOCALAPPDATA%\ClaudeHybridQwen35\claude_qwen35_hybrid.ps1`
- Home-level Claude assets: `%USERPROFILE%\.claude`

Typical usage:

```bash
powershell -ExecutionPolicy Bypass -File %LOCALAPPDATA%\ClaudeHybridQwen35\claude_qwen35_hybrid.ps1 -Action start-app-runtime
powershell -ExecutionPolicy Bypass -File %LOCALAPPDATA%\ClaudeHybridQwen35\claude_qwen35_hybrid.ps1 -Action launch-claude -WorkingDir .
```

## Skillpack Sync

Third-party git-backed skillpacks can still be mirrored into this repo's `.claude/` directories with `scripts/sync_claude_skillpack.ps1`.

Operator flow:

1. Copy `.claude/skillpacks.example.json` to `.claude/skillpacks.local.json`.
2. Preview with `powershell -ExecutionPolicy Bypass -File scripts/sync_claude_skillpack.ps1 -DryRun`.
3. Apply with `powershell -ExecutionPolicy Bypass -File scripts/sync_claude_skillpack.ps1`.

Notes:

- Active local and cloud model aliases now come from the external runtime, not repo-local launcher scripts.
- `settings.local.json` should stay uncommitted.
- If Claude cannot find Bash automatically on Windows, set `CLAUDE_CODE_GIT_BASH_PATH` in the external runtime `.env` or in your shell.

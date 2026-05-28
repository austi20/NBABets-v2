# Full Review Governance Bootstrap

Purpose: operating contract for the Full Project Review and Simplification Program.

## 1) Lane policy (mandatory)

- Engineer-lane work must run through local Claude on the externalized `qwen3.5-9b` + `llama.cpp` runtime.
- Intern-lane work is limited to prep/support:
  - search and dependency mapping
  - command shortlist and artifact capture
  - narrow validation runs
  - docs/log updates

## 2) Engineer runtime contract

- Required local runtime:
  - external control script: `%LOCALAPPDATA%\ClaudeHybridQwen35\claude_qwen35_hybrid.ps1`
  - `ANTHROPIC_BASE_URL=http://127.0.0.1:8080`
  - `--model sonnet`
  - provider model remap: `qwen35-9b-q8`
- Engineer packets must include:
  - explicit file scope
  - explicit checks
  - explicit done criteria
  - rollback note

## 3) Packet acceptance template

Use this checklist to accept/reject every packet:

1. Scope integrity: only approved files changed.
2. Behavior safety: no unapproved behavior drift.
3. Validation proof: command + exit code + key output.
4. Taxonomy tag: primary failure code recorded.
5. Handoff clarity: next owner and next action present.

Reject packet if any item is missing.

## 4) Cadence and ownership

- Daily:
  - interns produce next packet candidate list and command shortlist
  - engineer executes highest-priority heavy packet via local Claude
- Weekly:
  - milestone gate run (full quality gate + one heavy path + one automation path)
  - lead review of incident board and defer/close decisions

## 5) Reporting artifacts

- Required updates each cycle:
  - `AGENT_COWORK.md` activity entry with taxonomy + evidence
  - `ACTION_PLAN.md` progress note with validation summary
  - incident board status updates in `docs/debug_incident_board.md`

# Incident: BallDontLie API Key Leaked in Source

**Severity:** CRITICAL  **Status:** key rotation REQUIRED  **Owner:** operator

## What happened
Commit history through 2026-05-26 contained a hardcoded BDL GOAT-tier API key as the
default value of `app/config/settings.py:301`. The repo is local-first but the key is
permanently leaked in any git clone.

The leaked key value can be found in git history prior to commit `baseline-pre-deep-review-2026-05-26`
by running: `git show baseline-pre-deep-review-2026-05-26:app/config/settings.py | grep BALLDONTLIE`

## What is already done in code
- Default removed; field is now `SecretStr | None` (`fix/deep-review-2026-05-26` branch).
- Settings sanitizer redacts the field in `/api/settings` responses (Task 8.1).

## What the operator must still do
1. Log in to the BallDontLie dashboard, rotate the GOAT-tier key, save the new value.
2. Put the new key in `.env` as `BALLDONTLIE_API_KEY=...` (NOT in code).
3. Scrub history. Two options:
   - Use `git filter-repo --replace-text` with the old key value (retrieve it from git history
     as shown above) followed by force-push to all branches and tags. Coordinate with anyone
     with a working tree.
   - Or treat the key as permanently burned and only rely on rotation.
4. Confirm no test or fixture file embeds the old key: `git grep <OLD_KEY_VALUE>` -> empty.

## Verification
- `git grep <OLD_KEY_VALUE>` returns nothing on HEAD after scrub.
- Settings round-trip test (`tests/unit/test_settings_secret.py`) green.

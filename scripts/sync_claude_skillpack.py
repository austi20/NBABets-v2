#!/usr/bin/env python3
"""Sync git-backed skillpacks into the repo's .claude/ asset directories.

Usage:
    python scripts/sync_claude_skillpack.py [options]

Options:
    --dry-run          Report intended actions without writing active files.
    --prune            Remove generated assets no longer declared (default: on).
    --no-prune         Disable pruning of stale generated assets.
    --pack <id>        Sync only the named pack.
    --force-refetch    Re-fetch all packs even if the cached ref is current.
"""
from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VALID_ACTIVATIONS = frozenset({"local", "cloud-plan", "cloud-review", "mirror-only", "disabled"})

INVALID_COMMAND_ACTIVATIONS = frozenset({"mirror-only"})
INVALID_AGENT_ACTIVATIONS = frozenset({"mirror-only"})
INVALID_SKILL_ACTIVATIONS = frozenset({"local", "cloud-plan", "cloud-review"})

# Patterns whose presence in an asset marks it as incompatible with the local runtime.
_INCOMPATIBILITY_PATTERNS: list[tuple[str, str]] = [
    (r"TodoWrite", "uses TodoWrite tool"),
    (r"EnterPlanMode", "uses EnterPlanMode tool"),
    (r"ExitPlanMode", "uses ExitPlanMode tool"),
    (r"ToolSearch", "uses ToolSearch tool"),
    (r"AskUserQuestion", "uses AskUserQuestion tool"),
    (r"Skill tool", "references Skill tool"),
    (r"agent\s+-f\s+", "uses unsupported `agent -f` flag"),
    (r"~/\.claude\.json", "references home-dir ~\\.claude.json"),
]

_COMMAND_ACTIVATION_MODEL: dict[str, str] = {
    "local": "qwen35-9b-q8-local",
    "cloud-plan": "cloud-plan",
    "cloud-review": "cloud-review",
}

_AGENT_ACTIVATION_MODEL: dict[str, str] = {
    "local": "inherit",
    "cloud-plan": "cloud-plan",
    "cloud-review": "cloud-review",
}

# Classification outcomes used in the manifest.
CLS_ACTIVE_LOCAL = "active_local"
CLS_ACTIVE_CLOUD_PLAN = "active_cloud_plan"
CLS_ACTIVE_CLOUD_REVIEW = "active_cloud_review"
CLS_MIRRORED_ONLY = "mirrored_only"
CLS_SKIPPED_INCOMPATIBLE = "skipped_incompatible"
CLS_SKIPPED_DISABLED = "skipped_disabled"
CLS_ERROR_INVALID_CONFIG = "error_invalid_config"

_ACTIVATION_TO_CLASS: dict[str, str] = {
    "local": CLS_ACTIVE_LOCAL,
    "cloud-plan": CLS_ACTIVE_CLOUD_PLAN,
    "cloud-review": CLS_ACTIVE_CLOUD_REVIEW,
    "mirror-only": CLS_MIRRORED_ONLY,
    "disabled": CLS_SKIPPED_DISABLED,
}

_FRONTMATTER_RE = re.compile(r"^---\s*\n(.*?)\n---\s*\n?(.*)", re.DOTALL)


@dataclass(frozen=True)
class PackCheckout:
    worktree_dir: Path
    resolved_commit: str


class SkillpackSyncError(RuntimeError):
    """Raised when sync work cannot safely continue."""


# ---------------------------------------------------------------------------
# Config loading and validation
# ---------------------------------------------------------------------------


def load_config(config_path: Path) -> dict[str, Any]:
    if not config_path.exists():
        print(
            f"[skillpack-sync] Config not found at '{config_path}'.\n"
            "Copy '.claude/skillpacks.example.json' to '.claude/skillpacks.local.json' and edit it.",
            file=sys.stderr,
        )
        sys.exit(1)
    with config_path.open(encoding="utf-8") as fh:
        return json.load(fh)  # type: ignore[no-any-return]


def validate_config(config: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if config.get("version") != 1:
        errors.append("config 'version' must be 1")
    packs = config.get("packs")
    if not isinstance(packs, list):
        errors.append("'packs' must be a list")
        return errors
    seen_ids: set[str] = set()
    for i, pack in enumerate(packs):
        prefix = f"packs[{i}]"
        if not isinstance(pack, dict):
            errors.append(f"{prefix} must be an object")
            continue
        pack_id = pack.get("id", "")
        if not isinstance(pack_id, str) or not pack_id.strip():
            errors.append(f"{prefix}.id is required")
        elif pack_id in seen_ids:
            errors.append(f"{prefix}.id '{pack_id}' is duplicated")
        seen_ids.add(str(pack_id))
        repo_url = pack.get("repo_url", "")
        if not isinstance(repo_url, str) or not repo_url.strip():
            errors.append(f"{prefix}.repo_url is required")
        ref = pack.get("ref", "")
        if not isinstance(ref, str) or not ref.strip():
            errors.append(f"{prefix}.ref is required")
        include = pack.get("include", {})
        if not isinstance(include, dict):
            errors.append(f"{prefix}.include must be an object")
            continue
        for asset_type in ("commands", "agents", "skills"):
            assets = include.get(asset_type, [])
            if not isinstance(assets, list):
                errors.append(f"{prefix}.include.{asset_type} must be a list")
                continue
            for j, asset in enumerate(assets):
                a_prefix = f"{prefix}.include.{asset_type}[{j}]"
                if not isinstance(asset, dict):
                    errors.append(f"{a_prefix} must be an object")
                    continue
                source = asset.get("source", "")
                if not isinstance(source, str) or not source.strip():
                    errors.append(f"{a_prefix}.source is required")
                target = asset.get("target", "")
                if not isinstance(target, str) or not target.strip():
                    errors.append(f"{a_prefix}.target is required")
                activation = asset.get("activation", "")
                if activation not in VALID_ACTIVATIONS:
                    errors.append(
                        f"{a_prefix}.activation '{activation}' is invalid; "
                        f"must be one of: {', '.join(sorted(VALID_ACTIVATIONS))}"
                    )
                    continue
                if asset_type == "commands" and activation in INVALID_COMMAND_ACTIVATIONS:
                    errors.append(
                        f"{a_prefix}.activation 'mirror-only' is not valid for commands; "
                        "use 'disabled' to suppress activation"
                    )
                if asset_type == "agents" and activation in INVALID_AGENT_ACTIVATIONS:
                    errors.append(
                        f"{a_prefix}.activation 'mirror-only' is not valid for agents; "
                        "use 'disabled' to suppress activation"
                    )
                if asset_type == "skills" and activation in INVALID_SKILL_ACTIVATIONS:
                    errors.append(
                        f"{a_prefix}.activation '{activation}' is not valid for skills; "
                        "use 'mirror-only' or 'disabled'"
                    )
    return errors


# ---------------------------------------------------------------------------
# Git cache operations
# ---------------------------------------------------------------------------


def _run_git(args: list[str], cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        capture_output=True,
        text=True,
        cwd=str(cwd) if cwd else None,
        check=False,
    )


def _git_error_message(result: subprocess.CompletedProcess[str]) -> str:
    output = result.stderr.strip() or result.stdout.strip()
    return output or "unknown git error"


def _run_git_checked(args: list[str], *, cwd: Path | None = None, action: str) -> subprocess.CompletedProcess[str]:
    result = _run_git(args, cwd=cwd)
    if result.returncode != 0:
        raise SkillpackSyncError(f"{action} failed: {_git_error_message(result)}")
    return result


def _current_worktree_commit(worktree_dir: Path) -> str:
    if not worktree_dir.exists():
        return ""
    result = _run_git(["rev-parse", "HEAD"], cwd=worktree_dir)
    if result.returncode == 0:
        return result.stdout.strip()
    return ""


def _remote_commit_for_ref(cache_dir: Path, ref: str) -> str:
    result = _run_git(["ls-remote", "origin", ref], cwd=cache_dir)
    if result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip().split()[0]
    return ""


def _resolve_commitish(repo_dir: Path, ref: str) -> str:
    candidates = [
        f"{ref}^{{commit}}",
        f"refs/heads/{ref}^{{commit}}",
        f"refs/tags/{ref}^{{commit}}",
        f"refs/remotes/origin/{ref}^{{commit}}",
    ]
    for candidate in candidates:
        result = _run_git(["rev-parse", candidate], cwd=repo_dir)
        if result.returncode == 0:
            return result.stdout.strip()
    return ""


def _sanitize_ref_name(ref: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "_", ref.strip())
    return sanitized or "default"


def _recreate_worktree(bare_dir: Path, worktree_dir: Path, commit: str, *, pack_id: str) -> None:
    if worktree_dir.exists():
        shutil.rmtree(str(worktree_dir), ignore_errors=True)
    _run_git(["worktree", "prune"], cwd=bare_dir)
    worktree_dir.parent.mkdir(parents=True, exist_ok=True)
    result = _run_git(["worktree", "add", "--detach", str(worktree_dir), commit], cwd=bare_dir)
    if result.returncode == 0:
        return
    shutil.rmtree(str(worktree_dir), ignore_errors=True)
    raise SkillpackSyncError(
        f"could not check out commit '{commit}' for '{pack_id}': {_git_error_message(result)}"
    )


def _ensure_worktree_commit(bare_dir: Path, worktree_dir: Path, commit: str, *, pack_id: str) -> None:
    current_sha = _current_worktree_commit(worktree_dir)
    if current_sha == commit:
        return
    if worktree_dir.exists():
        result = _run_git(["checkout", "--detach", "-f", commit], cwd=worktree_dir)
        if result.returncode == 0 and _current_worktree_commit(worktree_dir) == commit:
            return
    _recreate_worktree(bare_dir, worktree_dir, commit, pack_id=pack_id)


def fetch_pack(
    pack: dict[str, Any],
    cache_root: Path,
    *,
    force: bool,
    dry_run: bool,
) -> PackCheckout:
    """Clone or update the pack cache and return the checked-out worktree + commit."""
    pack_id: str = pack["id"]
    repo_url: str = pack["repo_url"]
    ref: str = pack["ref"]
    bare_dir = cache_root / pack_id / "_bare"
    worktree_dir = cache_root / pack_id / _sanitize_ref_name(ref)

    if not bare_dir.exists():
        print(f"[skillpack-sync] cloning '{pack_id}' from {repo_url} ...")
        bare_dir.parent.mkdir(parents=True, exist_ok=True)
        _run_git_checked(["clone", "--bare", repo_url, str(bare_dir)], action=f"git clone for '{pack_id}'")

    remote_sha = ""
    current_sha = _current_worktree_commit(worktree_dir)
    if force:
        prefix = "[dry-run]" if dry_run else "[skillpack-sync]"
        print(f"{prefix} force-fetching '{pack_id}' ...")
        _run_git_checked(["fetch", "--prune", "origin"], cwd=bare_dir, action=f"git fetch for '{pack_id}'")
        remote_sha = _remote_commit_for_ref(bare_dir, ref)
    else:
        remote_sha = _remote_commit_for_ref(bare_dir, ref)
        if remote_sha and current_sha and current_sha == remote_sha:
            print(f"[skillpack-sync] pack '{pack_id}' cache is current (sha={current_sha[:8]}), skipping fetch")
        else:
            prefix = "[dry-run]" if dry_run else "[skillpack-sync]"
            print(f"{prefix} fetching updates for '{pack_id}' ...")
            _run_git_checked(["fetch", "--prune", "origin"], cwd=bare_dir, action=f"git fetch for '{pack_id}'")
            remote_sha = _remote_commit_for_ref(bare_dir, ref)

    resolved_commit = remote_sha or _resolve_commitish(bare_dir, ref)
    if not resolved_commit:
        raise SkillpackSyncError(f"could not resolve ref '{ref}' to a commit for '{pack_id}'")

    _ensure_worktree_commit(bare_dir, worktree_dir, resolved_commit, pack_id=pack_id)
    if dry_run:
        print(f"[dry-run] analyzed pack '{pack_id}' at commit {resolved_commit}")
    return PackCheckout(worktree_dir=worktree_dir, resolved_commit=resolved_commit)


# ---------------------------------------------------------------------------
# Frontmatter handling
# ---------------------------------------------------------------------------


def parse_frontmatter(content: str) -> tuple[dict[str, Any], str]:
    """Return (fields dict, body after frontmatter) using YAML parsing."""
    match = _FRONTMATTER_RE.match(content)
    if not match:
        return {}, content
    try:
        loaded = yaml.safe_load(match.group(1))
    except yaml.YAMLError as exc:
        raise ValueError(f"malformed YAML frontmatter: {exc}") from exc
    if loaded is None:
        loaded = {}
    if not isinstance(loaded, dict):
        raise ValueError("frontmatter must be a YAML mapping")
    return loaded, match.group(2)


def render_frontmatter(fields: dict[str, Any], body: str) -> str:
    yaml_text = yaml.safe_dump(fields, sort_keys=False, default_flow_style=False).strip()
    rendered = f"---\n{yaml_text}\n---"
    if body:
        if not body.startswith("\n"):
            rendered += "\n"
        rendered += body
    else:
        rendered += "\n"
    return rendered


def inject_model(content: str, model: str) -> str:
    """Inject or replace 'model: <value>' in the YAML frontmatter."""
    fields, body = parse_frontmatter(content)
    ordered_fields: dict[str, Any] = {"model": model}
    for key, value in fields.items():
        if key != "model":
            ordered_fields[key] = value
    return render_frontmatter(ordered_fields, body)


def make_generation_header(
    *,
    pack_id: str,
    repo_url: str,
    ref: str,
    resolved_commit: str,
    source: str,
    activation: str,
    timestamp: str,
) -> str:
    return (
        f"<!-- generated by sync_claude_skillpack\n"
        f"     pack:       {pack_id}\n"
        f"     repo:       {repo_url}\n"
        f"     ref:        {ref}\n"
        f"     commit:     {resolved_commit}\n"
        f"     source:     {source}\n"
        f"     activation: {activation}\n"
        f"     generated:  {timestamp}\n"
        f"-->\n"
    )


# ---------------------------------------------------------------------------
# Compatibility gate
# ---------------------------------------------------------------------------


def check_compatibility(content: str) -> tuple[bool, list[str]]:
    """Return (is_compatible, list_of_reasons). Incompatible if any pattern matches."""
    reasons: list[str] = []
    for pattern, description in _INCOMPATIBILITY_PATTERNS:
        if re.search(pattern, content):
            reasons.append(description)
    return len(reasons) == 0, reasons


# ---------------------------------------------------------------------------
# Asset processing
# ---------------------------------------------------------------------------

def _is_within_root(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _resolve_path_within_root(root: Path, relative_path: str, *, label: str) -> tuple[Path | None, str | None]:
    raw_path = Path(relative_path)
    if raw_path.is_absolute():
        return None, f"{label} path must be relative: {relative_path}"
    resolved_root = root.resolve()
    resolved_path = (root / raw_path).resolve()
    if not _is_within_root(resolved_path, resolved_root):
        return None, f"{label} path escapes its allowed root: {relative_path}"
    return resolved_path, None


def _make_timestamp() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _make_manifest_entry(asset_type: str, asset: dict[str, Any], pack: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": asset_type,
        "pack": pack["id"],
        "repo_url": pack["repo_url"],
        "ref": pack["ref"],
        "resolved_commit": pack.get("resolved_commit", ""),
        "source": asset["source"],
        "target": asset["target"],
        "activation": asset["activation"],
    }


def _record_invalid_config(entry: dict[str, Any], message: str, *, prefix: str) -> dict[str, Any]:
    entry["classification"] = CLS_ERROR_INVALID_CONFIG
    entry["error"] = message
    entry["active_path"] = None
    print(f"  [error] {prefix}: {message}", file=sys.stderr)
    return entry


def _process_text_asset(
    asset_type: str,
    asset: dict[str, Any],
    pack: dict[str, Any],
    worktree_dir: Path,
    output_dir: Path,
    staging_dir: Path,
    *,
    dry_run: bool,
    timestamp: str,
) -> dict[str, Any]:
    source: str = asset["source"]
    target: str = asset["target"]
    activation: str = asset["activation"]

    entry = _make_manifest_entry(asset_type, asset, pack)
    if activation == "disabled":
        entry["classification"] = CLS_SKIPPED_DISABLED
        entry["active_path"] = None
        print(f"  [skip/disabled] {asset_type} '{source}' -> not activated")
        return entry

    source_path, source_error = _resolve_path_within_root(worktree_dir, source, label=f"{asset_type} source")
    if source_error:
        return _record_invalid_config(entry, source_error, prefix=f"{asset_type} '{source}'")
    if source_path is None or not source_path.exists() or not source_path.is_file():
        return _record_invalid_config(entry, f"source file not found: {source}", prefix=f"{asset_type} '{source}'")

    active_path, active_error = _resolve_path_within_root(output_dir, target, label=f"{asset_type} target")
    if active_error:
        return _record_invalid_config(entry, active_error, prefix=f"{asset_type} '{source}'")

    staged_path, staged_error = _resolve_path_within_root(
        staging_dir / f"{asset_type}s",
        target,
        label=f"{asset_type} staging target",
    )
    if staged_error:
        return _record_invalid_config(entry, staged_error, prefix=f"{asset_type} '{source}'")

    content = source_path.read_text(encoding="utf-8")
    compatible, reasons = check_compatibility(content)
    if not compatible:
        entry["classification"] = CLS_SKIPPED_INCOMPATIBLE
        entry["incompatibility_reasons"] = reasons
        entry["active_path"] = None
        print(f"  [skip/incompatible] {asset_type} '{source}': {'; '.join(reasons)}")
        return entry

    model = (
        _COMMAND_ACTIVATION_MODEL[activation]
        if asset_type == "command"
        else _AGENT_ACTIVATION_MODEL[activation]
    )
    header = make_generation_header(
        pack_id=pack["id"],
        repo_url=pack["repo_url"],
        ref=pack["ref"],
        resolved_commit=str(pack.get("resolved_commit", "")),
        source=source,
        activation=activation,
        timestamp=timestamp,
    )
    try:
        active_content = header + inject_model(content, model)
    except ValueError as exc:
        return _record_invalid_config(entry, str(exc), prefix=f"{asset_type} '{source}'")

    if dry_run:
        print(
            f"  [dry-run] {asset_type} '{source}' -> {active_path} "
            f"(activation={activation}, model={model}, commit={pack.get('resolved_commit', '')})"
        )
    else:
        if staged_path is None or active_path is None:
            return _record_invalid_config(entry, "internal path resolution failure", prefix=f"{asset_type} '{source}'")
        staged_path.parent.mkdir(parents=True, exist_ok=True)
        staged_path.write_text(content, encoding="utf-8")
        active_path.parent.mkdir(parents=True, exist_ok=True)
        active_path.write_text(active_content, encoding="utf-8")
        print(f"  [write] {asset_type} '{source}' -> {active_path}")

    entry["classification"] = _ACTIVATION_TO_CLASS[activation]
    entry["active_path"] = str(active_path)
    return entry


def process_command(
    asset: dict[str, Any],
    pack: dict[str, Any],
    worktree_dir: Path,
    commands_dir: Path,
    staging_dir: Path,
    *,
    dry_run: bool,
    timestamp: str,
) -> dict[str, Any]:
    """Process a command asset. Returns a manifest entry dict."""
    return _process_text_asset(
        "command",
        asset,
        pack,
        worktree_dir,
        commands_dir,
        staging_dir,
        dry_run=dry_run,
        timestamp=timestamp,
    )


def process_agent(
    asset: dict[str, Any],
    pack: dict[str, Any],
    worktree_dir: Path,
    agents_dir: Path,
    staging_dir: Path,
    *,
    dry_run: bool,
    timestamp: str,
) -> dict[str, Any]:
    """Process an agent asset. Returns a manifest entry dict."""
    return _process_text_asset(
        "agent",
        asset,
        pack,
        worktree_dir,
        agents_dir,
        staging_dir,
        dry_run=dry_run,
        timestamp=timestamp,
    )


def process_skill(
    asset: dict[str, Any],
    pack: dict[str, Any],
    worktree_dir: Path,
    generated_skills_dir: Path,
    *,
    dry_run: bool,
) -> dict[str, Any]:
    """Process a skill asset (mirror-only in v1). Returns a manifest entry dict."""
    source: str = asset["source"]
    target: str = asset["target"]
    activation: str = asset["activation"]

    entry = _make_manifest_entry("skill", asset, pack)
    entry["classification"] = CLS_MIRRORED_ONLY
    entry["active_path"] = None
    entry["note"] = "skills are mirror-only in v1; no active invocation path generated"

    if activation == "disabled":
        entry["classification"] = CLS_SKIPPED_DISABLED
        print(f"  [skip/disabled] skill '{source}' -> not mirrored")
        return entry

    source_dir, source_error = _resolve_path_within_root(worktree_dir, source, label="skill source")
    if source_error:
        entry["classification"] = CLS_ERROR_INVALID_CONFIG
        entry["error"] = source_error
        print(f"  [error] skill '{source}': {source_error}", file=sys.stderr)
        return entry
    if source_dir is None or not source_dir.exists() or not source_dir.is_dir():
        entry["classification"] = CLS_ERROR_INVALID_CONFIG
        entry["error"] = f"source directory not found: {source}"
        print(f"  [error] skill '{source}': source directory not found: {source}", file=sys.stderr)
        return entry

    mirror_path, mirror_error = _resolve_path_within_root(generated_skills_dir, target, label="skill target")
    if mirror_error:
        entry["classification"] = CLS_ERROR_INVALID_CONFIG
        entry["error"] = mirror_error
        print(f"  [error] skill '{source}': {mirror_error}", file=sys.stderr)
        return entry

    if dry_run:
        print(
            f"  [dry-run] skill '{source}' -> mirror at {mirror_path} "
            f"(commit={pack.get('resolved_commit', '')})"
        )
    else:
        if mirror_path is None:
            entry["classification"] = CLS_ERROR_INVALID_CONFIG
            entry["error"] = "internal path resolution failure"
            print(f"  [error] skill '{source}': internal path resolution failure", file=sys.stderr)
            return entry
        if mirror_path.exists():
            shutil.rmtree(str(mirror_path))
        mirror_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(str(source_dir), str(mirror_path))
        print(f"  [mirror] skill '{source}' -> {mirror_path}")

    entry["mirror_path"] = str(mirror_path)
    return entry


# ---------------------------------------------------------------------------
# Pruning
# ---------------------------------------------------------------------------


def prune_stale_assets(
    old_manifest: dict[str, Any],
    new_active_paths: set[str],
    *,
    allowed_roots: tuple[Path, ...],
    dry_run: bool,
) -> list[str]:
    """Remove generated active files that were in the old manifest but not in the new one."""
    pruned: list[str] = []
    resolved_roots = tuple(root.resolve() for root in allowed_roots)
    for entry in old_manifest.get("assets", []):
        active_path_str = entry.get("active_path")
        if not active_path_str:
            continue
        if active_path_str in new_active_paths:
            continue
        active_path = Path(active_path_str).resolve()
        if not any(_is_within_root(active_path, root) for root in resolved_roots):
            print(f"  [skip-prune] '{active_path}' is outside allowed generated roots; not removing")
            continue
        if not active_path.exists():
            continue
        try:
            content = active_path.read_text(encoding="utf-8")
        except OSError:
            continue
        if "generated by sync_claude_skillpack" not in content:
            print(f"  [skip-prune] '{active_path}' lacks generation header; not removing")
            continue
        if dry_run:
            print(f"  [dry-run] would prune stale asset: {active_path}")
        else:
            active_path.unlink(missing_ok=True)
            print(f"  [prune] removed stale asset: {active_path}")
        pruned.append(active_path_str)
    return pruned


def prune_stale_mirrors(
    old_manifest: dict[str, Any],
    new_mirror_paths: set[str],
    *,
    allowed_root: Path,
    dry_run: bool,
) -> list[str]:
    """Remove generated skill mirror dirs that were in the old manifest but not in the new one."""
    pruned: list[str] = []
    resolved_root = allowed_root.resolve()
    for entry in old_manifest.get("assets", []):
        if entry.get("type") != "skill":
            continue
        mirror_path_str = entry.get("mirror_path")
        if not mirror_path_str:
            continue
        if mirror_path_str in new_mirror_paths:
            continue
        mirror_path = Path(mirror_path_str).resolve()
        if not _is_within_root(mirror_path, resolved_root):
            print(f"  [skip-prune] '{mirror_path}' is outside allowed mirror root; not removing")
            continue
        if not mirror_path.exists():
            continue
        if dry_run:
            print(f"  [dry-run] would prune stale skill mirror: {mirror_path}")
        else:
            shutil.rmtree(str(mirror_path), ignore_errors=True)
            print(f"  [prune] removed stale skill mirror: {mirror_path}")
        pruned.append(mirror_path_str)
    return pruned


# ---------------------------------------------------------------------------
# Manifest I/O
# ---------------------------------------------------------------------------


def load_manifest(manifest_path: Path) -> dict[str, Any]:
    if not manifest_path.exists():
        return {"version": 1, "assets": []}
    with manifest_path.open(encoding="utf-8") as fh:
        return json.load(fh)  # type: ignore[no-any-return]


def write_manifest(manifest: dict[str, Any], manifest_path: Path) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)
        fh.write("\n")


def write_generated_readme(assets: list[dict[str, Any]], readme_path: Path, *, timestamp: str) -> None:
    lines = [
        "# Skillpack Generated Assets",
        "",
        f"Generated: {timestamp}",
        "",
        "This directory is managed by `scripts/sync_claude_skillpack.py`.",
        "Do not manually edit files listed here; they will be overwritten on next sync.",
        "",
        "## Active commands",
        "",
    ]
    commands = [a for a in assets if a["type"] == "command" and a.get("active_path")]
    if commands:
        for a in commands:
            lines.append(
                f"- `{a['target']}` (from pack `{a['pack']}`, activation `{a.get('activation', 'unknown')}`, "
                f"commit `{str(a.get('resolved_commit', ''))[:12]}`)"
            )
    else:
        lines.append("_(none)_")
    lines += ["", "## Active agents", ""]
    agents = [a for a in assets if a["type"] == "agent" and a.get("active_path")]
    if agents:
        for a in agents:
            lines.append(
                f"- `{a['target']}` (from pack `{a['pack']}`, activation `{a.get('activation', 'unknown')}`, "
                f"commit `{str(a.get('resolved_commit', ''))[:12]}`)"
            )
    else:
        lines.append("_(none)_")
    lines += ["", "## Mirrored skills (inactive)", ""]
    skills = [a for a in assets if a["type"] == "skill" and a.get("mirror_path")]
    if skills:
        for a in skills:
            lines.append(
                f"- `{a['target']}` (from pack `{a['pack']}`, commit `{str(a.get('resolved_commit', ''))[:12]}`)"
            )
    else:
        lines.append("_(none)_")
    lines += ["", "## Skipped / incompatible", ""]
    skipped = [a for a in assets if a.get("classification") in (CLS_SKIPPED_INCOMPATIBLE, CLS_ERROR_INVALID_CONFIG)]
    if skipped:
        for a in skipped:
            reasons = "; ".join(a.get("incompatibility_reasons") or [a.get("error", "unknown")])
            lines.append(f"- `{a.get('source', '?')}` [{a['classification']}]: {reasons}")
    else:
        lines.append("_(none)_")
    lines.append("")
    readme_path.parent.mkdir(parents=True, exist_ok=True)
    readme_path.write_text("\n".join(lines), encoding="utf-8")


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------


def run_sync(
    *,
    config_path: Path,
    project_root: Path,
    dry_run: bool,
    prune: bool,
    pack_filter: str | None,
    force_refetch: bool,
) -> int:
    config = load_config(config_path)
    errors = validate_config(config)
    if errors:
        for err in errors:
            print(f"[skillpack-sync] config error: {err}", file=sys.stderr)
        return 1

    packs: list[dict[str, Any]] = config["packs"]
    if pack_filter:
        packs = [p for p in packs if p["id"] == pack_filter]
        if not packs:
            print(f"[skillpack-sync] no pack with id '{pack_filter}' found in config", file=sys.stderr)
            return 1

    cache_root = project_root / ".claude" / "cache" / "skillpacks"
    commands_dir = project_root / ".claude" / "commands"
    agents_dir = project_root / ".claude" / "agents"
    generated_dir = project_root / ".claude" / "generated"
    generated_skills_dir = generated_dir / "skills"
    staging_dir = generated_dir / "staging"
    manifest_path = generated_dir / "skillpacks-manifest.json"
    readme_path = generated_dir / "README.md"

    old_manifest = load_manifest(manifest_path)
    old_assets = list(old_manifest.get("assets", []))
    preserved_assets = [asset for asset in old_assets if asset.get("pack") != pack_filter] if pack_filter else []
    prune_manifest = (
        {"assets": [asset for asset in old_assets if asset.get("pack") == pack_filter]}
        if pack_filter
        else old_manifest
    )
    timestamp = _make_timestamp()
    new_assets: list[dict[str, Any]] = []
    try:
        for pack in packs:
            pack_id = pack["id"]
            print(f"\n[skillpack-sync] processing pack '{pack_id}' ...")
            checkout = fetch_pack(pack, cache_root, force=force_refetch, dry_run=dry_run)
            pack_context = dict(pack)
            pack_context["resolved_commit"] = checkout.resolved_commit
            include = pack.get("include", {})
            pack_staging = staging_dir / pack_id

            for asset in include.get("commands", []):
                new_assets.append(
                    process_command(
                        asset,
                        pack_context,
                        checkout.worktree_dir,
                        commands_dir,
                        pack_staging,
                        dry_run=dry_run,
                        timestamp=timestamp,
                    )
                )

            for asset in include.get("agents", []):
                new_assets.append(
                    process_agent(
                        asset,
                        pack_context,
                        checkout.worktree_dir,
                        agents_dir,
                        pack_staging,
                        dry_run=dry_run,
                        timestamp=timestamp,
                    )
                )

            for asset in include.get("skills", []):
                new_assets.append(
                    process_skill(
                        asset,
                        pack_context,
                        checkout.worktree_dir,
                        generated_skills_dir,
                        dry_run=dry_run,
                    )
                )
    except SkillpackSyncError as exc:
        print(f"[skillpack-sync] ERROR: {exc}", file=sys.stderr)
        return 1

    merged_assets = preserved_assets + new_assets if pack_filter else new_assets
    new_active_paths = {asset["active_path"] for asset in merged_assets if asset.get("active_path")}
    new_mirror_paths = {asset["mirror_path"] for asset in merged_assets if asset.get("mirror_path")}

    if prune:
        print("\n[skillpack-sync] pruning stale assets ...")
        prune_stale_assets(
            prune_manifest,
            new_active_paths,
            allowed_roots=(commands_dir, agents_dir),
            dry_run=dry_run,
        )
        prune_stale_mirrors(
            prune_manifest,
            new_mirror_paths,
            allowed_root=generated_skills_dir,
            dry_run=dry_run,
        )

    new_manifest: dict[str, Any] = {
        "version": 1,
        "generated_at": timestamp,
        "assets": merged_assets,
    }

    if dry_run:
        print(f"\n[dry-run] manifest would be written to {manifest_path}")
        print(json.dumps(new_manifest, indent=2))
    else:
        write_manifest(new_manifest, manifest_path)
        write_generated_readme(merged_assets, readme_path, timestamp=timestamp)
        print(f"\n[skillpack-sync] manifest written to {manifest_path}")

    total = len(new_assets)
    active = sum(1 for a in new_assets if a.get("active_path"))
    mirrored = sum(1 for a in new_assets if a.get("classification") == CLS_MIRRORED_ONLY)
    skipped = sum(1 for a in new_assets if a.get("classification") in (CLS_SKIPPED_INCOMPATIBLE, CLS_SKIPPED_DISABLED))
    errors_count = sum(1 for a in new_assets if a.get("classification") == CLS_ERROR_INVALID_CONFIG)
    print(
        f"\n[skillpack-sync] done: {total} processed | "
        f"{active} active | {mirrored} mirrored | {skipped} skipped | {errors_count} errors"
    )
    if pack_filter and preserved_assets:
        print(f"[skillpack-sync] preserved {len(preserved_assets)} manifest entries from other packs")
    return 1 if errors_count > 0 else 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sync git-backed skillpacks into the repo's .claude/ asset directories."
    )
    parser.add_argument("--dry-run", action="store_true", help="Report intended actions without writing files.")
    parser.add_argument("--prune", action="store_true", default=True, help="Remove stale generated assets (default on).")
    parser.add_argument("--no-prune", dest="prune", action="store_false", help="Disable pruning.")
    parser.add_argument("--pack", metavar="ID", help="Sync only the named pack.")
    parser.add_argument("--force-refetch", action="store_true", help="Re-fetch all packs even if cache is current.")
    parser.add_argument("--config", metavar="PATH", help="Path to skillpacks config (default: .claude/skillpacks.local.json).")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent
    config_path = Path(args.config) if args.config else project_root / ".claude" / "skillpacks.local.json"

    return run_sync(
        config_path=config_path,
        project_root=project_root,
        dry_run=args.dry_run,
        prune=args.prune,
        pack_filter=args.pack,
        force_refetch=args.force_refetch,
    )


if __name__ == "__main__":
    sys.exit(main())

"""Unit tests for scripts/sync_claude_skillpack.py."""
from __future__ import annotations

import importlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

# Make 'scripts' importable.
_SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

sync = importlib.import_module("sync_claude_skillpack")

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures" / "skillpacks"


def _body_without_generated_header(content: str) -> str:
    return content.split("-->\n", 1)[-1] if "-->" in content else content


def _init_git_repo(repo_dir: Path) -> None:
    subprocess.run(["git", "init", "-b", "main"], cwd=repo_dir, check=True, capture_output=True, text=True)
    subprocess.run(["git", "add", "."], cwd=repo_dir, check=True, capture_output=True, text=True)
    subprocess.run(
        ["git", "-c", "user.name=Test User", "-c", "user.email=test@example.com", "commit", "-m", "init"],
        cwd=repo_dir,
        check=True,
        capture_output=True,
        text=True,
    )


# ---------------------------------------------------------------------------
# validate_config
# ---------------------------------------------------------------------------


def _base_config(**overrides: object) -> dict:
    cfg: dict = {
        "version": 1,
        "packs": [
            {
                "id": "mypak",
                "repo_url": "https://example.com/repo.git",
                "ref": "main",
                "include": {
                    "commands": [{"source": "commands/greet.md", "target": "ext-greet.md", "activation": "local"}],
                    "agents": [],
                    "skills": [],
                },
            }
        ],
    }
    cfg.update(overrides)
    return cfg


def test_valid_config_has_no_errors():
    assert sync.validate_config(_base_config()) == []


def test_missing_version_is_an_error():
    cfg = _base_config()
    del cfg["version"]
    errors = sync.validate_config(cfg)
    assert any("version" in e for e in errors)


def test_invalid_version_is_an_error():
    errors = sync.validate_config(_base_config(version=2))
    assert any("version" in e for e in errors)


def test_invalid_activation_value_is_rejected():
    cfg = _base_config()
    cfg["packs"][0]["include"]["commands"][0]["activation"] = "bogus-mode"
    errors = sync.validate_config(cfg)
    assert any("bogus-mode" in e for e in errors)


def test_mirror_only_activation_rejected_for_commands():
    cfg = _base_config()
    cfg["packs"][0]["include"]["commands"][0]["activation"] = "mirror-only"
    errors = sync.validate_config(cfg)
    assert any("mirror-only" in e and "command" in e for e in errors)


def test_mirror_only_activation_rejected_for_agents():
    cfg = _base_config()
    cfg["packs"][0]["include"]["agents"] = [
        {"source": "agents/helper.md", "target": "ext-helper.md", "activation": "mirror-only"}
    ]
    errors = sync.validate_config(cfg)
    assert any("mirror-only" in e and "agent" in e for e in errors)


def test_non_mirror_skill_activation_is_rejected():
    cfg = _base_config()
    cfg["packs"][0]["include"]["skills"] = [
        {"source": "skills/brainstorm", "target": "ext-brainstorm", "activation": "local"}
    ]
    errors = sync.validate_config(cfg)
    assert any("skills" in e and "mirror-only" in e for e in errors)


def test_duplicate_pack_ids_are_errors():
    cfg = _base_config()
    cfg["packs"].append(dict(cfg["packs"][0]))
    errors = sync.validate_config(cfg)
    assert any("duplicated" in e for e in errors)


def test_missing_source_is_an_error():
    cfg = _base_config()
    del cfg["packs"][0]["include"]["commands"][0]["source"]
    errors = sync.validate_config(cfg)
    assert any("source" in e for e in errors)


# ---------------------------------------------------------------------------
# check_compatibility
# ---------------------------------------------------------------------------


def test_safe_content_is_compatible():
    compatible, reasons = sync.check_compatibility("Say hello and ask what you need.")
    assert compatible is True
    assert reasons == []


def test_todwrite_makes_incompatible():
    compatible, reasons = sync.check_compatibility("Use TodoWrite to track progress.")
    assert compatible is False
    assert any("TodoWrite" in r for r in reasons)


def test_enterplanmode_makes_incompatible():
    compatible, reasons = sync.check_compatibility("Call EnterPlanMode now.")
    assert compatible is False
    assert any("EnterPlanMode" in r for r in reasons)


def test_multiple_incompatible_patterns_all_reported():
    content = "Use TodoWrite and call AskUserQuestion and EnterPlanMode."
    compatible, reasons = sync.check_compatibility(content)
    assert compatible is False
    assert len(reasons) >= 3


# ---------------------------------------------------------------------------
# parse_frontmatter / inject_model
# ---------------------------------------------------------------------------


def test_parse_frontmatter_with_valid_header():
    content = "---\nmodel: sonnet\ndescription: A test.\n---\nBody text."
    fields, body = sync.parse_frontmatter(content)
    assert fields["model"] == "sonnet"
    assert fields["description"] == "A test."
    assert "Body text." in body


def test_parse_frontmatter_with_no_header():
    content = "Just body text."
    fields, body = sync.parse_frontmatter(content)
    assert fields == {}
    assert body == "Just body text."


def test_inject_model_adds_model_field():
    content = "---\ndescription: Hello.\n---\nBody."
    result = sync.inject_model(content, "cloud-plan")
    fields, _ = sync.parse_frontmatter(result)
    assert fields["model"] == "cloud-plan"


def test_inject_model_replaces_existing_model():
    content = "---\nmodel: old-model\ndescription: Hello.\n---\nBody."
    result = sync.inject_model(content, "cloud-review")
    fields, _ = sync.parse_frontmatter(result)
    assert fields["model"] == "cloud-review"


def test_inject_model_preserves_structured_yaml_fields():
    content = (
        "---\n"
        "name: helper\n"
        "tools:\n"
        "  - Read\n"
        "  - Grep\n"
        "skills:\n"
        "  - repo-guardrails\n"
        "  - quality-gate\n"
        "limits:\n"
        "  max_turns: 10\n"
        "---\n"
        "Body."
    )
    result = sync.inject_model(content, "local")
    fields, _ = sync.parse_frontmatter(result)
    assert fields["model"] == "local"
    assert fields["skills"] == ["repo-guardrails", "quality-gate"]
    assert fields["limits"] == {"max_turns": 10}


def test_inject_model_creates_frontmatter_when_missing():
    result = sync.inject_model("Just body text.", "cloud-plan")
    fields, body = sync.parse_frontmatter(result)
    assert fields["model"] == "cloud-plan"
    assert body == "Just body text."


def test_parse_frontmatter_raises_on_malformed_yaml():
    content = "---\ndescription: [unterminated\n---\nBody."
    with pytest.raises(ValueError, match="malformed YAML frontmatter"):
        sync.parse_frontmatter(content)


# ---------------------------------------------------------------------------
# process_command (using fixture files as the "worktree")
# ---------------------------------------------------------------------------


def test_process_command_local_activation(tmp_path: Path):
    worktree = FIXTURES / "safe_command_pack"
    commands_dir = tmp_path / "commands"
    staging_dir = tmp_path / "staging"
    asset = {"source": "commands/greet.md", "target": "ext-greet.md", "activation": "local"}
    pack = {"id": "test", "repo_url": "https://example.com/r.git", "ref": "main", "resolved_commit": "a" * 40}

    entry = sync.process_command(
        asset, pack, worktree, commands_dir, staging_dir, dry_run=False, timestamp="2026-01-01T00:00:00Z"
    )

    assert entry["classification"] == sync.CLS_ACTIVE_LOCAL
    active = Path(entry["active_path"])
    assert active.exists()
    content = active.read_text(encoding="utf-8")
    fields, _ = sync.parse_frontmatter(_body_without_generated_header(content))
    assert fields.get("model") == "qwen35-9b-q8-local"
    assert entry["resolved_commit"] == "a" * 40
    assert "generated by sync_claude_skillpack" in content
    assert "commit:     " in content


def test_process_command_cloud_plan_activation(tmp_path: Path):
    worktree = FIXTURES / "safe_command_pack"
    commands_dir = tmp_path / "commands"
    staging_dir = tmp_path / "staging"
    asset = {"source": "commands/greet.md", "target": "ext-greet-cloud.md", "activation": "cloud-plan"}
    pack = {"id": "test", "repo_url": "https://example.com/r.git", "ref": "main", "resolved_commit": "b" * 40}

    entry = sync.process_command(
        asset, pack, worktree, commands_dir, staging_dir, dry_run=False, timestamp="2026-01-01T00:00:00Z"
    )

    assert entry["classification"] == sync.CLS_ACTIVE_CLOUD_PLAN
    content = Path(entry["active_path"]).read_text(encoding="utf-8")
    fields, _ = sync.parse_frontmatter(_body_without_generated_header(content))
    assert fields.get("model") == "cloud-plan"


def test_process_command_disabled_activation(tmp_path: Path):
    worktree = FIXTURES / "safe_command_pack"
    commands_dir = tmp_path / "commands"
    staging_dir = tmp_path / "staging"
    asset = {"source": "commands/greet.md", "target": "ext-greet-off.md", "activation": "disabled"}
    pack = {"id": "test", "repo_url": "https://example.com/r.git", "ref": "main", "resolved_commit": "c" * 40}

    entry = sync.process_command(
        asset, pack, worktree, commands_dir, staging_dir, dry_run=False, timestamp="2026-01-01T00:00:00Z"
    )

    assert entry["classification"] == sync.CLS_SKIPPED_DISABLED
    assert entry["active_path"] is None
    assert not (commands_dir / "ext-greet-off.md").exists()


def test_process_command_incompatible_is_skipped(tmp_path: Path):
    worktree = FIXTURES / "incompatible_pack"
    commands_dir = tmp_path / "commands"
    staging_dir = tmp_path / "staging"
    asset = {"source": "commands/bad_command.md", "target": "ext-bad.md", "activation": "cloud-plan"}
    pack = {"id": "test", "repo_url": "https://example.com/r.git", "ref": "main", "resolved_commit": "d" * 40}

    entry = sync.process_command(
        asset, pack, worktree, commands_dir, staging_dir, dry_run=False, timestamp="2026-01-01T00:00:00Z"
    )

    assert entry["classification"] == sync.CLS_SKIPPED_INCOMPATIBLE
    assert entry["active_path"] is None
    assert not (commands_dir / "ext-bad.md").exists()


def test_process_command_dry_run_writes_nothing(tmp_path: Path):
    worktree = FIXTURES / "safe_command_pack"
    commands_dir = tmp_path / "commands"
    staging_dir = tmp_path / "staging"
    asset = {"source": "commands/greet.md", "target": "ext-greet-dry.md", "activation": "local"}
    pack = {"id": "test", "repo_url": "https://example.com/r.git", "ref": "main", "resolved_commit": "e" * 40}

    sync.process_command(
        asset, pack, worktree, commands_dir, staging_dir, dry_run=True, timestamp="2026-01-01T00:00:00Z"
    )

    assert not (commands_dir / "ext-greet-dry.md").exists()


def test_process_command_rejects_source_path_escape(tmp_path: Path):
    worktree = FIXTURES / "safe_command_pack"
    commands_dir = tmp_path / "commands"
    staging_dir = tmp_path / "staging"
    asset = {"source": "../outside.md", "target": "ext-escape.md", "activation": "local"}
    pack = {"id": "test", "repo_url": "https://example.com/r.git", "ref": "main", "resolved_commit": "f" * 40}

    entry = sync.process_command(
        asset, pack, worktree, commands_dir, staging_dir, dry_run=False, timestamp="2026-01-01T00:00:00Z"
    )

    assert entry["classification"] == sync.CLS_ERROR_INVALID_CONFIG
    assert "escapes its allowed root" in entry["error"]


def test_process_command_rejects_target_path_escape(tmp_path: Path):
    worktree = FIXTURES / "safe_command_pack"
    commands_dir = tmp_path / "commands"
    staging_dir = tmp_path / "staging"
    asset = {"source": "commands/greet.md", "target": "../escape.md", "activation": "local"}
    pack = {"id": "test", "repo_url": "https://example.com/r.git", "ref": "main", "resolved_commit": "g" * 40}

    entry = sync.process_command(
        asset, pack, worktree, commands_dir, staging_dir, dry_run=False, timestamp="2026-01-01T00:00:00Z"
    )

    assert entry["classification"] == sync.CLS_ERROR_INVALID_CONFIG
    assert "escapes its allowed root" in entry["error"]


# ---------------------------------------------------------------------------
# process_agent
# ---------------------------------------------------------------------------


def test_process_agent_cloud_review_activation(tmp_path: Path):
    worktree = FIXTURES / "safe_agent_pack"
    agents_dir = tmp_path / "agents"
    staging_dir = tmp_path / "staging"
    asset = {"source": "agents/helper.md", "target": "ext-helper.md", "activation": "cloud-review"}
    pack = {"id": "test", "repo_url": "https://example.com/r.git", "ref": "main", "resolved_commit": "h" * 40}

    entry = sync.process_agent(
        asset, pack, worktree, agents_dir, staging_dir, dry_run=False, timestamp="2026-01-01T00:00:00Z"
    )

    assert entry["classification"] == sync.CLS_ACTIVE_CLOUD_REVIEW
    content = Path(entry["active_path"]).read_text(encoding="utf-8")
    fields, _ = sync.parse_frontmatter(_body_without_generated_header(content))
    assert fields.get("model") == "cloud-review"


def test_process_agent_local_activation_uses_inherit(tmp_path: Path):
    worktree = FIXTURES / "safe_agent_pack"
    agents_dir = tmp_path / "agents"
    staging_dir = tmp_path / "staging"
    asset = {"source": "agents/helper.md", "target": "ext-helper-local.md", "activation": "local"}
    pack = {"id": "test", "repo_url": "https://example.com/r.git", "ref": "main", "resolved_commit": "i" * 40}

    entry = sync.process_agent(
        asset, pack, worktree, agents_dir, staging_dir, dry_run=False, timestamp="2026-01-01T00:00:00Z"
    )

    content = Path(entry["active_path"]).read_text(encoding="utf-8")
    fields, _ = sync.parse_frontmatter(_body_without_generated_header(content))
    assert fields.get("model") == "inherit"


def test_process_agent_malformed_yaml_is_invalid_config(tmp_path: Path):
    worktree = tmp_path / "worktree"
    (worktree / "agents").mkdir(parents=True)
    (worktree / "agents" / "broken.md").write_text(
        "---\ndescription: [unterminated\n---\nBody.",
        encoding="utf-8",
    )
    agents_dir = tmp_path / "agents"
    staging_dir = tmp_path / "staging"
    asset = {"source": "agents/broken.md", "target": "ext-broken.md", "activation": "local"}
    pack = {"id": "test", "repo_url": "https://example.com/r.git", "ref": "main", "resolved_commit": "j" * 40}

    entry = sync.process_agent(
        asset, pack, worktree, agents_dir, staging_dir, dry_run=False, timestamp="2026-01-01T00:00:00Z"
    )

    assert entry["classification"] == sync.CLS_ERROR_INVALID_CONFIG
    assert "malformed YAML frontmatter" in entry["error"]


# ---------------------------------------------------------------------------
# process_skill (mirror-only)
# ---------------------------------------------------------------------------


def test_process_skill_mirror_only(tmp_path: Path):
    worktree = FIXTURES / "skill_only_pack"
    skills_dir = tmp_path / "generated" / "skills"
    asset = {"source": "skills/brainstorm", "target": "ext-brainstorm", "activation": "mirror-only"}
    pack = {"id": "test", "repo_url": "https://example.com/r.git", "ref": "main"}

    entry = sync.process_skill(asset, pack, worktree, skills_dir, dry_run=False)

    assert entry["classification"] == sync.CLS_MIRRORED_ONLY
    assert entry["active_path"] is None
    mirror = Path(entry["mirror_path"])
    assert mirror.exists()
    assert (mirror / "SKILL.md").exists()


def test_process_skill_no_active_claude_skills_path(tmp_path: Path):
    """No .claude/skills/<target>/SKILL.md should be created - only mirror."""
    worktree = FIXTURES / "skill_only_pack"
    skills_dir = tmp_path / "generated" / "skills"
    claude_skills_dir = tmp_path / ".claude" / "skills"
    asset = {"source": "skills/brainstorm", "target": "ext-brainstorm", "activation": "mirror-only"}
    pack = {"id": "test", "repo_url": "https://example.com/r.git", "ref": "main"}

    sync.process_skill(asset, pack, worktree, skills_dir, dry_run=False)

    # The active .claude/skills path must NOT exist.
    assert not (claude_skills_dir / "ext-brainstorm").exists()


def test_process_skill_disabled(tmp_path: Path):
    worktree = FIXTURES / "skill_only_pack"
    skills_dir = tmp_path / "generated" / "skills"
    asset = {"source": "skills/brainstorm", "target": "ext-brainstorm", "activation": "disabled"}
    pack = {"id": "test", "repo_url": "https://example.com/r.git", "ref": "main"}

    entry = sync.process_skill(asset, pack, worktree, skills_dir, dry_run=False)

    assert entry["classification"] == sync.CLS_SKIPPED_DISABLED
    assert not (skills_dir / "ext-brainstorm").exists()


# ---------------------------------------------------------------------------
# Pruning
# ---------------------------------------------------------------------------


def test_prune_removes_stale_generated_asset(tmp_path: Path):
    stale_file = tmp_path / "commands" / "ext-old.md"
    stale_file.parent.mkdir(parents=True)
    stale_file.write_text("<!-- generated by sync_claude_skillpack -->\n---\nmodel: local\n---\nOld.", encoding="utf-8")

    old_manifest = {"assets": [{"type": "command", "active_path": str(stale_file)}]}
    new_active: set[str] = set()

    pruned = sync.prune_stale_assets(old_manifest, new_active, allowed_roots=(tmp_path / "commands",), dry_run=False)

    assert str(stale_file) in pruned
    assert not stale_file.exists()


def test_prune_skips_asset_still_in_new_manifest(tmp_path: Path):
    kept_file = tmp_path / "commands" / "ext-keep.md"
    kept_file.parent.mkdir(parents=True)
    kept_file.write_text("<!-- generated by sync_claude_skillpack -->\n---\nmodel: local\n---\nKeep.", encoding="utf-8")

    old_manifest = {"assets": [{"type": "command", "active_path": str(kept_file)}]}
    new_active = {str(kept_file)}

    pruned = sync.prune_stale_assets(old_manifest, new_active, allowed_roots=(tmp_path / "commands",), dry_run=False)

    assert pruned == []
    assert kept_file.exists()


def test_prune_skips_file_without_generation_header(tmp_path: Path):
    hand_authored = tmp_path / "commands" / "plan-cloud.md"
    hand_authored.parent.mkdir(parents=True)
    hand_authored.write_text("---\nmodel: cloud-plan\n---\nHand-authored.", encoding="utf-8")

    old_manifest = {"assets": [{"type": "command", "active_path": str(hand_authored)}]}
    new_active: set[str] = set()

    pruned = sync.prune_stale_assets(old_manifest, new_active, allowed_roots=(tmp_path / "commands",), dry_run=False)

    assert pruned == []
    assert hand_authored.exists()


def test_prune_dry_run_does_not_remove(tmp_path: Path):
    stale_file = tmp_path / "commands" / "ext-stale.md"
    stale_file.parent.mkdir(parents=True)
    stale_file.write_text("<!-- generated by sync_claude_skillpack -->\n---\nmodel: local\n---\n", encoding="utf-8")

    old_manifest = {"assets": [{"type": "command", "active_path": str(stale_file)}]}
    pruned = sync.prune_stale_assets(old_manifest, set(), allowed_roots=(tmp_path / "commands",), dry_run=True)

    assert str(stale_file) in pruned
    assert stale_file.exists()  # file still present in dry-run


def test_prune_skips_file_outside_allowed_root(tmp_path: Path):
    outside_file = tmp_path / "outside.md"
    outside_file.write_text("<!-- generated by sync_claude_skillpack -->\n---\nmodel: local\n---\n", encoding="utf-8")

    old_manifest = {"assets": [{"type": "command", "active_path": str(outside_file)}]}
    pruned = sync.prune_stale_assets(old_manifest, set(), allowed_roots=(tmp_path / "commands",), dry_run=False)

    assert pruned == []
    assert outside_file.exists()


# ---------------------------------------------------------------------------
# run_sync / fetch_pack
# ---------------------------------------------------------------------------


def test_fetch_pack_raises_on_clone_failure(tmp_path: Path, monkeypatch) -> None:
    pack = {"id": "broken", "repo_url": "https://example.com/repo.git", "ref": "main"}

    def _fake_run_git(args, cwd=None):
        return subprocess.CompletedProcess(args=args, returncode=1, stdout="", stderr="clone failed")

    monkeypatch.setattr(sync, "_run_git", _fake_run_git)

    with pytest.raises(sync.SkillpackSyncError, match="git clone"):
        sync.fetch_pack(pack, tmp_path, force=False, dry_run=False)


def test_run_sync_pack_filter_preserves_other_manifest_entries(tmp_path: Path, monkeypatch) -> None:
    project_root = tmp_path / "project"
    commands_dir = project_root / ".claude" / "commands"
    commands_dir.mkdir(parents=True)
    generated_dir = project_root / ".claude" / "generated"
    generated_dir.mkdir(parents=True)
    manifest_path = generated_dir / "skillpacks-manifest.json"

    stale_a = commands_dir / "ext-pack-a-old.md"
    stale_a.write_text("<!-- generated by sync_claude_skillpack -->\n---\nmodel: local\n---\n", encoding="utf-8")
    keep_b = commands_dir / "ext-pack-b.md"
    keep_b.write_text("<!-- generated by sync_claude_skillpack -->\n---\nmodel: local\n---\n", encoding="utf-8")

    sync.write_manifest(
        {
            "version": 1,
            "generated_at": "2026-01-01T00:00:00Z",
            "assets": [
                {"type": "command", "pack": "pack-a", "target": "ext-pack-a-old.md", "active_path": str(stale_a)},
                {"type": "command", "pack": "pack-b", "target": "ext-pack-b.md", "active_path": str(keep_b)},
            ],
        },
        manifest_path,
    )

    config = {
        "version": 1,
        "packs": [
            {
                "id": "pack-a",
                "repo_url": "https://example.com/a.git",
                "ref": "main",
                "include": {
                    "commands": [{"source": "commands/greet.md", "target": "ext-pack-a.md", "activation": "local"}],
                    "agents": [],
                    "skills": [],
                },
            },
            {
                "id": "pack-b",
                "repo_url": "https://example.com/b.git",
                "ref": "main",
                "include": {"commands": [], "agents": [], "skills": []},
            },
        ],
    }
    config_path = project_root / ".claude" / "skillpacks.local.json"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(json.dumps(config), encoding="utf-8")

    worktree = FIXTURES / "safe_command_pack"

    def _fake_fetch_pack(pack, cache_root, *, force, dry_run):
        return sync.PackCheckout(worktree_dir=worktree, resolved_commit="feedfacefeedfacefeedfacefeedfacefeedface")

    monkeypatch.setattr(sync, "fetch_pack", _fake_fetch_pack)

    rc = sync.run_sync(
        config_path=config_path,
        project_root=project_root,
        dry_run=False,
        prune=True,
        pack_filter="pack-a",
        force_refetch=False,
    )

    assert rc == 0
    assert not stale_a.exists()
    assert keep_b.exists()
    manifest = sync.load_manifest(manifest_path)
    assert any(asset["pack"] == "pack-b" for asset in manifest["assets"])
    assert any(asset["pack"] == "pack-a" and asset["target"] == "ext-pack-a.md" for asset in manifest["assets"])
    assert "ext-pack-b.md" in (generated_dir / "README.md").read_text(encoding="utf-8")


def test_run_sync_dry_run_with_uncached_local_repo_analyzes_without_repo_writes(tmp_path: Path, capsys) -> None:
    project_root = tmp_path / "project"
    repo_dir = tmp_path / "repo"
    (repo_dir / "commands").mkdir(parents=True)
    (repo_dir / "commands" / "greet.md").write_text("---\ndescription: hi\n---\nHello.\n", encoding="utf-8")
    _init_git_repo(repo_dir)

    config = {
        "version": 1,
        "packs": [
            {
                "id": "local-pack",
                "repo_url": str(repo_dir),
                "ref": "main",
                "include": {
                    "commands": [{"source": "commands/greet.md", "target": "ext-greet.md", "activation": "local"}],
                    "agents": [],
                    "skills": [],
                },
            }
        ],
    }
    config_path = project_root / ".claude" / "skillpacks.local.json"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(json.dumps(config), encoding="utf-8")

    rc = sync.run_sync(
        config_path=config_path,
        project_root=project_root,
        dry_run=True,
        prune=True,
        pack_filter=None,
        force_refetch=False,
    )

    captured = capsys.readouterr()
    assert rc == 0
    assert "source file not found" not in captured.err
    assert (project_root / ".claude" / "cache" / "skillpacks" / "local-pack").exists()
    assert not (project_root / ".claude" / "generated" / "skillpacks-manifest.json").exists()
    assert not (project_root / ".claude" / "commands" / "ext-greet.md").exists()


# ---------------------------------------------------------------------------
# Manifest I/O
# ---------------------------------------------------------------------------


def test_write_and_load_manifest_roundtrip(tmp_path: Path):
    manifest = {
        "version": 1,
        "generated_at": "2026-01-01T00:00:00Z",
        "assets": [
            {
                "type": "command",
                "pack": "mypak",
                "target": "ext-greet.md",
                "active_path": "/a/b.md",
                "resolved_commit": "abc123",
            }
        ],
    }
    path = tmp_path / "manifest.json"
    sync.write_manifest(manifest, path)
    loaded = sync.load_manifest(path)
    assert loaded == manifest


def test_load_manifest_returns_empty_when_missing(tmp_path: Path):
    result = sync.load_manifest(tmp_path / "nonexistent.json")
    assert result == {"version": 1, "assets": []}

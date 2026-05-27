from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import joblib

from app.config.settings import get_settings


@dataclass(frozen=True)
class ArtifactPaths:
    root: Path
    minutes_model: Path
    stat_models: Path
    calibrators: Path
    metadata: Path
    population_priors: Path
    consistency_scores: Path


def artifact_paths(model_version: str, namespace: str | None = None) -> ArtifactPaths:
    resolved_namespace = namespace or "default"
    root = get_settings().duckdb_path.parent / "models" / model_version / resolved_namespace
    root.mkdir(parents=True, exist_ok=True)
    return ArtifactPaths(
        root=root,
        minutes_model=root / "minutes_model.joblib",
        stat_models=root / "stat_models.joblib",
        calibrators=root / "calibrators.joblib",
        metadata=root / "metadata.joblib",
        population_priors=root / "population_priors.joblib",
        consistency_scores=root / "consistency_scores.joblib",
    )


class ArtifactIntegrityError(RuntimeError):
    pass


def _manifest_path(path: Path) -> Path:
    return path.with_suffix(path.suffix + ".sha256")


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def dump_artifact(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(payload, path)
    _manifest_path(path).write_text(_sha256_file(path))


def load_artifact(path: Path) -> Any:
    manifest = _manifest_path(path)
    if not manifest.exists():
        raise ArtifactIntegrityError(f"missing sha256 manifest for {path}")
    expected = manifest.read_text().strip()
    actual = _sha256_file(path)
    if expected != actual:
        raise ArtifactIntegrityError(
            f"sha256 mismatch for {path}: {expected[:8]}.. != {actual[:8]}.."
        )
    return joblib.load(path)


def reseal_existing_artifact(path: Path) -> None:
    """Write a manifest for an already-trusted on-disk artifact (operator-initiated)."""
    _manifest_path(path).write_text(_sha256_file(path))


def artifact_exists(path: Path) -> bool:
    return path.exists() and path.is_file()


def resolve_artifact_namespace(source: object, app_env: str = "dev") -> str:
    raw = f"{app_env}|{source}"
    slug_source = re.sub(r"[^a-z0-9]+", "_", str(source).lower()).strip("_") or "default"
    slug = slug_source[:48]
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]
    return f"{slug}_{digest}"

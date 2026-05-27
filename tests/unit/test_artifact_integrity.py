import hashlib

import pytest

from app.training import artifacts


def test_load_artifact_verifies_sha256(tmp_path):
    payload = {"foo": 1}
    art_path = tmp_path / "model.joblib"
    import joblib
    joblib.dump(payload, art_path)
    manifest = tmp_path / "model.joblib.sha256"
    manifest.write_text(hashlib.sha256(art_path.read_bytes()).hexdigest())
    assert artifacts.load_artifact(art_path) == payload


def test_load_artifact_rejects_tampered_file(tmp_path):
    art_path = tmp_path / "model.joblib"
    import joblib
    joblib.dump({"a": 1}, art_path)
    manifest = tmp_path / "model.joblib.sha256"
    manifest.write_text("0" * 64)  # bogus hash
    with pytest.raises(artifacts.ArtifactIntegrityError):
        artifacts.load_artifact(art_path)


def test_load_artifact_rejects_missing_manifest(tmp_path):
    art_path = tmp_path / "model.joblib"
    import joblib
    joblib.dump({"a": 1}, art_path)
    with pytest.raises(artifacts.ArtifactIntegrityError):
        artifacts.load_artifact(art_path)


def test_dump_artifact_writes_manifest(tmp_path):
    out = tmp_path / "x.joblib"
    artifacts.dump_artifact(out, {"k": "v"})
    assert (tmp_path / "x.joblib.sha256").exists()

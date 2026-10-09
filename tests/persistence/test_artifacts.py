"""Exact-byte artifact verification and private staging; no workflow imports."""

import hashlib
import os
import stat
from dataclasses import FrozenInstanceError

import pytest

from jobflow_gitlab_slurm.persistence import artifacts
from jobflow_gitlab_slurm.persistence.artifacts import (
    stage_verified_artifact,
    verify_artifact,
)


@pytest.mark.parametrize(
    "payload",
    [b"", b"\x00\xff\r\n", b'{ "opaque": true }\r\n', b"x" * (1024 * 1024 + 7)],
)
def test_verification_and_staging_preserve_exact_bytes(tmp_path, payload):
    source = tmp_path / "source"
    destination = tmp_path / "payload.json"
    source.write_bytes(payload)
    expected = hashlib.sha256(payload).hexdigest()

    verified = verify_artifact(source, expected)
    staged = stage_verified_artifact(source, destination, expected)

    assert verified.path == source
    assert staged.path == destination
    assert verified.sha256 == staged.sha256 == expected
    assert verified.size_bytes == staged.size_bytes == len(payload)
    assert source.read_bytes() == destination.read_bytes() == payload
    assert stat.S_IMODE(destination.stat().st_mode) == 0o600
    with pytest.raises(FrozenInstanceError):
        staged.sha256 = "0" * 64


@pytest.mark.parametrize("operation", ["verify", "stage"])
@pytest.mark.parametrize("digest", ["A" * 64, "a" * 63, "invalid", None])
def test_invalid_digest_rejected_before_file_access(tmp_path, operation, digest):
    source = tmp_path / "missing"
    destination = tmp_path / "destination"
    with pytest.raises(ValueError, match="64 lowercase hexadecimal"):
        if operation == "verify":
            verify_artifact(source, digest)
        else:
            stage_verified_artifact(source, destination, digest)
    assert not destination.exists()


@pytest.mark.parametrize("operation", ["verify", "stage"])
def test_digest_mismatch(tmp_path, operation):
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.write_bytes(b"actual bytes")
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        if operation == "verify":
            verify_artifact(source, "0" * 64)
        else:
            stage_verified_artifact(source, destination, "0" * 64)
    assert source.read_bytes() == b"actual bytes"
    assert not destination.exists()


@pytest.mark.parametrize("operation", ["verify", "stage"])
@pytest.mark.parametrize("kind", ["missing", "symlink", "directory", "fifo"])
def test_unsupported_source_is_rejected(tmp_path, operation, kind):
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    if kind == "symlink":
        target = tmp_path / "target"
        target.write_bytes(b"target")
        source.symlink_to(target)
    elif kind == "directory":
        source.mkdir()
    elif kind == "fifo":
        os.mkfifo(source)

    error = ValueError if kind in {"directory", "fifo"} else OSError
    with pytest.raises(error):
        if operation == "verify":
            verify_artifact(source, "0" * 64)
        else:
            stage_verified_artifact(source, destination, "0" * 64)
    assert not destination.exists()


@pytest.mark.parametrize("symlink", [False, True])
def test_existing_destination_is_never_overwritten(tmp_path, symlink):
    source = tmp_path / "source"
    source.write_bytes(b"source")
    destination = tmp_path / "destination"
    preserved = tmp_path / "preserved" if symlink else destination
    preserved.write_bytes(b"keep me")
    if symlink:
        destination.symlink_to(preserved)

    with pytest.raises(FileExistsError):
        stage_verified_artifact(
            source, destination, hashlib.sha256(b"source").hexdigest()
        )
    assert preserved.read_bytes() == b"keep me"
    assert destination.is_symlink() is symlink


def test_missing_destination_parent_is_not_created(tmp_path):
    source = tmp_path / "source"
    source.write_bytes(b"source")
    destination = tmp_path / "missing" / "destination"
    with pytest.raises(FileNotFoundError):
        stage_verified_artifact(
            source, destination, hashlib.sha256(b"source").hexdigest()
        )
    assert not destination.parent.exists()


def test_flush_failure_removes_only_own_staging_file(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.write_bytes(b"source")
    destination = tmp_path / "destination"
    preserved = tmp_path / "preserved"
    preserved.write_bytes(b"keep me")

    def fail_fsync(descriptor):
        raise OSError("injected fsync failure")

    monkeypatch.setattr(artifacts.os, "fsync", fail_fsync)
    with pytest.raises(OSError, match="injected fsync failure"):
        stage_verified_artifact(
            source, destination, hashlib.sha256(b"source").hexdigest()
        )
    assert not destination.exists()
    assert source.read_bytes() == b"source"
    assert preserved.read_bytes() == b"keep me"

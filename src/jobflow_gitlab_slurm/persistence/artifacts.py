"""Verify and stage opaque artifact bytes without importing workflow code."""

import hashlib
import os
import re
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

_CHUNK_SIZE = 1024 * 1024
_SHA256 = re.compile(r"[0-9a-f]{64}")


@dataclass(frozen=True)
class VerifiedArtifact:
    """Identify the bytes verified at a path, not their future immutability."""

    path: Path
    sha256: str
    size_bytes: int


def _validate_digest(expected_sha256: str) -> None:
    if not isinstance(expected_sha256, str) or not _SHA256.fullmatch(expected_sha256):
        raise ValueError("expected_sha256 must be 64 lowercase hexadecimal characters")


@contextmanager
def _open_regular(path: Path) -> Iterator[BinaryIO]:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError(f"artifact source must be a regular file: {path}")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            yield stream
    finally:
        os.close(descriptor)


def _consume(source: BinaryIO, destination: BinaryIO | None = None) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    while block := source.read(_CHUNK_SIZE):
        if destination is not None:
            destination.write(block)
        digest.update(block)
        size += len(block)
    return digest.hexdigest(), size


def _require_match(path: Path, expected: str, actual: str) -> None:
    if actual != expected:
        raise ValueError(
            f"SHA-256 mismatch for {path}: expected {expected}, observed {actual}"
        )


def verify_artifact(path: str | Path, expected_sha256: str) -> VerifiedArtifact:
    """Stream-check a regular file against an exact-byte SHA-256 claim.

    The final path component must not be a symlink. Parent directories must
    be trusted. Verification does not prevent subsequent file modification.
    """
    _validate_digest(expected_sha256)
    source = Path(path)
    with _open_regular(source) as stream:
        actual, size = _consume(stream)
    _require_match(source, expected_sha256, actual)
    return VerifiedArtifact(source, actual, size)


def stage_verified_artifact(
    source: str | Path,
    destination: str | Path,
    expected_sha256: str,
) -> VerifiedArtifact:
    """Copy and hash the same bytes into a new private staging file.

    Use only inside a trusted, unpublished staging directory. This helper
    does not atomically publish a run or create parent directories. It never
    overwrites an existing destination. A caught failure removes only the
    file created by this invocation; a hard crash may leave partial staging.
    """
    _validate_digest(expected_sha256)
    source_path = Path(source)
    destination_path = Path(destination)
    created = False
    try:
        with _open_regular(source_path) as stream:
            descriptor = os.open(
                destination_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
            )
            created = True
            try:
                with os.fdopen(descriptor, "wb", closefd=False) as output:
                    actual, size = _consume(stream, output)
                    _require_match(source_path, expected_sha256, actual)
                    output.flush()
                    os.fsync(output.fileno())
            finally:
                os.close(descriptor)
    except BaseException:
        if created:
            destination_path.unlink(missing_ok=True)
        raise
    return VerifiedArtifact(destination_path, actual, size)

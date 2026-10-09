"""Publish complete opaque bundles; never recover or execute calculations.

Directory selection, same-filesystem guards, and ordered flush primitives are
package-internal contracts shared with explicit recovery. Callers retain
invocation ownership and control transaction sequencing. Shared error-base
inheritance preserves the existing contextual diagnostics; it is not a new
public consumer interface.
"""

import os
import stat
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from jobflow_gitlab_slurm.persistence._filesystem import (
    regular_file_descriptor as _regular_descriptor,
)
from jobflow_gitlab_slurm.persistence._filesystem import (
    sync_directory as _sync_directory,
)
from jobflow_gitlab_slurm.persistence._filesystem import (
    sync_file as _sync_file,
)
from jobflow_gitlab_slurm.persistence.attempts.ownership import InvocationOwnership
from jobflow_gitlab_slurm.persistence.attempts.records import InvocationRecord
from jobflow_gitlab_slurm.persistence.bundles import inspection
from jobflow_gitlab_slurm.persistence.bundles.inspection import (
    BundleInspectionError,
    BundleIssue,
)
from jobflow_gitlab_slurm.persistence.bundles.records import (
    BundleCommit,
    BundleManifest,
    encode_bundle_commit,
    encode_bundle_manifest,
    validate_bundle_records,
)

_IDENTITY_FIELDS = (
    "run_id",
    "job_uuid",
    "job_index",
    "job_key",
    "attempt_id",
    "invocation_id",
)


@dataclass(frozen=True)
class BundleHandle:
    """Verified publication observation; not ownership or result eligibility."""

    path: Path
    manifest: BundleManifest
    commit: BundleCommit


class _BundleError(RuntimeError):
    def __init__(
        self,
        invocation: InvocationRecord,
        path: Path,
        code: str,
        issues: tuple[BundleIssue, ...] = (),
    ) -> None:
        self.invocation = invocation
        self.path = path
        self.code = code
        self.issues = issues
        self.run_id = invocation.run_id
        self.job_key = invocation.job_key
        self.job_index = invocation.job_index
        self.attempt_id = invocation.attempt_id
        self.invocation_id = invocation.invocation_id
        super().__init__(
            f"Bundle storage failure: code={code}; "
            f"run_id={self.run_id}; job_key={self.job_key}; "
            f"job_index={self.job_index}; attempt_id={self.attempt_id}; "
            f"invocation_id={self.invocation_id}. "
            "Preserve evidence and inspect the same identity. "
            "Do not overwrite, clean up, release dependent jobs, "
            "or launch a calculation automatically."
        )

    def to_report(self) -> dict[str, Any]:
        """Return detached diagnostics without payloads or raw exceptions."""
        return {
            "schema_version": 1,
            "kind": "bundle-storage-error",
            "error_type": type(self).__name__,
            "identity": {
                field: getattr(self.invocation, field) for field in _IDENTITY_FIELDS
            },
            "code": self.code,
            "path": str(self.path),
            "issues": [issue.to_report() for issue in self.issues],
            "hint": (
                "Preserve evidence and inspect this same identity. "
                "Any recovery or new calculation requires its separate "
                "validated and authorized operation."
            ),
        }


class BundleIntegrityError(_BundleError):
    """Absent, incomplete, unsafe, inconsistent, or ambiguous evidence is held."""


class BundleConflictError(_BundleError):
    """Valid observed metadata differs from the retained expected records."""

    def __init__(self, invocation: InvocationRecord, path: Path) -> None:
        super().__init__(invocation, path, "expected_record_conflict")


class BundleRecoveryRequiredError(_BundleError):
    """Published evidence is intact but unmarked; ordinary publication stops."""

    def __init__(self, invocation: InvocationRecord, path: Path) -> None:
        super().__init__(invocation, path, "explicit_recovery_required")


class BundlePublicationError(_BundleError):
    """Mutation or durable acknowledgment failed; publication may be uncertain."""

    def __init__(
        self,
        invocation: InvocationRecord,
        path: Path,
        *,
        operation: str,
        phase: str,
        staging_path: Path,
        published_path: Path,
        temporary_path: Path | None,
        publication_uncertain: bool,
        reason: str,
        errno: int | None,
        issues: tuple[BundleIssue, ...] = (),
    ) -> None:
        super().__init__(invocation, path, "publication_failed", issues)
        self.operation = operation
        self.phase = phase
        self.staging_path = staging_path
        self.published_path = published_path
        self.temporary_path = temporary_path
        self.publication_uncertain = publication_uncertain
        self.reason = reason
        self.errno = errno
        self.args = (
            (
                f"{self.args[0]} operation={operation}; phase={phase}; "
                f"publication_uncertain={publication_uncertain}; "
                f"reason={reason}; errno={errno}."
            ),
        )

    def to_report(self) -> dict[str, Any]:
        """Include operation phase and prospective/retained evidence paths."""
        report = super().to_report()
        report.update(
            {
                "operation": self.operation,
                "phase": self.phase,
                "staging_path": str(self.staging_path),
                "published_path": str(self.published_path),
                "temporary_path": (
                    None if self.temporary_path is None else str(self.temporary_path)
                ),
                "publication_uncertain": self.publication_uncertain,
                "reason": self.reason,
                "errno": self.errno,
            }
        )
        return report


def _expected_records(
    ownership: InvocationOwnership,
    expected_manifest: BundleManifest,
    expected_commit: BundleCommit,
) -> tuple[BundleManifest, BundleCommit]:
    invocation = ownership.invocation
    try:
        manifest = BundleManifest.model_validate(expected_manifest)
        commit = BundleCommit.model_validate(expected_commit)
        validate_bundle_records(
            invocation.attempt.definition.record,
            invocation.attempt.record,
            invocation.record,
            manifest,
            commit,
        )
    except ValueError:
        raise BundleIntegrityError(
            invocation.record,
            invocation.path,
            "invalid_expected_records",
        ) from None
    return manifest, commit


def _read_bytes(path: Path) -> bytes:
    with (
        _regular_descriptor(path) as descriptor,
        os.fdopen(descriptor, "rb", closefd=False) as stream,
    ):
        return stream.read()


def _require_exact(
    invocation: InvocationRecord,
    path: Path,
    expected: bytes,
) -> None:
    if _read_bytes(path) != expected:
        raise BundleConflictError(invocation, path)


def bundle_directories(unit: Path, manifest: BundleManifest) -> tuple[Path, ...]:
    """Select declared payload parents deepest-first without touching files."""
    directories = {unit}
    for reference in inspection._payloads(manifest):
        parent = (unit / reference.path).parent
        while parent != unit:
            directories.add(parent)
            parent = parent.parent
    return tuple(
        sorted(
            directories,
            key=lambda path: (-len(path.parts), str(path)),
        )
    )


def require_same_filesystem(
    ownership: InvocationOwnership,
    directories: tuple[Path, ...],
) -> None:
    """Check real bundle directories against the owned invocation's device."""
    invocation = ownership.invocation
    device = invocation.path.lstat().st_dev
    for path in (invocation.path, *directories):
        metadata = path.lstat()
        if not stat.S_ISDIR(metadata.st_mode):
            raise BundleIntegrityError(
                invocation.record,
                path,
                "unsafe_bundle_directory",
            )
        if metadata.st_dev != device:
            raise BundleIntegrityError(
                invocation.record,
                path,
                "cross_filesystem_bundle",
            )


def flush_payloads(
    unit: Path,
    manifest: BundleManifest,
    *,
    marker: bool,
) -> None:
    """Flush payloads, manifest, then an existing marker when explicitly selected."""
    for reference in inspection._payloads(manifest):
        _sync_file(unit / reference.path)
    _sync_file(unit / "payload-manifest.json")
    if marker:
        _sync_file(unit / "COMMIT.json")


def flush_scheduler_receipt(
    ownership: InvocationOwnership,
    manifest: BundleManifest,
) -> None:
    """Flush the qualified scheduler receipt then its parent; no scheduler query."""
    # Fixed layout: <run>/jobs/<key>/index-N/attempts/<attempt-id>.
    run_path = ownership.invocation.attempt.path.parents[4]
    receipt = run_path / manifest.scheduler_receipt.path
    _sync_file(receipt)
    _sync_directory(receipt.parent)


def flush_directories(directories: tuple[Path, ...]) -> None:
    """Flush directories in caller-provided order without acquiring run locks."""
    for path in directories:
        _sync_directory(path)


def _require_absent(path: Path) -> None:
    try:
        path.lstat()
    except FileNotFoundError:
        return
    raise ValueError("publication destination is not absent")


def _verified_handle(
    ownership: InvocationOwnership,
    manifest: BundleManifest,
    commit: BundleCommit,
) -> BundleHandle:
    invocation = ownership.invocation
    published = invocation.path / "published"
    report = inspection.inspect_owned_bundle(ownership)
    if report.status != "committed" or report.content_integrity != "valid":
        raise BundleIntegrityError(
            invocation.record,
            published,
            "final_bundle_not_committed",
            report.issues,
        )
    _require_exact(
        invocation.record,
        published / "payload-manifest.json",
        encode_bundle_manifest(manifest),
    )
    _require_exact(
        invocation.record,
        published / "COMMIT.json",
        encode_bundle_commit(commit),
    )
    return BundleHandle(published, manifest, commit)


def publish_owned_bundle(
    ownership: InvocationOwnership,
    expected_manifest: BundleManifest,
    expected_commit: BundleCommit,
) -> BundleHandle:
    """Publish complete staging or durably acknowledge an exact existing commit.

    Consume active ownership without acquiring invocation/run locks again.
    Payloads and receipt bytes remain opaque. Published-but-unmarked evidence
    requires a separate explicit recovery operation.

    The caller retains the expected records, IDs, and timestamps across retries.
    This API neither creates missing payloads nor authorizes scientific execution,
    scheduler success, recovery, result selection, or descendant execution.

    Pathname operations rely on the trusted-root/cooperating-writer contract.
    They do not provide an adversarial no-replace or filesystem security boundary.
    """
    ownership.require_active()
    invocation = ownership.invocation
    manifest, commit = _expected_records(ownership, expected_manifest, expected_commit)
    manifest_bytes = encode_bundle_manifest(manifest)
    commit_bytes = encode_bundle_commit(commit)

    staging = invocation.path / "staging"
    published = invocation.path / "published"
    report = inspection.inspect_owned_bundle(ownership)
    if (
        report.status not in ("staging_only", "published_uncommitted", "committed")
        or report.content_integrity != "valid"
    ):
        raise BundleIntegrityError(
            invocation.record,
            invocation.path,
            f"bundle_{report.status}_{report.content_integrity}",
            report.issues,
        )

    acknowledge = report.status == "committed"
    unit = staging if report.status == "staging_only" else published
    directories = bundle_directories(unit, manifest)
    try:
        _require_exact(
            invocation.record,
            unit / "payload-manifest.json",
            manifest_bytes,
        )
        if acknowledge:
            _require_exact(
                invocation.record,
                unit / "COMMIT.json",
                commit_bytes,
            )
        require_same_filesystem(ownership, directories)
    except OSError as error:
        raise BundleInspectionError(
            invocation.record, invocation.path, error.errno
        ) from None
    except ValueError:
        raise BundleIntegrityError(
            invocation.record,
            unit,
            "unsafe_stored_metadata",
        ) from None

    if report.status == "published_uncommitted":
        raise BundleRecoveryRequiredError(invocation.record, published)

    operation = "acknowledge" if acknowledge else "publish"
    publication_uncertain = acknowledge
    temporary_path = None
    phase = "payload-flush"

    try:
        flush_payloads(unit, manifest, marker=acknowledge)
        phase = "receipt-flush"
        flush_scheduler_receipt(ownership, manifest)
        phase = "bundle-directory-flush"
        flush_directories(directories)
        phase = "invocation-directory-flush"
        _sync_directory(invocation.path)

        if not acknowledge:
            phase = "destination-check"
            _require_absent(published)
            phase = "directory-rename"
            publication_uncertain = True
            os.rename(staging, published)
            phase = "published-directory-flush"
            _sync_directory(published)
            phase = "rename-parent-flush"
            _sync_directory(invocation.path)

            phase = "marker-create"
            descriptor, name = tempfile.mkstemp(
                prefix=".bundle-commit-",
                dir=invocation.path,
            )
            temporary_path = Path(name)
            try:
                phase = "marker-write"
                with os.fdopen(descriptor, "wb", closefd=False) as stream:
                    stream.write(commit_bytes)
                    stream.flush()
                    phase = "marker-file-flush"
                    os.fsync(descriptor)
            finally:
                os.close(descriptor)

            phase = "marker-parent-flush"
            _sync_directory(invocation.path)
            phase = "marker-destination-check"
            _require_absent(published / "COMMIT.json")
            phase = "marker-rename"
            os.rename(temporary_path, published / "COMMIT.json")
            phase = "final-directory-flush"
            _sync_directory(published)
            phase = "final-parent-flush"
            _sync_directory(invocation.path)

        phase = "final-verification"
        return _verified_handle(ownership, manifest, commit)
    except (
        OSError,
        ValueError,
        BundleInspectionError,
        _BundleError,
    ) as error:
        raise BundlePublicationError(
            invocation.record,
            invocation.path,
            operation=operation,
            phase=phase,
            staging_path=staging,
            published_path=published,
            temporary_path=temporary_path,
            publication_uncertain=publication_uncertain,
            reason=getattr(error, "code", "operation_failed"),
            errno=getattr(error, "errno", None),
            issues=getattr(error, "issues", ()),
        ) from None

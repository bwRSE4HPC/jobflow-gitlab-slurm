"""Read-only inspection of opaque bundle evidence under invocation ownership."""

import hashlib
import os
import stat
from collections.abc import Callable
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Literal

from jobflow_gitlab_slurm.persistence._filesystem import (
    regular_file_descriptor as _regular_descriptor,
)
from jobflow_gitlab_slurm.persistence.artifacts import verify_artifact
from jobflow_gitlab_slurm.persistence.attempts.ownership import (
    InvocationOwnership,
    owned_invocation,
)
from jobflow_gitlab_slurm.persistence.attempts.records import (
    InvocationRecord,
    RecordArtifactReference,
)
from jobflow_gitlab_slurm.persistence.bundles.records import (
    BundleCommit,
    BundleManifest,
    decode_bundle_commit,
    decode_bundle_manifest,
    validate_bundle_records,
)

BundleStatus = Literal[
    "absent",
    "staging_only",
    "published_uncommitted",
    "committed",
    "ambiguous",
    "invalid",
]
ContentIntegrity = Literal["not_evaluated", "incomplete", "valid", "invalid"]
EntryKind = Literal["absent", "directory", "file", "unsafe"]

_IDENTITY_FIELDS = (
    "run_id",
    "job_uuid",
    "job_index",
    "job_key",
    "attempt_id",
    "invocation_id",
)
_PROVENANCE_FIELDS = (
    "definition_id",
    "definition_sha256",
    "consumer_code_sha256",
    "worker_runtime_sha256",
)
_HINTS = {
    "absent": (
        "Preserve the invocation. Absence of bundle evidence does not prove "
        "that execution never occurred. Do not automatically rerun."
    ),
    "staging_only": (
        "Preserve staging and inspect its findings. Staging is not committed; "
        "publication or recovery requires a later authorized operation."
    ),
    "published_uncommitted": (
        "Preserve published evidence. No completion marker was acknowledged; "
        "do not automatically finalize or release dependent jobs."
    ),
    "committed": (
        "Filesystem evidence verifies. Jobflow semantics, scheduler outcome, "
        "and result eligibility still require their separate checks."
    ),
    "ambiguous": (
        "Preserve both directories. Neither inventory was selected or verified. "
        "Do not delete, finalize, or rerun automatically."
    ),
    "invalid": (
        "Preserve evidence and inspect the issue paths. This report does not "
        "authorize repair, replacement, descendant execution, or a rerun."
    ),
}


@dataclass(frozen=True)
class BundleIssue:
    """A stable finding without payload content or raw decoder messages."""

    code: str
    path: Path

    def to_report(self) -> dict[str, str]:
        """Return detached JSON-compatible diagnostics."""
        return {"code": self.code, "path": str(self.path)}


@dataclass(frozen=True)
class BundleInspection:
    """An owned observation, not a retained lock or execution authorization."""

    invocation: InvocationRecord
    invocation_path: Path
    staging_kind: EntryKind
    published_kind: EntryKind
    status: BundleStatus
    content_integrity: ContentIntegrity
    inspected_path: Path | None = None
    manifest_present: bool | None = None
    commit_present: bool | None = None
    issues: tuple[BundleIssue, ...] = ()

    def to_report(self) -> dict[str, Any]:
        """Return detached JSON-compatible findings without scientific bodies."""
        return {
            "schema_version": 1,
            "kind": "bundle-inspection",
            "identity": {
                field: getattr(self.invocation, field) for field in _IDENTITY_FIELDS
            },
            "evidence": {
                "invocation_path": str(self.invocation_path),
                "staging": {
                    "path": str(self.invocation_path / "staging"),
                    "kind": self.staging_kind,
                },
                "published": {
                    "path": str(self.invocation_path / "published"),
                    "kind": self.published_kind,
                },
                "inspected_path": (
                    None if self.inspected_path is None else str(self.inspected_path)
                ),
                "manifest_present": self.manifest_present,
                "commit_present": self.commit_present,
            },
            "status": self.status,
            "content_integrity": self.content_integrity,
            "jobflow_semantics": "not_evaluated",
            "scheduler_state": "not_evaluated",
            "result_eligibility": "not_evaluated",
            "issues": [issue.to_report() for issue in self.issues],
            "hint": _HINTS[self.status],
        }


class BundleInspectionError(RuntimeError):
    """Inspection was unavailable; absence or corruption was not established."""

    def __init__(
        self,
        invocation: InvocationRecord,
        path: Path,
        errno: int | None,
    ) -> None:
        self.run_id = invocation.run_id
        self.job_key = invocation.job_key
        self.job_index = invocation.job_index
        self.attempt_id = invocation.attempt_id
        self.invocation_id = invocation.invocation_id
        self.path = path
        self.errno = errno
        self.code = "inspection_io_unavailable"
        super().__init__(
            "Bundle inspection unavailable: "
            f"run_id={self.run_id}; job_key={self.job_key}; "
            f"job_index={self.job_index}; attempt_id={self.attempt_id}; "
            f"invocation_id={self.invocation_id}; errno={errno}. "
            "Preserve evidence. Check storage access and retry inspection "
            "of the same identity; do not infer corruption or rerun."
        )


class _Invalid(Exception):
    def __init__(self, code: str, path: Path) -> None:
        self.issue = BundleIssue(code, path)
        super().__init__(code)


def _kind(path: Path) -> EntryKind:
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError:
        return "absent"
    if stat.S_ISDIR(mode):
        return "directory"
    if stat.S_ISREG(mode):
        return "file"
    return "unsafe"


def _inventory(unit: Path) -> dict[str, EntryKind]:
    """Enumerate every entry without following symlinks or opening special files."""
    entries: dict[str, EntryKind] = {}
    pending = [unit]
    while pending:
        parent = pending.pop()
        for path in sorted(parent.iterdir()):
            kind = _kind(path)
            if kind == "absent":
                # A disappearing enumerated entry is an unavailable observation,
                # not evidence that the inventory was complete.
                raise OSError("bundle entry disappeared during inspection")
            if kind == "unsafe":
                raise _Invalid("unsafe_entry", path)
            entries[path.relative_to(unit).as_posix()] = kind
            if kind == "directory":
                pending.append(path)
    return entries


def _decode[T](path: Path, decoder: Callable[[bytes], T]) -> T:
    try:
        with (
            _regular_descriptor(path) as descriptor,
            os.fdopen(descriptor, "rb", closefd=False) as stream,
        ):
            return decoder(stream.read())
    except ValueError:
        raise _Invalid("invalid_metadata", path) from None


def _identity(
    record: InvocationRecord | BundleManifest | BundleCommit,
) -> tuple[object, ...]:
    return tuple(getattr(record, field) for field in _IDENTITY_FIELDS)


def _bind_manifest(
    ownership: InvocationOwnership,
    manifest: BundleManifest,
) -> None:
    """Validate unmarked evidence without manufacturing a completion marker."""
    invocation = ownership.invocation
    attempt = invocation.attempt.record
    if (
        _identity(manifest) != _identity(invocation.record)
        or tuple(getattr(manifest, field) for field in _PROVENANCE_FIELDS)
        != tuple(getattr(attempt, field) for field in _PROVENANCE_FIELDS)
        or manifest.created_at < invocation.record.created_at
    ):
        raise ValueError("manifest binding mismatch")


def _payloads(manifest: BundleManifest) -> tuple[RecordArtifactReference, ...]:
    return (
        manifest.document,
        manifest.response,
        *manifest.files,
        *(item.artifact for item in manifest.additional_data),
    )


def _expected_inventory(
    manifest: BundleManifest,
    marker_present: bool,
) -> dict[str, EntryKind]:
    expected: dict[str, EntryKind] = {
        reference.path: "file" for reference in _payloads(manifest)
    }
    expected["payload-manifest.json"] = "file"
    if marker_present:
        expected["COMMIT.json"] = "file"
    for name in tuple(expected):
        parts = name.split("/")
        for end in range(1, len(parts)):
            expected["/".join(parts[:end])] = "directory"
    return expected


def _verify_reference(
    base: Path,
    reference: RecordArtifactReference,
) -> BundleIssue | None:
    """Check parents separately before streaming a final no-follow regular file."""
    parts = reference.path.split("/")
    path = base
    parents = [base]
    for component in parts[:-1]:
        path /= component
        parents.append(path)
    target = path / parts[-1]

    for parent in parents:
        kind = _kind(parent)
        if kind == "absent":
            return BundleIssue("artifact_missing", target)
        if kind != "directory":
            return BundleIssue("unsafe_artifact_path", parent)

    kind = _kind(target)
    if kind == "absent":
        return BundleIssue("artifact_missing", target)
    if kind != "file":
        return BundleIssue("unsafe_artifact_path", target)

    try:
        verified = verify_artifact(target, reference.sha256)
    except ValueError:
        return BundleIssue("artifact_bytes_invalid", target)
    if verified.size_bytes != reference.size_bytes:
        return BundleIssue("artifact_size_mismatch", target)
    return None


def _inspect_unit(
    ownership: InvocationOwnership,
    report: BundleInspection,
    unit: Path,
) -> BundleInspection:
    manifest_path = unit / "payload-manifest.json"
    commit_path = unit / "COMMIT.json"
    report = replace(
        report,
        inspected_path=unit,
        manifest_present=_kind(manifest_path) != "absent",
        commit_present=_kind(commit_path) != "absent",
    )

    try:
        if report.status == "staging_only" and report.commit_present:
            raise _Invalid("marker_in_staging", commit_path)

        inventory = _inventory(unit)
        if not report.manifest_present:
            return replace(
                report,
                status="invalid" if report.commit_present else report.status,
                content_integrity="invalid" if report.commit_present else "incomplete",
                issues=(BundleIssue("manifest_missing", manifest_path),),
            )

        manifest = _decode(manifest_path, decode_bundle_manifest)
        expected = _expected_inventory(manifest, bool(report.commit_present))
        for name, kind in inventory.items():
            if expected.get(name) != kind:
                raise _Invalid("inventory_mismatch", unit / name)

        try:
            _bind_manifest(ownership, manifest)
            if report.commit_present:
                commit = _decode(commit_path, decode_bundle_commit)
                manifest_bytes = _read_manifest_bytes(manifest_path)
                if commit.manifest.sha256 != hashlib.sha256(
                    manifest_bytes
                ).hexdigest() or commit.manifest.size_bytes != len(manifest_bytes):
                    raise _Invalid("commit_manifest_mismatch", commit_path)
                invocation = ownership.invocation
                validate_bundle_records(
                    invocation.attempt.definition.record,
                    invocation.attempt.record,
                    invocation.record,
                    manifest,
                    commit,
                )
        except ValueError:
            raise _Invalid("bundle_binding_invalid", manifest_path) from None

        # Fixed layout: <run>/jobs/<key>/index-N/attempts/<attempt-id>.
        run_path = ownership.invocation.attempt.path.parents[4]
        references = (
            *((unit, reference) for reference in _payloads(manifest)),
            (run_path, manifest.scheduler_receipt),
        )
        issues = tuple(
            issue
            for base, reference in references
            if (issue := _verify_reference(base, reference)) is not None
        )
        integrity: ContentIntegrity = "valid"
        if issues:
            integrity = (
                "invalid"
                if any(issue.code != "artifact_missing" for issue in issues)
                else "incomplete"
            )
        if report.commit_present and integrity == "incomplete":
            integrity = "invalid"

        status = report.status
        if integrity == "invalid":
            status = "invalid"
        elif report.commit_present:
            status = "committed"
        return replace(
            report,
            status=status,
            content_integrity=integrity,
            issues=issues,
        )
    except _Invalid as error:
        return replace(
            report,
            status="invalid",
            content_integrity="invalid",
            issues=(error.issue,),
        )


def _read_manifest_bytes(path: Path) -> bytes:
    with (
        _regular_descriptor(path) as descriptor,
        os.fdopen(descriptor, "rb", closefd=False) as stream,
    ):
        return stream.read()


def _inspect(ownership: InvocationOwnership) -> BundleInspection:
    invocation = ownership.invocation
    staging = invocation.path / "staging"
    published = invocation.path / "published"
    staging_kind = _kind(staging)
    published_kind = _kind(published)
    report = BundleInspection(
        invocation=invocation.record,
        invocation_path=invocation.path,
        staging_kind=staging_kind,
        published_kind=published_kind,
        status="absent",
        content_integrity="not_evaluated",
    )

    unsafe = tuple(
        BundleIssue("unsafe_bundle_path", path)
        for path, kind in ((staging, staging_kind), (published, published_kind))
        if kind not in ("absent", "directory")
    )
    if unsafe:
        return replace(
            report,
            status="invalid",
            content_integrity="invalid",
            issues=unsafe,
        )
    if staging_kind == "directory" and published_kind == "directory":
        return replace(report, status="ambiguous")
    if staging_kind == "absent" and published_kind == "absent":
        return replace(report, manifest_present=False, commit_present=False)
    if staging_kind == "directory":
        return _inspect_unit(
            ownership,
            replace(report, status="staging_only"),
            staging,
        )
    return _inspect_unit(
        ownership,
        replace(report, status="published_uncommitted"),
        published,
    )


def inspect_owned_bundle(ownership: InvocationOwnership) -> BundleInspection:
    """Inspect using active ownership without reacquiring invocation/run locks.

    Parent metadata was validated when ownership was acquired. Payloads stay
    opaque; receipt bytes are checked without interpreting scheduler evidence.
    No filesystem publication, repair, result selection, or rerun is performed.
    """
    ownership.require_active()
    try:
        return _inspect(ownership)
    except OSError as error:
        raise BundleInspectionError(
            ownership.invocation.record,
            ownership.invocation.path,
            error.errno,
        ) from None


def inspect_invocation_bundle(
    runs_root: str | Path,
    run_id: str,
    job_uuid: str,
    job_index: int,
    attempt_id: str,
    invocation_id: str,
) -> BundleInspection:
    """Acquire existing ownership and return read-only filesystem findings.

    Ownership, parent-metadata, and contention errors retain their existing
    exception types. Unexpected bundle I/O raises BundleInspectionError rather
    than presenting an unavailable observation as absence or corruption.
    """
    with owned_invocation(
        runs_root,
        run_id,
        job_uuid,
        job_index,
        attempt_id,
        invocation_id,
    ) as ownership:
        return inspect_owned_bundle(ownership)

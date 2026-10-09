"""Owned immutable completion receipts; no bundle repair or execution."""

import hashlib
import os
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from jobflow_gitlab_slurm.persistence.attempts.definitions import (
    _sync_directory,
    _sync_file,
)
from jobflow_gitlab_slurm.persistence.attempts.ownership import InvocationOwnership
from jobflow_gitlab_slurm.persistence.bundles import storage as bundles
from jobflow_gitlab_slurm.persistence.bundles.inspection import (
    BundleInspectionError,
    BundleIssue,
    inspect_owned_bundle,
)
from jobflow_gitlab_slurm.persistence.bundles.records import (
    encode_bundle_commit,
    encode_bundle_manifest,
)
from jobflow_gitlab_slurm.persistence.publication.records import PublicationIntentRecord
from jobflow_gitlab_slurm.persistence.recovery import requests
from jobflow_gitlab_slurm.persistence.recovery.records import (
    RecoveryReceiptRecord,
    RecoveryRequestRecord,
    decode_recovery_receipt,
    encode_recovery_receipt,
    validate_recovery_receipt,
)

_TEMPORARY_PREFIX = requests._RECEIPT_TEMPORARY_PREFIX
_read_bytes = requests._read_bytes
_HINT = (
    "Preserve evidence and inspect/retry this same recovery identity and "
    "exact records. Do not overwrite records, remove temporary evidence, "
    "select another recovery ID, repair a bundle, release dependent jobs, "
    "or launch a calculation automatically. A receipt describes filesystem "
    "completion, not scheduler or scientific success."
)


@dataclass(frozen=True)
class RecoveryReceiptHandle:
    """Validated historical metadata, not current result eligibility."""

    path: Path
    record: RecoveryReceiptRecord
    sha256: str
    size_bytes: int


@dataclass(frozen=True)
class _Layout:
    root: Path
    unit: Path
    final: Path
    final_exists: bool
    temporary_paths: tuple[Path, ...]


@dataclass
class _Progress:
    operation: str
    phase: str = "preparation"
    temporary_path: Path | None = None
    preparation_uncertain: bool = False
    receipt_publication_uncertain: bool = False
    bundle_acknowledged: bool = False


class _ReceiptError(RuntimeError):
    def __init__(
        self,
        ownership: InvocationOwnership,
        recovery_id: str | None,
        path: Path,
        code: str,
        *,
        errno: int | None = None,
        temporary_paths: tuple[Path, ...] = (),
        existing_ids: tuple[str, ...] = (),
        issues: tuple[BundleIssue, ...] = (),
    ) -> None:
        self.invocation = ownership.invocation.record
        self.recovery_id = recovery_id
        self.path = path
        self.code = code
        self.errno = errno
        self.temporary_paths = temporary_paths
        self.existing_recovery_ids = existing_ids
        self.issues = issues
        self.bundle_path = ownership.invocation.path / "published"
        for field in requests._IDENTITY_FIELDS:
            setattr(self, field, getattr(self.invocation, field))
        super().__init__(
            f"Recovery-receipt storage failure: code={code}; "
            f"run_id={self.run_id}; job_key={self.job_key}; "
            f"job_index={self.job_index}; attempt_id={self.attempt_id}; "
            f"invocation_id={self.invocation_id}; recovery_id={recovery_id}; "
            f"path={path}; errno={errno}. {_HINT}"
        )

    def to_report(self) -> dict[str, Any]:
        """Return detached diagnostics without record bodies or raw causes."""
        return {
            "schema_version": 1,
            "kind": "recovery-receipt-storage-error",
            "error_type": type(self).__name__,
            "identity": {
                field: getattr(self.invocation, field)
                for field in requests._IDENTITY_FIELDS
            },
            "recovery_id": self.recovery_id,
            "path": str(self.path),
            "bundle_path": str(self.bundle_path),
            "code": self.code,
            "errno": self.errno,
            "temporary_paths": [str(path) for path in self.temporary_paths],
            "existing_recovery_ids": list(self.existing_recovery_ids),
            "issues": [issue.to_report() for issue in self.issues],
            "hint": _HINT,
        }


class RecoveryReceiptMissingError(_ReceiptError):
    """No final receipt; bundle or calculation completion remains unknown."""


class RecoveryReceiptIntegrityError(_ReceiptError):
    """Unsafe, malformed, ambiguous, incomplete, or incorrectly bound evidence."""


class RecoveryReceiptConflictError(_ReceiptError):
    """Intact evidence differs from the exact retained expected records."""


class RecoveryReceiptInspectionError(_ReceiptError):
    """I/O did not establish the available evidence."""


class RecoveryReceiptPublicationError(_ReceiptError):
    """Acknowledgment/publication failed; preserve the same operation identity."""

    def __init__(
        self,
        ownership: InvocationOwnership,
        recovery_id: str,
        path: Path,
        progress: _Progress,
        error: Exception,
    ) -> None:
        super().__init__(
            ownership,
            recovery_id,
            path,
            "receipt_publication_failed",
            errno=getattr(error, "errno", None),
            issues=getattr(error, "issues", ()),
        )
        self.operation = progress.operation
        self.phase = progress.phase
        self.temporary_path = progress.temporary_path
        self.preparation_uncertain = progress.preparation_uncertain
        self.receipt_publication_uncertain = progress.receipt_publication_uncertain
        self.bundle_acknowledged = progress.bundle_acknowledged
        self.reason = getattr(error, "code", "operation_failed")
        self.args = (
            (
                f"{self.args[0]} operation={self.operation}; phase={self.phase}; "
                f"preparation_uncertain={self.preparation_uncertain}; "
                f"receipt_publication_uncertain="
                f"{self.receipt_publication_uncertain}; "
                f"bundle_acknowledged_in_this_call={self.bundle_acknowledged}; "
                f"reason={self.reason}."
            ),
        )

    def to_report(self) -> dict[str, Any]:
        """Separate bundle acknowledgment from receipt publication uncertainty."""
        report = super().to_report()
        report.update(
            {
                "operation": self.operation,
                "phase": self.phase,
                "temporary_path": (
                    None if self.temporary_path is None else str(self.temporary_path)
                ),
                "preparation_uncertain": self.preparation_uncertain,
                "receipt_publication_uncertain": self.receipt_publication_uncertain,
                "bundle_acknowledged_in_this_call": self.bundle_acknowledged,
                "reason": self.reason,
            }
        )
        return report


def _translate(
    ownership: InvocationOwnership,
    recovery_id: str,
    error: requests._RequestError,
) -> _ReceiptError:
    if isinstance(error, requests.RecoveryRequestConflictError):
        error_type = RecoveryReceiptConflictError
    elif isinstance(error, requests.RecoveryRequestInspectionError):
        error_type = RecoveryReceiptInspectionError
    else:
        error_type = RecoveryReceiptIntegrityError
    return error_type(
        ownership,
        recovery_id,
        error.path,
        f"request_{error.code}",
        errno=error.errno,
        temporary_paths=error.temporary_paths,
        existing_ids=error.existing_recovery_ids,
        issues=error.issues,
    )


def _layout(ownership: InvocationOwnership, recovery_id: str) -> _Layout:
    try:
        audit = requests._layout(ownership, recovery_id)
        entries = (
            () if requests._absent(audit.unit) else tuple(sorted(audit.unit.iterdir()))
        )
    except requests._RequestError as error:
        raise _translate(ownership, recovery_id, error) from None
    except OSError as error:
        raise RecoveryReceiptInspectionError(
            ownership,
            recovery_id,
            ownership.invocation.path / "recoveries",
            "receipt_namespace_unavailable",
            errno=error.errno,
        ) from None

    final = audit.unit / "receipt.json"
    return _Layout(
        audit.root,
        audit.unit,
        final,
        final in entries,
        tuple(path for path in entries if path.name.startswith(_TEMPORARY_PREFIX)),
    )


def _parents(
    ownership: InvocationOwnership,
    recovery_id: str,
) -> tuple[RecoveryRequestRecord, PublicationIntentRecord]:
    try:
        request = requests.read_owned_recovery_request(ownership, recovery_id).record
        intent = requests._intent(ownership, recovery_id)
    except requests._RequestError as error:
        raise _translate(ownership, recovery_id, error) from None
    return request, intent


def _bind(
    ownership: InvocationOwnership,
    intent: PublicationIntentRecord,
    request: RecoveryRequestRecord,
    receipt: RecoveryReceiptRecord,
) -> None:
    invocation = ownership.invocation
    validate_recovery_receipt(
        invocation.attempt.definition.record,
        invocation.attempt.record,
        invocation.record,
        intent,
        request,
        receipt=receipt,
    )


def _read(
    ownership: InvocationOwnership,
    recovery_id: str,
    path: Path,
    request: RecoveryRequestRecord,
    intent: PublicationIntentRecord,
) -> RecoveryReceiptHandle:
    try:
        data = _read_bytes(path)
        record = decode_recovery_receipt(data)
        _bind(ownership, intent, request, record)
    except OSError as error:
        raise RecoveryReceiptInspectionError(
            ownership,
            recovery_id,
            path,
            "receipt_unavailable",
            errno=error.errno,
        ) from None
    except ValueError:
        raise RecoveryReceiptIntegrityError(
            ownership,
            recovery_id,
            path,
            "invalid_stored_receipt",
        ) from None
    return RecoveryReceiptHandle(
        path,
        record,
        hashlib.sha256(data).hexdigest(),
        len(data),
    )


def _require_exact(
    ownership: InvocationOwnership,
    handle: RecoveryReceiptHandle,
    expected: bytes,
) -> None:
    if encode_recovery_receipt(handle.record) != expected:
        raise RecoveryReceiptConflictError(
            ownership,
            handle.record.recovery_id,
            handle.path,
            "expected_receipt_conflict",
        )


def _verified_bundle(
    ownership: InvocationOwnership,
    recovery_id: str,
    intent: PublicationIntentRecord,
) -> tuple[Path, ...]:
    unit = ownership.invocation.path / "published"
    try:
        report = inspect_owned_bundle(ownership)
        if report.status != "committed" or report.content_integrity != "valid":
            raise RecoveryReceiptIntegrityError(
                ownership,
                recovery_id,
                unit,
                "committed_bundle_required",
                issues=report.issues,
            )
        for filename, expected in (
            ("payload-manifest.json", encode_bundle_manifest(intent.expected_manifest)),
            ("COMMIT.json", encode_bundle_commit(intent.expected_commit)),
        ):
            if _read_bytes(unit / filename) != expected:
                raise RecoveryReceiptConflictError(
                    ownership,
                    recovery_id,
                    unit / filename,
                    "expected_bundle_record_conflict",
                )
        directories = bundles.bundle_directories(unit, intent.expected_manifest)
        bundles.require_same_filesystem(ownership, directories)
    except (OSError, BundleInspectionError) as error:
        raise RecoveryReceiptInspectionError(
            ownership,
            recovery_id,
            unit,
            "bundle_verification_unavailable",
            errno=error.errno,
        ) from None
    except (ValueError, bundles._BundleError):
        raise RecoveryReceiptIntegrityError(
            ownership,
            recovery_id,
            unit,
            "unsafe_committed_bundle",
        ) from None
    return directories


def _candidate(
    ownership: InvocationOwnership,
    recovery_id: str,
    layout: _Layout,
    request: RecoveryRequestRecord,
    intent: PublicationIntentRecord,
    expected: bytes,
) -> Path | None:
    if len(layout.temporary_paths) > 1:
        raise RecoveryReceiptIntegrityError(
            ownership,
            recovery_id,
            layout.unit,
            "ambiguous_temporary_receipts",
            temporary_paths=layout.temporary_paths,
        )
    if not layout.temporary_paths:
        return None
    path = layout.temporary_paths[0]
    handle = _read(ownership, recovery_id, path, request, intent)
    _require_exact(ownership, handle, expected)
    return path


def _step[T](
    progress: _Progress,
    phase: str,
    operation: Callable[..., T],
    *args: Any,
) -> T:
    progress.phase = phase
    return operation(*args)


def _new_temporary(
    ownership: InvocationOwnership,
    layout: _Layout,
    expected: bytes,
    progress: _Progress,
) -> None:
    descriptor, name = _step(
        progress,
        "temporary-create",
        lambda: tempfile.mkstemp(prefix=_TEMPORARY_PREFIX, dir=layout.unit),
    )
    progress.temporary_path = Path(name)
    try:
        _step(
            progress,
            "temporary-check",
            requests._check_temporary,
            ownership,
            descriptor,
        )
        _step(progress, "temporary-write", requests._write_bytes, descriptor, expected)
        _step(progress, "temporary-file-flush", os.fsync, descriptor)
    finally:
        os.close(descriptor)


def read_owned_recovery_receipt(
    ownership: InvocationOwnership,
    recovery_id: str,
) -> RecoveryReceiptHandle:
    """Read final audit metadata without writes, flushing, or payload inspection."""
    ownership.require_active()
    selected = requests._Selection(recovery_id=recovery_id).recovery_id
    layout = _layout(ownership, selected)
    if not layout.final_exists:
        raise RecoveryReceiptMissingError(
            ownership,
            selected,
            layout.final,
            "receipt_missing",
            temporary_paths=layout.temporary_paths,
        )
    request, intent = _parents(ownership, selected)
    return _read(ownership, selected, layout.final, request, intent)


def persist_owned_recovery_receipt(
    ownership: InvocationOwnership,
    receipt: RecoveryReceiptRecord,
) -> RecoveryReceiptHandle:
    """Acknowledge an exact existing commit before publishing/reacknowledging audit.

    Never publish staging, create a marker, regenerate metadata, or execute
    calculations. Pathname operations assume a trusted root and cooperating
    writers; ownership alone does not exclude unmanaged/orphan writers.
    """
    ownership.require_active()
    try:
        validated = RecoveryReceiptRecord.model_validate(receipt)
    except ValueError:
        raise RecoveryReceiptIntegrityError(
            ownership,
            None,
            ownership.invocation.path / "recoveries",
            "invalid_expected_receipt",
        ) from None

    recovery_id = validated.recovery_id
    layout = _layout(ownership, recovery_id)
    request, intent = _parents(ownership, recovery_id)
    try:
        _bind(ownership, intent, request, validated)
        expected = encode_recovery_receipt(validated)
    except ValueError:
        raise RecoveryReceiptIntegrityError(
            ownership,
            recovery_id,
            layout.final,
            "receipt_binding_invalid",
        ) from None

    existing = None
    temporary_path = None
    if layout.final_exists:
        existing = _read(ownership, recovery_id, layout.final, request, intent)
        _require_exact(ownership, existing, expected)
    else:
        temporary_path = _candidate(
            ownership, recovery_id, layout, request, intent, expected
        )
    directories = _verified_bundle(ownership, recovery_id, intent)

    operation = (
        "acknowledge"
        if existing is not None
        else "resume"
        if temporary_path is not None
        else "publish"
    )
    progress = _Progress(
        operation,
        temporary_path=temporary_path,
        receipt_publication_uncertain=existing is not None,
    )
    try:
        _step(
            progress,
            "request-acknowledgment",
            requests.persist_owned_recovery_request,
            ownership,
            request,
        )
        _step(
            progress,
            "intent-file-flush",
            _sync_file,
            ownership.invocation.path / "publication-intent.json",
        )
        _step(
            progress,
            "bundle-payload-flush",
            lambda: bundles.flush_payloads(
                ownership.invocation.path / "published",
                intent.expected_manifest,
                marker=True,
            ),
        )
        _step(
            progress,
            "scheduler-receipt-flush",
            bundles.flush_scheduler_receipt,
            ownership,
            intent.expected_manifest,
        )
        _step(
            progress,
            "bundle-directory-flush",
            bundles.flush_directories,
            directories,
        )
        _step(
            progress,
            "bundle-parent-flush",
            _sync_directory,
            ownership.invocation.path,
        )
        _step(
            progress,
            "bundle-verification",
            _verified_bundle,
            ownership,
            recovery_id,
            intent,
        )
        progress.bundle_acknowledged = True

        if existing is None:
            progress.preparation_uncertain = True
            if temporary_path is None:
                _new_temporary(ownership, layout, expected, progress)
            else:
                _step(
                    progress,
                    "temporary-file-flush",
                    _sync_file,
                    temporary_path,
                )
            _step(
                progress,
                "temporary-parent-flush",
                _sync_directory,
                layout.unit,
            )
            _step(
                progress,
                "receipt-destination-check",
                requests._require_absent,
                layout.final,
            )
            progress.receipt_publication_uncertain = True
            _step(
                progress,
                "receipt-rename",
                os.rename,
                progress.temporary_path,
                layout.final,
            )

        _step(progress, "final-file-flush", _sync_file, layout.final)
        for phase, directory in (
            ("recovery-directory-flush", layout.unit),
            ("recoveries-directory-flush", layout.root),
            ("invocation-directory-flush", ownership.invocation.path),
        ):
            _step(progress, phase, _sync_directory, directory)
        handle = _step(
            progress,
            "final-verification",
            read_owned_recovery_receipt,
            ownership,
            recovery_id,
        )
        _require_exact(ownership, handle, expected)
        return handle
    except (OSError, ValueError, _ReceiptError, requests._RequestError) as error:
        raise RecoveryReceiptPublicationError(
            ownership, recovery_id, layout.final, progress, error
        ) from None

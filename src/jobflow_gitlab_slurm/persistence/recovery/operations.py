"""Finish intact expected bundle publication; never execute calculations."""

import os
import stat
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
    inspect_owned_bundle,
)
from jobflow_gitlab_slurm.persistence.bundles.records import (
    decode_bundle_commit,
    encode_bundle_commit,
    encode_bundle_manifest,
    validate_bundle_records,
)
from jobflow_gitlab_slurm.persistence.publication.records import PublicationIntentRecord
from jobflow_gitlab_slurm.persistence.recovery import receipts, requests
from jobflow_gitlab_slurm.persistence.recovery.receipts import RecoveryReceiptHandle
from jobflow_gitlab_slurm.persistence.recovery.records import (
    RecoveryReceiptRecord,
    RecoveryRequestRecord,
    encode_recovery_receipt,
    encode_recovery_request,
    validate_recovery_receipt,
)

_MARKER_PREFIX = ".bundle-commit-"
_FORWARD = {
    "staging_only": ("staging_only", "published_uncommitted", "committed"),
    "published_uncommitted": ("published_uncommitted", "committed"),
    "committed": ("committed",),
}
_read_bytes = requests._read_bytes
_HINT = (
    "Preserve all evidence and inspect/retry this same recovery identity "
    "and exact records. Do not overwrite records, delete temporary evidence, "
    "select another recovery ID, reconstruct missing results, release "
    "dependent jobs, or launch a calculation automatically. Recovery requires "
    "separate authorization and writer quiescence; filesystem completion "
    "does not establish scheduler or scientific success."
)


@dataclass(frozen=True)
class _Evidence:
    status: str
    unit: Path
    directories: tuple[Path, ...]
    temporary_path: Path | None
    audit_present: bool


@dataclass
class _Progress:
    phase: str = "request-acknowledgment"
    temporary_path: Path | None = None
    request_acknowledged: bool = False
    bundle_publication_uncertain: bool = False
    audit_completion_uncertain: bool = False


class _RecoveryError(receipts._ReceiptError):
    def __init__(
        self,
        ownership: InvocationOwnership,
        recovery_id: str | None,
        path: Path,
        code: str,
        **details: Any,
    ) -> None:
        super().__init__(ownership, recovery_id, path, code, **details)
        self.args = (
            (
                f"Bundle recovery failure: code={code}; run_id={self.run_id}; "
                f"job_key={self.job_key}; job_index={self.job_index}; "
                f"attempt_id={self.attempt_id}; invocation_id={self.invocation_id}; "
                f"recovery_id={recovery_id}; path={path}; errno={self.errno}. "
                f"{_HINT}"
            ),
        )

    def to_report(self) -> dict[str, Any]:
        """Return detached findings without record bodies or raw causes."""
        report = super().to_report()
        report["kind"] = "bundle-recovery-error"
        report["hint"] = _HINT
        report["staging_path"] = str(self.bundle_path.parent / "staging")
        report["published_path"] = str(self.bundle_path)
        return report


class BundleRecoveryIntegrityError(_RecoveryError):
    """Invalid, incomplete, unsafe, ambiguous, or regressed evidence is held."""


class BundleRecoveryConflictError(_RecoveryError):
    """Intact evidence differs from the retained exact expected records."""


class BundleRecoveryInspectionError(_RecoveryError):
    """I/O did not establish the available evidence."""


class BundleRecoveryCompletionError(_RecoveryError):
    """Recovery or acknowledgment failed; mutations may already have occurred."""

    def __init__(
        self,
        ownership: InvocationOwnership,
        recovery_id: str,
        progress: _Progress,
        error: Exception,
    ) -> None:
        super().__init__(
            ownership,
            recovery_id,
            ownership.invocation.path,
            "recovery_completion_failed",
            errno=getattr(error, "errno", None),
            issues=getattr(error, "issues", ()),
        )
        self.phase = progress.phase
        self.temporary_path = progress.temporary_path
        self.request_acknowledged = progress.request_acknowledged
        self.request_publication_uncertain = getattr(
            error, "request_publication_uncertain", False
        )
        self.bundle_publication_uncertain = progress.bundle_publication_uncertain
        self.audit_completion_uncertain = progress.audit_completion_uncertain
        self.reason = getattr(error, "code", "operation_failed")
        self.inner_phase = getattr(error, "phase", None)
        self.args = (
            (
                f"{self.args[0]} phase={self.phase}; "
                f"request_acknowledged={self.request_acknowledged}; "
                f"request_publication_uncertain={self.request_publication_uncertain}; "
                f"bundle_publication_uncertain={self.bundle_publication_uncertain}; "
                f"audit_completion_uncertain={self.audit_completion_uncertain}; "
                f"reason={self.reason}; inner_phase={self.inner_phase}."
            ),
        )

    def to_report(self) -> dict[str, Any]:
        """Distinguish request, bundle, and audit-completion boundaries."""
        report = super().to_report()
        report.update(
            {
                "phase": self.phase,
                "temporary_path": (
                    None if self.temporary_path is None else str(self.temporary_path)
                ),
                "request_acknowledged": self.request_acknowledged,
                "request_publication_uncertain": self.request_publication_uncertain,
                "bundle_publication_uncertain": self.bundle_publication_uncertain,
                "audit_completion_uncertain": self.audit_completion_uncertain,
                "reason": self.reason,
                "inner_phase": self.inner_phase,
            }
        )
        return report


def _translate(
    ownership: InvocationOwnership,
    recovery_id: str,
    error: requests._RequestError | receipts._ReceiptError,
) -> _RecoveryError:
    if isinstance(
        error,
        (
            requests.RecoveryRequestConflictError,
            receipts.RecoveryReceiptConflictError,
        ),
    ):
        error_type = BundleRecoveryConflictError
    elif isinstance(
        error,
        (
            requests.RecoveryRequestInspectionError,
            receipts.RecoveryReceiptInspectionError,
        ),
    ):
        error_type = BundleRecoveryInspectionError
    else:
        error_type = BundleRecoveryIntegrityError
    return error_type(
        ownership,
        recovery_id,
        error.path,
        error.code,
        errno=error.errno,
        temporary_paths=error.temporary_paths,
        existing_ids=error.existing_recovery_ids,
        issues=error.issues,
    )


def _guard[T](
    ownership: InvocationOwnership,
    recovery_id: str,
    operation: Callable[..., T],
    *args: Any,
) -> T:
    try:
        return operation(*args)
    except _RecoveryError:
        raise
    except (requests._RequestError, receipts._ReceiptError) as error:
        raise _translate(ownership, recovery_id, error) from None
    except (OSError, BundleInspectionError) as error:
        raise BundleRecoveryInspectionError(
            ownership,
            recovery_id,
            ownership.invocation.path,
            "recovery_evidence_unavailable",
            errno=error.errno,
        ) from None
    except (ValueError, bundles._BundleError) as error:
        raise BundleRecoveryIntegrityError(
            ownership,
            recovery_id,
            ownership.invocation.path,
            "invalid_recovery_evidence",
            issues=getattr(error, "issues", ()),
        ) from None


def _validated(
    ownership: InvocationOwnership,
    request: RecoveryRequestRecord,
    receipt: RecoveryReceiptRecord,
) -> tuple[RecoveryRequestRecord, RecoveryReceiptRecord, PublicationIntentRecord]:
    try:
        validated_request = RecoveryRequestRecord.model_validate(request)
        validated_receipt = RecoveryReceiptRecord.model_validate(receipt)
    except ValueError:
        raise BundleRecoveryIntegrityError(
            ownership,
            None,
            ownership.invocation.path,
            "invalid_expected_recovery_records",
        ) from None

    intent = _guard(
        ownership,
        validated_request.recovery_id,
        requests._intent,
        ownership,
        validated_request.recovery_id,
    )
    invocation = ownership.invocation
    try:
        validate_recovery_receipt(
            invocation.attempt.definition.record,
            invocation.attempt.record,
            invocation.record,
            intent,
            validated_request,
            receipt=validated_receipt,
        )
    except ValueError:
        raise BundleRecoveryIntegrityError(
            ownership,
            validated_request.recovery_id,
            ownership.invocation.path,
            "recovery_binding_invalid",
        ) from None
    return validated_request, validated_receipt, intent


def _audit(
    ownership: InvocationOwnership,
    request: RecoveryRequestRecord,
    receipt: RecoveryReceiptRecord,
    intent: PublicationIntentRecord,
) -> bool:
    recovery_id = request.recovery_id
    layout = requests._layout(ownership, recovery_id)
    expected_request = encode_recovery_request(request)
    if layout.final_exists:
        handle = requests._read(ownership, recovery_id, layout.final, intent)
        requests._require_exact(ownership, handle, expected_request)
    else:
        requests._candidate(
            ownership,
            recovery_id,
            layout,
            intent,
            expected_request,
        )

    audit = receipts._layout(ownership, recovery_id)
    expected_receipt = encode_recovery_receipt(receipt)
    if audit.final_exists:
        handle = receipts._read(ownership, recovery_id, audit.final, request, intent)
        receipts._require_exact(ownership, handle, expected_receipt)
    else:
        receipts._candidate(
            ownership,
            recovery_id,
            audit,
            request,
            intent,
            expected_receipt,
        )
    return audit.final_exists or bool(audit.temporary_paths)


def _marker_candidate(
    ownership: InvocationOwnership,
    request: RecoveryRequestRecord,
    intent: PublicationIntentRecord,
    status: str,
) -> Path | None:
    invocation = ownership.invocation
    device = invocation.path.lstat().st_dev
    candidates = []
    for path in sorted(invocation.path.iterdir()):
        if not path.name.startswith(_MARKER_PREFIX):
            continue
        metadata = path.lstat()
        if (
            len(path.name) == len(_MARKER_PREFIX)
            or not stat.S_ISREG(metadata.st_mode)
            or metadata.st_dev != device
        ):
            raise BundleRecoveryIntegrityError(
                ownership,
                request.recovery_id,
                path,
                "unsafe_temporary_marker",
            )
        candidates.append(path)

    if status == "committed":
        return None
    if len(candidates) > 1:
        raise BundleRecoveryIntegrityError(
            ownership,
            request.recovery_id,
            invocation.path,
            "ambiguous_temporary_markers",
            temporary_paths=tuple(candidates),
        )
    if not candidates:
        return None

    path = candidates[0]
    data = _read_bytes(path)
    try:
        marker = decode_bundle_commit(data)
        validate_bundle_records(
            invocation.attempt.definition.record,
            invocation.attempt.record,
            invocation.record,
            intent.expected_manifest,
            marker,
        )
    except ValueError:
        raise BundleRecoveryIntegrityError(
            ownership,
            request.recovery_id,
            path,
            "invalid_temporary_marker",
        ) from None
    if data != encode_bundle_commit(intent.expected_commit):
        raise BundleRecoveryConflictError(
            ownership,
            request.recovery_id,
            path,
            "expected_temporary_marker_conflict",
        )
    return path


def _evidence(
    ownership: InvocationOwnership,
    request: RecoveryRequestRecord,
    intent: PublicationIntentRecord,
    audit_present: bool,
) -> _Evidence:
    report = inspect_owned_bundle(ownership)
    if (
        report.status not in _FORWARD[request.observed_status]
        or report.content_integrity != "valid"
    ):
        raise BundleRecoveryIntegrityError(
            ownership,
            request.recovery_id,
            ownership.invocation.path,
            "bundle_not_valid_forward_progress",
            issues=report.issues,
        )
    if audit_present and report.status != "committed":
        raise BundleRecoveryIntegrityError(
            ownership,
            request.recovery_id,
            ownership.invocation.path,
            "audit_completion_with_uncommitted_bundle",
        )

    unit = ownership.invocation.path / (
        "staging" if report.status == "staging_only" else "published"
    )
    if _read_bytes(unit / "payload-manifest.json") != encode_bundle_manifest(
        intent.expected_manifest
    ):
        raise BundleRecoveryConflictError(
            ownership,
            request.recovery_id,
            unit / "payload-manifest.json",
            "expected_manifest_conflict",
        )
    if report.status == "committed" and (
        _read_bytes(unit / "COMMIT.json")
        != encode_bundle_commit(intent.expected_commit)
    ):
        raise BundleRecoveryConflictError(
            ownership,
            request.recovery_id,
            unit / "COMMIT.json",
            "expected_commit_conflict",
        )
    directories = bundles.bundle_directories(unit, intent.expected_manifest)
    bundles.require_same_filesystem(ownership, directories)
    candidate = _marker_candidate(ownership, request, intent, report.status)
    return _Evidence(
        report.status,
        unit,
        directories,
        candidate,
        audit_present,
    )


def _preflight(
    ownership: InvocationOwnership,
    request: RecoveryRequestRecord,
    receipt: RecoveryReceiptRecord,
    intent: PublicationIntentRecord,
) -> _Evidence:
    audit_present = _guard(
        ownership, request.recovery_id, _audit, ownership, request, receipt, intent
    )
    return _guard(
        ownership,
        request.recovery_id,
        _evidence,
        ownership,
        request,
        intent,
        audit_present,
    )


def _step[T](
    progress: _Progress,
    phase: str,
    operation: Callable[..., T],
    *args: Any,
) -> T:
    progress.phase = phase
    return operation(*args)


def _flush_unit(
    ownership: InvocationOwnership,
    intent: PublicationIntentRecord,
    evidence: _Evidence,
    progress: _Progress,
    prefix: str,
) -> None:
    _step(
        progress,
        f"{prefix}-payload-flush",
        lambda: bundles.flush_payloads(
            evidence.unit, intent.expected_manifest, marker=False
        ),
    )
    _step(
        progress,
        f"{prefix}-scheduler-receipt-flush",
        bundles.flush_scheduler_receipt,
        ownership,
        intent.expected_manifest,
    )
    _step(
        progress,
        f"{prefix}-directory-flush",
        bundles.flush_directories,
        evidence.directories,
    )
    _step(
        progress,
        f"{prefix}-parent-flush",
        _sync_directory,
        ownership.invocation.path,
    )


def _new_marker(
    ownership: InvocationOwnership,
    expected: bytes,
    progress: _Progress,
) -> None:
    descriptor, name = _step(
        progress,
        "marker-create",
        lambda: tempfile.mkstemp(prefix=_MARKER_PREFIX, dir=ownership.invocation.path),
    )
    progress.temporary_path = Path(name)
    try:
        _step(
            progress,
            "marker-check",
            requests._check_temporary,
            ownership,
            descriptor,
        )
        _step(progress, "marker-write", requests._write_bytes, descriptor, expected)
        _step(progress, "marker-file-flush", os.fsync, descriptor)
    finally:
        os.close(descriptor)


def _require_candidate(
    ownership: InvocationOwnership,
    request: RecoveryRequestRecord,
    intent: PublicationIntentRecord,
    selected: Path,
) -> None:
    candidate = _marker_candidate(ownership, request, intent, "published_uncommitted")
    if candidate != selected:
        raise BundleRecoveryIntegrityError(
            ownership,
            request.recovery_id,
            selected,
            "temporary_marker_selection_changed",
        )


def _publish_marker(
    ownership: InvocationOwnership,
    request: RecoveryRequestRecord,
    intent: PublicationIntentRecord,
    evidence: _Evidence,
    progress: _Progress,
) -> None:
    _flush_unit(ownership, intent, evidence, progress, "published")
    if evidence.temporary_path is None:
        _new_marker(ownership, encode_bundle_commit(intent.expected_commit), progress)
    else:
        progress.temporary_path = evidence.temporary_path
        _step(
            progress,
            "marker-file-flush",
            _sync_file,
            progress.temporary_path,
        )
    _step(
        progress,
        "marker-parent-flush",
        _sync_directory,
        ownership.invocation.path,
    )
    _step(
        progress,
        "marker-ready-check",
        _require_candidate,
        ownership,
        request,
        intent,
        progress.temporary_path,
    )
    final = ownership.invocation.path / "published" / "COMMIT.json"
    _step(
        progress,
        "marker-destination-check",
        requests._require_absent,
        final,
    )
    progress.bundle_publication_uncertain = True
    _step(progress, "marker-rename", os.rename, progress.temporary_path, final)
    _step(progress, "marker-final-file-flush", _sync_file, final)
    _step(
        progress,
        "marker-final-directory-flush",
        _sync_directory,
        final.parent,
    )
    _step(
        progress,
        "marker-final-parent-flush",
        _sync_directory,
        ownership.invocation.path,
    )


def recover_owned_bundle(
    ownership: InvocationOwnership,
    request: RecoveryRequestRecord,
    *,
    receipt: RecoveryReceiptRecord,
) -> RecoveryReceiptHandle:
    """Finish exact intact publication under separately authorized ownership.

    The caller must establish producer/orphan-writer quiescence. This API
    does not query Slurm, authenticate actor labels, reconstruct missing
    results, run calculations, mutate the journal, or release descendants.
    Retain the exact request and intended receipt across all retries.
    """
    ownership.require_active()
    request, receipt, intent = _validated(ownership, request, receipt)
    evidence = _preflight(ownership, request, receipt, intent)
    progress = _Progress(
        temporary_path=evidence.temporary_path,
        bundle_publication_uncertain=evidence.status != "staging_only",
        audit_completion_uncertain=evidence.audit_present,
    )
    try:
        _step(
            progress,
            "request-acknowledgment",
            requests.persist_owned_recovery_request,
            ownership,
            request,
        )
        progress.request_acknowledged = True
        _step(
            progress,
            "intent-file-flush",
            _sync_file,
            ownership.invocation.path / "publication-intent.json",
        )
        evidence = _step(
            progress,
            "recovery-reinspection",
            _preflight,
            ownership,
            request,
            receipt,
            intent,
        )

        if evidence.status == "staging_only":
            _flush_unit(ownership, intent, evidence, progress, "staging")
            published = ownership.invocation.path / "published"
            _step(
                progress,
                "published-destination-check",
                requests._require_absent,
                published,
            )
            progress.bundle_publication_uncertain = True
            _step(
                progress,
                "directory-rename",
                os.rename,
                evidence.unit,
                published,
            )
            _step(
                progress,
                "renamed-directory-flush",
                _sync_directory,
                published,
            )
            _step(
                progress,
                "rename-parent-flush",
                _sync_directory,
                ownership.invocation.path,
            )
            evidence = _step(
                progress,
                "published-reinspection",
                _preflight,
                ownership,
                request,
                receipt,
                intent,
            )

        if evidence.status == "published_uncommitted":
            _publish_marker(ownership, request, intent, evidence, progress)

        _step(
            progress,
            "committed-verification",
            receipts._verified_bundle,
            ownership,
            request.recovery_id,
            intent,
        )
        _step(
            progress,
            "marker-evidence-verification",
            _marker_candidate,
            ownership,
            request,
            intent,
            "committed",
        )
        progress.audit_completion_uncertain = True
        return _step(
            progress,
            "receipt-completion",
            receipts.persist_owned_recovery_receipt,
            ownership,
            receipt,
        )
    except (
        OSError,
        ValueError,
        requests._RequestError,
        receipts._ReceiptError,
        bundles._BundleError,
        BundleInspectionError,
    ) as error:
        raise BundleRecoveryCompletionError(
            ownership, request.recovery_id, progress, error
        ) from None

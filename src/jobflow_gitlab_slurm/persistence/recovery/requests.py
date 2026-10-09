"""Owned immutable recovery-request storage; no bundle mutation."""

import hashlib
import os
import stat
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict

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
from jobflow_gitlab_slurm.persistence.publication.storage import (
    IntentInspectionError,
    IntentIntegrityError,
    IntentMissingError,
    read_owned_publication_intent,
)
from jobflow_gitlab_slurm.persistence.recovery.records import (
    RecoveryRequestRecord,
    decode_recovery_request,
    encode_recovery_request,
    validate_recovery_request,
)
from jobflow_gitlab_slurm.persistence.runs.records import RunId

_TEMPORARY_PREFIX = ".recovery-request-"
_RECEIPT_TEMPORARY_PREFIX = ".recovery-receipt-"
_IDENTITY_FIELDS = (
    "run_id",
    "job_uuid",
    "job_index",
    "job_key",
    "attempt_id",
    "invocation_id",
)
_HINT = (
    "Preserve evidence and inspect this same recovery identity. "
    "Do not replace records, delete temporary evidence, select another "
    "recovery ID, mutate a bundle, or launch a calculation automatically. "
    "A readable request is not permission to perform recovery."
)


class _Selection(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    recovery_id: RunId


@dataclass(frozen=True)
class RecoveryRequestHandle:
    """Validated observation; not ownership or recovery authorization."""

    path: Path
    record: RecoveryRequestRecord
    sha256: str
    size_bytes: int


@dataclass(frozen=True)
class _Layout:
    root: Path
    unit: Path
    final: Path
    final_exists: bool = False
    temporary_paths: tuple[Path, ...] = ()


@dataclass
class _Progress:
    operation: str
    phase: str = "preparation"
    temporary_path: Path | None = None
    preparation_uncertain: bool = False
    request_publication_uncertain: bool = False


class _RequestError(RuntimeError):
    def __init__(
        self,
        invocation: InvocationRecord,
        recovery_id: str | None,
        path: Path,
        code: str,
        *,
        errno: int | None = None,
        existing_ids: tuple[str, ...] = (),
        temporary_paths: tuple[Path, ...] = (),
        issues: tuple[BundleIssue, ...] = (),
    ) -> None:
        self.invocation = invocation
        self.recovery_id = recovery_id
        self.path = path
        self.code = code
        self.errno = errno
        self.existing_recovery_ids = existing_ids
        self.temporary_paths = temporary_paths
        self.issues = issues
        for field in _IDENTITY_FIELDS:
            setattr(self, field, getattr(invocation, field))
        super().__init__(
            f"Recovery-request storage failure: code={code}; "
            f"run_id={self.run_id}; job_key={self.job_key}; "
            f"job_index={self.job_index}; attempt_id={self.attempt_id}; "
            f"invocation_id={self.invocation_id}; recovery_id={recovery_id}; "
            f"path={path}; errno={errno}. {_HINT}"
        )

    def to_report(self) -> dict[str, Any]:
        """Return detached diagnostics without record bodies or raw causes."""
        return {
            "schema_version": 1,
            "kind": "recovery-request-storage-error",
            "error_type": type(self).__name__,
            "identity": {
                field: getattr(self.invocation, field) for field in _IDENTITY_FIELDS
            },
            "recovery_id": self.recovery_id,
            "path": str(self.path),
            "code": self.code,
            "errno": self.errno,
            "existing_recovery_ids": list(self.existing_recovery_ids),
            "temporary_paths": [str(path) for path in self.temporary_paths],
            "issues": [issue.to_report() for issue in self.issues],
            "hint": _HINT,
        }


class RecoveryRequestMissingError(_RequestError):
    """No final request; this does not establish absence of execution."""


class RecoveryRequestIntegrityError(_RequestError):
    """Unsafe, malformed, ambiguous, or incorrectly bound evidence."""


class RecoveryRequestConflictError(_RequestError):
    """A retained identity or intact record conflicts with the request."""


class RecoveryRequestInspectionError(_RequestError):
    """I/O did not establish the available evidence."""


class RecoveryRequestPublicationError(_RequestError):
    """Publication or durable acknowledgment was not completed."""

    def __init__(
        self,
        invocation: InvocationRecord,
        recovery_id: str,
        path: Path,
        progress: _Progress,
        error: Exception,
    ) -> None:
        super().__init__(
            invocation,
            recovery_id,
            path,
            "request_publication_failed",
            errno=getattr(error, "errno", None),
        )
        self.operation = progress.operation
        self.phase = progress.phase
        self.temporary_path = progress.temporary_path
        self.preparation_uncertain = progress.preparation_uncertain
        self.request_publication_uncertain = progress.request_publication_uncertain
        self.reason = getattr(error, "code", "operation_failed")
        self.args = (
            (
                f"{self.args[0]} operation={self.operation}; phase={self.phase}; "
                f"preparation_uncertain={self.preparation_uncertain}; "
                f"request_publication_uncertain="
                f"{self.request_publication_uncertain}; reason={self.reason}."
            ),
        )

    def to_report(self) -> dict[str, Any]:
        """Include phase-specific uncertainty without implying bundle mutation."""
        report = super().to_report()
        report.update(
            {
                "operation": self.operation,
                "phase": self.phase,
                "temporary_path": (
                    None if self.temporary_path is None else str(self.temporary_path)
                ),
                "preparation_uncertain": self.preparation_uncertain,
                "request_publication_uncertain": self.request_publication_uncertain,
                "reason": self.reason,
            }
        )
        return report


def _absent(path: Path) -> bool:
    try:
        path.lstat()
    except FileNotFoundError:
        return True
    return False


def _directory_on_device(path: Path, device: int) -> None:
    metadata = path.lstat()
    if not stat.S_ISDIR(metadata.st_mode):
        raise ValueError("audit path must be a real directory")
    if metadata.st_dev != device:
        raise ValueError("audit path crosses filesystems")


def _scan(
    ownership: InvocationOwnership,
    recovery_id: str,
) -> _Layout:
    invocation = ownership.invocation
    device = invocation.path.lstat().st_dev
    _directory_on_device(invocation.path, device)
    root = invocation.path / "recoveries"
    unit = root / recovery_id
    layout = _Layout(root, unit, unit / "request.json")

    if _absent(root):
        return layout
    _directory_on_device(root, device)

    children = tuple(sorted(root.iterdir()))
    for child in children:
        _Selection(recovery_id=child.name)
        _directory_on_device(child, device)

    identifiers = tuple(child.name for child in children)
    if len(children) > 1:
        raise RecoveryRequestIntegrityError(
            invocation.record,
            recovery_id,
            root,
            "ambiguous_recovery_ids",
            existing_ids=identifiers,
        )
    if children and identifiers != (recovery_id,):
        raise RecoveryRequestConflictError(
            invocation.record,
            recovery_id,
            children[0],
            "recovery_id_reserved",
            existing_ids=identifiers,
        )

    final_exists = False
    receipt_evidence = False
    temporary_paths = []
    if children:
        for path in sorted(unit.iterdir()):
            if not stat.S_ISREG(path.lstat().st_mode):
                raise ValueError("audit entry must be a regular file")
            if path.name == "request.json":
                final_exists = True
            elif path.name.startswith(_TEMPORARY_PREFIX) and (
                len(path.name) > len(_TEMPORARY_PREFIX)
            ):
                temporary_paths.append(path)
            elif path.name == "receipt.json" or (
                path.name.startswith(_RECEIPT_TEMPORARY_PREFIX)
                and len(path.name) > len(_RECEIPT_TEMPORARY_PREFIX)
            ):
                receipt_evidence = True
            else:
                raise ValueError("unrecognized audit entry")
    if receipt_evidence and not final_exists:
        raise RecoveryRequestIntegrityError(
            invocation.record,
            recovery_id,
            unit,
            "receipt_without_final_request",
        )

    return _Layout(root, unit, layout.final, final_exists, tuple(temporary_paths))


def _layout(
    ownership: InvocationOwnership,
    recovery_id: str,
) -> _Layout:
    path = ownership.invocation.path / "recoveries"
    try:
        return _scan(ownership, recovery_id)
    except OSError as error:
        raise RecoveryRequestInspectionError(
            ownership.invocation.record,
            recovery_id,
            path,
            "audit_namespace_unavailable",
            errno=error.errno,
        ) from None
    except ValueError:
        raise RecoveryRequestIntegrityError(
            ownership.invocation.record,
            recovery_id,
            path,
            "invalid_audit_namespace",
        ) from None


def _intent(
    ownership: InvocationOwnership,
    recovery_id: str,
) -> PublicationIntentRecord:
    path = ownership.invocation.path / "publication-intent.json"
    try:
        return read_owned_publication_intent(ownership).record
    except IntentInspectionError as error:
        raise RecoveryRequestInspectionError(
            ownership.invocation.record,
            recovery_id,
            path,
            "retained_intent_unavailable",
            errno=error.errno,
        ) from None
    except (IntentMissingError, IntentIntegrityError):
        raise RecoveryRequestIntegrityError(
            ownership.invocation.record,
            recovery_id,
            path,
            "invalid_retained_intent",
        ) from None


def _bind(
    ownership: InvocationOwnership,
    intent: PublicationIntentRecord,
    request: RecoveryRequestRecord,
) -> None:
    invocation = ownership.invocation
    validate_recovery_request(
        invocation.attempt.definition.record,
        invocation.attempt.record,
        invocation.record,
        intent,
        request,
    )


def _read_bytes(path: Path) -> bytes:
    with (
        _regular_descriptor(path) as descriptor,
        os.fdopen(descriptor, "rb", closefd=False) as source,
    ):
        return source.read()


def _read(
    ownership: InvocationOwnership,
    recovery_id: str,
    path: Path,
    intent: PublicationIntentRecord,
) -> RecoveryRequestHandle:
    try:
        data = _read_bytes(path)
        record = decode_recovery_request(data)
        _bind(ownership, intent, record)
        if record.recovery_id != recovery_id:
            raise ValueError("stored recovery ID does not match its directory")
    except OSError as error:
        raise RecoveryRequestInspectionError(
            ownership.invocation.record,
            recovery_id,
            path,
            "request_unavailable",
            errno=error.errno,
        ) from None
    except ValueError:
        raise RecoveryRequestIntegrityError(
            ownership.invocation.record,
            recovery_id,
            path,
            "invalid_stored_request",
        ) from None

    return RecoveryRequestHandle(
        path,
        record,
        hashlib.sha256(data).hexdigest(),
        len(data),
    )


def _require_exact(
    ownership: InvocationOwnership,
    handle: RecoveryRequestHandle,
    expected: bytes,
) -> None:
    if encode_recovery_request(handle.record) != expected:
        raise RecoveryRequestConflictError(
            ownership.invocation.record,
            handle.record.recovery_id,
            handle.path,
            "expected_request_conflict",
        )


def _preflight(
    ownership: InvocationOwnership,
    request: RecoveryRequestRecord,
    intent: PublicationIntentRecord,
) -> None:
    invocation = ownership.invocation
    try:
        report = inspect_owned_bundle(ownership)
        if (
            report.status != request.observed_status
            or report.content_integrity != "valid"
        ):
            raise RecoveryRequestIntegrityError(
                invocation.record,
                request.recovery_id,
                invocation.path,
                "bundle_observation_mismatch",
                issues=report.issues,
            )

        unit = invocation.path / (
            "staging" if report.status == "staging_only" else "published"
        )
        manifest_path = unit / "payload-manifest.json"
        if _read_bytes(manifest_path) != encode_bundle_manifest(
            intent.expected_manifest
        ):
            raise RecoveryRequestConflictError(
                invocation.record,
                request.recovery_id,
                manifest_path,
                "expected_manifest_conflict",
            )

        if report.status == "committed":
            commit_path = unit / "COMMIT.json"
            if _read_bytes(commit_path) != encode_bundle_commit(intent.expected_commit):
                raise RecoveryRequestConflictError(
                    invocation.record,
                    request.recovery_id,
                    commit_path,
                    "expected_commit_conflict",
                )
    except (OSError, BundleInspectionError) as error:
        raise RecoveryRequestInspectionError(
            invocation.record,
            request.recovery_id,
            invocation.path,
            "bundle_preflight_unavailable",
            errno=error.errno,
        ) from None


def _candidate(
    ownership: InvocationOwnership,
    recovery_id: str,
    layout: _Layout,
    intent: PublicationIntentRecord,
    expected: bytes,
) -> Path | None:
    if len(layout.temporary_paths) > 1:
        raise RecoveryRequestIntegrityError(
            ownership.invocation.record,
            recovery_id,
            layout.unit,
            "ambiguous_temporary_requests",
            temporary_paths=layout.temporary_paths,
        )
    if not layout.temporary_paths:
        return None
    path = layout.temporary_paths[0]
    handle = _read(ownership, recovery_id, path, intent)
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


def _ensure_directory(path: Path, device: int) -> None:
    try:
        path.mkdir(mode=0o700)
    except FileExistsError:
        pass
    _directory_on_device(path, device)


def _check_temporary(
    ownership: InvocationOwnership,
    descriptor: int,
) -> None:
    metadata = os.fstat(descriptor)
    if not stat.S_ISREG(metadata.st_mode):
        raise ValueError("temporary request must be a regular file")
    if metadata.st_dev != ownership.invocation.path.lstat().st_dev:
        raise ValueError("temporary request crosses filesystems")


def _write_bytes(descriptor: int, data: bytes) -> None:
    with os.fdopen(descriptor, "wb", closefd=False) as output:
        output.write(data)
        output.flush()


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
            _check_temporary,
            ownership,
            descriptor,
        )
        _step(progress, "temporary-write", _write_bytes, descriptor, expected)
        _step(progress, "temporary-file-flush", os.fsync, descriptor)
    finally:
        os.close(descriptor)


def _require_absent(path: Path) -> None:
    if not _absent(path):
        raise ValueError("request destination already exists")


def read_owned_recovery_request(
    ownership: InvocationOwnership,
    recovery_id: str,
) -> RecoveryRequestHandle:
    """Read final metadata without writes, flushing, or bundle inspection."""
    ownership.require_active()
    selected = _Selection(recovery_id=recovery_id).recovery_id
    layout = _layout(ownership, selected)
    if not layout.final_exists:
        raise RecoveryRequestMissingError(
            ownership.invocation.record,
            selected,
            layout.final,
            "request_missing",
            temporary_paths=layout.temporary_paths,
        )
    return _read(
        ownership,
        selected,
        layout.final,
        _intent(ownership, selected),
    )


def persist_owned_recovery_request(
    ownership: InvocationOwnership,
    request: RecoveryRequestRecord,
) -> RecoveryRequestHandle:
    """Publish/reacknowledge one exact request without changing bundle evidence."""
    ownership.require_active()
    invocation = ownership.invocation
    try:
        validated = RecoveryRequestRecord.model_validate(request)
    except ValueError:
        raise RecoveryRequestIntegrityError(
            invocation.record,
            None,
            invocation.path / "recoveries",
            "invalid_expected_request",
        ) from None

    recovery_id = validated.recovery_id
    intent = _intent(ownership, recovery_id)
    try:
        _bind(ownership, intent, validated)
        expected = encode_recovery_request(validated)
    except ValueError:
        raise RecoveryRequestIntegrityError(
            invocation.record,
            recovery_id,
            invocation.path / "recoveries" / recovery_id / "request.json",
            "request_binding_invalid",
        ) from None

    layout = _layout(ownership, recovery_id)
    existing = None
    temporary_path = None
    if layout.final_exists:
        existing = _read(ownership, recovery_id, layout.final, intent)
        _require_exact(ownership, existing, expected)
    else:
        _preflight(ownership, validated, intent)
        temporary_path = _candidate(
            ownership,
            recovery_id,
            layout,
            intent,
            expected,
        )

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
        request_publication_uncertain=existing is not None,
    )
    try:
        if existing is None:
            progress.preparation_uncertain = True
            device = invocation.path.lstat().st_dev
            _step(
                progress,
                "recoveries-directory-create",
                _ensure_directory,
                layout.root,
                device,
            )
            _step(
                progress,
                "recovery-directory-create",
                _ensure_directory,
                layout.unit,
                device,
            )
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
                "request-destination-check",
                _require_absent,
                layout.final,
            )
            progress.request_publication_uncertain = True
            _step(
                progress,
                "request-rename",
                os.rename,
                progress.temporary_path,
                layout.final,
            )

        _step(progress, "final-file-flush", _sync_file, layout.final)
        for phase, directory in (
            ("recovery-directory-flush", layout.unit),
            ("recoveries-directory-flush", layout.root),
            ("invocation-directory-flush", invocation.path),
        ):
            _step(progress, phase, _sync_directory, directory)

        handle = _step(
            progress,
            "final-verification",
            read_owned_recovery_request,
            ownership,
            recovery_id,
        )
        _require_exact(ownership, handle, expected)
        return handle
    except (OSError, ValueError, _RequestError) as error:
        raise RecoveryRequestPublicationError(
            invocation.record,
            recovery_id,
            layout.final,
            progress,
            error,
        ) from None

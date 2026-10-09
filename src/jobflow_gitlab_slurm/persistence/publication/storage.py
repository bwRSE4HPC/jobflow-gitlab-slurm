"""Immutable publication-intent storage; no bundle mutation or recovery."""

import hashlib
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
from jobflow_gitlab_slurm.persistence.publication.records import (
    PublicationIntentRecord,
    decode_publication_intent,
    encode_publication_intent,
    validate_publication_intent,
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
class PublicationIntentHandle:
    """Validated metadata observation; not ownership or recovery authority."""

    path: Path
    record: PublicationIntentRecord
    sha256: str
    size_bytes: int


class _IntentError(RuntimeError):
    def __init__(
        self,
        invocation: InvocationRecord,
        path: Path,
        code: str,
        errno: int | None = None,
    ) -> None:
        self.invocation = invocation
        self.path = path
        self.code = code
        self.errno = errno
        self.run_id = invocation.run_id
        self.job_key = invocation.job_key
        self.job_index = invocation.job_index
        self.attempt_id = invocation.attempt_id
        self.invocation_id = invocation.invocation_id
        super().__init__(
            f"Publication-intent storage failure: code={code}; "
            f"run_id={self.run_id}; job_key={self.job_key}; "
            f"job_index={self.job_index}; attempt_id={self.attempt_id}; "
            f"invocation_id={self.invocation_id}; errno={errno}. "
            "Preserve evidence and inspect the same identity. "
            "Do not replace metadata, clean up, finalize a bundle, "
            "or launch a calculation automatically."
        )

    def to_report(self) -> dict[str, Any]:
        """Return detached diagnostics without metadata bodies or causes."""
        return {
            "schema_version": 1,
            "kind": "publication-intent-storage-error",
            "error_type": type(self).__name__,
            "identity": {
                field: getattr(self.invocation, field) for field in _IDENTITY_FIELDS
            },
            "path": str(self.path),
            "code": self.code,
            "errno": self.errno,
            "hint": (
                "Preserve evidence and inspect this same identity. "
                "Missing intent does not prove that execution never occurred. "
                "Recovery or a new calculation needs separate authorization."
            ),
        }


class IntentMissingError(_IntentError):
    """No final intent is available; this says nothing about execution."""


class IntentIntegrityError(_IntentError):
    """Unsafe, malformed, incompatible, or incorrectly bound evidence."""


class IntentConflictError(_IntentError):
    """Valid stored metadata differs from the retained expected record."""


class IntentInspectionError(_IntentError):
    """Read-only/preflight I/O could not establish the available evidence."""


class IntentPublicationError(_IntentError):
    """Intent publication or durable acknowledgment could not complete."""

    def __init__(
        self,
        invocation: InvocationRecord,
        path: Path,
        *,
        operation: str,
        phase: str,
        temporary_path: Path | None,
        intent_publication_uncertain: bool,
        reason: str,
        errno: int | None,
    ) -> None:
        super().__init__(invocation, path, "intent_publication_failed", errno)
        self.operation = operation
        self.phase = phase
        self.temporary_path = temporary_path
        self.intent_publication_uncertain = intent_publication_uncertain
        self.reason = reason
        self.args = (
            (
                f"{self.args[0]} operation={operation}; phase={phase}; "
                f"intent_publication_uncertain={intent_publication_uncertain}; "
                f"reason={reason}."
            ),
        )

    def to_report(self) -> dict[str, Any]:
        """Include intent-specific publication phases and uncertainty."""
        report = super().to_report()
        report.update(
            {
                "operation": self.operation,
                "phase": self.phase,
                "temporary_path": (
                    None if self.temporary_path is None else str(self.temporary_path)
                ),
                "intent_publication_uncertain": (self.intent_publication_uncertain),
                "reason": self.reason,
            }
        )
        return report


def _location(ownership: InvocationOwnership) -> Path:
    ownership.require_active()
    return ownership.invocation.path / "publication-intent.json"


def _require_directory(ownership: InvocationOwnership, path: Path) -> None:
    invocation = ownership.invocation
    try:
        if not stat.S_ISDIR(invocation.path.lstat().st_mode):
            raise ValueError("unsafe invocation directory")
    except OSError as error:
        raise IntentInspectionError(
            invocation.record, path, "invocation_unavailable", error.errno
        ) from None
    except ValueError:
        raise IntentIntegrityError(
            invocation.record, path, "unsafe_invocation_directory"
        ) from None


def _validated(
    ownership: InvocationOwnership,
    record: PublicationIntentRecord,
) -> PublicationIntentRecord:
    invocation = ownership.invocation
    validated = PublicationIntentRecord.model_validate(record)
    validate_publication_intent(
        invocation.attempt.definition.record,
        invocation.attempt.record,
        invocation.record,
        validated,
    )
    return validated


def _read_bytes(path: Path) -> bytes:
    with (
        _regular_descriptor(path) as descriptor,
        os.fdopen(descriptor, "rb", closefd=False) as source,
    ):
        return source.read()


def _read(
    ownership: InvocationOwnership,
    path: Path,
) -> PublicationIntentHandle:
    invocation = ownership.invocation.record
    try:
        if not stat.S_ISREG(path.lstat().st_mode):
            raise ValueError("unsafe intent file")
        data = _read_bytes(path)
        record = _validated(ownership, decode_publication_intent(data))
    except FileNotFoundError:
        raise IntentMissingError(invocation, path, "intent_missing") from None
    except OSError as error:
        raise IntentInspectionError(
            invocation, path, "intent_unavailable", error.errno
        ) from None
    except ValueError:
        raise IntentIntegrityError(invocation, path, "invalid_stored_intent") from None

    return PublicationIntentHandle(
        path,
        record,
        hashlib.sha256(data).hexdigest(),
        len(data),
    )


def _require_absent(
    invocation: InvocationRecord,
    path: Path,
    code: str,
) -> None:
    try:
        path.lstat()
    except FileNotFoundError:
        return
    raise IntentIntegrityError(invocation, path, code)


def _require_exact(
    handle: PublicationIntentHandle,
    expected: bytes,
    invocation: InvocationRecord,
) -> None:
    if encode_publication_intent(handle.record) != expected:
        raise IntentConflictError(invocation, handle.path, "expected_intent_conflict")


def _check_temporary(
    ownership: InvocationOwnership,
    descriptor: int,
    temporary_path: Path,
) -> None:
    invocation = ownership.invocation
    metadata = os.fstat(descriptor)
    if not stat.S_ISREG(metadata.st_mode):
        raise IntentIntegrityError(
            invocation.record, temporary_path, "unsafe_temporary_file"
        )
    if metadata.st_dev != invocation.path.lstat().st_dev:
        raise IntentIntegrityError(
            invocation.record, temporary_path, "cross_filesystem"
        )


def read_owned_publication_intent(
    ownership: InvocationOwnership,
) -> PublicationIntentHandle:
    """Read and validate existing intent without mutation or flushing."""
    path = _location(ownership)
    _require_directory(ownership, path)
    return _read(ownership, path)


def publish_owned_publication_intent(
    ownership: InvocationOwnership,
    intent: PublicationIntentRecord,
) -> PublicationIntentHandle:
    """Persist expected records, or durably acknowledge an exact prior intent.

    No bundle payload is checked or changed. The caller must not proceed
    to ordinary bundle publication unless this operation acknowledges success.
    """
    path = _location(ownership)
    invocation = ownership.invocation

    try:
        expected = encode_publication_intent(_validated(ownership, intent))
    except ValueError:
        raise IntentIntegrityError(
            invocation.record, path, "invalid_expected_intent"
        ) from None

    _require_directory(ownership, path)
    try:
        existing = _read(ownership, path)
    except IntentMissingError:
        existing = None

    if existing is not None:
        _require_exact(existing, expected, invocation.record)
    else:
        try:
            _require_absent(
                invocation.record,
                invocation.path / "published",
                "retroactive_intent_forbidden",
            )
        except OSError as error:
            raise IntentInspectionError(
                invocation.record, path, "published_path_unavailable", error.errno
            ) from None

    operation = "acknowledge" if existing is not None else "publish"
    uncertain = existing is not None
    temporary_path = None
    phase = "temporary-create"

    try:
        if existing is None:
            descriptor, name = tempfile.mkstemp(
                prefix=".publication-intent-",
                dir=invocation.path,
            )
            temporary_path = Path(name)
            try:
                phase = "temporary-check"
                _check_temporary(ownership, descriptor, temporary_path)
                phase = "temporary-write"
                with os.fdopen(descriptor, "wb", closefd=False) as output:
                    output.write(expected)
                    output.flush()
                    phase = "temporary-file-flush"
                    os.fsync(descriptor)
            finally:
                os.close(descriptor)

            phase = "temporary-parent-flush"
            _sync_directory(invocation.path)
            phase = "published-path-check"
            _require_absent(
                invocation.record,
                invocation.path / "published",
                "retroactive_intent_forbidden",
            )
            phase = "intent-destination-check"
            _require_absent(invocation.record, path, "intent_destination_exists")
            phase = "intent-rename"
            uncertain = True
            os.rename(temporary_path, path)

        phase = "final-file-flush"
        _sync_file(path)
        phase = "final-parent-flush"
        _sync_directory(invocation.path)
        phase = "final-verification"
        handle = _read(ownership, path)
        _require_exact(handle, expected, invocation.record)
        return handle
    except (OSError, ValueError, _IntentError) as error:
        raise IntentPublicationError(
            invocation.record,
            path,
            operation=operation,
            phase=phase,
            temporary_path=temporary_path,
            intent_publication_uncertain=uncertain,
            reason=getattr(error, "code", "operation_failed"),
            errno=getattr(error, "errno", None),
        ) from None

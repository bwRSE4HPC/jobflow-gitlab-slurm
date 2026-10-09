"""Associate exact completed recovery evidence with the anchored run journal."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from jobflow_gitlab_slurm.config.request import Digest
from jobflow_gitlab_slurm.persistence.attempts.ownership import InvocationOwnership
from jobflow_gitlab_slurm.persistence.attempts.records import (
    RecordArtifactReference,
    job_key,
)
from jobflow_gitlab_slurm.persistence.journal import storage as journal
from jobflow_gitlab_slurm.persistence.journal.records import EventRecord
from jobflow_gitlab_slurm.persistence.recovery import receipts, requests
from jobflow_gitlab_slurm.persistence.recovery.records import (
    RecoveryReceiptRecord,
    RecoveryRequestRecord,
    encode_recovery_receipt,
    encode_recovery_request,
    validate_recovery_receipt,
)
from jobflow_gitlab_slurm.persistence.runs.records import RunId

EVENT_TYPE = "bundle.recovery_completed"

_HINT = (
    "Preserve bundle, audit, event, anchor, and temporary evidence. "
    "Inspect/retry this same recovery identity, retained event ID, and exact "
    "records. Do not manufacture a receipt, repeat recovery, select a fresh "
    "event ID, release dependent jobs, or launch a calculation automatically. "
    "Filesystem completion and journal registration do not establish scheduler "
    "or scientific success. False uncertainty flags do not prove that an "
    "earlier operation is absent."
)


class RecoveryCompletedPayload(BaseModel):
    """Strict domain metadata; not scientific state or execution permission."""

    model_config = ConfigDict(
        extra="forbid",
        strict=True,
        frozen=True,
        revalidate_instances="always",
    )

    schema_version: int
    kind: Literal["bundle-recovery-completed"]
    run_id: RunId
    job_uuid: str = Field(min_length=1)
    job_index: int = Field(ge=1)
    job_key: Digest
    attempt_id: RunId
    invocation_id: RunId
    recovery_id: RunId
    intent: RecordArtifactReference
    request: RecordArtifactReference
    receipt: RecordArtifactReference
    bundle_manifest: RecordArtifactReference
    bundle_commit: RecordArtifactReference
    result: Literal["committed_bundle_verified"]

    @field_validator("schema_version")
    @classmethod
    def check_version(cls, value: int) -> int:
        if value != 1:
            raise ValueError("unsupported recovery event payload schema_version")
        return value

    @model_validator(mode="after")
    def check_identity_and_paths(self) -> Self:
        if self.job_key != job_key(self.job_uuid):
            raise ValueError("job_key does not match exact job_uuid bytes")

        prefix = (
            f"jobs/{self.job_key}/index-{self.job_index}/"
            f"attempts/{self.attempt_id}/invocations/{self.invocation_id}"
        )
        audit = f"{prefix}/recoveries/{self.recovery_id}"
        for reference, expected in (
            (self.intent, f"{prefix}/publication-intent.json"),
            (self.request, f"{audit}/request.json"),
            (self.receipt, f"{audit}/receipt.json"),
            (self.bundle_manifest, f"{prefix}/published/payload-manifest.json"),
            (self.bundle_commit, f"{prefix}/published/COMMIT.json"),
        ):
            if reference.size_bytes == 0 or reference.path != expected:
                raise ValueError(
                    "recovery event reference must be nonempty and identity-qualified"
                )
        return self


@dataclass
class _Progress:
    phase: str = "caller-validation"
    request: RecoveryRequestRecord | None = None
    filesystem_acknowledged: bool = False


def _run_path(ownership: InvocationOwnership) -> Path:
    # Installed layout:
    # run/jobs/key/index-N/attempts/attempt/invocations/invocation
    return ownership.invocation.attempt.path.parents[4]


def _path_text(path: Path | None) -> str | None:
    return None if path is None else str(path)


class _AssociationError(receipts._ReceiptError):
    def __init__(
        self,
        ownership: InvocationOwnership,
        progress: _Progress,
        code: str,
        error: Exception,
    ) -> None:
        request = progress.request
        recovery_id = None if request is None else request.recovery_id
        cause = getattr(error, "__cause__", None)
        super().__init__(
            ownership,
            recovery_id,
            getattr(error, "path", ownership.invocation.path),
            code,
            errno=getattr(error, "errno", getattr(cause, "errno", None)),
            temporary_paths=getattr(error, "temporary_paths", ()),
            existing_ids=getattr(error, "existing_recovery_ids", ()),
            issues=getattr(error, "issues", ()),
        )
        pending = getattr(error, "pending_event", None)
        self.event_id = None if request is None else request.journal_event_id
        self.phase = progress.phase
        self.filesystem_acknowledged = progress.filesystem_acknowledged
        self.filesystem_acknowledgment_uncertain = isinstance(
            error, receipts.RecoveryReceiptPublicationError
        )
        self.journal_registration_uncertain = (
            isinstance(error, journal.JournalPublicationError)
            and error.publication_uncertain
        )
        self.inner_phase = getattr(error, "phase", None)
        self.inner_operation = getattr(error, "operation", None)
        self.sequence = getattr(error, "sequence", None)
        self.event_path = getattr(error, "event_path", None)
        self.event_staging_path = getattr(error, "event_staging_path", None)
        self.head_staging_path = getattr(error, "head_staging_path", None)
        self.head_path = _run_path(ownership) / "journal-head.json"
        self.pending_event_id = getattr(pending, "event_id", None)
        self.pending_sequence = getattr(pending, "sequence", None)
        self.pending_path = getattr(error, "pending_path", None)
        self.args = (
            (
                f"Recovery journal association failure: code={code}; "
                f"run_id={self.run_id}; job_key={self.job_key}; "
                f"job_index={self.job_index}; attempt_id={self.attempt_id}; "
                f"invocation_id={self.invocation_id}; recovery_id={recovery_id}; "
                f"event_id={self.event_id}; phase={self.phase}; "
                f"path={self.path}; errno={self.errno}; "
                f"filesystem_acknowledged_in_this_call="
                f"{self.filesystem_acknowledged}; "
                f"filesystem_acknowledgment_uncertain="
                f"{self.filesystem_acknowledgment_uncertain}; "
                f"journal_registration_uncertain="
                f"{self.journal_registration_uncertain}. {_HINT}"
            ),
        )

    def to_report(self) -> dict[str, Any]:
        """Return detached metadata without record bodies or raw causes."""
        report = super().to_report()
        report.update(
            {
                "kind": "recovery-journal-error",
                "hint": _HINT,
                "event_id": self.event_id,
                "phase": self.phase,
                "filesystem_acknowledged_in_this_call": (self.filesystem_acknowledged),
                "filesystem_acknowledgment_uncertain": (
                    self.filesystem_acknowledgment_uncertain
                ),
                "journal_registration_uncertain": (self.journal_registration_uncertain),
                "inner_phase": self.inner_phase,
                "inner_operation": self.inner_operation,
                "sequence": self.sequence,
                "event_path": _path_text(self.event_path),
                "event_staging_path": _path_text(self.event_staging_path),
                "head_path": str(self.head_path),
                "head_staging_path": _path_text(self.head_staging_path),
                "pending_event_id": self.pending_event_id,
                "pending_sequence": self.pending_sequence,
                "pending_path": _path_text(self.pending_path),
            }
        )
        return report


class RecoveryJournalIntegrityError(_AssociationError):
    """Missing, malformed, unsafe, or incorrectly bound evidence is held."""


class RecoveryJournalConflictError(_AssociationError):
    """Intact evidence or recorded event input differs from expectations."""


class RecoveryJournalInspectionError(_AssociationError):
    """I/O did not establish the required evidence."""


class RecoveryJournalIncompleteError(_AssociationError):
    """An unrelated unanchored journal tail prevents registration."""


class RecoveryJournalRegistrationError(_AssociationError):
    """Durability acknowledgment failed; preserve the same identities."""


def _failure(
    ownership: InvocationOwnership,
    progress: _Progress,
    error: Exception,
) -> _AssociationError:
    if isinstance(
        error,
        (
            requests.RecoveryRequestConflictError,
            receipts.RecoveryReceiptConflictError,
            journal.JournalEventConflictError,
        ),
    ):
        error_type = RecoveryJournalConflictError
    elif isinstance(error, journal.JournalIncompleteError):
        error_type = RecoveryJournalIncompleteError
    elif isinstance(
        error,
        (
            receipts.RecoveryReceiptPublicationError,
            journal.JournalPublicationError,
        ),
    ):
        error_type = RecoveryJournalRegistrationError
    elif isinstance(
        error,
        (
            requests.RecoveryRequestInspectionError,
            receipts.RecoveryReceiptInspectionError,
            OSError,
        ),
    ):
        error_type = RecoveryJournalInspectionError
    else:
        error_type = RecoveryJournalIntegrityError

    if isinstance(error, journal.JournalPublicationError):
        code = "journal_publication_failed"
    elif isinstance(error, journal.JournalIncompleteError):
        code = "journal_incomplete"
    elif isinstance(error, journal.JournalEventConflictError):
        code = "journal_event_conflict"
    elif isinstance(error, journal.JournalIntegrityError):
        code = "journal_integrity_invalid"
    else:
        code = getattr(error, "code", "invalid_registration_evidence")
    return error_type(ownership, progress, code, error)


def _existing(
    ownership: InvocationOwnership,
    request: RecoveryRequestRecord,
    receipt: RecoveryReceiptRecord,
) -> receipts.RecoveryReceiptHandle:
    intent = requests._intent(ownership, request.recovery_id)
    invocation = ownership.invocation
    validate_recovery_receipt(
        invocation.attempt.definition.record,
        invocation.attempt.record,
        invocation.record,
        intent,
        request,
        receipt=receipt,
    )

    retained_request = requests.read_owned_recovery_request(
        ownership, request.recovery_id
    )
    requests._require_exact(
        ownership, retained_request, encode_recovery_request(request)
    )
    retained_receipt = receipts.read_owned_recovery_receipt(
        ownership, request.recovery_id
    )
    receipts._require_exact(
        ownership, retained_receipt, encode_recovery_receipt(receipt)
    )
    return retained_receipt


def _payload(
    ownership: InvocationOwnership,
    request: RecoveryRequestRecord,
    receipt: RecoveryReceiptRecord,
    handle: receipts.RecoveryReceiptHandle,
) -> RecoveryCompletedPayload:
    identity = {field: getattr(request, field) for field in requests._IDENTITY_FIELDS}
    return RecoveryCompletedPayload.model_validate(
        {
            **identity,
            "schema_version": 1,
            "kind": "bundle-recovery-completed",
            "recovery_id": request.recovery_id,
            "intent": request.intent,
            "request": receipt.request,
            "receipt": {
                "path": handle.path.relative_to(_run_path(ownership)).as_posix(),
                "sha256": handle.sha256,
                "size_bytes": handle.size_bytes,
            },
            "bundle_manifest": receipt.bundle_manifest,
            "bundle_commit": receipt.bundle_commit,
            "result": "committed_bundle_verified",
        }
    )


def register_owned_recovery_event(
    ownership: InvocationOwnership,
    request: RecoveryRequestRecord,
    *,
    receipt: RecoveryReceiptRecord,
) -> EventRecord:
    """Register exact existing completion evidence; never finish recovery.

    Require final request and receipt files and reacknowledge the exact
    committed bundle before journal mutation. Payload I/O occurs outside
    the run lock while invocation ownership remains active.

    The request's event identity is retained across retries. Existing exact
    events and matching pending tails use installed append_event semantics.
    Busy and ownership exceptions propagate unchanged. Other failures expose
    sanitized diagnostics without authorizing recovery or scientific reruns.

    Trusted roots, cooperating/quiescent writers, and independent live-site
    durability verification remain prerequisites.
    """
    ownership.require_active()
    progress = _Progress()
    try:
        request = RecoveryRequestRecord.model_validate(request)
        progress.request = request
        receipt = RecoveryReceiptRecord.model_validate(receipt)

        progress.phase = "evidence-validation"
        handle = _existing(ownership, request, receipt)

        progress.phase = "payload-validation"
        payload = _payload(ownership, request, receipt, handle)

        progress.phase = "filesystem-acknowledgment"
        receipts.persist_owned_recovery_receipt(ownership, receipt)
        progress.filesystem_acknowledged = True

        ownership.require_active()
        progress.phase = "journal-registration"
        run_path = _run_path(ownership)
        return journal.append_event(
            run_path.parent,
            request.run_id,
            event_id=request.journal_event_id,
            event_type=EVENT_TYPE,
            payload=payload.model_dump(mode="json"),
        )
    except (
        requests._RequestError,
        receipts._ReceiptError,
        journal.JournalIntegrityError,
        journal.JournalIncompleteError,
        journal.JournalPublicationError,
        OSError,
        ValueError,
    ) as error:
        raise _failure(ownership, progress, error) from None

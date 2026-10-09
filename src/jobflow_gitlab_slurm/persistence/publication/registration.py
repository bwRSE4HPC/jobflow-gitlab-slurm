"""Associate exact committed publication evidence with the anchored journal."""

import hashlib
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
from jobflow_gitlab_slurm.persistence.bundles import inspection
from jobflow_gitlab_slurm.persistence.bundles import storage as bundles
from jobflow_gitlab_slurm.persistence.bundles.records import (
    encode_bundle_commit,
    encode_bundle_manifest,
)
from jobflow_gitlab_slurm.persistence.journal import storage as journal
from jobflow_gitlab_slurm.persistence.journal.records import EventRecord
from jobflow_gitlab_slurm.persistence.publication import storage as intents
from jobflow_gitlab_slurm.persistence.publication.records import (
    PublicationIntentRecord,
    encode_publication_intent,
    validate_publication_intent,
)
from jobflow_gitlab_slurm.persistence.runs.records import RunId

EVENT_TYPE = "bundle.publication_completed"

_HINT = (
    "Preserve intent, bundle, audit, event, anchor, and temporary evidence. "
    "Inspect/retry the same intent identity and exact expected records. "
    "The publication event ID is the retained intent ID. Do not replace "
    "the intent, create missing evidence, finish recovery, release dependent "
    "jobs, or launch a calculation automatically. Filesystem acknowledgment "
    "and journal registration do not establish scheduler or scientific success. "
    "False uncertainty flags do not prove that an earlier operation is absent."
)


class PublicationCompletedPayload(BaseModel):
    """Strict domain metadata, not scientific state or execution permission."""

    model_config = ConfigDict(
        extra="forbid",
        strict=True,
        frozen=True,
        revalidate_instances="always",
    )

    schema_version: int
    kind: Literal["bundle-publication-completed"]
    run_id: RunId
    job_uuid: str = Field(min_length=1)
    job_index: int = Field(ge=1)
    job_key: Digest
    attempt_id: RunId
    invocation_id: RunId
    intent_id: RunId
    intent: RecordArtifactReference
    bundle_manifest: RecordArtifactReference
    bundle_commit: RecordArtifactReference
    result: Literal["committed_bundle_verified"]

    @field_validator("schema_version")
    @classmethod
    def check_version(cls, value: int) -> int:
        if value != 1:
            raise ValueError("unsupported publication event payload schema_version")
        return value

    @model_validator(mode="after")
    def check_identity_and_paths(self) -> Self:
        if self.job_key != job_key(self.job_uuid):
            raise ValueError("job_key does not match exact job_uuid bytes")

        prefix = (
            f"jobs/{self.job_key}/index-{self.job_index}/"
            f"attempts/{self.attempt_id}/invocations/{self.invocation_id}"
        )
        for reference, expected in (
            (self.intent, f"{prefix}/publication-intent.json"),
            (self.bundle_manifest, f"{prefix}/published/payload-manifest.json"),
            (self.bundle_commit, f"{prefix}/published/COMMIT.json"),
        ):
            if reference.size_bytes == 0 or reference.path != expected:
                raise ValueError(
                    "publication event reference must be nonempty "
                    "and identity-qualified"
                )
        return self


@dataclass
class _Progress:
    phase: str = "caller-validation"
    intent: PublicationIntentRecord | None = None
    intent_acknowledged: bool = False
    bundle_acknowledged: bool = False


def _run_path(ownership: InvocationOwnership) -> Path:
    # run/jobs/key/index-N/attempts/attempt/invocations/invocation
    return ownership.invocation.attempt.path.parents[4]


def _path_text(path: Path | None) -> str | None:
    return None if path is None else str(path)


class _AssociationError(bundles._BundleError):
    def __init__(
        self,
        ownership: InvocationOwnership,
        progress: _Progress,
        code: str,
        error: Exception,
    ) -> None:
        super().__init__(
            ownership.invocation.record,
            getattr(error, "path", ownership.invocation.path),
            code,
            getattr(error, "issues", ()),
        )
        cause = getattr(error, "__cause__", None)
        pending = getattr(error, "pending_event", None)
        intent = progress.intent

        self.intent_id = None if intent is None else intent.intent_id
        self.event_id = self.intent_id
        self.phase = progress.phase
        self.errno = getattr(error, "errno", getattr(cause, "errno", None))
        self.intent_acknowledged = progress.intent_acknowledged
        self.bundle_acknowledged = progress.bundle_acknowledged
        self.intent_acknowledgment_uncertain = isinstance(
            error, intents.IntentPublicationError
        )
        self.bundle_acknowledgment_uncertain = isinstance(
            error, bundles.BundlePublicationError
        )
        self.journal_registration_uncertain = (
            isinstance(error, journal.JournalPublicationError)
            and error.publication_uncertain
        )
        self.inner_phase = getattr(error, "phase", None)
        self.inner_operation = getattr(error, "operation", None)
        self.sequence = getattr(error, "sequence", None)
        self.intent_path = ownership.invocation.path / "publication-intent.json"
        self.published_path = ownership.invocation.path / "published"
        self.head_path = _run_path(ownership) / "journal-head.json"
        self.event_path = getattr(error, "event_path", None)
        self.event_staging_path = getattr(error, "event_staging_path", None)
        self.head_staging_path = getattr(error, "head_staging_path", None)
        self.pending_event_id = getattr(pending, "event_id", None)
        self.pending_sequence = getattr(pending, "sequence", None)
        self.pending_path = getattr(error, "pending_path", None)
        self.args = (
            (
                f"Publication journal association failure: code={code}; "
                f"run_id={self.run_id}; job_key={self.job_key}; "
                f"job_index={self.job_index}; attempt_id={self.attempt_id}; "
                f"invocation_id={self.invocation_id}; intent_id={self.intent_id}; "
                f"event_id={self.event_id}; phase={self.phase}; "
                f"path={self.path}; errno={self.errno}; "
                f"intent_acknowledged_in_this_call={self.intent_acknowledged}; "
                f"bundle_acknowledged_in_this_call={self.bundle_acknowledged}; "
                f"intent_acknowledgment_uncertain="
                f"{self.intent_acknowledgment_uncertain}; "
                f"bundle_acknowledgment_uncertain="
                f"{self.bundle_acknowledgment_uncertain}; "
                f"journal_registration_uncertain="
                f"{self.journal_registration_uncertain}. {_HINT}"
            ),
        )

    def to_report(self) -> dict[str, Any]:
        """Return detached diagnostics without payloads or raw causes."""
        report = super().to_report()
        report.update(
            {
                "kind": "publication-journal-error",
                "hint": _HINT,
                "intent_id": self.intent_id,
                "event_id": self.event_id,
                "phase": self.phase,
                "errno": self.errno,
                "intent_acknowledged_in_this_call": self.intent_acknowledged,
                "bundle_acknowledged_in_this_call": self.bundle_acknowledged,
                "intent_acknowledgment_uncertain": (
                    self.intent_acknowledgment_uncertain
                ),
                "bundle_acknowledgment_uncertain": (
                    self.bundle_acknowledgment_uncertain
                ),
                "journal_registration_uncertain": (self.journal_registration_uncertain),
                "inner_phase": self.inner_phase,
                "inner_operation": self.inner_operation,
                "sequence": self.sequence,
                "intent_path": str(self.intent_path),
                "published_path": str(self.published_path),
                "head_path": str(self.head_path),
                "event_path": _path_text(self.event_path),
                "event_staging_path": _path_text(self.event_staging_path),
                "head_staging_path": _path_text(self.head_staging_path),
                "pending_event_id": self.pending_event_id,
                "pending_sequence": self.pending_sequence,
                "pending_path": _path_text(self.pending_path),
            }
        )
        return report


class PublicationJournalIntegrityError(_AssociationError):
    """Missing, malformed, unsafe, or incorrectly bound evidence is held."""


class PublicationJournalConflictError(_AssociationError):
    """Valid evidence or recorded event input differs from expectations."""


class PublicationJournalInspectionError(_AssociationError):
    """I/O could not establish required evidence."""


class PublicationJournalIncompleteError(_AssociationError):
    """An unrelated unanchored journal tail prevents registration."""


class PublicationJournalRegistrationError(_AssociationError):
    """Durability acknowledgment failed; preserve the same identities."""


def _failure(
    ownership: InvocationOwnership,
    progress: _Progress,
    error: Exception,
) -> _AssociationError:
    if isinstance(
        error,
        (
            intents.IntentConflictError,
            bundles.BundleConflictError,
            journal.JournalEventConflictError,
        ),
    ):
        error_type = PublicationJournalConflictError
    elif isinstance(error, journal.JournalIncompleteError):
        error_type = PublicationJournalIncompleteError
    elif isinstance(
        error,
        (
            intents.IntentPublicationError,
            bundles.BundlePublicationError,
            journal.JournalPublicationError,
        ),
    ):
        error_type = PublicationJournalRegistrationError
    elif isinstance(
        error,
        (
            intents.IntentInspectionError,
            inspection.BundleInspectionError,
            OSError,
        ),
    ):
        error_type = PublicationJournalInspectionError
    else:
        error_type = PublicationJournalIntegrityError

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


def _existing_intent(
    ownership: InvocationOwnership,
    intent: PublicationIntentRecord,
) -> intents.PublicationIntentHandle:
    invocation = ownership.invocation
    validate_publication_intent(
        invocation.attempt.definition.record,
        invocation.attempt.record,
        invocation.record,
        intent,
    )
    handle = intents.read_owned_publication_intent(ownership)
    intents._require_exact(
        handle,
        encode_publication_intent(intent),
        invocation.record,
    )
    return handle


def _require_committed(
    ownership: InvocationOwnership,
    intent: PublicationIntentRecord,
) -> None:
    report = inspection.inspect_owned_bundle(ownership)
    invocation = ownership.invocation
    published = invocation.path / "published"
    if report.status != "committed" or report.content_integrity != "valid":
        raise bundles.BundleIntegrityError(
            invocation.record,
            published,
            f"registration_requires_committed_{report.status}",
            report.issues,
        )
    bundles._require_exact(
        invocation.record,
        published / "payload-manifest.json",
        encode_bundle_manifest(intent.expected_manifest),
    )
    bundles._require_exact(
        invocation.record,
        published / "COMMIT.json",
        encode_bundle_commit(intent.expected_commit),
    )


def _reference(path: str, data: bytes) -> dict[str, object]:
    return {
        "path": path,
        "sha256": hashlib.sha256(data).hexdigest(),
        "size_bytes": len(data),
    }


def _payload(
    ownership: InvocationOwnership,
    intent: PublicationIntentRecord,
    handle: intents.PublicationIntentHandle,
) -> PublicationCompletedPayload:
    invocation = ownership.invocation
    identity = {
        field: getattr(invocation.record, field) for field in bundles._IDENTITY_FIELDS
    }
    prefix = invocation.path.relative_to(_run_path(ownership)).as_posix()
    return PublicationCompletedPayload.model_validate(
        {
            **identity,
            "schema_version": 1,
            "kind": "bundle-publication-completed",
            "intent_id": intent.intent_id,
            "intent": {
                "path": handle.path.relative_to(_run_path(ownership)).as_posix(),
                "sha256": handle.sha256,
                "size_bytes": handle.size_bytes,
            },
            "bundle_manifest": _reference(
                f"{prefix}/published/payload-manifest.json",
                encode_bundle_manifest(intent.expected_manifest),
            ),
            "bundle_commit": _reference(
                f"{prefix}/published/COMMIT.json",
                encode_bundle_commit(intent.expected_commit),
            ),
            "result": "committed_bundle_verified",
        }
    )


def register_owned_publication_event(
    ownership: InvocationOwnership,
    intent: PublicationIntentRecord,
) -> EventRecord:
    """Register exact existing publication evidence without producing it.

    Require an existing final intent and committed bundle. Reacknowledge
    their durability before appending the completion event. The intent ID
    is reserved as this event's stable identity.

    Payload I/O remains outside the run lock under invocation ownership.
    Existing events and matching pending tails use installed journal retry
    semantics. Busy and ownership exceptions propagate unchanged.

    No staging publication, recovery, scientific execution, scheduler
    interpretation, or result selection is performed. Trusted roots,
    cooperating writers, and live durability verification remain prerequisites.
    """
    ownership.require_active()
    progress = _Progress()
    try:
        intent = PublicationIntentRecord.model_validate(intent)
        progress.intent = intent

        progress.phase = "intent-validation"
        handle = _existing_intent(ownership, intent)

        progress.phase = "bundle-validation"
        _require_committed(ownership, intent)

        progress.phase = "payload-validation"
        payload = _payload(ownership, intent, handle)

        progress.phase = "intent-acknowledgment"
        intents.publish_owned_publication_intent(ownership, intent)
        progress.intent_acknowledged = True

        progress.phase = "bundle-acknowledgment"
        ownership.require_active()
        _require_committed(ownership, intent)
        bundles.publish_owned_bundle(
            ownership,
            intent.expected_manifest,
            intent.expected_commit,
        )
        progress.bundle_acknowledged = True

        progress.phase = "journal-registration"
        ownership.require_active()
        run_path = _run_path(ownership)
        return journal.append_event(
            run_path.parent,
            ownership.invocation.record.run_id,
            event_id=intent.intent_id,
            event_type=EVENT_TYPE,
            payload=payload.model_dump(mode="json"),
        )
    except (
        intents._IntentError,
        bundles._BundleError,
        inspection.BundleInspectionError,
        journal.JournalIntegrityError,
        journal.JournalIncompleteError,
        journal.JournalPublicationError,
        OSError,
        ValueError,
    ) as error:
        raise _failure(ownership, progress, error) from None

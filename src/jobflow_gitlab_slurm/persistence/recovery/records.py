"""Immutable recovery provenance; no filesystem I/O or recovery execution."""

import hashlib
import json
from typing import Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from jobflow_gitlab_slurm.config.request import Digest
from jobflow_gitlab_slurm.persistence.attempts.records import (
    AttemptRecord,
    InvocationRecord,
    JobDefinitionRecord,
    RecordArtifactReference,
    job_key,
)
from jobflow_gitlab_slurm.persistence.bundles import records as bundles
from jobflow_gitlab_slurm.persistence.bundles.records import (
    BundleManifest,
    encode_bundle_commit,
    encode_bundle_manifest,
)
from jobflow_gitlab_slurm.persistence.publication.records import (
    PublicationIntentRecord,
    encode_publication_intent,
    validate_publication_intent,
)
from jobflow_gitlab_slurm.persistence.runs.records import RunId, UtcTimestamp


class _FrozenModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        strict=True,
        frozen=True,
        revalidate_instances="always",
    )


class RecoveryActor(_FrozenModel):
    """Caller-asserted provenance, not authenticated authorization."""

    kind: Literal["operator", "controller"]
    identifier: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$",
    )


class _RecoveryIdentity(_FrozenModel):
    schema_version: int
    created_at: UtcTimestamp
    run_id: RunId
    job_uuid: str = Field(min_length=1)
    job_index: int = Field(ge=1)
    job_key: Digest
    attempt_id: RunId
    invocation_id: RunId
    recovery_id: RunId

    @field_validator("schema_version")
    @classmethod
    def check_version(cls, value: int) -> int:
        if value != 1:
            raise ValueError("unsupported recovery record schema_version")
        return value

    @field_validator("job_uuid")
    @classmethod
    def check_job_uuid(cls, value: str) -> str:
        job_key(value)
        return value

    @model_validator(mode="after")
    def check_job_key(self) -> Self:
        if self.job_key != job_key(self.job_uuid):
            raise ValueError("job_key does not match exact job_uuid bytes")
        return self


def _invocation_path(record: _RecoveryIdentity) -> str:
    return (
        f"jobs/{record.job_key}/index-{record.job_index}/"
        f"attempts/{record.attempt_id}/invocations/{record.invocation_id}"
    )


def _check_paths(
    references: tuple[tuple[RecordArtifactReference, str], ...],
) -> None:
    for reference, expected_path in references:
        if reference.size_bytes == 0:
            raise ValueError("referenced recovery metadata must be nonempty")
        if reference.path != expected_path:
            raise ValueError("recovery reference path does not match its identity")


class RecoveryRequestRecord(_RecoveryIdentity):
    """Retain one intended operation without authorizing its execution."""

    kind: Literal["bundle-recovery-request"]
    journal_event_id: RunId
    actor: RecoveryActor
    intent_id: RunId
    intent: RecordArtifactReference
    observed_status: Literal[
        "staging_only",
        "published_uncommitted",
        "committed",
    ]
    action: Literal[
        "publish_staging",
        "complete_marker",
        "acknowledge_commit",
    ]

    @model_validator(mode="after")
    def check_request(self) -> Self:
        expected_actions = {
            "staging_only": "publish_staging",
            "published_uncommitted": "complete_marker",
            "committed": "acknowledge_commit",
        }
        if self.action != expected_actions[self.observed_status]:
            raise ValueError("recovery action does not match original observation")

        _check_paths(
            (
                (
                    self.intent,
                    f"{_invocation_path(self)}/publication-intent.json",
                ),
            )
        )
        return self


class RecoveryReceiptRecord(_RecoveryIdentity):
    """Describe verified expected bytes, not scheduler or scientific success."""

    kind: Literal["bundle-recovery-receipt"]
    request: RecordArtifactReference
    bundle_manifest: RecordArtifactReference
    bundle_commit: RecordArtifactReference
    result: Literal["committed_bundle_verified"]

    @model_validator(mode="after")
    def check_receipt(self) -> Self:
        prefix = _invocation_path(self)
        _check_paths(
            (
                (
                    self.request,
                    f"{prefix}/recoveries/{self.recovery_id}/request.json",
                ),
                (
                    self.bundle_manifest,
                    f"{prefix}/published/payload-manifest.json",
                ),
                (
                    self.bundle_commit,
                    f"{prefix}/published/COMMIT.json",
                ),
            )
        )
        return self


def _canonical_bytes(record: BaseModel) -> bytes:
    return json.dumps(
        record.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def encode_recovery_request(record: RecoveryRequestRecord) -> bytes:
    """Revalidate and encode canonical recovery-request-json-v1 bytes."""
    return _canonical_bytes(RecoveryRequestRecord.model_validate(record))


def decode_recovery_request(data: bytes) -> RecoveryRequestRecord:
    """Decode canonical metadata without consumer or scientific imports."""
    text = bundles.validated_record_json(data)
    record = RecoveryRequestRecord.model_validate_json(text)
    if encode_recovery_request(record) != data:
        raise ValueError("request must use canonical recovery-request-json-v1")
    return record


def encode_recovery_receipt(record: RecoveryReceiptRecord) -> bytes:
    """Revalidate and encode canonical recovery-receipt-json-v1 bytes."""
    return _canonical_bytes(RecoveryReceiptRecord.model_validate(record))


def decode_recovery_receipt(data: bytes) -> RecoveryReceiptRecord:
    """Decode canonical metadata without establishing physical completion."""
    text = bundles.validated_record_json(data)
    record = RecoveryReceiptRecord.model_validate_json(text)
    if encode_recovery_receipt(record) != data:
        raise ValueError("receipt must use canonical recovery-receipt-json-v1")
    return record


def _identity(
    record: _RecoveryIdentity | BundleManifest,
) -> tuple[str, str, int, str, str, str]:
    return (
        record.run_id,
        record.job_uuid,
        record.job_index,
        record.job_key,
        record.attempt_id,
        record.invocation_id,
    )


def _check_reference(
    reference: RecordArtifactReference,
    expected_bytes: bytes,
    label: str,
) -> None:
    if reference.size_bytes != len(expected_bytes):
        raise ValueError(f"{label} size mismatch")
    if reference.sha256 != hashlib.sha256(expected_bytes).hexdigest():
        raise ValueError(f"{label} SHA-256 mismatch")


def validate_recovery_request(
    definition: JobDefinitionRecord,
    attempt: AttemptRecord,
    invocation: InvocationRecord,
    intent: PublicationIntentRecord,
    request: RecoveryRequestRecord,
) -> None:
    """Check declared provenance and byte binding, not permission or files."""
    validated_intent = PublicationIntentRecord.model_validate(intent)
    validated_request = RecoveryRequestRecord.model_validate(request)
    validate_publication_intent(
        definition,
        attempt,
        invocation,
        validated_intent,
    )

    if _identity(validated_request) != _identity(validated_intent.expected_manifest):
        raise ValueError("recovery request identity mismatch")
    if validated_request.intent_id != validated_intent.intent_id:
        raise ValueError("recovery request intent_id mismatch")
    if validated_request.created_at < validated_intent.created_at:
        raise ValueError("recovery request creation precedes intent")

    _check_reference(
        validated_request.intent,
        encode_publication_intent(validated_intent),
        "recovery request intent",
    )


def validate_recovery_receipt(
    definition: JobDefinitionRecord,
    attempt: AttemptRecord,
    invocation: InvocationRecord,
    intent: PublicationIntentRecord,
    request: RecoveryRequestRecord,
    *,
    receipt: RecoveryReceiptRecord,
) -> None:
    """Check the complete parent chain and exact expected completion records."""
    validate_recovery_request(
        definition,
        attempt,
        invocation,
        intent,
        request,
    )
    validated_intent = PublicationIntentRecord.model_validate(intent)
    validated_request = RecoveryRequestRecord.model_validate(request)
    validated_receipt = RecoveryReceiptRecord.model_validate(receipt)

    if _identity(validated_receipt) != _identity(validated_request):
        raise ValueError("recovery receipt identity mismatch")
    if validated_receipt.recovery_id != validated_request.recovery_id:
        raise ValueError("recovery receipt recovery_id mismatch")
    if validated_receipt.created_at < validated_request.created_at:
        raise ValueError("recovery receipt creation precedes request")

    _check_reference(
        validated_receipt.request,
        encode_recovery_request(validated_request),
        "recovery receipt request",
    )
    _check_reference(
        validated_receipt.bundle_manifest,
        encode_bundle_manifest(validated_intent.expected_manifest),
        "recovery receipt manifest",
    )
    _check_reference(
        validated_receipt.bundle_commit,
        encode_bundle_commit(validated_intent.expected_commit),
        "recovery receipt commit",
    )

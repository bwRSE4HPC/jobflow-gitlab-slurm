"""Immutable publication expectations; no filesystem I/O or recovery."""

import hashlib
import json
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from jobflow_gitlab_slurm.persistence.attempts.records import (
    AttemptRecord,
    InvocationRecord,
    JobDefinitionRecord,
)
from jobflow_gitlab_slurm.persistence.bundles import records as bundles
from jobflow_gitlab_slurm.persistence.bundles.records import (
    BundleCommit,
    BundleManifest,
    encode_bundle_manifest,
    validate_bundle_records,
)
from jobflow_gitlab_slurm.persistence.runs.records import RunId, UtcTimestamp


def _identity(
    record: BundleManifest | BundleCommit,
) -> tuple[str, str, int, str, str, str]:
    return (
        record.run_id,
        record.job_uuid,
        record.job_index,
        record.job_key,
        record.attempt_id,
        record.invocation_id,
    )


class PublicationIntentRecord(BaseModel):
    """Retain exact expected records without authorizing publication or recovery."""

    model_config = ConfigDict(
        extra="forbid",
        strict=True,
        frozen=True,
        revalidate_instances="always",
    )

    schema_version: int
    kind: Literal["publication-intent"]
    intent_id: RunId
    created_at: UtcTimestamp
    expected_manifest: BundleManifest
    expected_commit: BundleCommit

    @field_validator("schema_version")
    @classmethod
    def check_version(cls, value: int) -> int:
        if value != 1:
            raise ValueError("unsupported publication intent schema_version")
        return value

    @model_validator(mode="after")
    def check_expected_records(self) -> Self:
        manifest = self.expected_manifest
        commit = self.expected_commit

        if _identity(manifest) != _identity(commit):
            raise ValueError("expected manifest/commit identity mismatch")
        if manifest.created_at > commit.created_at:
            raise ValueError("expected marker creation precedes manifest")
        if manifest.created_at > self.created_at:
            raise ValueError("intent creation precedes expected manifest")

        encoded = encode_bundle_manifest(manifest)
        if commit.manifest.size_bytes != len(encoded):
            raise ValueError("expected marker manifest size mismatch")
        if commit.manifest.sha256 != hashlib.sha256(encoded).hexdigest():
            raise ValueError("expected marker manifest SHA-256 mismatch")
        return self


def encode_publication_intent(record: PublicationIntentRecord) -> bytes:
    """Revalidate and encode canonical publication-intent-json-v1 bytes."""
    validated = PublicationIntentRecord.model_validate(record)
    return json.dumps(
        validated.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def decode_publication_intent(data: bytes) -> PublicationIntentRecord:
    """Decode strict canonical metadata without scientific object decoding."""
    # Package-local reuse of the existing strict JSON boundary, including
    # byte input, duplicate-key, nonfinite-value, and recursion checks.
    text = bundles.validated_record_json(data)
    record = PublicationIntentRecord.model_validate_json(text)
    if encode_publication_intent(record) != data:
        raise ValueError("intent must use canonical publication-intent-json-v1")
    return record


def validate_publication_intent(
    definition: JobDefinitionRecord,
    attempt: AttemptRecord,
    invocation: InvocationRecord,
    intent: PublicationIntentRecord,
) -> None:
    """Validate parent binding, not bytes, durability, or recovery authority."""
    validated = PublicationIntentRecord.model_validate(intent)
    validate_bundle_records(
        definition,
        attempt,
        invocation,
        validated.expected_manifest,
        validated.expected_commit,
    )

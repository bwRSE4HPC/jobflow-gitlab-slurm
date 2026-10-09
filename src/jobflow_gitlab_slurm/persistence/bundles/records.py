"""Strict bundle metadata and canonical codecs; no publication or execution."""

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
    validate_attempt_records,
)
from jobflow_gitlab_slurm.persistence.runs.records import RunId, UtcTimestamp


class _FrozenModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        strict=True,
        frozen=True,
        revalidate_instances="always",
    )


class BundleArtifactReference(RecordArtifactReference):
    """Describe bytes at a bundle-relative, not run-relative, path."""


class AdditionalDataReference(_FrozenModel):
    """Pin opaque additional-store data without implementing store access."""

    store_name: str = Field(min_length=1)
    blob_uuid: str = Field(min_length=1)
    artifact: BundleArtifactReference

    @field_validator("store_name", "blob_uuid")
    @classmethod
    def check_utf8_identity(cls, value: str) -> str:
        value.encode("utf-8")
        return value

    @model_validator(mode="after")
    def check_data_path(self) -> Self:
        if not self.artifact.path.startswith("data/"):
            raise ValueError("additional-store artifact must be below data/")
        return self


class _BundleIdentity(_FrozenModel):
    schema_version: int
    run_id: RunId
    created_at: UtcTimestamp
    job_uuid: str = Field(min_length=1)
    job_index: int = Field(ge=1)
    job_key: Digest
    attempt_id: RunId
    invocation_id: RunId

    @field_validator("schema_version")
    @classmethod
    def check_version(cls, value: int) -> int:
        if value != 1:
            raise ValueError("unsupported bundle record schema_version")
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


class BundleManifest(_BundleIdentity):
    """Declare invocation evidence without proving its availability or validity."""

    kind: Literal["bundle-manifest"]
    definition_id: RunId
    definition_sha256: Digest
    consumer_code_sha256: Digest
    worker_runtime_sha256: Digest
    jobflow_version: Literal["0.3.1"]
    scheduler_receipt: RecordArtifactReference
    document: BundleArtifactReference
    response: BundleArtifactReference
    files: tuple[BundleArtifactReference, ...]
    additional_data: tuple[AdditionalDataReference, ...]

    @model_validator(mode="after")
    def check_artifact_roles(self) -> Self:
        for reference, expected in (
            (self.document, "job-document.json"),
            (self.response, "response.json"),
        ):
            if reference.path != expected:
                raise ValueError(f"required artifact path must be {expected}")
            if reference.size_bytes == 0:
                raise ValueError(f"required artifact {expected} must be nonempty")

        expected_receipt = (
            f"jobs/{self.job_key}/index-{self.job_index}/attempts/"
            f"{self.attempt_id}/slurm-receipt.json"
        )
        if self.scheduler_receipt.path != expected_receipt:
            raise ValueError("scheduler receipt path does not match attempt identity")
        if self.scheduler_receipt.size_bytes == 0:
            raise ValueError("scheduler receipt must be nonempty")

        for reference in self.files:
            if not reference.path.startswith("files/"):
                raise ValueError("application artifact must be below files/")
        return self

    @model_validator(mode="after")
    def check_inventory(self) -> Self:
        file_paths = [reference.path for reference in self.files]
        data_paths = [reference.artifact.path for reference in self.additional_data]

        if file_paths != sorted(file_paths):
            raise ValueError("files inventory must be ordered by path")
        if data_paths != sorted(data_paths):
            raise ValueError("additional_data inventory must be ordered by path")

        paths = [
            self.document.path,
            self.response.path,
            *file_paths,
            *data_paths,
        ]
        declared = set(paths)
        if len(declared) != len(paths):
            raise ValueError("duplicate declared payload path")

        for path in paths:
            components = path.split("/")
            for end in range(1, len(components)):
                if "/".join(components[:end]) in declared:
                    raise ValueError("declared payload path is an ancestor of another")

        identities = [
            (reference.store_name, reference.blob_uuid)
            for reference in self.additional_data
        ]
        if len(set(identities)) != len(identities):
            raise ValueError("duplicate additional-store/blob identity")
        return self


class BundleCommit(_BundleIdentity):
    """Bind an invocation to exact manifest bytes, not to execution success."""

    kind: Literal["bundle-commit"]
    manifest: BundleArtifactReference

    @model_validator(mode="after")
    def check_manifest_reference(self) -> Self:
        if self.manifest.path != "payload-manifest.json":
            raise ValueError("manifest path must be payload-manifest.json")
        if self.manifest.size_bytes == 0:
            raise ValueError("manifest reference must be nonempty")
        return self


def _canonical_bytes(record: BundleManifest | BundleCommit) -> bytes:
    return json.dumps(
        record.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def encode_bundle_manifest(record: BundleManifest) -> bytes:
    """Revalidate and encode a manifest using bundle-json-v1."""
    return _canonical_bytes(BundleManifest.model_validate(record))


def encode_bundle_commit(record: BundleCommit) -> bytes:
    """Revalidate and encode a marker using bundle-json-v1."""
    return _canonical_bytes(BundleCommit.model_validate(record))


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValueError(f"nonfinite JSON constant: {value}")


def validated_record_json(data: bytes) -> str:
    if type(data) is not bytes:
        raise ValueError("encoded bundle record input must be bytes")
    try:
        text = data.decode("utf-8")
        json.loads(
            text,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except RecursionError as error:
        raise ValueError("JSON nesting exceeds supported depth") from error
    return text


def decode_bundle_manifest(data: bytes) -> BundleManifest:
    """Decode strict canonical metadata without importing payload callables."""
    record = BundleManifest.model_validate_json(validated_record_json(data))
    if encode_bundle_manifest(record) != data:
        raise ValueError("manifest must use canonical bundle-json-v1")
    return record


def decode_bundle_commit(data: bytes) -> BundleCommit:
    """Decode strict canonical marker metadata without inferring success."""
    record = BundleCommit.model_validate_json(validated_record_json(data))
    if encode_bundle_commit(record) != data:
        raise ValueError("marker must use canonical bundle-json-v1")
    return record


def _identity(
    record: InvocationRecord | BundleManifest | BundleCommit,
) -> tuple[str, str, int, str, str, str]:
    return (
        record.run_id,
        record.job_uuid,
        record.job_index,
        record.job_key,
        record.attempt_id,
        record.invocation_id,
    )


def validate_bundle_records(
    definition: JobDefinitionRecord,
    attempt: AttemptRecord,
    invocation: InvocationRecord,
    manifest: BundleManifest,
    commit: BundleCommit,
) -> None:
    """Validate linked metadata and manifest binding, not result eligibility.

    Payload files, scheduler receipts, jobflow semantics, authorization,
    writer ownership, publication, and Slurm outcomes are not checked.
    """
    definition = JobDefinitionRecord.model_validate(definition)
    attempt = AttemptRecord.model_validate(attempt)
    invocation = InvocationRecord.model_validate(invocation)
    manifest = BundleManifest.model_validate(manifest)
    commit = BundleCommit.model_validate(commit)
    validate_attempt_records(definition, attempt, invocation)

    expected = _identity(invocation)
    for record in (manifest, commit):
        if _identity(record) != expected:
            raise ValueError("bundle run/job/attempt/invocation identity mismatch")

    for field in (
        "definition_id",
        "definition_sha256",
        "consumer_code_sha256",
        "worker_runtime_sha256",
    ):
        if getattr(manifest, field) != getattr(attempt, field):
            raise ValueError(f"bundle {field} does not match execution attempt")

    # Both definition and manifest enforce the same literal jobflow version.
    if invocation.created_at > manifest.created_at:
        raise ValueError("manifest creation precedes worker invocation")
    if manifest.created_at > commit.created_at:
        raise ValueError("marker creation precedes bundle manifest")

    encoded = encode_bundle_manifest(manifest)
    if commit.manifest.size_bytes != len(encoded):
        raise ValueError("marker manifest size does not match canonical bytes")
    if commit.manifest.sha256 != hashlib.sha256(encoded).hexdigest():
        raise ValueError("marker manifest SHA-256 does not match canonical bytes")

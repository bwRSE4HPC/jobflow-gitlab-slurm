"""Strict job-definition, attempt, and invocation metadata; no filesystem I/O."""

import hashlib
from typing import Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from jobflow_gitlab_slurm.config.request import Digest
from jobflow_gitlab_slurm.persistence.runs.records import RunId, UtcTimestamp


def job_key(job_uuid: str) -> str:
    """Return a safe locator without normalizing the jobflow identity."""
    if not isinstance(job_uuid, str) or not job_uuid:
        raise ValueError("job_uuid must be a nonempty string")
    return hashlib.sha256(job_uuid.encode("utf-8")).hexdigest()


class _FrozenModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        strict=True,
        frozen=True,
        revalidate_instances="always",
    )


class RecordArtifactReference(_FrozenModel):
    """Describe exact artifact bytes without verifying filesystem contents."""

    path: str
    sha256: Digest
    size_bytes: int = Field(ge=0)

    @field_validator("path")
    @classmethod
    def check_path(cls, value: str) -> str:
        if (
            "\\" in value
            or "\0" in value
            or any(part in {"", ".", ".."} for part in value.split("/"))
        ):
            raise ValueError("artifact path must be a safe run-relative POSIX path")
        value.encode("utf-8")
        return value


class _JobRecord(_FrozenModel):
    schema_version: int
    run_id: RunId
    created_at: UtcTimestamp
    job_uuid: str = Field(min_length=1)
    job_index: int = Field(ge=1)
    job_key: Digest

    @field_validator("schema_version")
    @classmethod
    def check_version(cls, value: int) -> int:
        if value != 1:
            raise ValueError("unsupported attempt record schema_version")
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


class JobDefinitionRecord(_JobRecord):
    """Pin a job payload and its source without decoding or authorizing it."""

    kind: Literal["job-definition"]
    definition_id: RunId
    jobflow_version: Literal["0.3.1"]
    payload: RecordArtifactReference
    origin: Literal["original_flow", "dynamic_response", "amendment"]
    source: RecordArtifactReference

    @model_validator(mode="after")
    def check_payload_and_source_paths(self) -> Self:
        expected = (
            f"jobs/{self.job_key}/index-{self.job_index}/definitions/"
            f"{self.definition_id}/job.json"
        )
        if self.payload.path != expected:
            raise ValueError("job definition payload path does not match its identity")
        if self.origin == "original_flow" and self.source.path != "flow/payload.json":
            raise ValueError("original_flow source must be flow/payload.json")
        return self


class AttemptRecord(_JobRecord):
    """Pin an execution intention, not a submission or successful result."""

    kind: Literal["execution-attempt"]
    attempt_id: RunId
    definition_id: RunId
    definition_sha256: Digest
    consumer_code_sha256: Digest
    worker_runtime_sha256: Digest


class InvocationRecord(_JobRecord):
    """Identify one worker entry without asserting its activity or outcome."""

    kind: Literal["worker-invocation"]
    invocation_id: RunId
    attempt_id: RunId


def validate_attempt_records(
    definition: JobDefinitionRecord,
    attempt: AttemptRecord,
    invocation: InvocationRecord,
) -> None:
    """Revalidate linked metadata without checking artifacts or execution.

    This does not verify the run request, authorization, scheduler association,
    jobflow semantics, writer ownership, or result eligibility.
    """
    definition = JobDefinitionRecord.model_validate(definition)
    attempt = AttemptRecord.model_validate(attempt)
    invocation = InvocationRecord.model_validate(invocation)

    identity = (
        definition.run_id,
        definition.job_uuid,
        definition.job_index,
        definition.job_key,
    )
    for record in (attempt, invocation):
        if (
            record.run_id,
            record.job_uuid,
            record.job_index,
            record.job_key,
        ) != identity:
            raise ValueError("run/job identity does not match job definition")

    if attempt.definition_id != definition.definition_id:
        raise ValueError("attempt definition_id does not match job definition")
    if attempt.definition_sha256 != definition.payload.sha256:
        raise ValueError("attempt definition_sha256 does not match job payload")
    if invocation.attempt_id != attempt.attempt_id:
        raise ValueError("invocation attempt_id does not match execution attempt")
    if definition.created_at > attempt.created_at:
        raise ValueError("attempt creation precedes job definition")
    if attempt.created_at > invocation.created_at:
        raise ValueError("invocation creation precedes execution attempt")

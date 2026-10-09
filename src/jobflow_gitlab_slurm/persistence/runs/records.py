"""Version-one metadata records; no filesystem publication or workflow imports."""

import hashlib
import json
from datetime import datetime, timedelta
from pathlib import PurePosixPath
from typing import Annotated, Literal

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from jobflow_gitlab_slurm.config.request import Digest, RunRequest, validate_for_site
from jobflow_gitlab_slurm.config.site import SiteConfig

RunId = Annotated[
    str,
    Field(
        pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    ),
]


def _check_utc(value: str) -> str:
    instant = datetime.fromisoformat(value)
    if instant.tzinfo is None or instant.utcoffset() != timedelta(0):
        raise ValueError("timestamp must be UTC and timezone-aware")
    canonical = instant.isoformat(timespec="microseconds").replace("+00:00", "Z")
    if value != canonical:
        raise ValueError("timestamp must use YYYY-MM-DDTHH:MM:SS.ffffffZ")
    return value


UtcTimestamp = Annotated[str, AfterValidator(_check_utc)]


class _FrozenModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, revalidate_instances="always"
    )


def site_snapshot_bytes(site: SiteConfig) -> bytes:
    """Encode a revalidated site using site-json-v1, with no trailing newline."""
    validated = SiteConfig.model_validate(site.model_dump(mode="json"))
    return json.dumps(
        validated.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


class SiteSnapshot(_FrozenModel):
    """Store immutable canonical JSON text; accessors return detached site models."""

    encoding: Literal["site-json-v1"]
    content: str
    sha256: Digest

    @model_validator(mode="after")
    def check_integrity(self) -> "SiteSnapshot":
        site = SiteConfig.model_validate_json(self.content)
        payload = site_snapshot_bytes(site)
        if payload != self.content.encode("utf-8"):
            raise ValueError("site snapshot content is not canonical site-json-v1")
        if hashlib.sha256(payload).hexdigest() != self.sha256:
            raise ValueError("site snapshot SHA-256 mismatch")
        return self

    def to_site(self) -> SiteConfig:
        """Return a new validated site, never a mutable alias to the snapshot."""
        return SiteConfig.model_validate_json(self.content)


def snapshot_site(site: SiteConfig) -> SiteSnapshot:
    """Detach and fingerprint a validated site configuration."""
    payload = site_snapshot_bytes(site)
    return SiteSnapshot(
        encoding="site-json-v1",
        content=payload.decode("utf-8"),
        sha256=hashlib.sha256(payload).hexdigest(),
    )


class FlowPayloadReference(_FrozenModel):
    path: Literal["flow/payload.json"]
    sha256: Digest
    size_bytes: int = Field(ge=0)


class _Envelope(_FrozenModel):
    schema_version: int
    run_id: RunId
    created_at: UtcTimestamp

    @field_validator("schema_version")
    @classmethod
    def check_version(cls, value: int) -> int:
        if value != 1:
            raise ValueError("unsupported record schema_version")
        return value


class FlowEnvelope(_Envelope):
    """Metadata for flow/original.json, separate from the exact payload bytes."""

    kind: Literal["original-flow"]
    payload: FlowPayloadReference


class RunManifest(_Envelope):
    """Metadata for run.json; constructing it does not create or publish a run."""

    kind: Literal["run-manifest"]
    backend_version: str = Field(min_length=1)
    jobflow_version: Literal["0.3.1"]
    request: RunRequest
    site_snapshot: SiteSnapshot
    workspace_expires_at: UtcTimestamp | None = None

    @field_validator("backend_version")
    @classmethod
    def check_backend_version(cls, value: str) -> str:
        if any(char.isspace() for char in value):
            raise ValueError("backend_version must not contain whitespace")
        return value

    @field_validator("request", mode="before")
    @classmethod
    def detach_request(cls, value: object) -> object:
        if isinstance(value, RunRequest):
            return value.model_dump(mode="json")
        return value

    @model_validator(mode="after")
    def check_binding(self) -> "RunManifest":
        validate_for_site(self.request, self.site_snapshot.to_site())
        if (
            self.workspace_expires_at is not None
            and self.workspace_expires_at <= self.created_at
        ):
            raise ValueError("workspace expiry must be later than creation")
        return self


def validate_run_records(manifest: RunManifest, flow: FlowEnvelope) -> None:
    """Revalidate metadata and reject inconsistent cross-record identities.

    This checks metadata claims only; artifact bytes require separate checks.
    Backend-version compatibility is a later loader policy, not proved here.
    """
    manifest = RunManifest.model_validate_json(manifest.model_dump_json())
    flow = FlowEnvelope.model_validate_json(flow.model_dump_json())
    if manifest.run_id != flow.run_id:
        raise ValueError("flow envelope run_id does not match manifest")
    if manifest.created_at != flow.created_at:
        raise ValueError("flow envelope created_at does not match manifest")
    if manifest.request.workflow.serialized_flow_sha256 != flow.payload.sha256:
        raise ValueError("flow payload SHA-256 does not match run request")


class ExternalArtifact(BaseModel):
    """A verified external file reference, not a promise of future availability."""

    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, revalidate_instances="always"
    )
    path: str
    sha256: Digest
    size_bytes: int = Field(ge=0)

    @field_validator("path")
    @classmethod
    def check_path(cls, value: str) -> str:
        path = PurePosixPath(value)
        if (
            not path.is_absolute()
            or value == "/"
            or path.as_posix() != value
            or ".." in path.parts
            or any(ord(char) < 32 or ord(char) == 127 for char in value)
        ):
            raise ValueError("artifact path must be canonical absolute POSIX below /")
        return value


class ExternalArtifacts(BaseModel):
    """Versioned artifacts/references.json metadata."""

    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, revalidate_instances="always"
    )
    schema_version: int
    kind: Literal["external-artifacts"]
    run_id: RunId
    created_at: UtcTimestamp
    consumer_code: ExternalArtifact
    worker_runtime: ExternalArtifact

    @field_validator("schema_version")
    @classmethod
    def check_schema_version(cls, value: int) -> int:
        if value != 1:
            raise ValueError("unsupported artifact-reference schema_version")
        return value

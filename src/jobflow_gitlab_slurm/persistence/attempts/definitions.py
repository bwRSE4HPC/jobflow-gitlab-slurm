"""Immutable opaque job definitions on the existing trusted POSIX run root."""

import json
import os
import stat
import tempfile
from dataclasses import dataclass
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from jobflow_gitlab_slurm.persistence._filesystem import (
    regular_file_descriptor as _regular_descriptor,
)
from jobflow_gitlab_slurm.persistence._filesystem import (
    sync_directory as _sync_directory,
)
from jobflow_gitlab_slurm.persistence._filesystem import (
    sync_file as _sync_file,
)
from jobflow_gitlab_slurm.persistence.artifacts import (
    stage_verified_artifact,
    verify_artifact,
)
from jobflow_gitlab_slurm.persistence.attempts.records import (
    JobDefinitionRecord,
    job_key,
)
from jobflow_gitlab_slurm.persistence.runs.records import RunId
from jobflow_gitlab_slurm.persistence.runs.storage import RunHandle, locked_run


@dataclass(frozen=True)
class DefinitionHandle:
    """Verified metadata/location; neither a retained lock nor authorization."""

    path: Path
    record: JobDefinitionRecord


class DefinitionIntegrityError(RuntimeError):
    """Stored definition evidence is missing, unsafe, or inconsistent."""

    def __init__(self, run_id: str, path: Path) -> None:
        self.run_id = run_id
        self.path = path
        super().__init__(
            f"Definition integrity failure: run_id={run_id}; path={path}. "
            "Preserve evidence and hold this definition. Inspect the files "
            "and chained cause; do not overwrite or launch calculations."
        )


class DefinitionConflictError(RuntimeError):
    """An intact published definition differs from the caller's retained record."""

    def __init__(self, record: JobDefinitionRecord, path: Path) -> None:
        self.run_id = record.run_id
        self.job_key = record.job_key
        self.job_index = record.job_index
        self.definition_id = record.definition_id
        self.path = path
        super().__init__(
            f"Definition conflict: run_id={record.run_id}; "
            f"job_key={record.job_key}; job_index={record.job_index}; "
            f"definition_id={record.definition_id}; path={path}. "
            "The same ID cannot identify changed content or provenance. "
            "Preserve evidence; inspect the original record. "
            "Do not overwrite it or implicitly launch calculations."
        )


class DefinitionPublicationError(RuntimeError):
    """Publication/acknowledgment failed; retain identity and inspect evidence."""

    def __init__(
        self,
        record: JobDefinitionRecord,
        path: Path,
        staging_path: Path | None,
        *,
        operation: str,
        phase: str,
        publication_uncertain: bool,
    ) -> None:
        self.run_id = record.run_id
        self.job_key = record.job_key
        self.job_index = record.job_index
        self.definition_id = record.definition_id
        self.path = path
        self.staging_path = staging_path
        self.operation = operation
        self.phase = phase
        self.publication_uncertain = publication_uncertain
        super().__init__(
            f"Definition publication failure: run_id={record.run_id}; "
            f"job_key={record.job_key}; job_index={record.job_index}; "
            f"definition_id={record.definition_id}; operation={operation}; "
            f"phase={phase}; path={path}; staging_path={staging_path}; "
            f"publication_uncertain={publication_uncertain}. "
            "Preserve evidence. Inspect with read_job_definition and explicitly "
            "retry the same definition ID and original record. "
            "Do not adopt/delete staging or launch calculations automatically."
        )


class _Selection(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    job_uuid: str = Field(min_length=1)
    job_index: int = Field(ge=1)
    definition_id: RunId


def _paths(
    run: RunHandle,
    identity: JobDefinitionRecord | _Selection,
) -> tuple[tuple[Path, ...], Path]:
    jobs = run.path / "jobs"
    job = jobs / job_key(identity.job_uuid)
    index = job / f"index-{identity.job_index}"
    definitions = index / "definitions"
    parents = (run.path, jobs, job, index, definitions)
    return parents, definitions / identity.definition_id


def _directory(path: Path) -> None:
    if not stat.S_ISDIR(path.lstat().st_mode):
        raise ValueError("definition path component must be a real directory")


def _containers_exist(parents: tuple[Path, ...]) -> bool:
    for path in parents[1:]:
        try:
            _directory(path)
        except FileNotFoundError:
            return False
    return True


def _ensure_directory(path: Path) -> None:
    try:
        path.mkdir(mode=0o700)
    except FileExistsError:
        _directory(path)


def _sync_parents(parents: tuple[Path, ...]) -> None:
    for path in reversed(parents):
        _sync_directory(path)


def _write_metadata(path: Path, record: JobDefinitionRecord) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as output:
            output.write(record.model_dump_json().encode("utf-8"))
            output.flush()
            os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _unique_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate definition metadata key")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValueError("nonfinite definition metadata constant")


def _read_metadata(path: Path) -> JobDefinitionRecord:
    with (
        _regular_descriptor(path) as descriptor,
        os.fdopen(descriptor, "rb", closefd=False) as source,
    ):
        data = json.loads(
            source.read().decode("utf-8"),
            object_pairs_hook=_unique_keys,
            parse_constant=_reject_constant,
        )
    return JobDefinitionRecord.model_validate_json(json.dumps(data, allow_nan=False))


def _bind(record: JobDefinitionRecord, run: RunHandle) -> None:
    if (record.run_id, record.jobflow_version) != (
        run.manifest.run_id,
        run.manifest.jobflow_version,
    ):
        raise ValueError("definition run identity/version does not match run")
    if record.origin != "original_flow":
        raise ValueError("definition storage currently supports original_flow only")
    if record.created_at < run.manifest.created_at:
        raise ValueError("definition creation precedes run creation")
    if record.source.model_dump() != run.flow.payload.model_dump():
        raise ValueError("definition source does not match original Flow envelope")


def _check_size(actual: int, expected: int) -> None:
    if actual != expected:
        raise ValueError("definition payload size mismatch")


def _read_locked(
    run: RunHandle,
    identity: JobDefinitionRecord | _Selection,
) -> DefinitionHandle:
    parents, path = _paths(run, identity)
    try:
        if not _containers_exist(parents):
            raise ValueError("definition containers are missing")
        _directory(path)
        if {entry.name for entry in path.iterdir()} != {
            "definition.json",
            "job.json",
        }:
            raise ValueError("published definition has missing/unexpected entries")

        record = _read_metadata(path / "definition.json")
        _bind(record, run)
        if (
            record.job_uuid,
            record.job_index,
            record.definition_id,
        ) != (
            identity.job_uuid,
            identity.job_index,
            identity.definition_id,
        ):
            raise ValueError("definition identity does not match requested location")

        verified = verify_artifact(path / "job.json", record.payload.sha256)
        _check_size(verified.size_bytes, record.payload.size_bytes)
    except (OSError, ValueError, RecursionError) as error:
        raise DefinitionIntegrityError(run.manifest.run_id, path) from error
    return DefinitionHandle(path, record)


def read_job_definition(
    runs_root: str | Path,
    run_id: str,
    job_uuid: str,
    job_index: int,
    definition_id: str,
) -> DefinitionHandle:
    """Verify a definition without mutation, imports, or a retained lock."""
    identity = _Selection(
        job_uuid=job_uuid,
        job_index=job_index,
        definition_id=definition_id,
    )
    job_key(identity.job_uuid)
    with locked_run(runs_root, run_id) as run:
        return _read_locked(run, identity)


def publish_job_definition(
    runs_root: str | Path,
    record: JobDefinitionRecord,
    payload_source: str | Path,
) -> DefinitionHandle:
    """Publish opaque input or acknowledge an intact exact same-ID retry.

    All writers must cooperate through the existing run lock. Backend-owned
    path components are checked; trusted ancestors are not an adversarial
    filesystem sandbox. No abandoned stage is adopted or automatically removed.
    """
    record = JobDefinitionRecord.model_validate(record)
    with locked_run(runs_root, record.run_id) as run:
        _bind(record, run)
        parents, path = _paths(run, record)
        try:
            containers_exist = _containers_exist(parents)
        except (OSError, ValueError) as error:
            raise DefinitionIntegrityError(record.run_id, path) from error

        if containers_exist and os.path.lexists(path):
            existing = _read_locked(run, record)
            if existing.record != record:
                raise DefinitionConflictError(record, path)
            try:
                _sync_file(path / "definition.json")
                _sync_file(path / "job.json")
                _sync_directory(path)
                _sync_parents(parents)
            except Exception as error:
                raise DefinitionPublicationError(
                    record,
                    path,
                    None,
                    operation="acknowledge",
                    phase="acknowledge-flush",
                    publication_uncertain=True,
                ) from error
            return existing

        verified = verify_artifact(payload_source, record.payload.sha256)
        _check_size(verified.size_bytes, record.payload.size_bytes)

        staging = None
        phase = "containers"
        publication_uncertain = False
        try:
            for parent in parents[1:]:
                _ensure_directory(parent)

            phase = "staging"
            staging = Path(
                tempfile.mkdtemp(
                    prefix=f".staging-{record.definition_id}-",
                    dir=parents[-1],
                )
            )

            phase = "metadata"
            _write_metadata(staging / "definition.json", record)

            phase = "payload"
            staged = stage_verified_artifact(
                payload_source,
                staging / "job.json",
                record.payload.sha256,
            )
            _check_size(staged.size_bytes, record.payload.size_bytes)

            phase = "staging-flush"
            _sync_directory(staging)

            phase = "rename"
            if os.path.lexists(path):
                raise FileExistsError("definition destination appeared")
            publication_uncertain = True
            os.rename(staging, path)

            phase = "parent-flush"
            _sync_parents(parents)
        except Exception as error:
            raise DefinitionPublicationError(
                record,
                path,
                staging,
                operation="publish",
                phase=phase,
                publication_uncertain=publication_uncertain,
            ) from error

        return DefinitionHandle(path, record)

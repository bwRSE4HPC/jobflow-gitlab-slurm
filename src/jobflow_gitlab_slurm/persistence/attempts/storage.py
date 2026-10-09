"""Immutable attempt/invocation metadata; no execution or scheduler policy."""

import json
import os
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from jobflow_gitlab_slurm.persistence.attempts import definitions
from jobflow_gitlab_slurm.persistence.attempts.definitions import DefinitionHandle
from jobflow_gitlab_slurm.persistence.attempts.records import (
    AttemptRecord,
    InvocationRecord,
    JobDefinitionRecord,
    job_key,
    validate_attempt_records,
)
from jobflow_gitlab_slurm.persistence.runs.records import RunId
from jobflow_gitlab_slurm.persistence.runs.storage import RunHandle, locked_run


@dataclass(frozen=True)
class AttemptHandle:
    """Verified intention and definition; not a scheduling lease."""

    path: Path
    record: AttemptRecord
    definition: DefinitionHandle


@dataclass(frozen=True)
class InvocationHandle:
    """Verified process-entry identity; not liveness or writer exclusion."""

    path: Path
    record: InvocationRecord
    attempt: AttemptHandle


class _AttemptSelection(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    job_uuid: str = Field(min_length=1)
    job_index: int = Field(ge=1)
    attempt_id: RunId


class _InvocationSelection(_AttemptSelection):
    invocation_id: RunId


type _Identity = AttemptRecord | InvocationRecord | _AttemptSelection


class _StorageError(RuntimeError):
    def __init__(
        self,
        run_id: str,
        identity: _Identity,
        path: Path,
        message: str,
    ) -> None:
        self.run_id = run_id
        self.job_key = job_key(identity.job_uuid)
        self.job_index = identity.job_index
        self.attempt_id = identity.attempt_id
        self.invocation_id = getattr(identity, "invocation_id", None)
        self.path = path
        super().__init__(
            f"{message} "
            f"run_id={run_id}; job_key={self.job_key}; "
            f"job_index={self.job_index}; attempt_id={self.attempt_id}; "
            f"invocation_id={self.invocation_id}; path={path}."
        )


class AttemptIntegrityError(_StorageError):
    """Required stored metadata or its parent chain is unsafe/inconsistent."""

    def __init__(self, run_id: str, identity: _Identity, path: Path) -> None:
        super().__init__(
            run_id,
            identity,
            path,
            "Attempt/invocation integrity failure. Preserve evidence and hold "
            "this entry. Inspect required metadata, its parents, and the "
            "chained cause; do not overwrite or launch calculations.",
        )


class AttemptConflictError(_StorageError):
    """An intact record differs from the retained record for the same location."""

    def __init__(
        self,
        record: AttemptRecord | InvocationRecord,
        path: Path,
    ) -> None:
        super().__init__(
            record.run_id,
            record,
            path,
            "Attempt/invocation conflict. Preserve evidence. The same identity "
            "cannot identify changed metadata. Inspect the original record; "
            "do not overwrite it or implicitly launch calculations.",
        )


class AttemptPublicationError(_StorageError):
    """Metadata publication/acknowledgment failed; inspect before explicit retry."""

    def __init__(
        self,
        record: AttemptRecord | InvocationRecord,
        path: Path,
        staging_path: Path | None,
        *,
        operation: str,
        phase: str,
        publication_uncertain: bool,
    ) -> None:
        self.staging_path = staging_path
        self.operation = operation
        self.phase = phase
        self.publication_uncertain = publication_uncertain
        super().__init__(
            record.run_id,
            record,
            path,
            f"Attempt/invocation publication failure; operation={operation}; "
            f"phase={phase}; staging_path={staging_path}; "
            f"publication_uncertain={publication_uncertain}. "
            "Preserve evidence. Inspect using read_attempt/read_invocation "
            "and explicitly retry the same identity and original record. "
            "Do not adopt/delete staging or launch calculations automatically.",
        )


def _identity(
    record: JobDefinitionRecord | AttemptRecord | InvocationRecord,
) -> tuple[str, str, int, str]:
    return record.run_id, record.job_uuid, record.job_index, record.job_key


def _attempt_paths(
    run: RunHandle,
    identity: _Identity,
) -> tuple[tuple[Path, ...], Path]:
    jobs = run.path / "jobs"
    job = jobs / job_key(identity.job_uuid)
    index = job / f"index-{identity.job_index}"
    attempts = index / "attempts"
    return (run.path, jobs, job, index, attempts), attempts / identity.attempt_id


def _invocation_paths(
    run: RunHandle,
    identity: InvocationRecord | _InvocationSelection,
) -> tuple[tuple[Path, ...], Path]:
    parents, attempt = _attempt_paths(run, identity)
    invocations = attempt / "invocations"
    return (*parents, attempt, invocations), invocations / identity.invocation_id


def _require_paths(parents: tuple[Path, ...], path: Path) -> None:
    if not definitions._containers_exist(parents):
        raise ValueError("attempt/invocation containers are missing")
    definitions._directory(path)


def _write_record(path: Path, record: AttemptRecord | InvocationRecord) -> None:
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
            raise ValueError("duplicate attempt/invocation metadata key")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValueError("nonfinite attempt/invocation metadata constant")


def _read_record[T: AttemptRecord | InvocationRecord](
    path: Path,
    model: type[T],
) -> T:
    with (
        definitions._regular_descriptor(path) as descriptor,
        os.fdopen(descriptor, "rb", closefd=False) as source,
    ):
        data = json.loads(
            source.read().decode("utf-8"),
            object_pairs_hook=_unique_keys,
            parse_constant=_reject_constant,
        )
    return model.model_validate_json(json.dumps(data, allow_nan=False))


def _definition_locked(
    run: RunHandle,
    record: AttemptRecord,
) -> DefinitionHandle:
    selection = definitions._Selection(
        job_uuid=record.job_uuid,
        job_index=record.job_index,
        definition_id=record.definition_id,
    )
    return definitions._read_locked(run, selection)


def _validate_attempt(
    run: RunHandle,
    record: AttemptRecord,
    definition: DefinitionHandle,
) -> None:
    if _identity(record) != _identity(definition.record):
        raise ValueError("attempt run/job identity does not match definition")
    if (record.definition_id, record.definition_sha256) != (
        definition.record.definition_id,
        definition.record.payload.sha256,
    ):
        raise ValueError("attempt definition reference does not match payload")
    if record.created_at < definition.record.created_at:
        raise ValueError("attempt creation precedes definition creation")

    request = run.manifest.request
    if (record.consumer_code_sha256, record.worker_runtime_sha256) != (
        request.workflow.consumer_code_sha256,
        request.runtime.worker_runtime_sha256,
    ):
        raise ValueError(
            "attempt code/runtime differs from immutable request; "
            "changed-runtime amendments are not supported"
        )


def _read_attempt_locked(
    run: RunHandle,
    identity: _Identity,
) -> AttemptHandle:
    parents, path = _attempt_paths(run, identity)
    try:
        _require_paths(parents, path)
        record = _read_record(path / "attempt.json", AttemptRecord)
        if (
            record.run_id,
            record.job_uuid,
            record.job_index,
            record.attempt_id,
        ) != (
            run.manifest.run_id,
            identity.job_uuid,
            identity.job_index,
            identity.attempt_id,
        ):
            raise ValueError("attempt identity does not match requested location")

        definition = _definition_locked(run, record)
        _validate_attempt(run, record, definition)
    except (
        OSError,
        ValueError,
        RecursionError,
        definitions.DefinitionIntegrityError,
    ) as error:
        raise AttemptIntegrityError(run.manifest.run_id, identity, path) from error
    return AttemptHandle(path, record, definition)


def _read_invocation_locked(
    run: RunHandle,
    identity: InvocationRecord | _InvocationSelection,
) -> InvocationHandle:
    parents, path = _invocation_paths(run, identity)
    try:
        _require_paths(parents, path)
        record = _read_record(path / "invocation.json", InvocationRecord)
        if (
            record.run_id,
            record.job_uuid,
            record.job_index,
            record.attempt_id,
            record.invocation_id,
        ) != (
            run.manifest.run_id,
            identity.job_uuid,
            identity.job_index,
            identity.attempt_id,
            identity.invocation_id,
        ):
            raise ValueError("invocation identity does not match requested location")

        attempt = _read_attempt_locked(run, identity)
        validate_attempt_records(attempt.definition.record, attempt.record, record)
    except (
        OSError,
        ValueError,
        RecursionError,
        AttemptIntegrityError,
    ) as error:
        raise AttemptIntegrityError(run.manifest.run_id, identity, path) from error
    return InvocationHandle(path, record, attempt)


def _publish_locked[T: AttemptHandle | InvocationHandle](
    new: T,
    parents: tuple[Path, ...],
    filename: str,
    operation: str,
    read_existing: Callable[[], T],
) -> T:
    record = new.record
    path = new.path
    try:
        containers_exist = definitions._containers_exist(parents)
    except (OSError, ValueError) as error:
        raise AttemptIntegrityError(record.run_id, record, path) from error

    if containers_exist and os.path.lexists(path):
        existing = read_existing()
        if existing.record != record:
            raise AttemptConflictError(record, path)
        try:
            definitions._sync_file(path / filename)
            definitions._sync_directory(path)
            definitions._sync_parents(parents)
        except Exception as error:
            raise AttemptPublicationError(
                record,
                path,
                None,
                operation="acknowledge",
                phase="acknowledge-flush",
                publication_uncertain=True,
            ) from error
        return existing

    staging = None
    phase = "containers"
    publication_uncertain = False
    try:
        for parent in parents[1:]:
            definitions._ensure_directory(parent)

        phase = "staging"
        staging = Path(
            tempfile.mkdtemp(
                prefix=f".staging-{Path(filename).stem}-{path.name}-",
                dir=parents[-1],
            )
        )

        phase = "metadata"
        _write_record(staging / filename, record)

        phase = "staging-flush"
        definitions._sync_directory(staging)

        phase = "rename"
        if os.path.lexists(path):
            raise FileExistsError("attempt/invocation destination appeared")
        publication_uncertain = True
        os.rename(staging, path)

        phase = "parent-flush"
        definitions._sync_parents(parents)
    except Exception as error:
        raise AttemptPublicationError(
            record,
            path,
            staging,
            operation=operation,
            phase=phase,
            publication_uncertain=publication_uncertain,
        ) from error
    return new


def reserve_attempt(
    runs_root: str | Path,
    record: AttemptRecord,
) -> AttemptHandle:
    """Persist an intention, not authorization or an active-attempt decision."""
    record = AttemptRecord.model_validate(record)
    with locked_run(runs_root, record.run_id) as run:
        definition = _definition_locked(run, record)
        _validate_attempt(run, record, definition)
        parents, path = _attempt_paths(run, record)
        return _publish_locked(
            AttemptHandle(path, record, definition),
            parents,
            "attempt.json",
            "reserve",
            lambda: _read_attempt_locked(run, record),
        )


def read_attempt(
    runs_root: str | Path,
    run_id: str,
    job_uuid: str,
    job_index: int,
    attempt_id: str,
) -> AttemptHandle:
    """Verify intention and parent definition without mutation or execution."""
    identity = _AttemptSelection(
        job_uuid=job_uuid,
        job_index=job_index,
        attempt_id=attempt_id,
    )
    job_key(identity.job_uuid)
    with locked_run(runs_root, run_id) as run:
        return _read_attempt_locked(run, identity)


def register_invocation(
    runs_root: str | Path,
    record: InvocationRecord,
) -> InvocationHandle:
    """Persist one entry identity, not liveness, success, or writer exclusion."""
    record = InvocationRecord.model_validate(record)
    with locked_run(runs_root, record.run_id) as run:
        attempt = _read_attempt_locked(run, record)
        validate_attempt_records(attempt.definition.record, attempt.record, record)
        parents, path = _invocation_paths(run, record)
        return _publish_locked(
            InvocationHandle(path, record, attempt),
            parents,
            "invocation.json",
            "register",
            lambda: _read_invocation_locked(run, record),
        )


def read_invocation(
    runs_root: str | Path,
    run_id: str,
    job_uuid: str,
    job_index: int,
    attempt_id: str,
    invocation_id: str,
) -> InvocationHandle:
    """Verify an entry and its parent chain without inspecting worker output."""
    identity = _InvocationSelection(
        job_uuid=job_uuid,
        job_index=job_index,
        attempt_id=attempt_id,
        invocation_id=invocation_id,
    )
    job_key(identity.job_uuid)
    with locked_run(runs_root, run_id) as run:
        return _read_invocation_locked(run, identity)

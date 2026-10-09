"""Create and reopen opaque runs on a trusted, existing POSIX runs root."""

import fcntl
import json
import os
import stat
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path

from pydantic import BaseModel, TypeAdapter

from jobflow_gitlab_slurm.config.request import RunRequest
from jobflow_gitlab_slurm.config.site import SiteConfig, StorageSite
from jobflow_gitlab_slurm.persistence._filesystem import (
    sync_directory as _sync_directory,
)
from jobflow_gitlab_slurm.persistence.artifacts import (
    stage_verified_artifact,
    verify_artifact,
)
from jobflow_gitlab_slurm.persistence.runs.records import (
    ExternalArtifact,
    ExternalArtifacts,
    FlowEnvelope,
    RunId,
    RunManifest,
    snapshot_site,
    validate_run_records,
)


@dataclass(frozen=True)
class RunHandle:
    """Validated metadata and the location of an existing run."""

    path: Path
    manifest: RunManifest
    flow: FlowEnvelope
    artifacts: ExternalArtifacts


class RunBusyError(RuntimeError):
    """Another cooperating process holds the persistent per-run lock."""


class RunCreationError(RuntimeError):
    """Creation failed; inspect these paths before any explicit retry."""

    def __init__(
        self,
        run_id: str,
        path: Path,
        staging_path: Path | None,
        publication_uncertain: bool,
    ) -> None:
        self.run_id = run_id
        self.path = path
        self.staging_path = staging_path
        self.publication_uncertain = publication_uncertain
        super().__init__(
            f"Run creation failed: run_id={run_id}; published_path={path}; "
            f"staging_path={staging_path}; "
            f"publication_uncertain={publication_uncertain}. "
            "Inspect/reopen this same run ID; preserve staging evidence. "
            "Do not automatically create a new run or launch calculations."
        )


def _directory(path: Path) -> None:
    if not stat.S_ISDIR(path.lstat().st_mode):
        raise ValueError(f"expected a real directory, not a symlink: {path}")


def _root(runs_root: str | Path) -> Path:
    root = Path(StorageSite(provider="posix", runs_root=os.fspath(runs_root)).runs_root)
    _directory(root)
    return root


def _run_id(value: str) -> str:
    return TypeAdapter(RunId).validate_python(value, strict=True)


@contextmanager
def _lock(root: Path, run_id: str, *, create: bool) -> Iterator[None]:
    directory = root / ".locks"
    if create:
        directory.mkdir(mode=0o700, exist_ok=True)
    _directory(directory)
    path = directory / f"{run_id}.lock"
    flags = os.O_NOFOLLOW | os.O_NONBLOCK
    flags |= os.O_RDWR | os.O_CREAT if create else os.O_RDONLY
    descriptor = os.open(path, flags, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError(f"lock must be a regular file: {path}")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RunBusyError(f"run {run_id} is busy; retry later: {path}") from error
        if create:
            os.fsync(descriptor)
            _sync_directory(directory)
            _sync_directory(root)
        yield
    finally:
        # Closing releases flock; never unlink a lock that another process may open.
        os.close(descriptor)


def _write_record(path: Path, record: BaseModel) -> None:
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
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValueError(f"nonfinite JSON constant: {value}")


def _read_record(path: Path, model: type[BaseModel]) -> BaseModel:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError(f"record must be a regular file: {path}")
        with os.fdopen(descriptor, "rb", closefd=False) as source:
            data = json.loads(
                source.read().decode("utf-8"),
                object_pairs_hook=_unique_keys,
                parse_constant=_reject_constant,
            )
    finally:
        os.close(descriptor)
    return model.model_validate_json(json.dumps(data, allow_nan=False))


def _validate_references(manifest: RunManifest, artifacts: ExternalArtifacts) -> None:
    if (artifacts.run_id, artifacts.created_at) != (
        manifest.run_id,
        manifest.created_at,
    ):
        raise ValueError("external-artifact identity does not match manifest")
    request = manifest.request
    if (
        artifacts.consumer_code.sha256 != request.workflow.consumer_code_sha256
        or artifacts.worker_runtime.sha256 != request.runtime.worker_runtime_sha256
    ):
        raise ValueError("external-artifact SHA-256 does not match request")


def _external_reference(source: str | Path, digest: str) -> ExternalArtifact:
    path = Path(source).absolute()
    reference = ExternalArtifact(path=str(path), sha256=digest, size_bytes=0)
    verified = verify_artifact(reference.path, reference.sha256)
    return ExternalArtifact(
        path=reference.path, sha256=verified.sha256, size_bytes=verified.size_bytes
    )


def _verify_size(path: Path, digest: str, expected_size: int) -> None:
    verified = verify_artifact(path, digest)
    if verified.size_bytes != expected_size:
        raise ValueError(f"artifact size mismatch: {path}")


def create_run(
    site: SiteConfig,
    request: RunRequest,
    flow_source: str | Path,
    consumer_source: str | Path,
    runtime_source: str | Path,
    *,
    run_id: str,
    workspace_expires_at: str | None = None,
) -> RunHandle:
    """Verify and publish a new run; callers retain one UUIDv4 across retries.

    The root must exist and be trusted. All writers must use this locking
    protocol. Parent directories are trusted; this is not an adversarial
    filesystem sandbox. Existing run IDs are never overwritten or resumed by
    this function: inspect them with open_run instead. Failed staging is retained.
    """
    manifest = RunManifest(
        schema_version=1,
        kind="run-manifest",
        run_id=_run_id(run_id),
        created_at=datetime.now(UTC)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z"),
        backend_version=version("jobflow-gitlab-slurm"),
        jobflow_version=version("jobflow"),
        request=request,
        site_snapshot=snapshot_site(site),
        workspace_expires_at=workspace_expires_at,
    )
    root = _root(manifest.site_snapshot.to_site().storage.runs_root)
    path = root / run_id
    with _lock(root, run_id, create=True):
        if os.path.lexists(path):
            raise FileExistsError(f"run already exists; inspect/reopen it: {path}")
        staging = None
        publication_started = False
        try:
            staging = Path(tempfile.mkdtemp(prefix=f".staging-{run_id}-", dir=root))
            (staging / "flow").mkdir(mode=0o700)
            (staging / "artifacts").mkdir(mode=0o700)
            claims = manifest.request
            artifacts = ExternalArtifacts(
                schema_version=1,
                kind="external-artifacts",
                run_id=manifest.run_id,
                created_at=manifest.created_at,
                consumer_code=_external_reference(
                    consumer_source, claims.workflow.consumer_code_sha256
                ),
                worker_runtime=_external_reference(
                    runtime_source, claims.runtime.worker_runtime_sha256
                ),
            )
            staged = stage_verified_artifact(
                flow_source,
                staging / "flow/payload.json",
                claims.workflow.serialized_flow_sha256,
            )
            flow = FlowEnvelope(
                schema_version=1,
                kind="original-flow",
                run_id=manifest.run_id,
                created_at=manifest.created_at,
                payload={
                    "path": "flow/payload.json",
                    "sha256": staged.sha256,
                    "size_bytes": staged.size_bytes,
                },
            )
            validate_run_records(manifest, flow)
            _validate_references(manifest, artifacts)
            _write_record(staging / "flow/original.json", flow)
            _write_record(staging / "artifacts/references.json", artifacts)
            _write_record(staging / "run.json", manifest)
            _sync_directory(staging / "flow")
            _sync_directory(staging / "artifacts")
            _sync_directory(staging)
            publication_started = True
            os.rename(staging, path)
            _sync_directory(root)
        except Exception as error:
            raise RunCreationError(
                run_id, path, staging, publication_started
            ) from error
    return RunHandle(path, manifest, flow, artifacts)


@contextmanager
def locked_run(
    runs_root: str | Path,
    run_id: str,
    *,
    verify_external: bool = False,
) -> Iterator[RunHandle]:
    """Hold the persistent lock and read validated state without executing code.

    Existing records and lock files are required; this operation creates none.
    External bytes are checked only when requested (required before execution).
    The initial compatibility policy accepts only the installed backend version.
    """
    run_id = _run_id(run_id)
    root = _root(runs_root)
    with _lock(root, run_id, create=False):
        path = root / run_id
        for directory in (path, path / "flow", path / "artifacts"):
            _directory(directory)
        manifest = _read_record(path / "run.json", RunManifest)
        flow = _read_record(path / "flow/original.json", FlowEnvelope)
        artifacts = _read_record(path / "artifacts/references.json", ExternalArtifacts)
        validate_run_records(manifest, flow)
        _validate_references(manifest, artifacts)
        if manifest.run_id != run_id:
            raise ValueError("manifest run_id does not match directory name")
        if manifest.site_snapshot.to_site().storage.runs_root != str(root):
            raise ValueError("manifest runs_root does not match supplied root")
        if manifest.backend_version != version("jobflow-gitlab-slurm"):
            raise ValueError("unsupported backend version; no automatic migration")
        _verify_size(
            path / flow.payload.path, flow.payload.sha256, flow.payload.size_bytes
        )
        if verify_external:
            for reference in (artifacts.consumer_code, artifacts.worker_runtime):
                _verify_size(
                    Path(reference.path), reference.sha256, reference.size_bytes
                )
        yield RunHandle(path, manifest, flow, artifacts)


def open_run(
    runs_root: str | Path,
    run_id: str,
    *,
    verify_external: bool = False,
) -> RunHandle:
    """Read/check a run under its lock; the returned handle does not retain it."""
    with locked_run(runs_root, run_id, verify_external=verify_external) as handle:
        return handle

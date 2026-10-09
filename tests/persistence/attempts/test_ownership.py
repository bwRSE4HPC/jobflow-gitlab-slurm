"""Persistent invocation locks, ownership lifetimes, and lock ordering."""

import fcntl
import hashlib
import json
import os
import stat
import subprocess
import sys
from contextlib import contextmanager
from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from jobflow_gitlab_slurm.config.request import RunRequest
from jobflow_gitlab_slurm.config.site import SiteConfig
from jobflow_gitlab_slurm.persistence.attempts import ownership as locking
from jobflow_gitlab_slurm.persistence.attempts.definitions import publish_job_definition
from jobflow_gitlab_slurm.persistence.attempts.ownership import (
    InvocationBusyError,
    InvocationLockIntegrityError,
    InvocationLockProvisionError,
    InvocationOwnershipError,
    owned_invocation,
    provision_invocation_lock,
)
from jobflow_gitlab_slurm.persistence.attempts.records import (
    AttemptRecord,
    InvocationRecord,
    JobDefinitionRecord,
    job_key,
)
from jobflow_gitlab_slurm.persistence.attempts.storage import (
    AttemptIntegrityError,
    register_invocation,
    reserve_attempt,
)
from jobflow_gitlab_slurm.persistence.runs.storage import (
    RunBusyError,
    create_run,
    locked_run,
)

RUN_ID = "00000000-0000-4000-8000-000000000001"

DEFINITION_ID = "aaaaaaaa-0000-4000-8000-000000000002"

ATTEMPT_ID = "bbbbbbbb-0000-4000-8000-000000000003"

INVOCATION_ID = "cccccccc-0000-4000-8000-000000000004"

SECOND_INVOCATION_ID = "cccccccc-0000-4000-8000-000000000009"

JOB_UUID = "../../opaque-lock-job-é"

FLOW_BYTES = b'{ "@module": "do_not_import_lock_flow" }\r\n'

JOB_BYTES = b'{ "@module": "do_not_import_lock_job", "value": 42 }\r\n'


def timestamp(handle, offset):
    instant = datetime.fromisoformat(handle.manifest.created_at)
    return (
        (instant + timedelta(seconds=offset))
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


@pytest.fixture
def case(tmp_path):
    root = tmp_path / "runs"
    root.mkdir()
    sources = (
        tmp_path / "flow.json",
        tmp_path / "consumer.whl",
        tmp_path / "runtime.sif",
    )
    for path, data in zip(sources, (FLOW_BYTES, b"consumer", b"\0runtime")):
        path.write_bytes(data)
    digests = [hashlib.sha256(path.read_bytes()).hexdigest() for path in sources]

    site = SiteConfig.model_validate(
        {
            "schema_version": 1,
            "site_id": "example-cluster",
            "slurm": {
                "cluster_name": "example-slurm",
                "account": "example-account",
                "partitions": ["short"],
                "default_partition": "short",
            },
            "storage": {"provider": "posix", "runs_root": str(root)},
            "worker": {
                "launch_mode": "apptainer",
                "apptainer_command": "apptainer",
            },
        }
    )
    request = RunRequest.model_validate(
        {
            "schema_version": 1,
            "site_id": site.site_id,
            "workflow": {
                "name": "ownership-example",
                "serialized_flow_sha256": digests[0],
                "consumer_code_sha256": digests[1],
            },
            "runtime": {"worker_runtime_sha256": digests[2]},
            "resources": {
                "partition": "short",
                "nodes": 1,
                "tasks_per_node": 2,
                "cpus_per_task": 1,
                "memory_mb_per_node": 4096,
                "walltime_seconds": 1800,
            },
        }
    )
    handle = create_run(site, request, *sources, run_id=RUN_ID)
    payload = tmp_path / "job-source.json"
    payload.write_bytes(JOB_BYTES)
    key = job_key(JOB_UUID)
    definition_record = JobDefinitionRecord(
        schema_version=1,
        kind="job-definition",
        run_id=RUN_ID,
        created_at=handle.manifest.created_at,
        job_uuid=JOB_UUID,
        job_index=1,
        job_key=key,
        definition_id=DEFINITION_ID,
        jobflow_version="0.3.1",
        payload={
            "path": f"jobs/{key}/index-1/definitions/{DEFINITION_ID}/job.json",
            "sha256": hashlib.sha256(JOB_BYTES).hexdigest(),
            "size_bytes": len(JOB_BYTES),
        },
        origin="original_flow",
        source=handle.flow.payload.model_dump(),
    )
    definition = publish_job_definition(root, definition_record, payload)
    attempt = reserve_attempt(
        root,
        AttemptRecord(
            schema_version=1,
            kind="execution-attempt",
            run_id=RUN_ID,
            created_at=timestamp(handle, 1),
            job_uuid=JOB_UUID,
            job_index=1,
            job_key=key,
            attempt_id=ATTEMPT_ID,
            definition_id=DEFINITION_ID,
            definition_sha256=definition.record.payload.sha256,
            consumer_code_sha256=request.workflow.consumer_code_sha256,
            worker_runtime_sha256=request.runtime.worker_runtime_sha256,
        ),
    )
    invocation = register_invocation(
        root,
        InvocationRecord(
            schema_version=1,
            kind="worker-invocation",
            run_id=RUN_ID,
            created_at=timestamp(handle, 2),
            job_uuid=JOB_UUID,
            job_index=1,
            job_key=key,
            attempt_id=ATTEMPT_ID,
            invocation_id=INVOCATION_ID,
        ),
    )
    return SimpleNamespace(
        root=root,
        handle=handle,
        definition=definition,
        attempt=attempt,
        invocation=invocation,
        lock_path=invocation.path / ".bundle.lock",
    )


def parameters(case, invocation_id=INVOCATION_ID):
    return (
        case.root,
        RUN_ID,
        JOB_UUID,
        1,
        ATTEMPT_ID,
        invocation_id,
    )


def provision(case, invocation_id=INVOCATION_ID):
    return provision_invocation_lock(*parameters(case, invocation_id))


def snapshot(root):
    result = []
    for path in sorted(root.rglob("*")):
        info = path.lstat()
        kind = stat.S_IFMT(info.st_mode)
        if stat.S_ISREG(info.st_mode):
            data = path.read_bytes()
        elif stat.S_ISLNK(info.st_mode):
            data = os.readlink(path)
        else:
            data = None
        result.append(
            (
                path.relative_to(root).as_posix(),
                kind,
                stat.S_IMODE(info.st_mode),
                info.st_dev,
                info.st_ino,
                data,
            )
        )
    return result


def assert_identity(error, case):
    assert error.run_id == RUN_ID
    assert error.job_key == job_key(JOB_UUID)
    assert error.job_index == 1
    assert error.attempt_id == ATTEMPT_ID
    assert error.invocation_id == INVOCATION_ID
    assert error.lock_path == case.lock_path
    assert JOB_UUID not in str(error)


def capture_lock_descriptors(monkeypatch):
    descriptors = []
    original = locking.os.open

    def observed(path, *args, **kwargs):
        descriptor = original(path, *args, **kwargs)
        if os.fspath(path).endswith("/.bundle.lock"):
            descriptors.append(descriptor)
        return descriptor

    monkeypatch.setattr(locking.os, "open", observed)
    return descriptors


def assert_closed(descriptor):
    with pytest.raises(OSError):
        fcntl.fcntl(descriptor, fcntl.F_GETFD)


CHILD_PREFIX = """
import os
import sys

from jobflow_gitlab_slurm.persistence.attempts.ownership import (
    InvocationBusyError,
    owned_invocation,
    provision_invocation_lock,
)

arguments = (
    sys.argv[1],
    sys.argv[2],
    sys.argv[3],
    int(sys.argv[4]),
    sys.argv[5],
    sys.argv[6],
)
"""


def child(case, body, invocation_id=INVOCATION_ID):
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(
        Path(__import__("jobflow_gitlab_slurm").__file__).resolve().parent.parent
    )
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    return subprocess.run(
        [
            sys.executable,
            "-c",
            dedent(CHILD_PREFIX) + dedent(body),
            *(str(value) for value in parameters(case, invocation_id)),
        ],
        cwd=case.root.parent,
        env=environment,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )


def test_provision_creates_only_persistent_lock(case):
    before = snapshot(case.root)
    result = provision(case)

    assert result.invocation == case.invocation
    assert result.lock_path == case.lock_path
    assert case.lock_path.read_bytes() == b""
    assert stat.S_IMODE(case.lock_path.stat().st_mode) == 0o600
    assert not (case.invocation.path / "staging").exists()
    assert not (case.invocation.path / "published").exists()
    assert not (case.handle.path / "events").exists()

    after_without_lock = [
        entry
        for entry in snapshot(case.root)
        if entry[0] != case.lock_path.relative_to(case.root).as_posix()
    ]
    assert after_without_lock == before
    with pytest.raises(FrozenInstanceError):
        result.lock_path = case.root

    assert "do_not_import_lock_flow" not in sys.modules
    assert "do_not_import_lock_job" not in sys.modules


def test_repeated_provision_preserves_inode_and_bytes(case):
    first = provision(case)
    case.lock_path.write_bytes(b"opaque existing lock content")
    before = snapshot(case.root)
    second = provision(case)

    assert second == first
    assert snapshot(case.root) == before


def test_provision_flushes_file_then_invocation_directory(case, monkeypatch):
    calls = []
    original_fsync = locking.os.fsync
    original_sync_directory = locking.definitions._sync_directory

    def sync_file(descriptor):
        calls.append("file")
        original_fsync(descriptor)

    def sync_directory(path):
        calls.append(("directory", path))
        original_sync_directory(path)

    monkeypatch.setattr(locking.os, "fsync", sync_file)
    monkeypatch.setattr(locking.definitions, "_sync_directory", sync_directory)
    provision(case)

    # The directory helper invokes fsync on its own descriptor.
    assert calls == [
        "file",
        ("directory", case.invocation.path),
        "file",
    ]


def test_provision_does_not_acquire_invocation_lock(case, monkeypatch):
    original = locking.fcntl.flock
    lock_descriptors = capture_lock_descriptors(monkeypatch)

    def observed(descriptor, operation):
        assert descriptor not in lock_descriptors
        return original(descriptor, operation)

    monkeypatch.setattr(locking.fcntl, "flock", observed)
    provision(case)


@pytest.mark.parametrize(
    ("position", "value"),
    [
        (1, "../run"),
        (1, "AAAAAAAA-0000-4000-8000-000000000001"),
        (1, None),
        (2, ""),
        (2, None),
        (2, "\ud800"),
        (3, 0),
        (3, True),
        (4, "../attempt"),
        (4, None),
        (5, "../invocation"),
        (5, None),
    ],
)
@pytest.mark.parametrize("operation", ["provision", "own"])
def test_invalid_identity_fails_before_changes(case, position, value, operation):
    arguments = list(parameters(case))
    arguments[position] = value
    before = snapshot(case.root)

    with pytest.raises((ValidationError, ValueError)):
        if operation == "provision":
            provision_invocation_lock(*arguments)
        else:
            with owned_invocation(*arguments):
                pytest.fail("invalid identity acquired ownership")

    assert snapshot(case.root) == before


def test_missing_invocation_is_not_created(case):
    before = snapshot(case.root)
    with pytest.raises(AttemptIntegrityError):
        provision(case, SECOND_INVOCATION_ID)
    assert snapshot(case.root) == before

    with (
        pytest.raises(InvocationLockIntegrityError),
        owned_invocation(*parameters(case, SECOND_INVOCATION_ID)),
    ):
        pytest.fail("missing invocation acquired ownership")
    assert snapshot(case.root) == before


def test_missing_lock_is_not_created_by_ownership(case):
    before = snapshot(case.root)
    with (
        pytest.raises(InvocationLockIntegrityError) as caught,
        owned_invocation(*parameters(case)),
    ):
        pytest.fail("missing lock acquired ownership")

    assert_identity(caught.value, case)
    assert isinstance(caught.value.__cause__, FileNotFoundError)
    assert snapshot(case.root) == before


@pytest.mark.parametrize("operation", ["provision", "own"])
@pytest.mark.parametrize("kind", ["directory", "symlink", "fifo"])
def test_unsafe_lock_is_preserved(case, operation, kind):
    outside = case.root.parent / "outside-lock"
    outside.write_bytes(b"untouched")
    if kind == "directory":
        case.lock_path.mkdir()
    elif kind == "symlink":
        case.lock_path.symlink_to(outside)
    else:
        os.mkfifo(case.lock_path)

    before = snapshot(case.root)
    with pytest.raises(InvocationLockIntegrityError) as caught:
        if operation == "provision":
            provision(case)
        else:
            with owned_invocation(*parameters(case)):
                pytest.fail("unsafe lock acquired ownership")

    assert_identity(caught.value, case)
    assert snapshot(case.root) == before
    assert outside.read_bytes() == b"untouched"


@pytest.mark.parametrize("kind", ["missing", "file", "symlink"])
def test_ownership_rejects_unsafe_path_component(case, kind):
    provision(case)
    original = case.invocation.path
    retained = original.with_name("retained-invocation")
    original.rename(retained)
    if kind == "file":
        original.write_bytes(b"not a directory")
    elif kind == "symlink":
        original.symlink_to(retained, target_is_directory=True)

    before = snapshot(case.root)
    with (
        pytest.raises(InvocationLockIntegrityError) as caught,
        owned_invocation(*parameters(case)),
    ):
        pytest.fail("unsafe path acquired ownership")

    assert_identity(caught.value, case)
    assert snapshot(case.root) == before


def test_provision_requires_valid_invocation_metadata(case):
    metadata = case.invocation.path / "invocation.json"
    metadata.write_bytes(b"invalid JSON")
    before = snapshot(case.root)

    with pytest.raises(AttemptIntegrityError):
        provision(case)

    assert not case.lock_path.exists()
    assert snapshot(case.root) == before


def test_ownership_context_preserves_files_and_releases_run_lock(case):
    provision(case)
    output = case.invocation.path / "staging"
    output.mkdir()
    (output / "partial.bin").write_bytes(b"retained partial output")
    os.mkfifo(output / "diagnostic-fifo")
    (case.invocation.path / "diagnostic-link").symlink_to(
        case.root.parent / "missing-diagnostic"
    )
    before = snapshot(case.root)

    with owned_invocation(*parameters(case)) as ownership:
        assert ownership.invocation == case.invocation
        assert ownership.lock_path == case.lock_path
        assert ownership.require_active() is None
        with pytest.raises(FrozenInstanceError):
            ownership.lock_path = case.root

        # Run metadata remains accessible while invocation ownership is held.
        with locked_run(case.root, RUN_ID) as handle:
            assert handle.manifest.run_id == RUN_ID

        result = child(
            case,
            """
            from jobflow_gitlab_slurm.persistence.runs.storage import locked_run

            with locked_run(arguments[0], arguments[1]):
                print("RUN_LOCK_AVAILABLE")
            """,
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "RUN_LOCK_AVAILABLE"

    with pytest.raises(InvocationOwnershipError) as caught:
        ownership.require_active()
    assert_identity(caught.value, case)
    assert snapshot(case.root) == before


def test_ownership_handle_rejects_other_process_identity(case, monkeypatch):
    provision(case)
    original_pid = os.getpid()
    with owned_invocation(*parameters(case)) as ownership:
        with monkeypatch.context() as patch:
            patch.setattr(locking.os, "getpid", lambda: original_pid + 1)
            with pytest.raises(InvocationOwnershipError) as caught:
                ownership.require_active()
            assert_identity(caught.value, case)
        ownership.require_active()


def test_invocation_before_run_lock_and_run_release_before_yield(case, monkeypatch):
    provision(case)
    lock_info = case.lock_path.stat()
    events = []
    original_flock = locking.fcntl.flock
    original_locked_run = locking.locked_run

    def observed_flock(descriptor, operation):
        info = os.fstat(descriptor)
        if (info.st_dev, info.st_ino) == (
            lock_info.st_dev,
            lock_info.st_ino,
        ):
            original_flock(descriptor, operation)
            events.append("invocation-acquired")
        else:
            original_flock(descriptor, operation)

    @contextmanager
    def observed_run(*args, **kwargs):
        assert events == ["invocation-acquired"]
        events.append("run-enter")
        with original_locked_run(*args, **kwargs) as handle:
            yield handle
        events.append("run-exit")

    monkeypatch.setattr(locking.fcntl, "flock", observed_flock)
    monkeypatch.setattr(locking, "locked_run", observed_run)
    with owned_invocation(*parameters(case)):
        events.append("body")

    assert events == [
        "invocation-acquired",
        "run-enter",
        "run-exit",
        "body",
    ]


def test_metadata_is_revalidated_after_invocation_acquisition(case, monkeypatch):
    provision(case)
    lock_info = case.lock_path.stat()
    original_flock = locking.fcntl.flock

    def corrupt_after_acquiring(descriptor, operation):
        original_flock(descriptor, operation)
        info = os.fstat(descriptor)
        if (info.st_dev, info.st_ino) == (
            lock_info.st_dev,
            lock_info.st_ino,
        ):
            metadata = case.invocation.path / "invocation.json"
            metadata.write_bytes(b"corrupted after lock acquisition")

    with monkeypatch.context() as patch:
        patch.setattr(locking.fcntl, "flock", corrupt_after_acquiring)
        with (
            pytest.raises(AttemptIntegrityError),
            owned_invocation(*parameters(case)),
        ):
            pytest.fail("corrupted metadata acquired ownership")

    # Failure after acquisition must still release the invocation lock.
    descriptor = os.open(case.lock_path, os.O_RDONLY)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    finally:
        os.close(descriptor)


def test_definition_corruption_is_detected_without_lock_replacement(case):
    provision(case)
    inode = case.lock_path.stat().st_ino
    (case.definition.path / "job.json").write_bytes(b"changed payload")
    before = snapshot(case.root)

    with (
        pytest.raises(AttemptIntegrityError),
        owned_invocation(*parameters(case)),
    ):
        pytest.fail("corrupted definition acquired ownership")

    assert case.lock_path.stat().st_ino == inode
    assert snapshot(case.root) == before


def test_same_invocation_excludes_second_process(case):
    provision(case)
    with owned_invocation(*parameters(case)):
        result = child(
            case,
            """
            try:
                with owned_invocation(*arguments):
                    print("UNEXPECTED_OWNERSHIP")
            except InvocationBusyError as error:
                print(error.invocation_id)
                sys.exit(17)
            """,
        )
        assert result.returncode == 17, result.stderr
        assert result.stdout.strip() == INVOCATION_ID

    result = child(
        case,
        """
        with owned_invocation(*arguments) as ownership:
            ownership.require_active()
            print("OWNED")
        """,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "OWNED"


def test_same_process_duplicate_context_is_busy(case):
    provision(case)
    with owned_invocation(*parameters(case)):
        with (
            pytest.raises(InvocationBusyError) as caught,
            owned_invocation(*parameters(case)),
        ):
            pytest.fail("duplicate context acquired ownership")
        assert_identity(caught.value, case)


def test_distinct_invocations_can_be_owned_concurrently(case):
    second_record = case.invocation.record.model_copy(
        update={"invocation_id": SECOND_INVOCATION_ID}
    )
    register_invocation(case.root, second_record)
    provision(case)
    provision(case, SECOND_INVOCATION_ID)

    with owned_invocation(*parameters(case)):
        result = child(
            case,
            """
            with owned_invocation(*arguments) as ownership:
                ownership.require_active()
                print(ownership.invocation.record.invocation_id)
            """,
            invocation_id=SECOND_INVOCATION_ID,
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == SECOND_INVOCATION_ID


def test_provisioning_while_owned_preserves_exclusion_and_inode(case):
    provision(case)
    inode = case.lock_path.stat().st_ino
    with owned_invocation(*parameters(case)):
        result = child(
            case,
            """
            provision_invocation_lock(*arguments)
            print("PROVISIONED")
            try:
                with owned_invocation(*arguments):
                    print("UNEXPECTED_OWNERSHIP")
            except InvocationBusyError:
                print("STILL_BUSY")
            """,
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.splitlines() == ["PROVISIONED", "STILL_BUSY"]
        assert case.lock_path.stat().st_ino == inode


def test_run_contention_releases_acquired_invocation_lock(case):
    provision(case)
    with locked_run(case.root, RUN_ID):
        with (
            pytest.raises(RunBusyError),
            owned_invocation(*parameters(case)),
        ):
            pytest.fail("busy run yielded ownership")

        descriptor = os.open(case.lock_path, os.O_RDONLY)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(descriptor)

    with owned_invocation(*parameters(case)) as ownership:
        ownership.require_active()


def test_run_contention_does_not_provision_lock(case):
    before = snapshot(case.root)
    with locked_run(case.root, RUN_ID), pytest.raises(RunBusyError):
        provision(case)
    assert not case.lock_path.exists()
    assert snapshot(case.root) == before


def test_body_exception_expires_handle_and_closes_descriptor(case, monkeypatch):
    provision(case)
    descriptors = capture_lock_descriptors(monkeypatch)
    with (
        pytest.raises(RuntimeError, match="body failed"),
        owned_invocation(*parameters(case)) as ownership,
    ):
        assert not os.get_inheritable(descriptors[-1])
        raise RuntimeError("body failed")

    assert_closed(descriptors[-1])
    with pytest.raises(InvocationOwnershipError):
        ownership.require_active()

    with owned_invocation(*parameters(case)) as fresh:
        fresh.require_active()


def test_lock_open_error_is_classified_without_mutation(case, monkeypatch):
    provision(case)
    original_open = locking.os.open
    before = snapshot(case.root)

    def fail_lock_open(path, *args, **kwargs):
        if Path(path) == case.lock_path:
            raise PermissionError("injected lock open failure")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(locking.os, "open", fail_lock_open)
    with (
        pytest.raises(InvocationLockIntegrityError) as caught,
        owned_invocation(*parameters(case)),
    ):
        pytest.fail("inaccessible lock acquired ownership")

    assert_identity(caught.value, case)
    assert isinstance(caught.value.__cause__, PermissionError)
    assert snapshot(case.root) == before


def test_fstat_error_closes_open_descriptor(case, monkeypatch):
    provision(case)
    identity = locking._selection(*parameters(case)[1:])
    descriptors = capture_lock_descriptors(monkeypatch)

    def fail_fstat(descriptor):
        raise OSError("injected fstat failure")

    monkeypatch.setattr(locking.os, "fstat", fail_fstat)
    with (
        pytest.raises(InvocationLockIntegrityError) as caught,
        locking._lock_file(identity, case.lock_path, create=False),
    ):
        pytest.fail("invalid descriptor was yielded")

    assert_identity(caught.value, case)
    assert_closed(descriptors[-1])


def test_non_contention_flock_error_closes_descriptor(case, monkeypatch):
    provision(case)
    descriptors = capture_lock_descriptors(monkeypatch)

    def fail_flock(descriptor, operation):
        raise OSError("injected flock failure")

    with monkeypatch.context() as patch:
        patch.setattr(locking.fcntl, "flock", fail_flock)
        with (
            pytest.raises(InvocationLockIntegrityError) as caught,
            owned_invocation(*parameters(case)),
        ):
            pytest.fail("failed flock acquired ownership")

    assert_identity(caught.value, case)
    assert_closed(descriptors[-1])
    with owned_invocation(*parameters(case)):
        pass


@pytest.mark.parametrize("phase", ["file-flush", "directory-flush"])
def test_failed_provision_flush_retains_inode_for_explicit_retry(
    case, monkeypatch, phase
):
    descriptors = capture_lock_descriptors(monkeypatch)

    def fail_flush(*args):
        raise OSError("injected provisioning flush failure")

    with monkeypatch.context() as patch:
        if phase == "file-flush":
            patch.setattr(locking.os, "fsync", fail_flush)
        else:
            patch.setattr(locking.definitions, "_sync_directory", fail_flush)
        with pytest.raises(InvocationLockProvisionError) as caught:
            provision(case)

    error = caught.value
    assert_identity(error, case)
    assert error.phase == phase
    assert error.provisioning_uncertain is True
    assert "same identity" in str(error)
    assert case.lock_path.is_file()
    assert_closed(descriptors[-1])
    inode = case.lock_path.stat().st_ino

    result = provision(case)
    assert result.lock_path.stat().st_ino == inode
    with owned_invocation(*parameters(case)) as ownership:
        ownership.require_active()


def test_abrupt_provisioning_exit_retains_same_lock_for_retry(case):
    result = child(
        case,
        """
        from jobflow_gitlab_slurm.persistence.attempts import ownership as locking

        def stop_before_flush(descriptor):
            os._exit(73)

        locking.os.fsync = stop_before_flush
        provision_invocation_lock(*arguments)
        """,
    )
    assert result.returncode == 73, result.stderr
    assert case.lock_path.is_file()
    inode = case.lock_path.stat().st_ino

    provision(case)
    assert case.lock_path.stat().st_ino == inode
    with owned_invocation(*parameters(case)) as ownership:
        ownership.require_active()


def test_abrupt_owner_exit_releases_lock_without_replacing_it(case):
    provision(case)
    before = snapshot(case.root)
    result = child(
        case,
        """
        with owned_invocation(*arguments) as ownership:
            ownership.require_active()
            os._exit(73)
        """,
    )
    assert result.returncode == 73, result.stderr

    with owned_invocation(*parameters(case)) as ownership:
        ownership.require_active()
    assert snapshot(case.root) == before


def test_second_process_reuses_provisioned_lock_without_replacement(case):
    provision(case)
    before = snapshot(case.root)
    result = child(
        case,
        """
        provision_invocation_lock(*arguments)
        with owned_invocation(*arguments) as ownership:
            ownership.require_active()
            print(ownership.invocation.record.invocation_id)
        """,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == INVOCATION_ID
    assert snapshot(case.root) == before


def test_ownership_does_not_decode_or_execute_payloads(case):
    provision(case)
    with owned_invocation(*parameters(case)) as ownership:
        ownership.require_active()
        assert ownership.invocation.attempt.definition.record.payload.sha256 == (
            hashlib.sha256(JOB_BYTES).hexdigest()
        )

    assert "do_not_import_lock_flow" not in sys.modules
    assert "do_not_import_lock_job" not in sys.modules


def test_parent_attempt_binding_is_revalidated(case):
    provision(case)
    metadata = case.attempt.path / "attempt.json"
    data = json.loads(metadata.read_text(encoding="utf-8"))
    data["consumer_code_sha256"] = "0" * 64
    metadata.write_text(json.dumps(data), encoding="utf-8")
    before = snapshot(case.root)

    with (
        pytest.raises(AttemptIntegrityError),
        owned_invocation(*parameters(case)),
    ):
        pytest.fail("changed attempt binding acquired ownership")

    assert snapshot(case.root) == before

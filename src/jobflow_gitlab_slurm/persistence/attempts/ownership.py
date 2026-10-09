"""Persistent invocation ownership; no bundle mutation or execution."""

import fcntl
import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

from jobflow_gitlab_slurm.persistence.attempts import definitions
from jobflow_gitlab_slurm.persistence.attempts import storage as attempts
from jobflow_gitlab_slurm.persistence.attempts.records import job_key
from jobflow_gitlab_slurm.persistence.attempts.storage import InvocationHandle
from jobflow_gitlab_slurm.persistence.runs import storage as runs
from jobflow_gitlab_slurm.persistence.runs.records import RunId
from jobflow_gitlab_slurm.persistence.runs.storage import locked_run


class _Selection(attempts._InvocationSelection):
    run_id: RunId


class _LockError(RuntimeError):
    def __init__(
        self,
        identity: _Selection,
        lock_path: Path,
        message: str,
    ) -> None:
        self.run_id = identity.run_id
        self.job_key = job_key(identity.job_uuid)
        self.job_index = identity.job_index
        self.attempt_id = identity.attempt_id
        self.invocation_id = identity.invocation_id
        self.lock_path = lock_path
        super().__init__(
            f"{message} "
            f"run_id={self.run_id}; job_key={self.job_key}; "
            f"job_index={self.job_index}; attempt_id={self.attempt_id}; "
            f"invocation_id={self.invocation_id}; lock_path={lock_path}."
        )


class InvocationBusyError(_LockError):
    """Another cooperating process owns this invocation."""

    def __init__(self, identity: _Selection, lock_path: Path) -> None:
        super().__init__(
            identity,
            lock_path,
            "Invocation is busy. Defer and retry this same identity later; "
            "do not steal ownership or launch a duplicate calculation.",
        )


class InvocationLockIntegrityError(_LockError):
    """The required lock or its path is missing, unsafe, or inaccessible."""

    def __init__(self, identity: _Selection, lock_path: Path) -> None:
        super().__init__(
            identity,
            lock_path,
            "Invocation lock integrity failure. Preserve evidence. "
            "Explicitly provision a missing lock only for a valid invocation; "
            "do not replace existing locks or launch calculations.",
        )


class InvocationLockProvisionError(_LockError):
    """Lock provisioning could not be durably acknowledged."""

    def __init__(
        self,
        identity: _Selection,
        lock_path: Path,
        phase: str,
    ) -> None:
        self.phase = phase
        self.provisioning_uncertain = True
        super().__init__(
            identity,
            lock_path,
            f"Invocation lock provisioning failed; phase={phase}; "
            "provisioning_uncertain=True. Preserve the lock inode and "
            "explicitly retry provisioning for this same identity. "
            "No ownership or execution authorization was acknowledged.",
        )


class InvocationOwnershipError(_LockError):
    """An ownership handle is outside its originating process/context."""

    def __init__(self, identity: _Selection, lock_path: Path) -> None:
        super().__init__(
            identity,
            lock_path,
            "Invocation ownership is inactive or belongs to another process. "
            "Acquire a new ownership context; retaining a handle does not "
            "retain the lock or authorize execution.",
        )


@dataclass(frozen=True)
class InvocationLockHandle:
    """Provisioned lock location; this handle conveys no ownership."""

    invocation: InvocationHandle
    lock_path: Path


@dataclass
class _Lease:
    pid: int
    active: bool = True


@dataclass(frozen=True)
class InvocationOwnership:
    """Ownership valid only inside its originating process and context.

    Future bundle operations must call require_active before consuming this
    handle. The lifecycle guard is not an authenticated security capability.
    """

    invocation: InvocationHandle
    lock_path: Path
    _lease: _Lease = field(repr=False, compare=False)

    def require_active(self) -> None:
        """Reject expired or inherited ownership without filesystem mutation."""
        if not self._lease.active or self._lease.pid != os.getpid():
            record = self.invocation.record
            identity = _selection(
                record.run_id,
                record.job_uuid,
                record.job_index,
                record.attempt_id,
                record.invocation_id,
            )
            raise InvocationOwnershipError(identity, self.lock_path)


def _selection(
    run_id: str,
    job_uuid: str,
    job_index: int,
    attempt_id: str,
    invocation_id: str,
) -> _Selection:
    identity = _Selection(
        run_id=run_id,
        job_uuid=job_uuid,
        job_index=job_index,
        attempt_id=attempt_id,
        invocation_id=invocation_id,
    )
    job_key(identity.job_uuid)
    return identity


def _located_lock(root: Path, identity: _Selection) -> Path:
    parts = (
        identity.run_id,
        "jobs",
        job_key(identity.job_uuid),
        f"index-{identity.job_index}",
        "attempts",
        identity.attempt_id,
        "invocations",
        identity.invocation_id,
    )
    lock_path = root.joinpath(*parts, ".bundle.lock")
    current = root
    try:
        for part in parts:
            current = current / part
            definitions._directory(current)
    except (OSError, ValueError) as error:
        raise InvocationLockIntegrityError(identity, lock_path) from error
    return lock_path


@contextmanager
def _lock_file(
    identity: _Selection,
    lock_path: Path,
    *,
    create: bool,
) -> Iterator[int]:
    flags = os.O_NOFOLLOW | os.O_NONBLOCK
    flags |= os.O_RDWR | os.O_CREAT if create else os.O_RDONLY
    try:
        descriptor = os.open(lock_path, flags, 0o600)
    except OSError as error:
        raise InvocationLockIntegrityError(identity, lock_path) from error

    try:
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise ValueError("invocation lock must be a regular file")
        except (OSError, ValueError) as error:
            raise InvocationLockIntegrityError(identity, lock_path) from error
        yield descriptor
    finally:
        # Never unlink/replace a lock another process could already have opened.
        os.close(descriptor)


def provision_invocation_lock(
    runs_root: str | Path,
    run_id: str,
    job_uuid: str,
    job_index: int,
    attempt_id: str,
    invocation_id: str,
) -> InvocationLockHandle:
    """Create/reflush a persistent lock without acquiring invocation ownership.

    The parent invocation must already exist and validate. Provisioning holds
    only the run lock. Existing lock bytes and inode are preserved. No output
    directory or missing invocation is created.
    """
    identity = _selection(run_id, job_uuid, job_index, attempt_id, invocation_id)
    with locked_run(runs_root, identity.run_id) as run:
        invocation = attempts._read_invocation_locked(run, identity)
        lock_path = invocation.path / ".bundle.lock"
        with _lock_file(identity, lock_path, create=True) as descriptor:
            phase = "file-flush"
            try:
                os.fsync(descriptor)
                phase = "directory-flush"
                definitions._sync_directory(invocation.path)
            except OSError as error:
                raise InvocationLockProvisionError(
                    identity, lock_path, phase
                ) from error
        return InvocationLockHandle(invocation, lock_path)


@contextmanager
def owned_invocation(
    runs_root: str | Path,
    run_id: str,
    job_uuid: str,
    job_index: int,
    attempt_id: str,
    invocation_id: str,
) -> Iterator[InvocationOwnership]:
    """Own an existing invocation nonblockingly; never create its lock.

    Acquire invocation ownership before briefly taking the run lock to
    revalidate metadata. Release the run lock before yielding. RunBusyError
    and parent metadata errors retain their existing classifications.

    Ownership coordinates cooperating writers only. It is not proof of
    scheduler termination, orphaned subprocess termination, or permission
    to run or recover a calculation.
    """
    identity = _selection(run_id, job_uuid, job_index, attempt_id, invocation_id)
    root = runs._root(runs_root)
    lock_path = _located_lock(root, identity)
    with _lock_file(identity, lock_path, create=False) as descriptor:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise InvocationBusyError(identity, lock_path) from error
        except OSError as error:
            raise InvocationLockIntegrityError(identity, lock_path) from error

        with locked_run(root, identity.run_id) as run:
            invocation = attempts._read_invocation_locked(run, identity)

        lease = _Lease(pid=os.getpid())
        ownership = InvocationOwnership(invocation, lock_path, lease)
        try:
            yield ownership
        finally:
            lease.active = False

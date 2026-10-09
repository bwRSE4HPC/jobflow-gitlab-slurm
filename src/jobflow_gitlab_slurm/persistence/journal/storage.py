"""Journal replay, append, and explicit same-identity publication recovery."""

import json
import os
import re
import stat
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
from uuid import uuid4

from pydantic import TypeAdapter

from jobflow_gitlab_slurm.persistence.journal.records import (
    MAX_SEQUENCE,
    EventRecord,
    JournalHead,
    canonical_payload,
    decode_event,
    decode_head,
    encode_record,
    make_event,
    make_head,
)
from jobflow_gitlab_slurm.persistence.runs.records import RunId
from jobflow_gitlab_slurm.persistence.runs.storage import locked_run


@dataclass(frozen=True)
class JournalReplay:
    """Validated committed history; the returned snapshot retains no lock."""

    run_id: str
    head: JournalHead | None
    events: tuple[EventRecord, ...]


class JournalIntegrityError(RuntimeError):
    """Journal evidence is unsafe, unreadable, or internally inconsistent."""

    def __init__(self, run_id: str, path: Path, reason: str) -> None:
        self.run_id = run_id
        self.path = path
        self.reason = reason
        super().__init__(
            f"Journal integrity failure: run_id={run_id}; path={path}; "
            f"reason={reason}. Preserve evidence and hold this run. "
            "Do not regenerate the anchor, truncate history, or relaunch "
            "calculations automatically."
        )


class JournalIncompleteError(RuntimeError):
    """One valid event is visible but not committed by the head."""

    def __init__(
        self,
        run_id: str,
        head_path: Path,
        head: JournalHead,
        pending_path: Path,
        pending_event: EventRecord,
    ) -> None:
        self.run_id = run_id
        self.head_path = head_path
        self.head = head
        self.committed_count = head.last_sequence
        self.pending_path = pending_path
        self.pending_event = pending_event
        super().__init__(
            f"Journal publication incomplete: run_id={run_id}; "
            f"head_path={head_path}; committed_count={head.last_sequence}; "
            f"pending_event_id={pending_event.event_id}; "
            f"pending_sequence={pending_event.sequence}; "
            f"pending_path={pending_path}. Preserve evidence and hold this run. "
            "Recovery requires an explicit matching append_event retry with "
            "the same event ID, type, and canonical payload. "
            "Do not adopt the tail, create a new event ID, "
            "or relaunch calculations automatically."
        )


def _mode(path: Path, run_id: str) -> int | None:
    try:
        return path.lstat().st_mode
    except FileNotFoundError:
        return None
    except OSError as error:
        raise JournalIntegrityError(
            run_id, path, "cannot inspect journal path"
        ) from error


def _read_bytes(path: Path) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError("journal record must be a regular file")
        with os.fdopen(descriptor, "rb", closefd=False) as source:
            return source.read()
    finally:
        os.close(descriptor)


def _load[T: EventRecord | JournalHead](
    path: Path,
    run_id: str,
    decoder: Callable[[bytes], T],
) -> T:
    try:
        return decoder(_read_bytes(path))
    except (OSError, ValueError) as error:
        raise JournalIntegrityError(
            run_id,
            path,
            "record is unreadable or violates event-json-v1; "
            "inspect the file and chained cause",
        ) from error


def _history(directory: Path, run_id: str) -> tuple[EventRecord, ...]:
    try:
        paths = sorted(directory.iterdir(), key=lambda entry: entry.name)
    except OSError as error:
        raise JournalIntegrityError(
            run_id, directory, "cannot enumerate journal events"
        ) from error

    events: list[EventRecord] = []
    event_ids: set[str] = set()
    previous_sha256 = None

    for path in paths:
        if path.name.startswith(".staging-"):
            # Reserved unpublished evidence is neither replayed nor removed.
            continue
        if re.fullmatch(r"[0-9]{12}\.json", path.name) is None:
            raise JournalIntegrityError(
                run_id, path, "unexpected entry in events directory"
            )

        event = _load(path, run_id, decode_event)
        if event.run_id != run_id:
            raise JournalIntegrityError(
                run_id, path, "event run_id does not match the opened run"
            )
        if int(path.stem) != event.sequence:
            raise JournalIntegrityError(
                run_id, path, "event sequence does not match its filename"
            )
        if event.sequence != len(events) + 1:
            raise JournalIntegrityError(
                run_id, path, "event sequences must be contiguous from 1"
            )
        if event.event_id in event_ids:
            raise JournalIntegrityError(
                run_id, path, "duplicate event_id in journal history"
            )
        if event.previous_sha256 != previous_sha256:
            raise JournalIntegrityError(
                run_id, path, "event predecessor checksum does not match history"
            )

        events.append(event)
        event_ids.add(event.event_id)
        previous_sha256 = event.sha256

    return tuple(events)


def _read_events_locked(path: Path, run_id: str) -> JournalReplay:
    """Internal read-only replay; the caller already owns this run's lock.

    This reader never acquires a lock, flushes, repairs, or retries. Both the
    ordinary reader and run queries use it within their existing lock context;
    contextual integrity/incomplete exceptions propagate unchanged.
    """
    head_path = path / "journal-head.json"
    directory = path / "events"
    head_mode = _mode(head_path, run_id)
    directory_mode = _mode(directory, run_id)

    if directory_mode is not None and not stat.S_ISDIR(directory_mode):
        raise JournalIntegrityError(
            run_id, directory, "events path must be a real directory, not a symlink"
        )

    if head_mode is None:
        if directory_mode is not None:
            raise JournalIntegrityError(
                run_id, head_path, "events directory exists without a valid head"
            )
        return JournalReplay(run_id=run_id, head=None, events=())

    head = _load(head_path, run_id, decode_head)
    if head.run_id != run_id:
        raise JournalIntegrityError(
            run_id, head_path, "head run_id does not match the opened run"
        )

    events = _history(directory, run_id) if directory_mode is not None else ()

    if head.last_sequence > len(events):
        raise JournalIntegrityError(run_id, head_path, "head anchors missing events")

    anchored_sha256 = (
        events[head.last_sequence - 1].sha256 if head.last_sequence else None
    )
    if head.last_sha256 != anchored_sha256:
        raise JournalIntegrityError(
            run_id, head_path, "head checksum does not match its anchored event"
        )

    unanchored_count = len(events) - head.last_sequence
    if unanchored_count == 1:
        pending = events[-1]
        raise JournalIncompleteError(
            run_id,
            head_path,
            head,
            directory / f"{pending.sequence:012d}.json",
            pending,
        )
    if unanchored_count > 1:
        raise JournalIntegrityError(
            run_id,
            directory,
            "multiple unanchored events require explicit investigation",
        )

    return JournalReplay(run_id=run_id, head=head, events=events)


def read_events(
    runs_root: str | Path,
    run_id: str,
    *,
    verify_external: bool = False,
) -> JournalReplay:
    """Validate an existing run and replay only fully anchored journal history.

    The existing run lock is acquired once. RunBusyError and storage-validation
    errors propagate unchanged. JournalIntegrityError holds corrupt/unsafe
    history; JournalIncompleteError holds one valid uncommitted tail.

    No journal files are created, modified, repaired, flushed, or removed.
    Consumer code is not imported and Slurm is not contacted. External artifacts
    are checked only when requested. The returned snapshot does not retain the
    lock and does not authorize subsequent scheduling without reconciliation.

    Parent directories and cooperating writers are trusted, as in storage.py.
    Cross-host visibility and durability remain independent live-site gates.
    """
    with locked_run(runs_root, run_id, verify_external=verify_external) as handle:
        return _read_events_locked(handle.path, handle.manifest.run_id)


class JournalEventConflictError(ValueError):
    """A committed event ID was reused with different semantic input."""

    def __init__(self, run_id: str, event_id: str, path: Path) -> None:
        self.run_id = run_id
        self.event_id = event_id
        self.path = path
        super().__init__(
            f"Journal event conflict: run_id={run_id}; event_id={event_id}; "
            f"path={path}. This ID already identifies different event input. "
            "Inspect the recorded event; do not overwrite it. A new ID requires "
            "a separately authorized operation, not an automatic retry."
        )


@dataclass
class _AppendState:
    run_id: str
    event_id: str
    run_path: Path
    event_path: Path
    head_path: Path
    sequence: int
    operation: Literal["append", "retry", "recover"]
    phase: str = "prepare"
    event_staging_path: Path | None = None
    head_staging_path: Path | None = None
    publication_uncertain: bool = False


class JournalPublicationError(RuntimeError):
    """Publication or durability acknowledgment failed; evidence is retained."""

    def __init__(self, state: _AppendState) -> None:
        self.run_id = state.run_id
        self.event_id = state.event_id
        self.run_path = state.run_path
        self.event_path = state.event_path
        self.head_path = state.head_path
        self.sequence = state.sequence
        self.operation = state.operation
        self.phase = state.phase
        self.event_staging_path = state.event_staging_path
        self.head_staging_path = state.head_staging_path
        self.publication_uncertain = state.publication_uncertain
        super().__init__(
            f"Journal publication failed: run_id={self.run_id}; "
            f"event_id={self.event_id}; sequence={self.sequence}; "
            f"operation={self.operation}; phase={self.phase}; "
            f"event_path={self.event_path}; head_path={self.head_path}; "
            f"event_staging_path={self.event_staging_path}; "
            f"head_staging_path={self.head_staging_path}; "
            f"publication_uncertain={self.publication_uncertain}. "
            "Inspect these paths and the chained cause; preserve evidence. "
            "Explicitly retry the same run/event ID, type, and payload. "
            "Do not infer that the event is absent, remove staging evidence, "
            "or automatically relaunch calculations."
        )


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _write_staged(path: Path, record: EventRecord | JournalHead) -> None:
    data = encode_record(record)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as output:
            output.write(data)
            output.flush()
            os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _sync_record(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError("journal acknowledgment requires a regular file")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _rename_new(source: Path, destination: Path) -> None:
    # The run lock excludes cooperating writers. This check is not protection
    # against an adversarial writer racing rename in an untrusted directory.
    if os.path.lexists(destination):
        raise FileExistsError(f"refusing to replace journal evidence: {destination}")
    os.rename(source, destination)


def _publish_head(
    state: _AppendState,
    head: JournalHead,
    *,
    replace: bool,
) -> None:
    state.head_staging_path = state.run_path / f".staging-journal-head-{uuid4().hex}"
    state.phase = "head-write"
    _write_staged(state.head_staging_path, head)

    state.phase = "head-publish"
    # Conservative: an exception at a publication call does not prove absence.
    state.publication_uncertain = True
    if replace:
        os.replace(state.head_staging_path, state.head_path)
    else:
        _rename_new(state.head_staging_path, state.head_path)

    state.phase = "head-directory-sync"
    _sync_directory(state.run_path)


def _initialize(
    state: _AppendState,
    head: JournalHead | None,
) -> None:
    if head is None:
        zero_head = make_head(
            run_id=state.run_id,
            updated_at=_utc_now(),
            last_sequence=0,
            last_sha256=None,
        )
        _publish_head(state, zero_head, replace=False)
    else:
        # An existing zero head may be visible after a failed initialization
        # flush. Re-acknowledge it before creating or using events/.
        state.phase = "head-sync"
        _sync_record(state.head_path)
        state.phase = "run-directory-sync"
        _sync_directory(state.run_path)

    directory = state.run_path / "events"
    state.phase = "events-directory-create"
    directory.mkdir(mode=0o700, exist_ok=True)

    state.phase = "events-directory-sync"
    _sync_directory(directory)
    state.phase = "run-directory-sync"
    _sync_directory(state.run_path)


def _advance_head(state: _AppendState, event: EventRecord) -> None:
    head = make_head(
        run_id=state.run_id,
        updated_at=_utc_now(),
        last_sequence=event.sequence,
        last_sha256=event.sha256,
    )
    _publish_head(state, head, replace=True)


def _append_new(
    state: _AppendState,
    event: EventRecord,
    head: JournalHead | None,
) -> None:
    _initialize(state, head)

    state.event_staging_path = (
        state.run_path / "events" / f".staging-{state.event_id}-{uuid4().hex}.json"
    )
    state.phase = "event-write"
    _write_staged(state.event_staging_path, event)

    state.phase = "event-publish"
    state.publication_uncertain = True
    _rename_new(state.event_staging_path, state.event_path)

    state.phase = "events-directory-sync"
    _sync_directory(state.run_path / "events")
    _advance_head(state, event)


def _recover_tail(state: _AppendState, event: EventRecord) -> None:
    state.publication_uncertain = True
    state.phase = "recovery-event-sync"
    _sync_record(state.event_path)
    state.phase = "recovery-events-directory-sync"
    _sync_directory(state.run_path / "events")
    _advance_head(state, event)


def _ack_existing(state: _AppendState) -> None:
    # Even an older matching retry acknowledges the current head. It never
    # rewrites the head or moves it backward to the retried event.
    state.publication_uncertain = True
    state.phase = "retry-event-sync"
    _sync_record(state.event_path)
    state.phase = "retry-events-directory-sync"
    _sync_directory(state.run_path / "events")
    state.phase = "retry-head-sync"
    _sync_record(state.head_path)
    state.phase = "retry-run-directory-sync"
    _sync_directory(state.run_path)


def _matches(event: EventRecord, event_type: str, payload_json: str) -> bool:
    return event.event_type == event_type and event.payload_json == payload_json


def append_event(
    runs_root: str | Path,
    run_id: str,
    *,
    event_id: str,
    event_type: str,
    payload: dict[str, object],
    verify_external: bool = False,
) -> EventRecord:
    """Append or explicitly reconcile one caller-retained event identity.

    Validate all visible journal history under the existing run lock. A matching
    committed ID returns its original record after durability acknowledgment.
    A conflicting committed ID raises JournalEventConflictError.

    One valid unanchored tail can be finalized only by an exact matching retry.
    Other retries/new events remain held by JournalIncompleteError; corrupt
    history remains held by JournalIntegrityError.

    Publication failures raise JournalPublicationError with the original cause
    and evidence paths. Failed staging is never automatically removed. Retain
    the same event identity/input across retries; no scientific rerun is implied.

    All writers and parent directories must be trusted and use the run lock.
    Local flush/rename operations do not establish cross-host durability.
    """
    event_id = TypeAdapter(RunId).validate_python(event_id, strict=True)
    if (
        type(event_type) is not str
        or re.fullmatch(r"[a-z][a-z0-9_.-]*", event_type) is None
    ):
        raise ValueError("invalid event_type identifier")
    payload_json = canonical_payload(payload)

    with locked_run(runs_root, run_id, verify_external=verify_external) as handle:
        head = None
        operation: Literal["append", "retry", "recover"]

        try:
            replay = _read_events_locked(handle.path, handle.manifest.run_id)
        except JournalIncompleteError as incomplete:
            pending = incomplete.pending_event
            if pending.event_id != event_id or not _matches(
                pending, event_type, payload_json
            ):
                raise
            event = pending
            operation = "recover"
        else:
            head = replay.head
            recorded = next(
                (item for item in replay.events if item.event_id == event_id),
                None,
            )
            if recorded is not None:
                if not _matches(recorded, event_type, payload_json):
                    raise JournalEventConflictError(
                        run_id,
                        event_id,
                        handle.path / "events" / f"{recorded.sequence:012d}.json",
                    )
                event = recorded
                operation = "retry"
            else:
                if len(replay.events) >= MAX_SEQUENCE:
                    raise ValueError("journal sequence space exhausted")
                event = make_event(
                    run_id=handle.manifest.run_id,
                    event_id=event_id,
                    sequence=len(replay.events) + 1,
                    created_at=_utc_now(),
                    event_type=event_type,
                    payload=_json_payload(payload_json),
                    previous_sha256=head.last_sha256 if head is not None else None,
                )
                operation = "append"

        state = _AppendState(
            run_id=handle.manifest.run_id,
            event_id=event_id,
            run_path=handle.path,
            event_path=handle.path / "events" / f"{event.sequence:012d}.json",
            head_path=handle.path / "journal-head.json",
            sequence=event.sequence,
            operation=operation,
        )
        try:
            if operation == "append":
                _append_new(state, event, head)
            elif operation == "recover":
                _recover_tail(state, event)
            else:
                _ack_existing(state)
        except Exception as error:
            raise JournalPublicationError(state) from error

        return event


def _json_payload(payload_json: str) -> dict[str, object]:
    """Detach already validated canonical JSON without custom decoding."""
    return json.loads(payload_json)

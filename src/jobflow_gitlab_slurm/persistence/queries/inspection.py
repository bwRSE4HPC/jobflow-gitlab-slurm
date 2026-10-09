"""Read-only metadata and journal inspection under one existing run lock."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from jobflow_gitlab_slurm.persistence.journal import storage as journal
from jobflow_gitlab_slurm.persistence.runs.storage import RunHandle, locked_run

JournalStatus = Literal[
    "uninitialized",
    "valid",
    "incomplete",
    "invalid",
    "not_checked",
]

JOURNAL_STATUSES: tuple[JournalStatus, ...] = (
    "uninitialized",
    "valid",
    "incomplete",
    "invalid",
    "not_checked",
)


def _path(value: Path | None) -> str | None:
    return str(value) if value is not None else None


@dataclass(frozen=True)
class JournalInspection:
    """Detached structural findings, not scientific state or repair authority."""

    status: JournalStatus
    hint: str
    head_path: Path | None = None
    events_path: Path | None = None
    committed_count: int | None = None
    head_sha256: str | None = None
    last_event_sha256: str | None = None
    pending_event_id: str | None = None
    pending_sequence: int | None = None
    pending_event_sha256: str | None = None
    pending_path: Path | None = None
    problem_path: Path | None = None
    error_type: str | None = None
    error: str | None = None

    def to_report(self) -> dict[str, Any]:
        """Return fresh JSON-compatible metadata without event payloads."""
        return {
            "schema_version": 1,
            "kind": "journal-inspection",
            "status": self.status,
            "checked": self.status != "not_checked",
            "head_path": _path(self.head_path),
            "events_path": _path(self.events_path),
            "committed_count": self.committed_count,
            "head_sha256": self.head_sha256,
            "last_event_sha256": self.last_event_sha256,
            "pending_event_id": self.pending_event_id,
            "pending_sequence": self.pending_sequence,
            "pending_event_sha256": self.pending_event_sha256,
            "pending_path": _path(self.pending_path),
            "problem_path": _path(self.problem_path),
            "error_type": self.error_type,
            "error": self.error,
            "hint": self.hint,
        }


JOURNAL_NOT_CHECKED = JournalInspection(
    status="not_checked",
    hint=(
        "Metadata or lock validation prevented journal inspection. "
        "Inspect the metadata error, or retry later if the run is busy. "
        "Do not remove locks, infer an empty journal, or relaunch calculations."
    ),
)


@dataclass(frozen=True)
class RunInspection:
    """Metadata and journal findings observed during the same lock context."""

    handle: RunHandle
    journal: JournalInspection


def _inspect_journal_locked(handle: RunHandle) -> JournalInspection:
    head_path = handle.path / "journal-head.json"
    events_path = handle.path / "events"

    try:
        # Package-internal reader: the caller already owns locked_run.
        # Calling public read_events here would try to acquire the lock again.
        replay = journal._read_events_locked(handle.path, handle.manifest.run_id)
    except journal.JournalIncompleteError as error:
        pending = error.pending_event
        return JournalInspection(
            status="incomplete",
            hint=(
                "Preserve evidence and hold this run. Explicit recovery requires "
                "an append_event retry with the same pending event ID, type, "
                "and canonical payload. Inspection performs no recovery or "
                "scientific rerun."
            ),
            head_path=head_path,
            events_path=events_path,
            committed_count=error.committed_count,
            head_sha256=error.head.sha256,
            last_event_sha256=error.head.last_sha256,
            pending_event_id=pending.event_id,
            pending_sequence=pending.sequence,
            pending_event_sha256=pending.sha256,
            pending_path=error.pending_path,
            problem_path=error.pending_path,
            error_type=type(error).__name__,
            error="One valid event remains unanchored.",
        )
    except journal.JournalIntegrityError as error:
        return JournalInspection(
            status="invalid",
            hint=(
                "Hold this run and inspect the problem path and preserved "
                "journal evidence. Do not truncate history, regenerate the "
                "anchor, adopt records, or relaunch calculations automatically."
            ),
            head_path=head_path,
            events_path=events_path,
            problem_path=error.path,
            error_type=type(error).__name__,
            # Do not expose chained decoder exceptions or their input values.
            error=error.reason,
        )

    if replay.head is None:
        return JournalInspection(
            status="uninitialized",
            hint=(
                "No journal is initialized. Inspection creates nothing and "
                "does not infer a scientific execution state."
            ),
            head_path=head_path,
            events_path=events_path,
            committed_count=0,
        )

    return JournalInspection(
        status="valid",
        hint=(
            "Journal structure and anchor validated. Scientific execution "
            "state is not evaluated; this is not scheduling authorization."
        ),
        head_path=head_path,
        events_path=events_path,
        committed_count=replay.head.last_sequence,
        head_sha256=replay.head.sha256,
        last_event_sha256=replay.head.last_sha256,
    )


def inspect_run(
    runs_root: str | Path,
    run_id: str,
    *,
    verify_external: bool = False,
) -> RunInspection:
    """Inspect metadata and journal during one nonblocking run-lock context.

    Metadata/lock failures propagate unchanged. Journal failures are classified
    without exposing payload bodies or chained decoder errors. The returned
    snapshot retains no lock and authorizes neither scheduling nor recovery.

    No filesystem mutation, consumer-code import, scientific execution, or
    Slurm operation occurs. External artifacts are hashed only when requested.
    """
    with locked_run(runs_root, run_id, verify_external=verify_external) as handle:
        return RunInspection(
            handle=handle,
            journal=_inspect_journal_locked(handle),
        )

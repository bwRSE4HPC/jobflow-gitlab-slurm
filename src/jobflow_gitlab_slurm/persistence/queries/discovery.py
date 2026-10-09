"""Discover metadata and journal findings without scheduling or execution."""

import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from pydantic import TypeAdapter

from jobflow_gitlab_slurm.config.site import StorageSite
from jobflow_gitlab_slurm.persistence.queries.inspection import (
    JOURNAL_NOT_CHECKED,
    JournalInspection,
    inspect_run,
)
from jobflow_gitlab_slurm.persistence.runs.records import RunId
from jobflow_gitlab_slurm.persistence.runs.storage import RunBusyError, RunHandle

_RUN_ID = TypeAdapter(RunId)


@dataclass(frozen=True)
class DiscoveredRun:
    """One discovery result; metadata and journal classifications are separate."""

    path: Path
    run_id: str | None
    status: Literal["valid", "busy", "invalid"]
    handle: RunHandle | None = None
    error: str | None = None
    journal: JournalInspection = JOURNAL_NOT_CHECKED


def _inspect_entry(
    root: Path,
    entry: Path,
    *,
    verify_external: bool,
) -> DiscoveredRun:
    run_id = None
    try:
        run_id = _RUN_ID.validate_python(entry.name, strict=True)
        if not stat.S_ISDIR(entry.lstat().st_mode):
            raise ValueError(f"run entry must be a real directory: {entry}")
        inspected = inspect_run(root, run_id, verify_external=verify_external)
    except RunBusyError as error:
        return DiscoveredRun(
            path=entry,
            run_id=run_id,
            status="busy",
            error=str(error),
        )
    except (OSError, ValueError) as error:
        return DiscoveredRun(
            path=entry,
            run_id=run_id,
            status="invalid",
            error=str(error),
        )
    return DiscoveredRun(
        path=entry,
        run_id=run_id,
        status="valid",
        handle=inspected.handle,
        journal=inspected.journal,
    )


def discover_runs(
    runs_root: str | Path,
    *,
    verify_external: bool = False,
) -> tuple[DiscoveredRun, ...]:
    """Check immediate entries deterministically under individual run locks.

    The root must exist, be trusted, and not itself be a symlink. Parent
    directories are trusted. Only the operational .locks and .staging-*
    namespaces are ignored. Other unexpected entries are reported as invalid.

    Metadata status is separate from journal status. Each run's metadata and
    journal are inspected during one existing lock context, but this is not an
    atomic root snapshot. Entries may change between checks.

    No directories, records, or locks are created by discovery. Stored Flow
    bytes are verified; external files are checked only when requested.
    """
    binding = StorageSite(provider="posix", runs_root=os.fspath(runs_root))
    root = Path(binding.runs_root)
    if not stat.S_ISDIR(root.lstat().st_mode):
        raise ValueError(f"runs root must be a real directory: {root}")

    results = []
    for entry in sorted(root.iterdir(), key=lambda candidate: candidate.name):
        if entry.name == ".locks" or entry.name.startswith(".staging-"):
            continue
        results.append(_inspect_entry(root, entry, verify_external=verify_external))
    return tuple(results)

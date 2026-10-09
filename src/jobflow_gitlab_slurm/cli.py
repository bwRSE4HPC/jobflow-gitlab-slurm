"""Offline validation and POSIX run creation, inspection, and discovery."""

import argparse
import json
import sys
from collections.abc import Sequence
from typing import Any

import yaml
from pydantic import ValidationError

from jobflow_gitlab_slurm.config.loader import load_request_file, load_site_file
from jobflow_gitlab_slurm.config.registry import SiteRegistry
from jobflow_gitlab_slurm.config.request import validate_for_site
from jobflow_gitlab_slurm.persistence.queries.discovery import (
    DiscoveredRun,
    discover_runs,
)
from jobflow_gitlab_slurm.persistence.queries.inspection import (
    JOURNAL_NOT_CHECKED,
    JOURNAL_STATUSES,
    JournalInspection,
    inspect_run,
)
from jobflow_gitlab_slurm.persistence.runs.storage import (
    RunBusyError,
    RunCreationError,
    RunHandle,
    create_run,
)


def _print_json(document: dict[str, Any], *, error: bool = False) -> None:
    print(
        json.dumps(document, indent=2, sort_keys=True, allow_nan=False),
        file=sys.stderr if error else sys.stdout,
    )


def _run_report(
    handle: RunHandle,
    *,
    kind: str,
    external_verified: bool,
    journal_report: JournalInspection | None = None,
) -> dict[str, Any]:
    manifest = handle.manifest
    report = {
        "schema_version": 1,
        "kind": kind,
        "run_id": manifest.run_id,
        "path": str(handle.path),
        "created_at": manifest.created_at,
        "workspace_expires_at": manifest.workspace_expires_at,
        "backend_version": manifest.backend_version,
        "jobflow_version": manifest.jobflow_version,
        "site_id": manifest.request.site_id,
        "site_snapshot_sha256": manifest.site_snapshot.sha256,
        "request": manifest.request.model_dump(mode="json"),
        "metadata_status": "valid",
        "execution_state": "not_evaluated",
        "checks": {
            "flow_bytes_verified": True,
            "external_artifacts_verified": external_verified,
        },
        "flow_payload": {
            "path": str(handle.path / handle.flow.payload.path),
            "sha256": handle.flow.payload.sha256,
            "size_bytes": handle.flow.payload.size_bytes,
        },
        "consumer_code": handle.artifacts.consumer_code.model_dump(mode="json"),
        "worker_runtime": handle.artifacts.worker_runtime.model_dump(mode="json"),
    }
    if journal_report is not None:
        report["journal"] = journal_report.to_report()
    return report


def _entry_report(
    entry: DiscoveredRun,
    *,
    external_verified: bool,
) -> dict[str, Any]:
    report = {
        "name": entry.path.name,
        "path": str(entry.path),
        "run_id": entry.run_id,
        "metadata_status": entry.status,
        "error": entry.error,
        "journal": entry.journal.to_report(),
    }
    if entry.handle is not None:
        report["inspection"] = _run_report(
            entry.handle,
            kind="run-inspection",
            external_verified=external_verified,
            journal_report=entry.journal,
        )
    return report


def _command_error(
    command: str,
    error: Exception,
    hint: str,
    **details: Any,
) -> None:
    if command in {"inspect-run", "list-runs"}:
        details.setdefault("journal", JOURNAL_NOT_CHECKED.to_report())
    _print_json(
        {
            "schema_version": 1,
            "kind": "command-error",
            "command": command,
            "error_type": type(error).__name__,
            "error": str(error),
            "hint": hint,
            **details,
        },
        error=True,
    )


def _validate(args: argparse.Namespace) -> int:
    site = load_site_file(args.site_file)
    if args.command == "validate-site":
        print(f"Site validation passed: {site.site_id}")
    else:
        request = validate_for_site(load_request_file(args.request_file), site)
        print(
            f"Run request validation passed: {request.workflow.name} on {site.site_id}"
        )
    return 0


def _create(args: argparse.Namespace) -> int:
    request = load_request_file(args.request_file)
    registry = SiteRegistry.from_directory(args.sites_directory)
    site = registry.select(request.site_id)
    handle = create_run(
        site,
        request,
        args.flow,
        args.consumer_code,
        args.worker_runtime,
        run_id=args.run_id,
        workspace_expires_at=args.workspace_expires_at,
    )
    _print_json(_run_report(handle, kind="run-created", external_verified=True))
    return 0


def _inspection_exit(
    *,
    invalid: bool,
    incomplete: bool,
    busy: bool,
) -> int:
    if invalid:
        return 2
    if incomplete:
        return 4
    if busy:
        return 3
    return 0


def _inspect(args: argparse.Namespace) -> int:
    inspected = inspect_run(
        args.runs_root,
        args.run_id,
        verify_external=args.verify_external,
    )
    _print_json(
        _run_report(
            inspected.handle,
            kind="run-inspection",
            external_verified=args.verify_external,
            journal_report=inspected.journal,
        )
    )
    return _inspection_exit(
        invalid=inspected.journal.status == "invalid",
        incomplete=inspected.journal.status == "incomplete",
        busy=False,
    )


def _list(args: argparse.Namespace) -> int:
    entries = discover_runs(
        args.runs_root,
        verify_external=args.verify_external,
    )
    counts = {
        status: sum(entry.status == status for entry in entries)
        for status in ("valid", "busy", "invalid")
    }
    journal_counts = {
        status: sum(entry.journal.status == status for entry in entries)
        for status in JOURNAL_STATUSES
    }
    _print_json(
        {
            "schema_version": 1,
            "kind": "run-list",
            "runs_root": args.runs_root,
            "execution_state": "not_evaluated",
            "atomic_snapshot": False,
            "verify_external_requested": args.verify_external,
            "counts": counts,
            "journal_counts": journal_counts,
            "runs": [
                _entry_report(
                    entry,
                    external_verified=args.verify_external,
                )
                for entry in entries
            ],
        }
    )
    return _inspection_exit(
        invalid=bool(counts["invalid"] or journal_counts["invalid"]),
        incomplete=bool(journal_counts["incomplete"]),
        busy=bool(counts["busy"]),
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="jobflow-gitlab-slurm")
    commands = parser.add_subparsers(dest="command", required=True)

    validate = commands.add_parser("validate-site", help="validate a site YAML file")
    validate.add_argument("site_file")

    validate_request = commands.add_parser(
        "validate-request",
        help="validate a run request against one site YAML file",
    )
    validate_request.add_argument("request_file")
    validate_request.add_argument("site_file")

    create = commands.add_parser(
        "create-run",
        help="verify artifacts and publish a run; does not submit jobs",
    )
    create.add_argument("request_file")
    create.add_argument("--sites-directory", required=True)
    create.add_argument(
        "--run-id",
        required=True,
        help="caller-retained lowercase UUIDv4; reuse it after interruption",
    )
    create.add_argument("--flow", required=True)
    create.add_argument("--consumer-code", required=True)
    create.add_argument("--worker-runtime", required=True)
    create.add_argument("--workspace-expires-at")

    inspect = commands.add_parser(
        "inspect-run",
        help="verify Flow, metadata, and journal without executing workflow code",
    )
    inspect.add_argument("run_id")
    inspect.add_argument("--runs-root", required=True)
    inspect.add_argument(
        "--verify-external",
        action="store_true",
        help="also hash consumer-code and runtime files",
    )

    listing = commands.add_parser(
        "list-runs",
        help="discover metadata and journal findings; scientific state is not evaluated",
    )
    listing.add_argument("--runs-root", required=True)
    listing.add_argument(
        "--verify-external",
        action="store_true",
        help="also hash consumer-code and runtime files for each run",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command in {"validate-site", "validate-request"}:
            return _validate(args)
        if args.command == "create-run":
            return _create(args)
        if args.command == "inspect-run":
            return _inspect(args)
        return _list(args)
    except RunCreationError as error:
        _command_error(
            args.command,
            error,
            "Inspect/reopen the same run ID and preserve staging evidence. "
            "Do not automatically create another run or launch calculations.",
            run_id=error.run_id,
            published_path=str(error.path),
            staging_path=(
                str(error.staging_path) if error.staging_path is not None else None
            ),
            publication_uncertain=error.publication_uncertain,
            cause=str(error.__cause__),
        )
        return 4 if error.publication_uncertain else 2
    except RunBusyError as error:
        _command_error(
            args.command,
            error,
            "Another process holds the run lock. Retry inspection later; "
            "do not remove lock files or create a replacement run.",
        )
        return 3
    except (
        KeyError,
        OSError,
        UnicodeError,
        yaml.YAMLError,
        ValidationError,
        ValueError,
    ) as error:
        if args.command in {"validate-site", "validate-request"}:
            label = "Site" if args.command == "validate-site" else "Run request"
            print(f"{label} validation failed: {error}", file=sys.stderr)
        else:
            _command_error(
                args.command,
                error,
                "Inspect the input configuration and reported filesystem paths. "
                "Existing runs are not overwritten; no calculation was launched.",
            )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

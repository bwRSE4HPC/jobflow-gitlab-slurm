"""cli / test_inspect_run contracts."""

import json
from contextlib import ExitStack
from pathlib import Path

import pytest

from jobflow_gitlab_slurm.cli import main
from jobflow_gitlab_slurm.persistence.queries import discovery, inspection
from jobflow_gitlab_slurm.persistence.runs.storage import (
    locked_run,
)
from tests.support import inspection as inspection_setup
from tests.support import runs as runs_setup


@pytest.mark.parametrize("verify_external", [False, True])
def test_inspect_cli_reports_metadata_without_execution(
    discovery_inputs, capsys, verify_external
):
    handle = runs_setup.create(discovery_inputs)
    arguments = inspection_setup.inspect_arguments(discovery_inputs)
    if verify_external:
        arguments.append("--verify-external")
    assert main(arguments) == 0
    captured = capsys.readouterr()
    assert captured.err == ""
    report = json.loads(captured.out)
    assert report["kind"] == "run-inspection"
    assert report["run_id"] == inspection_setup.RUN_ID
    assert report["checks"]["external_artifacts_verified"] is verify_external
    assert report["execution_state"] == "not_evaluated"
    assert report["consumer_code"]["path"] == str(discovery_inputs.sources[1])
    assert report["worker_runtime"]["path"] == str(discovery_inputs.sources[2])
    assert report["flow_payload"]["path"] == str(handle.path / "flow/payload.json")


@pytest.mark.parametrize("command", ["create", "inspect"])
def test_cli_lock_contention_has_recovery_hint(discovery_inputs, capsys, command):
    runs_setup.create(discovery_inputs)
    arguments = (
        runs_setup.create_arguments(discovery_inputs)
        if command == "create"
        else inspection_setup.inspect_arguments(discovery_inputs)
    )
    with locked_run(discovery_inputs.root, inspection_setup.RUN_ID):
        assert main(arguments) == 3
    captured = capsys.readouterr()
    assert captured.out == ""
    report = json.loads(captured.err)
    assert report["kind"] == "command-error"
    assert report["error_type"] == "RunBusyError"
    assert "do not remove lock files" in report["hint"]


@pytest.mark.parametrize("command", ["inspect", "list"])
def test_cli_missing_root_reports_error_without_creating_it(
    discovery_inputs, capsys, command
):
    missing = discovery_inputs.root / "absent"
    arguments = (
        ["inspect-run", inspection_setup.RUN_ID, "--runs-root", str(missing)]
        if command == "inspect"
        else ["list-runs", "--runs-root", str(missing)]
    )
    assert main(arguments) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    report = json.loads(captured.err)
    assert report["error_type"] == "FileNotFoundError"
    assert not missing.exists()


def test_cli_inspection_external_failure_is_not_execution_failure(
    discovery_inputs, capsys
):
    runs_setup.create(discovery_inputs)
    discovery_inputs.sources[1].unlink()
    assert main(inspection_setup.inspect_arguments(discovery_inputs)) == 0
    capsys.readouterr()
    assert (
        main(
            inspection_setup.inspect_arguments(discovery_inputs) + ["--verify-external"]
        )
        == 2
    )
    report = json.loads(capsys.readouterr().err)
    assert report["error_type"] == "FileNotFoundError"
    assert "no calculation was launched" in report["hint"]


def test_discovery_root_scan_error_is_not_an_empty_success(
    discovery_inputs, capsys, monkeypatch
):
    def fail(self):
        raise PermissionError("cannot enumerate runs root")

    monkeypatch.setattr(discovery.Path, "iterdir", fail)
    assert main(inspection_setup.list_arguments(discovery_inputs)) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    report = json.loads(captured.err)
    assert report["error_type"] == "PermissionError"
    assert "cannot enumerate" in report["error"]


@pytest.mark.parametrize(
    ("state", "expected_exit", "journal_status"),
    [
        ("uninitialized", 0, "uninitialized"),
        ("empty", 0, "valid"),
        ("valid", 0, "valid"),
        ("incomplete", 4, "incomplete"),
        ("invalid", 2, "invalid"),
    ],
)
def test_inspect_cli_emits_findings_on_stdout_even_for_journal_holds(
    inspection_runs, capsys, state, expected_exit, journal_status
):
    handle = inspection_setup.prepare(inspection_runs, state)
    assert (
        main(
            inspection_setup.inspect_arguments(inspection_runs) + ["--verify-external"]
        )
        == expected_exit
    )
    captured = capsys.readouterr()
    assert captured.err == ""
    report = json.loads(captured.out)

    assert report["kind"] == "run-inspection"
    assert report["run_id"] == inspection_setup.RUN_ID
    assert report["path"] == str(handle.path)
    assert report["metadata_status"] == "valid"
    assert report["execution_state"] == "not_evaluated"
    assert report["journal"]["status"] == journal_status
    assert report["checks"] == {
        "flow_bytes_verified": True,
        "external_artifacts_verified": True,
    }
    assert report["consumer_code"]["path"] == str(inspection_runs.sources[1])
    assert report["worker_runtime"]["path"] == str(inspection_runs.sources[2])
    assert inspection_setup.PRIVATE_PAYLOAD not in captured.out


@pytest.mark.parametrize("failure", ["busy", "metadata"])
def test_journal_is_not_accessed_when_metadata_or_lock_check_fails(
    inspection_runs, capsys, monkeypatch, failure
):
    handle = inspection_setup.prepare(inspection_runs, "valid")

    def forbidden(*args, **kwargs):
        raise AssertionError("journal was accessed without valid locked metadata")

    monkeypatch.setattr(inspection.journal, "_read_events_locked", forbidden)
    with ExitStack() as stack:
        if failure == "busy":
            stack.enter_context(
                locked_run(inspection_runs.root, inspection_setup.RUN_ID)
            )
        else:
            (handle.path / "run.json").write_bytes(b"invalid metadata")

        assert main(inspection_setup.inspect_arguments(inspection_runs)) == (
            3 if failure == "busy" else 2
        )
        captured = capsys.readouterr()
        assert captured.out == ""
        report = json.loads(captured.err)
        assert report["kind"] == "command-error"
        assert report["journal"]["status"] == "not_checked"
        assert report["journal"]["committed_count"] is None

        assert main(inspection_setup.list_arguments(inspection_runs)) == (
            3 if failure == "busy" else 2
        )
        listing = json.loads(capsys.readouterr().out)
        assert listing["journal_counts"]["not_checked"] == 1
        assert listing["runs"][0]["journal"]["status"] == "not_checked"


@pytest.mark.parametrize("command", ["inspect", "list"])
def test_missing_root_error_reports_unchecked_journal(inspection_runs, capsys, command):
    missing = inspection_runs.root / "absent"
    arguments = (
        ["inspect-run", inspection_setup.RUN_ID, "--runs-root", str(missing)]
        if command == "inspect"
        else ["list-runs", "--runs-root", str(missing)]
    )
    assert main(arguments) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    report = json.loads(captured.err)
    assert report["error_type"] == "FileNotFoundError"
    assert report["journal"]["status"] == "not_checked"
    assert not missing.exists()


@pytest.mark.parametrize("state", ["valid", "incomplete", "invalid"])
def test_cli_inspection_preserves_evidence_without_repair_or_flush(
    inspection_runs, capsys, monkeypatch, state
):
    inspection_setup.prepare(inspection_runs, state)
    before = inspection_setup.snapshot(inspection_runs.root)

    def forbidden(*args, **kwargs):
        raise AssertionError("inspection attempted repair, mutation, or flush")

    monkeypatch.setattr(inspection.journal, "append_event", forbidden)
    for name in ("fsync", "rename", "replace"):
        monkeypatch.setattr(inspection.journal.os, name, forbidden)
    for name in ("mkdir", "write_bytes", "write_text", "unlink"):
        monkeypatch.setattr(Path, name, forbidden)

    expected = {"valid": 0, "incomplete": 4, "invalid": 2}[state]
    assert main(inspection_setup.inspect_arguments(inspection_runs)) == expected
    capsys.readouterr()
    assert main(inspection_setup.list_arguments(inspection_runs)) == expected
    capsys.readouterr()
    assert inspection_setup.snapshot(inspection_runs.root) == before


def test_external_artifact_checks_remain_optional_and_failure_is_metadata_error(
    inspection_runs, capsys
):
    inspection_setup.prepare(inspection_runs, "valid")
    inspection_runs.sources[1].write_bytes(b"changed consumer")
    assert main(inspection_setup.inspect_arguments(inspection_runs)) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["checks"]["external_artifacts_verified"] is False
    assert report["journal"]["status"] == "valid"

    assert (
        main(
            inspection_setup.inspect_arguments(inspection_runs) + ["--verify-external"]
        )
        == 2
    )
    error = json.loads(capsys.readouterr().err)
    assert error["journal"]["status"] == "not_checked"
    assert error["error_type"] == "ValueError"

    assert (
        main(inspection_setup.list_arguments(inspection_runs) + ["--verify-external"])
        == 2
    )
    listing = json.loads(capsys.readouterr().out)
    assert listing["counts"] == {"valid": 0, "busy": 0, "invalid": 1}
    assert listing["journal_counts"]["not_checked"] == 1

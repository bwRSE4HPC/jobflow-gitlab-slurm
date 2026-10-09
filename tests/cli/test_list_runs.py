"""cli / test_list_runs contracts."""

import json
from contextlib import ExitStack

import pytest

from jobflow_gitlab_slurm.cli import main
from jobflow_gitlab_slurm.persistence.queries.inspection import (
    JOURNAL_STATUSES,
)
from jobflow_gitlab_slurm.persistence.runs.storage import (
    locked_run,
)
from tests.support import inspection as inspection_setup
from tests.support import runs as runs_setup


@pytest.mark.parametrize("empty", [False, True])
def test_list_cli_success_and_empty_root(discovery_inputs, capsys, empty):
    if not empty:
        runs_setup.create(discovery_inputs)
    assert main(inspection_setup.list_arguments(discovery_inputs)) == 0
    captured = capsys.readouterr()
    assert captured.err == ""
    report = json.loads(captured.out)
    assert report["kind"] == "run-list"
    assert report["atomic_snapshot"] is False
    assert report["execution_state"] == "not_evaluated"
    assert report["verify_external_requested"] is False
    assert report["counts"] == {
        "valid": 0 if empty else 1,
        "busy": 0,
        "invalid": 0,
    }
    if not empty:
        assert report["runs"][0]["inspection"]["checks"] == {
            "flow_bytes_verified": True,
            "external_artifacts_verified": False,
        }


def test_list_cli_external_verification(discovery_inputs, capsys):
    runs_setup.create(discovery_inputs)
    assert (
        main(inspection_setup.list_arguments(discovery_inputs) + ["--verify-external"])
        == 0
    )
    report = json.loads(capsys.readouterr().out)
    assert report["verify_external_requested"] is True
    assert report["runs"][0]["inspection"]["checks"]["external_artifacts_verified"]


@pytest.mark.parametrize("with_busy_run", [False, True])
def test_list_cli_invalid_entries_take_priority(
    discovery_inputs, capsys, with_busy_run
):
    (discovery_inputs.root / "unexpected-file").write_bytes(b"evidence")
    if with_busy_run:
        runs_setup.create(discovery_inputs)
        with locked_run(discovery_inputs.root, runs_setup.RUN_ID):
            assert main(inspection_setup.list_arguments(discovery_inputs)) == 2
    else:
        assert main(inspection_setup.list_arguments(discovery_inputs)) == 2
    captured = capsys.readouterr()
    assert captured.err == ""
    report = json.loads(captured.out)
    assert report["counts"] == {
        "valid": 0,
        "busy": 1 if with_busy_run else 0,
        "invalid": 1,
    }
    invalid = next(
        entry for entry in report["runs"] if entry["metadata_status"] == "invalid"
    )
    assert invalid["error"]
    assert "inspection" not in invalid


def test_list_cli_busy_only_returns_three(discovery_inputs, capsys):
    runs_setup.create(discovery_inputs)
    with locked_run(discovery_inputs.root, runs_setup.RUN_ID):
        assert main(inspection_setup.list_arguments(discovery_inputs)) == 3
    report = json.loads(capsys.readouterr().out)
    assert report["counts"] == {"valid": 0, "busy": 1, "invalid": 0}
    assert report["runs"][0]["metadata_status"] == "busy"


@pytest.mark.parametrize(
    ("states", "expected_exit"),
    [
        (("uninitialized", "empty", "valid"), 0),
        (("incomplete",), 4),
        (("busy",), 3),
        (("incomplete", "busy"), 4),
        (("invalid", "incomplete"), 2),
        (("invalid", "busy"), 2),
        (("unexpected", "incomplete", "busy"), 2),
    ],
)
def test_list_exit_precedence_and_separate_journal_counts(
    inspection_runs, capsys, states, expected_exit
):
    before = None
    with ExitStack() as stack:
        for index, state in enumerate(states, start=1):
            if state == "unexpected":
                (inspection_runs.root / "unexpected-file").write_bytes(b"evidence")
                continue

            run_id = f"00000000-0000-4000-8000-{index:012d}"
            inspection_setup.prepare(
                inspection_runs, "valid" if state == "busy" else state, run_id
            )
            if state == "busy":
                stack.enter_context(locked_run(inspection_runs.root, run_id))

        before = inspection_setup.snapshot(inspection_runs.root)
        assert main(inspection_setup.list_arguments(inspection_runs)) == expected_exit
        captured = capsys.readouterr()
        assert captured.err == ""
        report = json.loads(captured.out)
        assert inspection_setup.snapshot(inspection_runs.root) == before

    assert report["kind"] == "run-list"
    assert report["atomic_snapshot"] is False
    assert report["execution_state"] == "not_evaluated"
    assert set(report["journal_counts"]) == set(JOURNAL_STATUSES)
    assert sum(report["journal_counts"].values()) == len(states)
    assert report["counts"] == {
        "valid": sum(state not in {"busy", "unexpected"} for state in states),
        "busy": states.count("busy"),
        "invalid": states.count("unexpected"),
    }

    for entry in report["runs"]:
        if entry["metadata_status"] == "valid":
            assert entry["inspection"]["execution_state"] == "not_evaluated"
            assert entry["journal"] == entry["inspection"]["journal"]
        else:
            assert entry["journal"]["status"] == "not_checked"
            assert entry["journal"]["checked"] is False
            assert "inspection" not in entry
    assert inspection_setup.PRIVATE_PAYLOAD not in captured.out


def test_empty_listing_has_zero_metadata_and_journal_counts(inspection_runs, capsys):
    assert main(inspection_setup.list_arguments(inspection_runs)) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["runs"] == []
    assert report["counts"] == {"valid": 0, "busy": 0, "invalid": 0}
    assert report["journal_counts"] == {status: 0 for status in JOURNAL_STATUSES}

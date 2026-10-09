"""cli / test_create_run contracts."""

import json
from pathlib import Path

import pytest
import yaml

from jobflow_gitlab_slurm import cli
from jobflow_gitlab_slurm.cli import main
from jobflow_gitlab_slurm.persistence.runs import storage
from jobflow_gitlab_slurm.persistence.runs.storage import (
    RunCreationError,
)
from tests.support import runs as runs_setup


def test_create_cli_selects_site_and_reports_pinned_inputs(discovery_inputs, capsys):
    arguments = runs_setup.create_arguments(discovery_inputs) + [
        "--workspace-expires-at",
        "2100-01-01T00:00:00.000000Z",
    ]
    assert main(arguments) == 0
    captured = capsys.readouterr()
    assert captured.err == ""
    report = json.loads(captured.out)
    assert report["kind"] == "run-created"
    assert report["run_id"] == runs_setup.RUN_ID
    assert report["path"] == str(discovery_inputs.root / runs_setup.RUN_ID)
    assert report["site_id"] == "example-cluster"
    assert report["workspace_expires_at"] == "2100-01-01T00:00:00.000000Z"
    assert report["execution_state"] == "not_evaluated"
    assert report["metadata_status"] == "valid"
    assert report["checks"] == {
        "flow_bytes_verified": True,
        "external_artifacts_verified": True,
    }
    assert (
        report["request"]["policy"]["allocated_cpu_hour_budget"]
        == "0.000000000000000001"
    )
    assert (
        discovery_inputs.root / runs_setup.RUN_ID / "flow/payload.json"
    ).read_bytes() == runs_setup.FLOW_BYTES


def test_cli_existing_run_is_not_overwritten(discovery_inputs, capsys):
    handle = runs_setup.create(discovery_inputs)
    before = (handle.path / "run.json").read_bytes()
    assert main(runs_setup.create_arguments(discovery_inputs)) == 2
    report = json.loads(capsys.readouterr().err)
    assert report["error_type"] == "FileExistsError"
    assert "Existing runs are not overwritten" in report["hint"]
    assert (handle.path / "run.json").read_bytes() == before


@pytest.mark.parametrize("failure", ["unknown-site", "missing-request", "bad-sites"])
def test_create_cli_configuration_errors(discovery_inputs, capsys, failure):
    if failure == "unknown-site":
        data = yaml.safe_load(discovery_inputs.request_file.read_text(encoding="utf-8"))
        data["site_id"] = "unknown-cluster"
        discovery_inputs.request_file.write_text(yaml.safe_dump(data), encoding="utf-8")
    elif failure == "missing-request":
        discovery_inputs.request_file.unlink()
    else:
        (discovery_inputs.sites / "unexpected.txt").write_bytes(b"not a site")
    assert main(runs_setup.create_arguments(discovery_inputs)) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    report = json.loads(captured.err)
    assert report["kind"] == "command-error"
    assert report["command"] == "create-run"
    assert not list(discovery_inputs.root.iterdir())


def test_create_cli_reports_artifact_failure_and_staging(discovery_inputs, capsys):
    discovery_inputs.sources[0].write_bytes(b"wrong flow bytes")
    assert main(runs_setup.create_arguments(discovery_inputs)) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    report = json.loads(captured.err)
    assert report["error_type"] == "RunCreationError"
    assert report["run_id"] == runs_setup.RUN_ID
    assert report["publication_uncertain"] is False
    assert "SHA-256 mismatch" in report["cause"]
    assert Path(report["staging_path"]).is_dir()
    assert not Path(report["published_path"]).exists()


@pytest.mark.parametrize("uncertain", [False, True])
def test_create_cli_reports_creation_error_without_staging(
    discovery_inputs, capsys, monkeypatch, uncertain
):
    def fail(*args, **kwargs):
        raise RunCreationError(
            runs_setup.RUN_ID,
            discovery_inputs.root / runs_setup.RUN_ID,
            None,
            uncertain,
        )

    monkeypatch.setattr(cli, "create_run", fail)
    assert main(runs_setup.create_arguments(discovery_inputs)) == (
        4 if uncertain else 2
    )
    report = json.loads(capsys.readouterr().err)
    assert report["staging_path"] is None
    assert report["publication_uncertain"] is uncertain
    assert "same run ID" in report["hint"]


def test_create_cli_uncertain_publication_can_be_inspected(
    discovery_inputs, capsys, monkeypatch
):
    original_sync = storage._sync_directory

    def fail_after_publication(path):
        if (
            path == discovery_inputs.root
            and (discovery_inputs.root / runs_setup.RUN_ID).exists()
        ):
            raise OSError("injected final directory flush failure")
        original_sync(path)

    monkeypatch.setattr(storage, "_sync_directory", fail_after_publication)
    assert main(runs_setup.create_arguments(discovery_inputs)) == 4
    report = json.loads(capsys.readouterr().err)
    assert report["publication_uncertain"] is True
    assert report["run_id"] == runs_setup.RUN_ID
    assert Path(report["published_path"]).is_dir()
    assert "injected" in report["cause"]
    assert main(runs_setup.inspect_arguments(discovery_inputs)) == 0
    assert json.loads(capsys.readouterr().out)["run_id"] == runs_setup.RUN_ID

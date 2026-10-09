"""cli / test_entrypoint contracts."""

import runpy
import sys

import pytest
import yaml

from jobflow_gitlab_slurm import cli
from jobflow_gitlab_slurm.cli import main
from tests.support import configuration as configuration_setup


def test_help_exposes_new_commands(capsys):
    with pytest.raises(SystemExit) as caught:
        main(["--help"])
    assert caught.value.code == 0
    output = capsys.readouterr().out
    for command in ("create-run", "inspect-run", "list-runs"):
        assert command in output


@pytest.mark.parametrize("available", [True, False])
def test_cli_module_entrypoint(tmp_path, monkeypatch, capsys, available):
    path = tmp_path / "site.yaml"
    if available:
        path.write_text(yaml.safe_dump(configuration_setup.SITE), encoding="utf-8")

    monkeypatch.setattr(
        sys,
        "argv",
        ["jobflow-gitlab-slurm", "validate-site", str(path)],
    )

    with pytest.raises(SystemExit) as error:
        runpy.run_path(cli.__file__, run_name="__main__")

    assert error.value.code == (0 if available else 2)

    captured = capsys.readouterr()
    if available:
        assert "Site validation passed: example-cluster" in captured.out
        assert captured.err == ""
    else:
        assert "Site validation failed:" in captured.err
        assert captured.out == ""

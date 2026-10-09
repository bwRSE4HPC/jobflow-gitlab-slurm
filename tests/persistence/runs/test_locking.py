"""persistence / runs / test_locking contracts."""

import os

import pytest

from jobflow_gitlab_slurm.persistence.runs.storage import (
    RunBusyError,
    locked_run,
    open_run,
)
from tests.persistence.runs import _run_support as run_setup


def test_lock_exclusion_and_release_in_another_process(run_inputs):
    run_setup.create(run_inputs)
    code = """
import sys
from jobflow_gitlab_slurm.persistence.runs.storage import RunBusyError, open_run
try:
    open_run(sys.argv[1], sys.argv[2])
except RunBusyError:
    print("busy")
    sys.exit(17)
print("opened")
"""
    with locked_run(run_inputs.root, run_setup.RUN_ID):
        result = run_setup.process(code, run_inputs.root, run_setup.RUN_ID)
        assert result.returncode == 17, result.stderr
        assert result.stdout.strip() == "busy"
        with pytest.raises(RunBusyError):
            run_setup.create(run_inputs)
    result = run_setup.process(code, run_inputs.root, run_setup.RUN_ID)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "opened"


def test_missing_lock_is_not_recreated(run_inputs):
    run_setup.create(run_inputs)
    (run_inputs.root / ".locks" / f"{run_setup.RUN_ID}.lock").unlink()
    with pytest.raises(FileNotFoundError):
        open_run(run_inputs.root, run_setup.RUN_ID)
    assert not (run_inputs.root / ".locks" / f"{run_setup.RUN_ID}.lock").exists()


@pytest.mark.parametrize("kind", ["symlink", "fifo"])
def test_unsafe_lock_file_is_rejected(run_inputs, kind):
    directory = run_inputs.root / ".locks"
    directory.mkdir()
    path = directory / f"{run_setup.RUN_ID}.lock"
    if kind == "symlink":
        path.symlink_to(run_inputs.sources[0])
        error = OSError
    else:
        os.mkfifo(path)
        error = ValueError
    with pytest.raises(error):
        run_setup.create(run_inputs)
    assert not (run_inputs.root / run_setup.RUN_ID).exists()


def test_symlinked_lock_directory_is_rejected(run_inputs, tmp_path):
    target = tmp_path / "other-locks"
    target.mkdir()
    (run_inputs.root / ".locks").symlink_to(target, target_is_directory=True)
    with pytest.raises(ValueError, match="real directory"):
        run_setup.create(run_inputs)


@pytest.mark.parametrize("after_rename", [False, True])
def test_abrupt_process_exit_retains_same_id_and_releases_lock(
    run_inputs, after_rename
):
    code = """
import json
import os
import sys
from jobflow_gitlab_slurm.persistence.runs import storage
from jobflow_gitlab_slurm.config.request import RunRequest
from jobflow_gitlab_slurm.config.site import SiteConfig

if sys.argv[1] == "True":
    original = storage.os.rename
    def terminate(source, destination):
        original(source, destination)
        os._exit(73)
    storage.os.rename = terminate
else:
    original = storage._write_record
    def terminate(path, record):
        original(path, record)
        if path.name == "run.json":
            os._exit(73)
    storage._write_record = terminate

storage.create_run(
    SiteConfig.model_validate_json(sys.argv[2]),
    RunRequest.model_validate_json(sys.argv[3]),
    *sys.argv[4:7],
    run_id=sys.argv[7],
)
"""
    result = run_setup.process(
        code,
        after_rename,
        run_inputs.site.model_dump_json(),
        run_inputs.request.model_dump_json(),
        *run_inputs.sources,
        run_setup.RUN_ID,
    )
    assert result.returncode == 73, result.stderr
    if after_rename:
        assert (
            open_run(run_inputs.root, run_setup.RUN_ID).manifest.run_id
            == run_setup.RUN_ID
        )
        with pytest.raises(FileExistsError):
            run_setup.create(run_inputs)
        assert not list(run_inputs.root.glob(".staging-*"))
    else:
        staging = list(run_inputs.root.glob(f".staging-{run_setup.RUN_ID}-*"))
        assert len(staging) == 1
        assert (staging[0] / "run.json").is_file()
        with pytest.raises(FileNotFoundError):
            open_run(run_inputs.root, run_setup.RUN_ID)
        handle = run_setup.create(run_inputs)
        assert handle.manifest.run_id == run_setup.RUN_ID
        assert staging[0].is_dir()

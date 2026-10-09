"""persistence / runs / test_reopening contracts."""

import json

import pytest

from jobflow_gitlab_slurm.persistence.runs.storage import (
    open_run,
)
from tests.persistence.runs import _run_support as run_setup


def test_reopen_in_another_process(run_inputs):
    handle = run_setup.create(run_inputs)
    result = run_setup.process(
        """
import json
import sys
from jobflow_gitlab_slurm.persistence.runs.storage import open_run
handle = open_run(sys.argv[1], sys.argv[2], verify_external=True)
print(json.dumps({
    "run_id": handle.manifest.run_id,
    "sha256": handle.flow.payload.sha256,
    "payload": (handle.path / "flow/payload.json").read_bytes().hex(),
}))
""",
        run_inputs.root,
        run_setup.RUN_ID,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "run_id": run_setup.RUN_ID,
        "sha256": handle.flow.payload.sha256,
        "payload": run_setup.FLOW_BYTES.hex(),
    }


def test_reopening_does_not_rewrite_files(run_inputs):
    handle = run_setup.create(run_inputs)
    paths = [path for path in run_inputs.root.rglob("*") if path.is_file()]
    before = {path: (path.read_bytes(), path.stat().st_mtime_ns) for path in paths}
    assert open_run(run_inputs.root, run_setup.RUN_ID) == handle
    assert {
        path: (path.read_bytes(), path.stat().st_mtime_ns) for path in paths
    } == before


@pytest.mark.parametrize("source_index", [1, 2])
@pytest.mark.parametrize("change", ["missing", "modified"])
def test_external_verification_is_explicit(run_inputs, source_index, change):
    handle = run_setup.create(run_inputs)
    source = run_inputs.sources[source_index]
    if change == "missing":
        source.unlink()
        error = FileNotFoundError
    else:
        source.write_bytes(b"changed")
        error = ValueError
    assert open_run(run_inputs.root, run_setup.RUN_ID) == handle
    with pytest.raises(error):
        open_run(run_inputs.root, run_setup.RUN_ID, verify_external=True)


def test_missing_run_is_not_created_by_reopening(run_inputs):
    with pytest.raises(FileNotFoundError):
        open_run(run_inputs.root, run_setup.RUN_ID)
    assert not list(run_inputs.root.iterdir())

"""persistence / bundles / test_inspection_failures contracts."""

import os
from pathlib import Path

import pytest

from jobflow_gitlab_slurm.persistence.bundles import inspection
from jobflow_gitlab_slurm.persistence.bundles.inspection import (
    BundleInspectionError,
)
from tests.persistence.bundles import _inspection_support as inspection_setup


@pytest.mark.parametrize("name", ["staging", "published"])
@pytest.mark.parametrize("entry_type", ["file", "symlink", "fifo"])
def test_unsafe_top_level_entries(bundle_invocation, name, entry_type):
    path = bundle_invocation.invocation.path / name
    if entry_type == "file":
        path.write_bytes(b"not a directory")
    elif entry_type == "symlink":
        path.symlink_to(bundle_invocation.attempt.path, target_is_directory=True)
    else:
        os.mkfifo(path)
    other = "published" if name == "staging" else "staging"
    (bundle_invocation.invocation.path / other).mkdir()
    report = inspection_setup.inspect(bundle_invocation)
    assert report.status == "invalid"
    assert report.content_integrity == "invalid"
    assert inspection_setup.issue_codes(report) == {"unsafe_bundle_path"}


def test_unsafe_structure_without_manifest_is_invalid(bundle_invocation):
    bundle = inspection_setup.make_bundle(
        bundle_invocation, manifest=False, marker=False
    )
    os.mkfifo(bundle.unit / "unsafe")
    assert inspection_setup.issue_codes(
        inspection_setup.inspect(bundle_invocation)
    ) == {"unsafe_entry"}


@pytest.mark.parametrize("phase", ["lstat", "inventory", "metadata", "payload"])
def test_io_failures_are_unavailable_not_absent_or_invalid(
    bundle_invocation, phase, monkeypatch
):
    bundle = inspection_setup.make_bundle(bundle_invocation)
    secret = "PRIVATE_IO_MESSAGE"

    if phase == "lstat":
        original = Path.lstat

        def lstat(path, *args, **kwargs):
            if path == bundle.unit:
                raise PermissionError(secret)
            return original(path, *args, **kwargs)

        monkeypatch.setattr(Path, "lstat", lstat)
    elif phase == "inventory":
        original = Path.iterdir

        def iterdir(path):
            if path == bundle.unit:
                raise PermissionError(secret)
            return original(path)

        monkeypatch.setattr(Path, "iterdir", iterdir)
    elif phase == "metadata":
        original = os.open

        def open_file(path, *args, **kwargs):
            if Path(path) == bundle.manifest_path:
                raise PermissionError(secret)
            return original(path, *args, **kwargs)

        monkeypatch.setattr(os, "open", open_file)
    else:

        def verify(*args, **kwargs):
            raise PermissionError(secret)

        monkeypatch.setattr(inspection, "verify_artifact", verify)

    with pytest.raises(BundleInspectionError) as captured:
        inspection_setup.inspect(bundle_invocation)
    error = captured.value
    assert error.code == "inspection_io_unavailable"
    assert error.run_id == inspection_setup.RUN_ID
    assert error.invocation_id == inspection_setup.INVOCATION_ID
    assert error.path == bundle_invocation.invocation.path
    assert secret not in str(error)
    assert error.__suppress_context__ is True


def test_enumerated_entry_disappearance_is_unavailable(bundle_invocation, monkeypatch):
    bundle = inspection_setup.make_bundle(bundle_invocation)
    target = bundle.unit / "disappearing"
    target.write_bytes(b"fixture")
    original = inspection._kind

    def observed_kind(path):
        return "absent" if path == target else original(path)

    monkeypatch.setattr(inspection, "_kind", observed_kind)
    with pytest.raises(BundleInspectionError):
        inspection_setup.inspect(bundle_invocation)

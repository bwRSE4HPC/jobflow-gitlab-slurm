"""persistence / bundles / test_publication_failures contracts."""

import errno
import json
import os
import stat
from pathlib import Path
from types import SimpleNamespace

import pytest

from jobflow_gitlab_slurm.persistence.attempts.ownership import (
    owned_invocation,
)
from jobflow_gitlab_slurm.persistence.bundles import storage
from jobflow_gitlab_slurm.persistence.bundles.inspection import (
    BundleInspectionError,
)
from jobflow_gitlab_slurm.persistence.bundles.storage import (
    BundleIntegrityError,
    BundlePublicationError,
)
from tests.persistence.bundles import _publication_support as publication_setup


@pytest.mark.parametrize("location", ["staging", "nested"])
def test_cross_filesystem_layout_is_rejected_before_writes(
    bundle_staging, location, monkeypatch
):
    target = (
        bundle_staging.staging
        if location == "staging"
        else bundle_staging.staging / "files/nested"
    )
    original = Path.lstat
    original_metadata = target.lstat()

    def lstat(path, *args, **kwargs):
        if path == target:
            return SimpleNamespace(
                st_mode=original_metadata.st_mode,
                st_dev=original_metadata.st_dev + 1,
            )
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "lstat", lstat)
    with pytest.raises(BundleIntegrityError) as captured:
        publication_setup.publish(bundle_staging)
    assert captured.value.code == "cross_filesystem_bundle"
    assert bundle_staging.staging.is_dir()
    assert not bundle_staging.published.exists()


@pytest.mark.parametrize("location", ["invocation", "staging"])
def test_filesystem_guard_rejects_unsafe_directories(
    bundle_staging, location, monkeypatch
):
    target = (
        bundle_staging.invocation.path
        if location == "invocation"
        else bundle_staging.staging
    )
    original = Path.lstat
    metadata = target.lstat()
    with owned_invocation(*bundle_staging.parameters) as ownership:

        def lstat(path, *args, **kwargs):
            if path == target:
                return SimpleNamespace(
                    st_mode=stat.S_IFLNK,
                    st_dev=metadata.st_dev,
                )
            return original(path, *args, **kwargs)

        monkeypatch.setattr(Path, "lstat", lstat)
        with pytest.raises(BundleIntegrityError) as captured:
            storage.require_same_filesystem(ownership, (bundle_staging.staging,))
    assert captured.value.code == "unsafe_bundle_directory"


@pytest.mark.parametrize("failure", ["io", "unsafe"])
def test_preflight_read_failure_does_not_become_publication_failure(
    bundle_staging, failure, monkeypatch
):
    def failed_read(*args, **kwargs):
        if failure == "io":
            raise OSError(errno.EACCES, publication_setup.SECRET)
        raise ValueError(publication_setup.SECRET)

    before = publication_setup.snapshot(bundle_staging.root)
    monkeypatch.setattr(storage, "_read_bytes", failed_read)
    expected = BundleInspectionError if failure == "io" else BundleIntegrityError
    with pytest.raises(expected) as captured:
        publication_setup.publish(bundle_staging)
    assert publication_setup.SECRET not in str(captured.value)
    assert captured.value.__suppress_context__ is True
    assert publication_setup.snapshot(bundle_staging.root) == before


@pytest.mark.parametrize("phase", publication_setup.PHASES)
def test_publication_failures_report_phase_and_preserve_evidence(
    bundle_staging, phase, monkeypatch
):
    publication_setup.install_failure(bundle_staging, monkeypatch, phase)
    with pytest.raises(BundlePublicationError) as captured:
        publication_setup.publish(bundle_staging)
    error = captured.value
    assert error.operation == "publish"
    assert error.phase == phase
    assert error.publication_uncertain is (
        publication_setup.PHASES.index(phase)
        >= publication_setup.PHASES.index("directory-rename")
    )
    assert error.staging_path == bundle_staging.staging
    assert error.published_path == bundle_staging.published
    assert error.invocation_id == publication_setup.INVOCATION_ID
    assert publication_setup.SECRET not in str(error)
    report = error.to_report()
    assert publication_setup.SECRET not in json.dumps(report)
    assert json.loads(json.dumps(report)) == report
    assert error.__suppress_context__ is True
    assert bundle_staging.staging.exists() or bundle_staging.published.exists()
    if error.temporary_path is not None:
        assert error.temporary_path.parent == bundle_staging.invocation.path
        if phase not in (
            "final-directory-flush",
            "final-parent-flush",
            "final-verification",
        ):
            assert error.temporary_path.exists()


@pytest.mark.parametrize(
    "phase",
    [
        "payload-flush",
        "receipt-flush",
        "bundle-directory-flush",
        "invocation-directory-flush",
        "final-verification",
    ],
)
def test_acknowledgment_failures_are_uncertain_without_rewriting(
    bundle_staging, phase, monkeypatch
):
    publication_setup.publish(bundle_staging)
    before = publication_setup.snapshot(bundle_staging.root)
    publication_setup.install_failure(bundle_staging, monkeypatch, phase)
    with pytest.raises(BundlePublicationError) as captured:
        publication_setup.publish(bundle_staging)
    error = captured.value
    assert error.operation == "acknowledge"
    assert error.phase == phase
    assert error.publication_uncertain is True
    assert error.temporary_path is None
    assert publication_setup.snapshot(bundle_staging.root) == before


def test_marker_descriptor_is_closed_when_stream_creation_fails(
    bundle_staging, monkeypatch
):
    original_temporary = storage.tempfile.mkstemp
    original_fdopen = os.fdopen
    descriptors = []

    def temporary(*args, **kwargs):
        descriptor, name = original_temporary(*args, **kwargs)
        descriptors.append(descriptor)
        return descriptor, name

    def fdopen(descriptor, mode, *args, **kwargs):
        if mode == "wb":
            raise OSError(errno.EIO, publication_setup.SECRET)
        return original_fdopen(descriptor, mode, *args, **kwargs)

    monkeypatch.setattr(storage.tempfile, "mkstemp", temporary)
    monkeypatch.setattr(storage.os, "fdopen", fdopen)
    with pytest.raises(BundlePublicationError) as captured:
        publication_setup.publish(bundle_staging)
    with pytest.raises(OSError) as closed:
        os.fstat(descriptors[0])
    assert closed.value.errno == errno.EBADF
    assert captured.value.phase == "marker-write"
    assert captured.value.temporary_path.exists()


def test_final_inspection_io_failure_retains_uncertainty(bundle_staging, monkeypatch):
    original = storage.inspection.inspect_owned_bundle
    calls = 0

    def observed(ownership):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise BundleInspectionError(
                ownership.invocation.record,
                ownership.invocation.path,
                errno.EACCES,
            )
        return original(ownership)

    monkeypatch.setattr(storage.inspection, "inspect_owned_bundle", observed)
    with pytest.raises(BundlePublicationError) as captured:
        publication_setup.publish(bundle_staging)
    assert captured.value.phase == "final-verification"
    assert captured.value.reason == "inspection_io_unavailable"
    assert captured.value.errno == errno.EACCES

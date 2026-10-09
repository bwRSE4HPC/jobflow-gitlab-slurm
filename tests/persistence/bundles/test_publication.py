"""persistence / bundles / test_publication contracts."""

import builtins
import json
import os
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from jobflow_gitlab_slurm.persistence.attempts.ownership import (
    InvocationBusyError,
    InvocationOwnershipError,
    owned_invocation,
)
from jobflow_gitlab_slurm.persistence.bundles import storage
from jobflow_gitlab_slurm.persistence.bundles.records import (
    encode_bundle_commit,
    encode_bundle_manifest,
)
from jobflow_gitlab_slurm.persistence.bundles.storage import (
    BundleConflictError,
    BundleIntegrityError,
    BundlePublicationError,
    BundleRecoveryRequiredError,
    publish_owned_bundle,
)
from jobflow_gitlab_slurm.persistence.runs.storage import locked_run
from tests.persistence.bundles import _publication_support as publication_setup


def test_publish_complete_staging(bundle_staging):
    handle = publication_setup.publish(bundle_staging)
    assert handle.path == bundle_staging.published
    assert handle.manifest == bundle_staging.manifest
    assert handle.commit == bundle_staging.commit
    assert not bundle_staging.staging.exists()
    assert (handle.path / "COMMIT.json").read_bytes() == encode_bundle_commit(
        bundle_staging.commit
    )
    for name, data in bundle_staging.payloads.items():
        assert (handle.path / name).read_bytes() == data
    report = publication_setup.inspect(bundle_staging)
    assert report.status == "committed"
    assert report.content_integrity == "valid"
    assert report.to_report()["result_eligibility"] == "not_evaluated"
    assert not list(bundle_staging.invocation.path.glob(".bundle-commit-*"))


def test_empty_optional_inventories(bundle_staging):
    for name in (
        "files/empty.bin",
        "files/nested/result-é.txt",
        "data/blob.bin",
    ):
        (bundle_staging.staging / name).unlink()
    for name in ("files/nested", "files", "data"):
        (bundle_staging.staging / name).rmdir()
    bundle_staging.manifest = bundle_staging.manifest.model_copy(
        update={"files": (), "additional_data": ()}
    )
    bundle_staging.commit = publication_setup.commit_for(
        bundle_staging.manifest,
        publication_setup.timestamp(bundle_staging.run, 4),
    )
    (bundle_staging.staging / "payload-manifest.json").write_bytes(
        encode_bundle_manifest(bundle_staging.manifest)
    )
    assert publication_setup.publish(bundle_staging).path == bundle_staging.published
    assert publication_setup.inspect(bundle_staging).status == "committed"


def test_exact_acknowledgment_does_not_rewrite(bundle_staging, monkeypatch):
    original = publication_setup.publish(bundle_staging)
    before = publication_setup.snapshot(bundle_staging.root)

    def forbidden(*args, **kwargs):
        pytest.fail("acknowledgment must not rename or create marker files")

    monkeypatch.setattr(storage.os, "rename", forbidden)
    monkeypatch.setattr(storage.tempfile, "mkstemp", forbidden)
    repeated = publication_setup.publish(bundle_staging)
    assert repeated == original
    assert publication_setup.snapshot(bundle_staging.root) == before


def test_handle_is_frozen_and_retains_no_ownership(bundle_staging):
    handle = publication_setup.publish(bundle_staging)
    with pytest.raises(FrozenInstanceError):
        handle.path = bundle_staging.staging
    with owned_invocation(*bundle_staging.parameters):
        assert handle.path == bundle_staging.published


@pytest.mark.parametrize(
    "damage",
    ["manifest_schema", "commit_schema", "definition_digest", "marker_binding"],
)
def test_invalid_expected_records_fail_before_writes(bundle_staging, damage):
    manifest = bundle_staging.manifest
    commit = bundle_staging.commit
    if damage == "manifest_schema":
        manifest = manifest.model_copy(update={"schema_version": True})
    elif damage == "commit_schema":
        commit = commit.model_copy(update={"schema_version": True})
    elif damage == "definition_digest":
        manifest = manifest.model_copy(update={"definition_sha256": "f" * 64})
        commit = publication_setup.commit_for(
            manifest, publication_setup.timestamp(bundle_staging.run, 4)
        )
    else:
        commit = commit.model_copy(
            update={"manifest": commit.manifest.model_copy(update={"sha256": "f" * 64})}
        )
    before = publication_setup.snapshot(bundle_staging.root)
    with pytest.raises(BundleIntegrityError) as captured:
        publication_setup.publish(bundle_staging, manifest, commit)
    assert captured.value.code == "invalid_expected_records"
    assert captured.value.__suppress_context__ is True
    assert publication_setup.snapshot(bundle_staging.root) == before


def test_valid_changed_manifest_is_a_conflict(bundle_staging):
    changed = bundle_staging.manifest.model_copy(
        update={"created_at": publication_setup.timestamp(bundle_staging.run, 3.5)}
    )
    commit = publication_setup.commit_for(
        changed, publication_setup.timestamp(bundle_staging.run, 4)
    )
    before = publication_setup.snapshot(bundle_staging.root)
    with pytest.raises(BundleConflictError) as captured:
        publication_setup.publish(bundle_staging, changed, commit)
    assert captured.value.path == bundle_staging.staging / "payload-manifest.json"
    assert publication_setup.snapshot(bundle_staging.root) == before


def test_valid_changed_commit_is_a_conflict(bundle_staging):
    publication_setup.publish(bundle_staging)
    changed = bundle_staging.commit.model_copy(
        update={"created_at": publication_setup.timestamp(bundle_staging.run, 5)}
    )
    before = publication_setup.snapshot(bundle_staging.root)
    with pytest.raises(BundleConflictError) as captured:
        publication_setup.publish(bundle_staging, commit=changed)
    assert captured.value.path == bundle_staging.published / "COMMIT.json"
    assert publication_setup.snapshot(bundle_staging.root) == before


@pytest.mark.parametrize(
    "state",
    [
        "absent",
        "staging_partial",
        "staging_invalid",
        "staging_unsafe",
        "staging_marker",
        "published_partial",
        "published_invalid",
        "ambiguous",
        "undeclared_file",
        "undeclared_directory",
    ],
)
def test_nonpublishable_evidence_is_held_unchanged(bundle_staging, state):
    if state == "absent":
        bundle_staging.staging.rename(bundle_staging.invocation.path / "saved-staging")
    elif state == "staging_partial":
        (bundle_staging.staging / "response.json").unlink()
    elif state == "staging_invalid":
        (bundle_staging.staging / "response.json").write_bytes(b"bad")
    elif state == "staging_unsafe":
        target = bundle_staging.staging / "files/empty.bin"
        target.unlink()
        target.symlink_to(bundle_staging.receipt)
    elif state == "staging_marker":
        (bundle_staging.staging / "COMMIT.json").write_bytes(
            encode_bundle_commit(bundle_staging.commit)
        )
    elif state == "published_partial":
        bundle_staging.staging.rename(bundle_staging.published)
        (bundle_staging.published / "response.json").unlink()
    elif state == "published_invalid":
        bundle_staging.staging.rename(bundle_staging.published)
        (bundle_staging.published / "response.json").write_bytes(b"bad")
    elif state == "ambiguous":
        bundle_staging.published.mkdir()
    elif state == "undeclared_file":
        (bundle_staging.staging / "extra").write_bytes(b"extra")
    else:
        (bundle_staging.staging / "extra").mkdir()

    before = publication_setup.snapshot(bundle_staging.root)
    with pytest.raises(BundleIntegrityError) as captured:
        publication_setup.publish(bundle_staging)
    assert captured.value.code.startswith("bundle_")
    assert publication_setup.snapshot(bundle_staging.root) == before


def test_unmarked_published_bundle_requires_explicit_recovery(bundle_staging):
    bundle_staging.staging.rename(bundle_staging.published)
    before = publication_setup.snapshot(bundle_staging.root)
    with pytest.raises(BundleRecoveryRequiredError) as captured:
        publication_setup.publish(bundle_staging)
    assert captured.value.code == "explicit_recovery_required"
    assert publication_setup.snapshot(bundle_staging.root) == before
    assert publication_setup.inspect(bundle_staging).status == "published_uncommitted"


def test_unmarked_conflicting_manifest_is_not_adopted(bundle_staging):
    bundle_staging.staging.rename(bundle_staging.published)
    changed = bundle_staging.manifest.model_copy(
        update={"created_at": publication_setup.timestamp(bundle_staging.run, 3.5)}
    )
    commit = publication_setup.commit_for(
        changed, publication_setup.timestamp(bundle_staging.run, 4)
    )
    with pytest.raises(BundleConflictError):
        publication_setup.publish(bundle_staging, changed, commit)
    assert not (bundle_staging.published / "COMMIT.json").exists()


def test_flush_and_rename_order(bundle_staging, monkeypatch):
    events = []
    original_file = storage._sync_file
    original_directory = storage._sync_directory
    original_rename = os.rename
    original_temporary = storage.tempfile.mkstemp

    def sync_file(path):
        events.append(("file", Path(path)))
        original_file(path)

    def sync_directory(path):
        events.append(("directory", Path(path)))
        original_directory(path)

    def rename(source, destination):
        events.append(("rename", Path(source), Path(destination)))
        original_rename(source, destination)

    def temporary(*args, **kwargs):
        events.append(("marker-create",))
        return original_temporary(*args, **kwargs)

    monkeypatch.setattr(storage, "_sync_file", sync_file)
    monkeypatch.setattr(storage, "_sync_directory", sync_directory)
    monkeypatch.setattr(storage.os, "rename", rename)
    monkeypatch.setattr(storage.tempfile, "mkstemp", temporary)

    publication_setup.publish(bundle_staging)
    directory_rename = next(
        index
        for index, event in enumerate(events)
        if event[0] == "rename" and event[2] == bundle_staging.published
    )
    marker_create = events.index(("marker-create",))
    marker_rename = next(
        index
        for index, event in enumerate(events)
        if event[0] == "rename" and event[2] == bundle_staging.published / "COMMIT.json"
    )
    assert directory_rename < marker_create < marker_rename
    for name in (*bundle_staging.payloads, "payload-manifest.json"):
        assert events.index(("file", bundle_staging.staging / name)) < directory_rename
    assert events.index(("file", bundle_staging.receipt)) < directory_rename
    assert events.index(("directory", bundle_staging.receipt.parent)) < directory_rename
    assert events.index(
        ("directory", bundle_staging.staging / "files/nested")
    ) < events.index(("directory", bundle_staging.staging / "files"))
    assert ("directory", bundle_staging.published) in events[marker_rename + 1 :]
    assert ("directory", bundle_staging.invocation.path) in events[marker_rename + 1 :]


def test_destination_that_appears_before_rename_is_not_replaced(
    bundle_staging, monkeypatch
):
    original = storage.flush_directories

    def flush(directories):
        original(directories)
        bundle_staging.published.mkdir()
        (bundle_staging.published / "evidence").write_bytes(b"retain this")

    monkeypatch.setattr(storage, "flush_directories", flush)
    with pytest.raises(BundlePublicationError) as captured:
        publication_setup.publish(bundle_staging)
    assert captured.value.phase == "destination-check"
    assert captured.value.publication_uncertain is False
    assert bundle_staging.staging.exists()
    assert (bundle_staging.published / "evidence").read_bytes() == b"retain this"


def test_marker_that_appears_before_rename_is_not_replaced(bundle_staging, monkeypatch):
    original = storage._sync_directory
    calls = 0

    def directory(path):
        nonlocal calls
        original(path)
        if path == bundle_staging.invocation.path:
            calls += 1
            if calls == 3:
                (bundle_staging.published / "COMMIT.json").write_bytes(b"retain marker")

    monkeypatch.setattr(storage, "_sync_directory", directory)
    with pytest.raises(BundlePublicationError) as captured:
        publication_setup.publish(bundle_staging)
    assert captured.value.phase == "marker-destination-check"
    assert (bundle_staging.published / "COMMIT.json").read_bytes() == b"retain marker"
    assert captured.value.temporary_path.exists()


def test_failed_final_integrity_check_is_not_acknowledged(bundle_staging, monkeypatch):
    original = os.rename

    def rename(source, destination):
        original(source, destination)
        if destination == bundle_staging.published / "COMMIT.json":
            (bundle_staging.published / "response.json").write_bytes(b"damaged")

    monkeypatch.setattr(storage.os, "rename", rename)
    with pytest.raises(BundlePublicationError) as captured:
        publication_setup.publish(bundle_staging)
    error = captured.value
    assert error.phase == "final-verification"
    assert error.reason == "final_bundle_not_committed"
    assert error.publication_uncertain is True
    assert error.to_report()["issues"]
    assert publication_setup.inspect(bundle_staging).status == "invalid"


def test_old_marker_temporary_evidence_is_not_adopted_or_deleted(
    bundle_staging,
):
    old = bundle_staging.invocation.path / ".bundle-commit-old"
    old.write_bytes(b"old evidence")
    inode = old.stat().st_ino
    publication_setup.publish(bundle_staging)
    assert old.read_bytes() == b"old evidence"
    assert old.stat().st_ino == inode


def test_expired_ownership_is_rejected_without_changes(bundle_staging):
    with owned_invocation(*bundle_staging.parameters) as ownership:
        pass
    before = publication_setup.snapshot(bundle_staging.root)
    with pytest.raises(InvocationOwnershipError):
        publish_owned_bundle(ownership, bundle_staging.manifest, bundle_staging.commit)
    assert publication_setup.snapshot(bundle_staging.root) == before


def test_publication_does_not_hold_run_lock_or_reacquire_invocation(
    bundle_staging, monkeypatch
):
    original = storage._sync_file
    checked = []

    def sync_file(path):
        with locked_run(bundle_staging.root, publication_setup.RUN_ID):
            checked.append(Path(path))
        with (
            pytest.raises(InvocationBusyError),
            owned_invocation(*bundle_staging.parameters),
        ):
            pytest.fail("invocation should remain owned")
        original(path)

    monkeypatch.setattr(storage, "_sync_file", sync_file)
    publication_setup.publish(bundle_staging)
    assert checked


def test_publication_does_not_import_consumer_payloads(bundle_staging, monkeypatch):
    original = builtins.__import__

    def guarded_import(name, *args, **kwargs):
        assert not name.startswith("do_not_import")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    assert publication_setup.publish(bundle_staging).path == bundle_staging.published


def test_error_reports_are_detached(bundle_staging):
    (bundle_staging.staging / "response.json").unlink()
    with pytest.raises(BundleIntegrityError) as captured:
        publication_setup.publish(bundle_staging)
    error = captured.value
    report = error.to_report()
    report["identity"]["run_id"] = publication_setup.OTHER_ID
    report["issues"][0]["code"] = "changed"
    fresh = error.to_report()
    assert fresh["identity"]["run_id"] == publication_setup.RUN_ID
    assert fresh["issues"][0]["code"] == "artifact_missing"
    assert "inert Response bytes" not in json.dumps(fresh)


def test_second_process_publishes_and_reopens(bundle_staging, tmp_path):
    result = publication_setup.child_publish(bundle_staging, tmp_path)
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["status"] == "committed"
    assert report["identity"]["invocation_id"] == publication_setup.INVOCATION_ID
    assert report["scheduler_state"] == "not_evaluated"
    assert publication_setup.inspect(bundle_staging).status == "committed"


def test_second_process_cannot_publish_owned_invocation(bundle_staging, tmp_path):
    before = publication_setup.snapshot(bundle_staging.root)
    with owned_invocation(*bundle_staging.parameters):
        result = publication_setup.child_publish(bundle_staging, tmp_path)
    assert result.returncode == 3, result.stderr
    assert result.stdout.strip() == "InvocationBusyError"
    assert publication_setup.snapshot(bundle_staging.root) == before


@pytest.mark.parametrize(
    ("mode", "expected_state", "temporary_count"),
    [
        ("before-directory", "staging_only", 0),
        ("after-directory", "published_uncommitted", 0),
        ("before-marker", "published_uncommitted", 1),
        ("after-marker", "committed", 0),
    ],
)
def test_abrupt_exit_preserves_observable_evidence(
    bundle_staging, tmp_path, mode, expected_state, temporary_count
):
    result = publication_setup.child_publish(bundle_staging, tmp_path, mode)
    assert result.returncode == 79, result.stderr
    report = publication_setup.inspect(bundle_staging)
    assert report.status == expected_state
    assert report.content_integrity == "valid"
    assert len(list(bundle_staging.invocation.path.glob(".bundle-commit-*"))) == (
        temporary_count
    )
    if expected_state == "published_uncommitted":
        before = publication_setup.snapshot(bundle_staging.root)
        with pytest.raises(BundleRecoveryRequiredError):
            publication_setup.publish(bundle_staging)
        assert publication_setup.snapshot(bundle_staging.root) == before
    else:
        # Explicit publication of intact staging, or exact committed
        # acknowledgment. This is not automatic controller recovery.
        assert (
            publication_setup.publish(bundle_staging).path == bundle_staging.published
        )

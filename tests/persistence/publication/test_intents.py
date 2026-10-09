"""persistence / publication / test_intents contracts."""

import os
import stat
from dataclasses import FrozenInstanceError, replace

import pytest

from jobflow_gitlab_slurm.persistence.attempts.ownership import (
    InvocationOwnershipError,
    owned_invocation,
)
from jobflow_gitlab_slurm.persistence.bundles.records import (
    BundleManifest,
    encode_bundle_commit,
    encode_bundle_manifest,
)
from jobflow_gitlab_slurm.persistence.bundles.storage import publish_owned_bundle
from jobflow_gitlab_slurm.persistence.publication import storage
from jobflow_gitlab_slurm.persistence.publication.records import (
    PublicationIntentRecord,
    encode_publication_intent,
)
from jobflow_gitlab_slurm.persistence.publication.storage import (
    IntentConflictError,
    IntentInspectionError,
    IntentIntegrityError,
    IntentMissingError,
    IntentPublicationError,
    publish_owned_publication_intent,
    read_owned_publication_intent,
)
from jobflow_gitlab_slurm.persistence.runs.storage import locked_run
from tests.persistence.publication import _intent_support as intent_setup


def test_publish_and_read_exact_intent(intent_staging):
    lock = intent_staging.invocation.path / ".bundle.lock"
    inode = lock.stat().st_ino
    handle = intent_setup.publish(intent_staging)
    expected = encode_publication_intent(intent_staging.intent)
    assert handle.path == intent_staging.path
    assert handle.record == intent_staging.intent
    assert handle.sha256 == intent_setup.digest(expected)
    assert handle.size_bytes == len(expected)
    assert intent_staging.path.read_bytes() == expected
    assert stat.S_IMODE(intent_staging.path.stat().st_mode) == 0o600
    assert intent_setup.read(intent_staging) == handle
    assert lock.stat().st_ino == inode
    assert not hasattr(handle, "ownership")
    with pytest.raises(FrozenInstanceError):
        handle.path = intent_staging.root


def test_read_is_non_mutating_and_never_flushes(intent_staging, monkeypatch):
    intent_setup.publish(intent_staging)
    before = intent_setup.snapshot(intent_staging.root)

    def forbidden(*args, **kwargs):
        raise AssertionError("read must not flush")

    monkeypatch.setattr(storage, "_sync_file", forbidden)
    monkeypatch.setattr(storage, "_sync_directory", forbidden)
    assert intent_setup.read(intent_staging).record == intent_staging.intent
    assert intent_setup.snapshot(intent_staging.root) == before


def test_missing_read_creates_nothing(intent_staging):
    before = intent_setup.snapshot(intent_staging.root)
    with pytest.raises(IntentMissingError) as caught:
        intent_setup.read(intent_staging)
    assert caught.value.code == "intent_missing"
    assert intent_setup.snapshot(intent_staging.root) == before
    assert "does not prove" in caught.value.to_report()["hint"]


def test_exact_retry_does_not_rewrite_or_generate_metadata(intent_staging, monkeypatch):
    intent_setup.publish(intent_staging)
    before = intent_setup.snapshot(intent_staging.root)

    def forbidden(*args, **kwargs):
        raise AssertionError("retry must not stage or rename")

    monkeypatch.setattr(storage.tempfile, "mkstemp", forbidden)
    monkeypatch.setattr(storage.os, "rename", forbidden)
    assert intent_setup.publish(intent_staging).record == intent_staging.intent
    assert intent_setup.snapshot(intent_staging.root) == before


@pytest.mark.parametrize("change", ["id", "time", "marker-time"])
def test_valid_changed_expectations_are_conflicts(intent_staging, change):
    intent_setup.publish(intent_staging)
    data = intent_staging.intent.model_dump()
    if change == "id":
        data["intent_id"] = intent_setup.OTHER_ID
    elif change == "time":
        data["created_at"] = intent_setup.timestamp(intent_staging.run, 6)
    else:
        data["expected_commit"]["created_at"] = intent_setup.timestamp(
            intent_staging.run, 6
        )
    changed = PublicationIntentRecord.model_validate(data)
    before = intent_setup.snapshot(intent_staging.root)
    with pytest.raises(IntentConflictError):
        intent_setup.publish(intent_staging, changed)
    assert intent_setup.snapshot(intent_staging.root) == before


@pytest.mark.parametrize("change", ["schema", "parent"])
def test_invalid_expected_records_fail_before_writes(intent_staging, change):
    if change == "schema":
        changed = intent_staging.intent.model_copy(update={"schema_version": 2})
    else:
        manifest = intent_staging.intent.expected_manifest.model_copy(
            update={"consumer_code_sha256": "f" * 64}
        )
        commit = intent_staging.intent.expected_commit.model_copy(
            update={
                "manifest": intent_setup.reference(
                    "payload-manifest.json", encode_bundle_manifest(manifest)
                )
            }
        )
        changed = intent_staging.intent.model_copy(
            update={"expected_manifest": manifest, "expected_commit": commit}
        )
    before = intent_setup.snapshot(intent_staging.root)
    with pytest.raises(IntentIntegrityError, match="invalid_expected_intent"):
        intent_setup.publish(intent_staging, changed)
    assert intent_setup.snapshot(intent_staging.root) == before


@pytest.mark.parametrize("operation", ["read", "publish"])
def test_valid_stored_intent_with_wrong_parent_is_held(intent_staging, operation):
    data = intent_staging.intent.model_dump()
    data["expected_manifest"]["consumer_code_sha256"] = "f" * 64
    manifest = BundleManifest.model_validate(data["expected_manifest"])
    data["expected_commit"]["manifest"] = intent_setup.reference(
        "payload-manifest.json", encode_bundle_manifest(manifest)
    )
    intent_staging.path.write_bytes(
        encode_publication_intent(PublicationIntentRecord.model_validate(data))
    )
    before = intent_setup.snapshot(intent_staging.root)
    with pytest.raises(IntentIntegrityError):
        if operation == "read":
            intent_setup.read(intent_staging)
        else:
            intent_setup.publish(intent_staging)
    assert intent_setup.snapshot(intent_staging.root) == before


@pytest.mark.parametrize("entry", ["directory", "file", "symlink"])
def test_new_intent_cannot_be_created_retroactively(intent_staging, entry):
    if entry == "directory":
        intent_staging.published.mkdir()
    elif entry == "file":
        intent_staging.published.write_bytes(b"retained")
    else:
        intent_staging.published.symlink_to(intent_staging.invocation.path / "missing")
    before = intent_setup.snapshot(intent_staging.root)
    with pytest.raises(IntentIntegrityError, match="retroactive_intent_forbidden"):
        intent_setup.publish(intent_staging)
    assert intent_setup.snapshot(intent_staging.root) == before


def test_intent_io_does_not_change_bundle_and_remains_usable_after_commit(
    intent_staging,
):
    staging = intent_setup.make_staging(intent_staging)
    before = intent_setup.snapshot(staging)
    intent_setup.publish(intent_staging)
    assert intent_setup.snapshot(staging) == before
    with owned_invocation(*intent_staging.parameters) as ownership:
        handle = publish_owned_bundle(
            ownership,
            intent_staging.intent.expected_manifest,
            intent_staging.intent.expected_commit,
        )
    before = intent_setup.snapshot(handle.path)
    assert intent_setup.read(intent_staging).record == intent_staging.intent
    assert intent_setup.publish(intent_staging).record == intent_staging.intent
    assert intent_setup.snapshot(handle.path) == before
    assert (handle.path / "COMMIT.json").read_bytes() == encode_bundle_commit(
        intent_staging.intent.expected_commit
    )


@pytest.mark.parametrize("operation", ["read", "publish"])
def test_expired_ownership_is_rejected(intent_staging, operation):
    with owned_invocation(*intent_staging.parameters) as ownership:
        pass
    before = intent_setup.snapshot(intent_staging.root)
    with pytest.raises(InvocationOwnershipError):
        if operation == "read":
            read_owned_publication_intent(ownership)
        else:
            publish_owned_publication_intent(ownership, intent_staging.intent)
    assert intent_setup.snapshot(intent_staging.root) == before


@pytest.mark.parametrize("operation", ["read", "publish"])
def test_missing_invocation_directory_is_not_created(intent_staging, operation):
    with owned_invocation(*intent_staging.parameters) as ownership:
        moved = intent_staging.invocation.path.with_name("retained-invocation")
        intent_staging.invocation.path.rename(moved)
        with pytest.raises(IntentInspectionError):
            if operation == "read":
                read_owned_publication_intent(ownership)
            else:
                publish_owned_publication_intent(ownership, intent_staging.intent)
        assert not intent_staging.invocation.path.exists()


def test_run_lock_is_available_during_intent_io(intent_staging, monkeypatch):
    original = storage._sync_directory

    def checked(path):
        with locked_run(intent_staging.root, intent_setup.RUN_ID):
            pass
        original(path)

    monkeypatch.setattr(storage, "_sync_directory", checked)
    assert intent_setup.publish(intent_staging).record == intent_staging.intent


def test_old_temporary_evidence_is_never_adopted_or_deleted(intent_staging):
    old = intent_staging.invocation.path / ".publication-intent-old"
    old.write_bytes(b"retained interrupted evidence")
    intent_setup.publish(intent_staging)
    assert old.read_bytes() == b"retained interrupted evidence"


def test_flush_and_rename_order(intent_staging, monkeypatch):
    operations = []
    real_fsync = os.fsync
    real_rename = os.rename

    def fsync(descriptor):
        operations.append("fsync")
        real_fsync(descriptor)

    def rename(source, destination):
        operations.append("rename")
        real_rename(source, destination)

    monkeypatch.setattr(storage.os, "fsync", fsync)
    monkeypatch.setattr(storage.os, "rename", rename)
    intent_setup.publish(intent_staging)
    assert operations == ["fsync", "fsync", "rename", "fsync", "fsync"]


def test_changed_final_record_is_not_acknowledged(intent_staging, monkeypatch):
    original = storage._read

    def changed(ownership, path):
        handle = original(ownership, path)
        return replace(
            handle,
            record=handle.record.model_copy(
                update={"intent_id": intent_setup.OTHER_ID}
            ),
        )

    monkeypatch.setattr(storage, "_read", changed)
    with pytest.raises(IntentPublicationError) as caught:
        intent_setup.publish(intent_staging)
    assert caught.value.phase == "final-verification"
    assert caught.value.reason == "expected_intent_conflict"
    assert caught.value.intent_publication_uncertain is True


def test_error_reports_are_detached(intent_staging):
    with pytest.raises(IntentMissingError) as caught:
        intent_setup.read(intent_staging)
    report = caught.value.to_report()
    report["identity"]["run_id"] = "changed"
    assert caught.value.to_report()["identity"]["run_id"] == intent_setup.RUN_ID


def test_second_process_publishes_and_reopens(intent_staging, tmp_path):
    expected = intent_setup.digest(encode_publication_intent(intent_staging.intent))
    result = intent_setup.child(intent_staging, tmp_path, "publish")
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == expected
    result = intent_setup.child(intent_staging, tmp_path, "read")
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == expected
    assert intent_setup.read(intent_staging).record == intent_staging.intent


def test_second_process_cannot_publish_owned_invocation(intent_staging, tmp_path):
    before = intent_setup.snapshot(intent_staging.root)
    with owned_invocation(*intent_staging.parameters):
        result = intent_setup.child(intent_staging, tmp_path, "publish")
    assert result.returncode == 3, result.stderr
    assert result.stdout.strip() == "InvocationBusyError"
    assert intent_setup.snapshot(intent_staging.root) == before


@pytest.mark.parametrize("mode", ["before", "after"])
def test_abrupt_exit_preserves_same_identity_recovery_evidence(
    intent_staging, tmp_path, mode
):
    result = intent_setup.child(intent_staging, tmp_path, mode)
    assert result.returncode == 79, result.stderr
    temporary_files = list(intent_staging.invocation.path.glob(".publication-intent-*"))
    if mode == "before":
        assert not intent_staging.path.exists()
        assert len(temporary_files) == 1
        retained = temporary_files[0].read_bytes()
        with pytest.raises(IntentMissingError):
            intent_setup.read(intent_staging)
        assert intent_setup.publish(intent_staging).record == intent_staging.intent
        assert temporary_files[0].read_bytes() == retained
    else:
        assert not temporary_files
        assert intent_setup.read(intent_staging).record == intent_staging.intent
        before = intent_setup.snapshot(intent_staging.root)
        assert intent_setup.publish(intent_staging).record == intent_staging.intent
        assert intent_setup.snapshot(intent_staging.root) == before

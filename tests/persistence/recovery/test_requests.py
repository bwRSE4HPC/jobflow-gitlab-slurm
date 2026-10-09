"""persistence / recovery / test_requests contracts."""

import errno
import os
import stat
from dataclasses import FrozenInstanceError

import pytest
from pydantic import ValidationError

from jobflow_gitlab_slurm.persistence.attempts.ownership import (
    InvocationOwnershipError,
    owned_invocation,
)
from jobflow_gitlab_slurm.persistence.bundles.records import (
    encode_bundle_commit,
    encode_bundle_manifest,
)
from jobflow_gitlab_slurm.persistence.publication.records import (
    encode_publication_intent,
)
from jobflow_gitlab_slurm.persistence.recovery import requests as storage
from jobflow_gitlab_slurm.persistence.recovery.records import (
    RecoveryRequestRecord,
    encode_recovery_request,
)
from jobflow_gitlab_slurm.persistence.recovery.requests import (
    RecoveryRequestConflictError,
    RecoveryRequestInspectionError,
    RecoveryRequestIntegrityError,
    RecoveryRequestMissingError,
    RecoveryRequestPublicationError,
    persist_owned_recovery_request,
    read_owned_recovery_request,
)
from jobflow_gitlab_slurm.persistence.runs.storage import locked_run
from tests.persistence.recovery import _request_support as request_setup


@pytest.mark.parametrize(
    "state", ["staging_only", "published_uncommitted", "committed"]
)
def test_publish_and_read_preserve_bundle(recovery_staging, state):
    bundle = request_setup.prepare_state(recovery_staging, state)
    before = request_setup.snapshot(bundle)
    lock_inode = (recovery_staging.invocation.path / ".bundle.lock").stat().st_ino
    handle = request_setup.persist(recovery_staging)
    expected = encode_recovery_request(recovery_staging.request)

    assert handle.path == recovery_staging.path
    assert handle.record == recovery_staging.request
    assert handle.sha256 == request_setup.digest(expected)
    assert handle.size_bytes == len(expected)
    assert recovery_staging.path.read_bytes() == expected
    assert request_setup.read(recovery_staging) == handle
    assert request_setup.snapshot(bundle) == before
    assert (
        recovery_staging.invocation.path / ".bundle.lock"
    ).stat().st_ino == lock_inode
    assert stat.S_IMODE(recovery_staging.path.stat().st_mode) == 0o600
    assert stat.S_IMODE(recovery_staging.unit.stat().st_mode) == 0o700
    assert stat.S_IMODE(recovery_staging.recovery_root.stat().st_mode) == 0o700
    with pytest.raises(FrozenInstanceError):
        handle.path = recovery_staging.root


def test_read_never_flushes_or_mutates(recovery_staging, monkeypatch):
    request_setup.persist(recovery_staging)
    before = request_setup.snapshot(recovery_staging.root)

    def forbidden(*args, **kwargs):
        pytest.fail("read must not flush or inspect bundle payloads")

    monkeypatch.setattr(storage, "_sync_file", forbidden)
    monkeypatch.setattr(storage, "_sync_directory", forbidden)
    monkeypatch.setattr(storage, "inspect_owned_bundle", forbidden)
    assert request_setup.read(recovery_staging).record == recovery_staging.request
    assert request_setup.snapshot(recovery_staging.root) == before


@pytest.mark.parametrize("preparation", ["absent", "empty-root", "empty-unit", "temp"])
def test_missing_read_creates_nothing(recovery_staging, preparation):
    if preparation == "empty-root":
        recovery_staging.recovery_root.mkdir()
    elif preparation == "empty-unit":
        recovery_staging.unit.mkdir(parents=True)
    elif preparation == "temp":
        request_setup.prepare_temporary(recovery_staging, b"retained partial evidence")

    before = request_setup.snapshot(recovery_staging.root)
    with pytest.raises(RecoveryRequestMissingError) as caught:
        request_setup.read(recovery_staging)
    assert caught.value.code == "request_missing"
    assert request_setup.snapshot(recovery_staging.root) == before
    assert len(caught.value.temporary_paths) == (1 if preparation == "temp" else 0)


@pytest.mark.parametrize("preparation", ["empty-root", "empty-unit"])
def test_empty_preparation_allows_same_id_publication(recovery_staging, preparation):
    if preparation == "empty-root":
        recovery_staging.recovery_root.mkdir()
    else:
        recovery_staging.unit.mkdir(parents=True)
    assert request_setup.persist(recovery_staging).record == recovery_staging.request


def test_exact_retry_preserves_original_observation_after_forward_progress(
    recovery_staging, monkeypatch
):
    request_setup.persist(recovery_staging)
    recovery_staging.staging.rename(recovery_staging.published)
    (recovery_staging.published / "COMMIT.json").write_bytes(
        encode_bundle_commit(recovery_staging.intent.expected_commit)
    )
    before = request_setup.snapshot(recovery_staging.root)

    def forbidden(*args, **kwargs):
        pytest.fail("exact retry must not stage, rename, or repeat bundle preflight")

    monkeypatch.setattr(storage.tempfile, "mkstemp", forbidden)
    monkeypatch.setattr(storage.os, "rename", forbidden)
    monkeypatch.setattr(storage, "_preflight", forbidden)
    assert (
        request_setup.persist(recovery_staging).record.observed_status == "staging_only"
    )
    assert request_setup.snapshot(recovery_staging.root) == before


@pytest.mark.parametrize("change", ["actor", "time", "event", "observation"])
def test_changed_valid_requests_are_conflicts(recovery_staging, change):
    request_setup.persist(recovery_staging)
    document = recovery_staging.request.model_dump()
    if change == "actor":
        document["actor"]["identifier"] = "operator-2"
    elif change == "time":
        document["created_at"] = request_setup.timestamp(recovery_staging.run, 7)
    elif change == "event":
        document["journal_event_id"] = request_setup.OTHER_ID
    else:
        document.update(observed_status="committed", action="acknowledge_commit")
    changed = RecoveryRequestRecord.model_validate(document)
    before = request_setup.snapshot(recovery_staging.root)
    with pytest.raises(RecoveryRequestConflictError, match="expected_request_conflict"):
        request_setup.persist(recovery_staging, changed)
    assert request_setup.snapshot(recovery_staging.root) == before


@pytest.mark.parametrize("preparation", ["empty", "request"])
def test_different_id_cannot_bypass_reserved_operation(recovery_staging, preparation):
    if preparation == "empty":
        recovery_staging.unit.mkdir(parents=True)
    else:
        request_setup.persist(recovery_staging)
    changed = recovery_staging.request.model_copy(
        update={"recovery_id": request_setup.OTHER_ID}
    )
    before = request_setup.snapshot(recovery_staging.root)
    for operation in (
        lambda: request_setup.persist(recovery_staging, changed),
        lambda: request_setup.read(recovery_staging, request_setup.OTHER_ID),
    ):
        with pytest.raises(RecoveryRequestConflictError) as caught:
            operation()
        assert caught.value.code == "recovery_id_reserved"
        assert caught.value.existing_recovery_ids == (request_setup.RECOVERY_ID,)
        assert caught.value.path == recovery_staging.unit
    assert request_setup.snapshot(recovery_staging.root) == before


def test_multiple_id_directories_are_ambiguous(recovery_staging):
    recovery_staging.unit.mkdir(parents=True)
    (recovery_staging.recovery_root / request_setup.OTHER_ID).mkdir()
    before = request_setup.snapshot(recovery_staging.root)
    with pytest.raises(RecoveryRequestIntegrityError) as caught:
        request_setup.persist(recovery_staging)
    assert caught.value.code == "ambiguous_recovery_ids"
    assert set(caught.value.existing_recovery_ids) == {
        request_setup.RECOVERY_ID,
        request_setup.OTHER_ID,
    }
    assert request_setup.snapshot(recovery_staging.root) == before


@pytest.mark.parametrize("damage", ["invalid", "wrong-id", "wrong-intent"])
def test_invalid_final_request_is_held(recovery_staging, damage):
    request_setup.persist(recovery_staging)
    if damage == "invalid":
        recovery_staging.path.write_bytes(request_setup.SECRET.encode())
    else:
        changed = recovery_staging.request.model_copy(
            update={
                "recovery_id"
                if damage == "wrong-id"
                else "intent_id": request_setup.OTHER_ID
            }
        )
        recovery_staging.path.write_bytes(encode_recovery_request(changed))
    before = request_setup.snapshot(recovery_staging.root)
    with pytest.raises(RecoveryRequestIntegrityError) as caught:
        request_setup.read(recovery_staging)
    assert caught.value.code == "invalid_stored_request"
    assert request_setup.SECRET not in str(caught.value)
    assert request_setup.snapshot(recovery_staging.root) == before


@pytest.mark.parametrize("change", ["schema", "intent-hash", "parent"])
def test_invalid_expected_request_fails_before_writes(recovery_staging, change):
    document = recovery_staging.request.model_dump()
    if change == "schema":
        changed = recovery_staging.request.model_copy(update={"schema_version": 2})
    else:
        if change == "intent-hash":
            document["intent"]["sha256"] = "f" * 64
        else:
            document["run_id"] = request_setup.OTHER_ID
        changed = RecoveryRequestRecord.model_validate(document)
    before = request_setup.snapshot(recovery_staging.root)
    with pytest.raises(RecoveryRequestIntegrityError):
        request_setup.persist(recovery_staging, changed)
    assert request_setup.snapshot(recovery_staging.root) == before


@pytest.mark.parametrize("recovery_id", ["invalid", "", 1, None])
def test_invalid_read_selection_fails_before_changes(recovery_staging, recovery_id):
    before = request_setup.snapshot(recovery_staging.root)
    with pytest.raises(ValidationError):
        request_setup.read(recovery_staging, recovery_id)
    assert request_setup.snapshot(recovery_staging.root) == before


@pytest.mark.parametrize("damage", ["missing", "invalid", "changed"])
def test_retained_intent_is_required(recovery_staging, damage):
    if damage == "missing":
        recovery_staging.intent_path.unlink()
    elif damage == "invalid":
        recovery_staging.intent_path.write_bytes(request_setup.SECRET.encode())
    else:
        changed = recovery_staging.intent.model_copy(
            update={"intent_id": request_setup.OTHER_ID}
        )
        recovery_staging.intent_path.write_bytes(encode_publication_intent(changed))
    before = request_setup.snapshot(recovery_staging.root)
    with pytest.raises(RecoveryRequestIntegrityError):
        request_setup.persist(recovery_staging)
    assert request_setup.snapshot(recovery_staging.root) == before


@pytest.mark.parametrize(
    "damage", ["absent", "incomplete", "invalid", "ambiguous", "observation"]
)
def test_new_request_requires_matching_valid_bundle(recovery_staging, damage):
    if damage == "absent":
        recovery_staging.staging.rename(recovery_staging.run.path / "retained-staging")
    elif damage == "incomplete":
        (recovery_staging.staging / "response.json").unlink()
    elif damage == "invalid":
        (recovery_staging.staging / "response.json").write_bytes(b"changed payload")
    elif damage == "ambiguous":
        recovery_staging.published.mkdir()
    else:
        recovery_staging.request = RecoveryRequestRecord.model_validate(
            {
                **recovery_staging.request.model_dump(),
                "observed_status": "committed",
                "action": "acknowledge_commit",
            }
        )
    before = request_setup.snapshot(recovery_staging.root)
    with pytest.raises(RecoveryRequestIntegrityError) as caught:
        request_setup.persist(recovery_staging)
    assert caught.value.code == "bundle_observation_mismatch"
    assert request_setup.snapshot(recovery_staging.root) == before
    assert not recovery_staging.recovery_root.exists()


@pytest.mark.parametrize("target", ["manifest", "commit"])
def test_valid_different_bundle_metadata_conflicts_with_intent(
    recovery_staging, target
):
    if target == "manifest":
        changed = recovery_staging.intent.expected_manifest.model_copy(
            update={"created_at": request_setup.timestamp(recovery_staging.run, 3.5)}
        )
        (recovery_staging.staging / "payload-manifest.json").write_bytes(
            encode_bundle_manifest(changed)
        )
    else:
        request_setup.prepare_state(recovery_staging, "committed")
        changed = recovery_staging.intent.expected_commit.model_copy(
            update={"created_at": request_setup.timestamp(recovery_staging.run, 5)}
        )
        (recovery_staging.published / "COMMIT.json").write_bytes(
            encode_bundle_commit(changed)
        )
    before = request_setup.snapshot(recovery_staging.root)
    with pytest.raises(
        RecoveryRequestConflictError, match=f"expected_{target}_conflict"
    ):
        request_setup.persist(recovery_staging)
    assert request_setup.snapshot(recovery_staging.root) == before


def test_exact_temporary_request_is_resumed_without_regeneration(
    recovery_staging, monkeypatch
):
    temporary = request_setup.prepare_temporary(
        recovery_staging, encode_recovery_request(recovery_staging.request)
    )
    inode = temporary.stat().st_ino

    def forbidden(*args, **kwargs):
        pytest.fail("matching temporary evidence must be reused")

    monkeypatch.setattr(storage.tempfile, "mkstemp", forbidden)
    assert request_setup.persist(recovery_staging).record == recovery_staging.request
    assert recovery_staging.path.stat().st_ino == inode
    assert not temporary.exists()


@pytest.mark.parametrize("damage", ["partial", "different", "multiple"])
def test_uncommitted_temporary_holds_preserve_evidence(recovery_staging, damage):
    if damage == "partial":
        request_setup.prepare_temporary(recovery_staging, b'{"schema_version":')
    elif damage == "different":
        changed = recovery_staging.request.model_copy(
            update={"created_at": request_setup.timestamp(recovery_staging.run, 7)}
        )
        request_setup.prepare_temporary(
            recovery_staging, encode_recovery_request(changed)
        )
    else:
        request_setup.prepare_temporary(
            recovery_staging, encode_recovery_request(recovery_staging.request), "one"
        )
        request_setup.prepare_temporary(
            recovery_staging, encode_recovery_request(recovery_staging.request), "two"
        )
    before = request_setup.snapshot(recovery_staging.root)
    error_type = (
        RecoveryRequestConflictError
        if damage == "different"
        else RecoveryRequestIntegrityError
    )
    with pytest.raises(error_type):
        request_setup.persist(recovery_staging)
    assert request_setup.snapshot(recovery_staging.root) == before
    assert not recovery_staging.path.exists()


def test_final_request_preserves_safe_leftover_temporaries(recovery_staging):
    request_setup.persist(recovery_staging)
    first = request_setup.prepare_temporary(
        recovery_staging, b"partial retained evidence", "one"
    )
    second = request_setup.prepare_temporary(
        recovery_staging, b"other retained evidence", "two"
    )
    before = request_setup.snapshot(recovery_staging.root)
    assert request_setup.persist(recovery_staging).record == recovery_staging.request
    assert first.read_bytes() == b"partial retained evidence"
    assert second.read_bytes() == b"other retained evidence"
    assert request_setup.snapshot(recovery_staging.root) == before


@pytest.mark.parametrize("operation", ["read", "persist"])
def test_expired_ownership_is_rejected(recovery_staging, operation):
    with owned_invocation(*recovery_staging.parameters) as ownership:
        pass
    before = request_setup.snapshot(recovery_staging.root)
    with pytest.raises(InvocationOwnershipError):
        if operation == "read":
            read_owned_recovery_request(ownership, request_setup.RECOVERY_ID)
        else:
            persist_owned_recovery_request(ownership, recovery_staging.request)
    assert request_setup.snapshot(recovery_staging.root) == before


@pytest.mark.parametrize("operation", ["read", "persist"])
def test_inherited_ownership_is_rejected_in_child_process(recovery_staging, operation):
    with owned_invocation(*recovery_staging.parameters) as ownership:
        pid = os.fork()
        if pid == 0:
            try:
                if operation == "read":
                    read_owned_recovery_request(ownership, request_setup.RECOVERY_ID)
                else:
                    persist_owned_recovery_request(ownership, recovery_staging.request)
                os._exit(1)
            except InvocationOwnershipError:
                os._exit(0)
            finally:
                os._exit(2)
        _, status = os.waitpid(pid, 0)
    assert os.waitstatus_to_exitcode(status) == 0
    assert not recovery_staging.recovery_root.exists()


@pytest.mark.parametrize("damage", ["missing", "symlink"])
def test_invocation_directory_must_remain_real(recovery_staging, damage):
    with owned_invocation(*recovery_staging.parameters) as ownership:
        moved = recovery_staging.invocation.path.with_name("retained-invocation")
        recovery_staging.invocation.path.rename(moved)
        if damage == "symlink":
            recovery_staging.invocation.path.symlink_to(moved, target_is_directory=True)
        error_type = (
            RecoveryRequestInspectionError
            if damage == "missing"
            else RecoveryRequestIntegrityError
        )
        with pytest.raises(error_type):
            read_owned_recovery_request(ownership, request_setup.RECOVERY_ID)
        assert not (moved / "recoveries").exists()


def test_run_lock_remains_available_during_request_io(recovery_staging, monkeypatch):
    original = storage._step

    def checked(progress, phase, operation, *args):
        with locked_run(recovery_staging.root, request_setup.RUN_ID):
            pass
        return original(progress, phase, operation, *args)

    monkeypatch.setattr(storage, "_step", checked)
    assert request_setup.persist(recovery_staging).record == recovery_staging.request


def test_flush_and_rename_order(recovery_staging, monkeypatch):
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
    request_setup.persist(recovery_staging)
    assert operations == [
        "fsync",
        "fsync",
        "rename",
        "fsync",
        "fsync",
        "fsync",
        "fsync",
    ]


def test_completed_rename_then_io_error_is_uncertain_and_retryable(
    recovery_staging, monkeypatch
):
    real_rename = os.rename

    def uncertain(source, destination):
        real_rename(source, destination)
        raise OSError(errno.EIO, request_setup.SECRET)

    with monkeypatch.context() as patch:
        patch.setattr(storage.os, "rename", uncertain)
        with pytest.raises(RecoveryRequestPublicationError) as caught:
            request_setup.persist(recovery_staging)
    assert caught.value.phase == "request-rename"
    assert caught.value.request_publication_uncertain
    assert recovery_staging.path.read_bytes() == encode_recovery_request(
        recovery_staging.request
    )
    assert request_setup.persist(recovery_staging).record == recovery_staging.request


@pytest.mark.parametrize("damage", ["missing", "changed", "invalid"])
def test_final_verification_cannot_acknowledge_changed_evidence(
    recovery_staging, monkeypatch, damage
):
    original = storage._step

    def changed(progress, phase, operation, *args):
        if phase == "final-verification":
            if damage == "missing":
                recovery_staging.path.unlink()
            elif damage == "changed":
                record = recovery_staging.request.model_copy(
                    update={
                        "created_at": request_setup.timestamp(recovery_staging.run, 7)
                    }
                )
                recovery_staging.path.write_bytes(encode_recovery_request(record))
            else:
                recovery_staging.path.write_bytes(request_setup.SECRET.encode())
        return original(progress, phase, operation, *args)

    monkeypatch.setattr(storage, "_step", changed)
    with pytest.raises(RecoveryRequestPublicationError) as caught:
        request_setup.persist(recovery_staging)
    assert caught.value.phase == "final-verification"
    assert caught.value.request_publication_uncertain
    assert request_setup.SECRET not in str(caught.value)


def test_error_reports_are_detached_and_include_inspection_hints(recovery_staging):
    request_setup.prepare_temporary(
        recovery_staging, encode_recovery_request(recovery_staging.request), "one"
    )
    request_setup.prepare_temporary(
        recovery_staging, encode_recovery_request(recovery_staging.request), "two"
    )
    with pytest.raises(RecoveryRequestIntegrityError) as caught:
        request_setup.persist(recovery_staging)
    report = caught.value.to_report()
    assert report["temporary_paths"]
    assert "Do not replace" in report["hint"]
    report["identity"]["run_id"] = request_setup.OTHER_ID
    report["temporary_paths"].clear()
    fresh = caught.value.to_report()
    assert fresh["identity"]["run_id"] == request_setup.RUN_ID
    assert len(fresh["temporary_paths"]) == 2


def test_second_process_publishes_and_reopens(recovery_staging, tmp_path):
    result = request_setup.child(recovery_staging, tmp_path, "normal")
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip().encode() == encode_recovery_request(
        recovery_staging.request
    )
    assert request_setup.read(recovery_staging).record == recovery_staging.request


@pytest.mark.parametrize("mode", ["before", "after"])
def test_abrupt_exit_preserves_same_id_request_retry(recovery_staging, tmp_path, mode):
    before = request_setup.snapshot(recovery_staging.staging)
    result = request_setup.child(recovery_staging, tmp_path, mode)
    assert result.returncode == 71, result.stderr
    assert request_setup.snapshot(recovery_staging.staging) == before
    assert not recovery_staging.published.exists()

    with pytest.raises(RecoveryRequestConflictError):
        request_setup.persist(
            recovery_staging,
            recovery_staging.request.model_copy(
                update={"recovery_id": request_setup.OTHER_ID}
            ),
        )

    if mode == "before":
        assert not recovery_staging.path.exists()
        temporary_paths = tuple(recovery_staging.unit.glob(".recovery-request-*"))
        assert len(temporary_paths) == 1
        assert temporary_paths[0].read_bytes() == encode_recovery_request(
            recovery_staging.request
        )
    else:
        assert recovery_staging.path.read_bytes() == encode_recovery_request(
            recovery_staging.request
        )

    assert request_setup.persist(recovery_staging).record == recovery_staging.request
    assert request_setup.snapshot(recovery_staging.staging) == before


@pytest.mark.parametrize(
    "filename",
    ["receipt.json", ".recovery-receipt-retained"],
)
def test_request_operations_recognize_safe_receipt_evidence(recovery_staging, filename):
    request_setup.persist(recovery_staging)
    (recovery_staging.unit / filename).write_bytes(b"opaque receipt evidence")
    before = request_setup.snapshot(recovery_staging.root)

    assert request_setup.read(recovery_staging).record == recovery_staging.request
    assert request_setup.persist(recovery_staging).record == recovery_staging.request
    assert request_setup.snapshot(recovery_staging.root) == before


@pytest.mark.parametrize(
    "filename",
    ["receipt.json", ".recovery-receipt-retained"],
)
def test_receipt_evidence_cannot_manufacture_missing_request(
    recovery_staging, filename
):
    recovery_staging.unit.mkdir(parents=True)
    (recovery_staging.unit / filename).write_bytes(b"retained evidence")
    before = request_setup.snapshot(recovery_staging.root)

    for operation in (
        lambda: request_setup.read(recovery_staging),
        lambda: request_setup.persist(recovery_staging),
    ):
        with pytest.raises(RecoveryRequestIntegrityError) as caught:
            operation()
        assert caught.value.code == "receipt_without_final_request"

    assert not recovery_staging.path.exists()
    assert request_setup.snapshot(recovery_staging.root) == before

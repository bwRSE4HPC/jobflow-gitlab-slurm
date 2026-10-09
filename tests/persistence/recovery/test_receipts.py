"""persistence / recovery / test_receipts contracts."""

import errno
import json
import os
import stat
import subprocess
import sys
from dataclasses import FrozenInstanceError

import pytest
from pydantic import ValidationError

from jobflow_gitlab_slurm.persistence.attempts.ownership import (
    InvocationOwnershipError,
    owned_invocation,
)
from jobflow_gitlab_slurm.persistence.bundles import storage as bundles
from jobflow_gitlab_slurm.persistence.bundles.records import (
    BundleCommit,
    encode_bundle_commit,
    encode_bundle_manifest,
)
from jobflow_gitlab_slurm.persistence.recovery import receipts as storage
from jobflow_gitlab_slurm.persistence.recovery import requests
from jobflow_gitlab_slurm.persistence.recovery.receipts import (
    RecoveryReceiptConflictError,
    RecoveryReceiptInspectionError,
    RecoveryReceiptIntegrityError,
    RecoveryReceiptMissingError,
    RecoveryReceiptPublicationError,
    persist_owned_recovery_receipt,
    read_owned_recovery_receipt,
)
from jobflow_gitlab_slurm.persistence.recovery.records import (
    RecoveryReceiptRecord,
    encode_recovery_receipt,
)
from tests.persistence.recovery import _receipt_support as receipt_setup
from tests.persistence.recovery import _request_support as request_setup


def test_publish_read_and_exact_retry(committed_recovery_bundle):
    context = committed_recovery_bundle
    before_bundle = request_setup.snapshot(context.published)
    before_request = context.path.read_bytes()
    handle = receipt_setup.persist(context)
    expected = encode_recovery_receipt(context.receipt)
    inode = context.audit.stat().st_ino

    assert handle.path == context.audit
    assert handle.record == context.receipt
    assert handle.sha256 == request_setup.digest(expected)
    assert handle.size_bytes == len(expected)
    assert context.audit.read_bytes() == expected
    assert receipt_setup.read(context) == handle
    assert receipt_setup.persist(context) == handle
    assert context.audit.stat().st_ino == inode
    assert context.path.read_bytes() == before_request
    assert request_setup.snapshot(context.published) == before_bundle
    assert stat.S_IMODE(context.audit.stat().st_mode) == 0o600
    with pytest.raises(FrozenInstanceError):
        handle.path = context.root


def test_read_is_metadata_only_and_never_flushes(
    committed_recovery_bundle, monkeypatch
):
    context = committed_recovery_bundle
    receipt_setup.persist(context)
    before = request_setup.snapshot(context.root)

    def forbidden(*args, **kwargs):
        pytest.fail("receipt read must not flush, acknowledge, or inspect payloads")

    monkeypatch.setattr(storage, "_sync_file", forbidden)
    monkeypatch.setattr(storage, "_sync_directory", forbidden)
    monkeypatch.setattr(storage, "inspect_owned_bundle", forbidden)
    monkeypatch.setattr(requests, "persist_owned_recovery_request", forbidden)
    monkeypatch.setattr(bundles, "publish_owned_bundle", forbidden)

    assert receipt_setup.read(context).record == context.receipt
    assert request_setup.snapshot(context.root) == before


def test_missing_audit_namespace_is_read_only(recovery_staging):
    before = request_setup.snapshot(recovery_staging.root)
    with pytest.raises(RecoveryReceiptMissingError) as caught:
        receipt_setup.read(recovery_staging)
    assert caught.value.code == "receipt_missing"
    assert not recovery_staging.recovery_root.exists()
    assert request_setup.snapshot(recovery_staging.root) == before


@pytest.mark.parametrize("with_temporary", [False, True])
def test_missing_final_receipt_preserves_evidence(
    committed_recovery_bundle, with_temporary
):
    context = committed_recovery_bundle
    if with_temporary:
        receipt_setup.temporary(context, b"partial retained evidence")
    before = request_setup.snapshot(context.root)
    with pytest.raises(RecoveryReceiptMissingError) as caught:
        receipt_setup.read(context)
    assert len(caught.value.temporary_paths) == int(with_temporary)
    assert request_setup.snapshot(context.root) == before


@pytest.mark.parametrize("operation", ["read", "persist"])
def test_inactive_ownership_is_rejected(committed_recovery_bundle, operation):
    context = committed_recovery_bundle
    with owned_invocation(*context.parameters) as ownership:
        pass
    before = request_setup.snapshot(context.root)
    with pytest.raises(InvocationOwnershipError):
        if operation == "read":
            read_owned_recovery_receipt(ownership, request_setup.RECOVERY_ID)
        else:
            persist_owned_recovery_receipt(ownership, context.receipt)
    assert request_setup.snapshot(context.root) == before


@pytest.mark.parametrize("operation", ["read", "persist"])
def test_inherited_ownership_is_rejected(committed_recovery_bundle, operation):
    context = committed_recovery_bundle
    with owned_invocation(*context.parameters) as ownership:
        pid = os.fork()
        if pid == 0:
            try:
                if operation == "read":
                    read_owned_recovery_receipt(ownership, request_setup.RECOVERY_ID)
                else:
                    persist_owned_recovery_receipt(ownership, context.receipt)
                os._exit(1)
            except InvocationOwnershipError:
                os._exit(0)
            finally:
                os._exit(2)
        _, status = os.waitpid(pid, 0)
    assert os.waitstatus_to_exitcode(status) == 0


@pytest.mark.parametrize(
    "recovery_id", ["", "../escape", request_setup.RECOVERY_ID.upper()]
)
def test_invalid_read_selection_changes_nothing(committed_recovery_bundle, recovery_id):
    context = committed_recovery_bundle
    before = request_setup.snapshot(context.root)
    with pytest.raises(ValidationError):
        receipt_setup.read(context, recovery_id)
    assert request_setup.snapshot(context.root) == before


@pytest.mark.parametrize("change", ["version", "kind", "nested-reference"])
def test_invalid_expected_receipt_changes_nothing(committed_recovery_bundle, change):
    context = committed_recovery_bundle
    updates = {
        "version": {"schema_version": 2},
        "kind": {"kind": "wrong-kind"},
        "nested-reference": {
            "request": context.receipt.request.model_copy(update={"size_bytes": -1})
        },
    }
    invalid = context.receipt.model_copy(update=updates[change])
    before = request_setup.snapshot(context.root)
    with pytest.raises(RecoveryReceiptIntegrityError) as caught:
        receipt_setup.persist(context, invalid)
    assert caught.value.code == "invalid_expected_receipt"
    assert caught.value.recovery_id is None
    assert request_setup.snapshot(context.root) == before


@pytest.mark.parametrize(
    "change", ["request-hash", "manifest-hash", "commit-hash", "time"]
)
def test_receipt_binding_is_verified_before_changes(committed_recovery_bundle, change):
    context = committed_recovery_bundle
    document = context.receipt.model_dump()
    if change == "time":
        document["created_at"] = request_setup.timestamp(context.run, 5)
    else:
        field = {
            "request-hash": "request",
            "manifest-hash": "bundle_manifest",
            "commit-hash": "bundle_commit",
        }[change]
        document[field]["sha256"] = "0" * 64
    changed = RecoveryReceiptRecord.model_validate(document)
    before = request_setup.snapshot(context.root)
    with pytest.raises(RecoveryReceiptIntegrityError) as caught:
        receipt_setup.persist(context, changed)
    assert caught.value.code == "receipt_binding_invalid"
    assert request_setup.snapshot(context.root) == before


@pytest.mark.parametrize("damage", ["missing", "malformed", "bad-intent"])
def test_missing_or_invalid_prerequisites_are_held(committed_recovery_bundle, damage):
    context = committed_recovery_bundle
    if damage == "missing":
        context.path.unlink()
    elif damage == "malformed":
        context.path.write_bytes(b"{")
    else:
        context.intent_path.write_bytes(b"{")
    before = request_setup.snapshot(context.root)
    with pytest.raises(RecoveryReceiptIntegrityError):
        receipt_setup.persist(context)
    assert not context.audit.exists()
    assert request_setup.snapshot(context.root) == before


def test_receipt_without_request_is_held(committed_recovery_bundle):
    context = committed_recovery_bundle
    context.audit.write_bytes(encode_recovery_receipt(context.receipt))
    context.path.unlink()
    before = request_setup.snapshot(context.root)
    for operation in (
        lambda: receipt_setup.read(context),
        lambda: receipt_setup.persist(context),
    ):
        with pytest.raises(RecoveryReceiptIntegrityError) as caught:
            operation()
        assert caught.value.code == "request_receipt_without_final_request"
    assert request_setup.snapshot(context.root) == before


@pytest.mark.parametrize("namespace", ["different-id", "multiple-ids"])
def test_reserved_identity_cannot_be_bypassed(committed_recovery_bundle, namespace):
    context = committed_recovery_bundle
    before = request_setup.snapshot(context.root)
    if namespace == "different-id":
        document = context.receipt.model_dump()
        document["recovery_id"] = request_setup.OTHER_ID
        document["request"]["path"] = (
            (context.recovery_root / request_setup.OTHER_ID / "request.json")
            .relative_to(context.run.path)
            .as_posix()
        )
        changed = RecoveryReceiptRecord.model_validate(document)
        with pytest.raises(RecoveryReceiptConflictError) as caught:
            receipt_setup.persist(context, changed)
        assert caught.value.existing_recovery_ids == (request_setup.RECOVERY_ID,)
        assert request_setup.snapshot(context.root) == before
    else:
        (context.recovery_root / request_setup.OTHER_ID).mkdir()
        before = request_setup.snapshot(context.root)
        with pytest.raises(RecoveryReceiptIntegrityError):
            receipt_setup.persist(context)
        assert request_setup.snapshot(context.root) == before


@pytest.mark.parametrize("damage", ["json", "noncanonical", "binding"])
def test_invalid_stored_receipt_is_held(committed_recovery_bundle, damage):
    context = committed_recovery_bundle
    if damage == "json":
        data = b"{"
    elif damage == "noncanonical":
        data = encode_recovery_receipt(context.receipt) + b"\n"
    else:
        changed = context.receipt.model_dump()
        changed["request"]["sha256"] = "0" * 64
        data = encode_recovery_receipt(RecoveryReceiptRecord.model_validate(changed))
    context.audit.write_bytes(data)
    before = request_setup.snapshot(context.root)
    for operation in (
        lambda: receipt_setup.read(context),
        lambda: receipt_setup.persist(context),
    ):
        with pytest.raises(RecoveryReceiptIntegrityError) as caught:
            operation()
        assert caught.value.code == "invalid_stored_receipt"
    assert request_setup.snapshot(context.root) == before


def test_changed_valid_receipt_is_not_replaced(committed_recovery_bundle):
    context = committed_recovery_bundle
    receipt_setup.persist(context)
    changed = context.receipt.model_copy(
        update={"created_at": request_setup.timestamp(context.run, 8)}
    )
    before = request_setup.snapshot(context.root)
    with pytest.raises(RecoveryReceiptConflictError) as caught:
        receipt_setup.persist(context, changed)
    assert caught.value.code == "expected_receipt_conflict"
    assert request_setup.snapshot(context.root) == before


@pytest.mark.parametrize("damage", ["partial", "conflicting", "multiple"])
def test_temporary_holds_preserve_evidence(committed_recovery_bundle, damage):
    context = committed_recovery_bundle
    if damage == "partial":
        receipt_setup.temporary(context, b"{")
    elif damage == "conflicting":
        changed = context.receipt.model_copy(
            update={"created_at": request_setup.timestamp(context.run, 8)}
        )
        receipt_setup.temporary(context, encode_recovery_receipt(changed))
    else:
        data = encode_recovery_receipt(context.receipt)
        receipt_setup.temporary(context, data, "one")
        receipt_setup.temporary(context, data, "two")
    before = request_setup.snapshot(context.root)
    with pytest.raises((RecoveryReceiptIntegrityError, RecoveryReceiptConflictError)):
        receipt_setup.persist(context)
    assert not context.audit.exists()
    assert request_setup.snapshot(context.root) == before


def test_matching_temporary_is_resumed(committed_recovery_bundle, monkeypatch):
    context = committed_recovery_bundle
    candidate = receipt_setup.temporary(
        context, encode_recovery_receipt(context.receipt)
    )
    inode = candidate.stat().st_ino

    def forbidden(*args, **kwargs):
        pytest.fail("matching temporary receipt must not be regenerated")

    monkeypatch.setattr(storage.tempfile, "mkstemp", forbidden)
    assert receipt_setup.persist(context).record == context.receipt
    assert context.audit.stat().st_ino == inode
    assert not candidate.exists()


def test_safe_leftover_temporary_evidence_is_retained(committed_recovery_bundle):
    context = committed_recovery_bundle
    receipt_setup.persist(context)
    first = receipt_setup.temporary(context, b"partial evidence", "one")
    second = receipt_setup.temporary(context, b"other evidence", "two")
    before = request_setup.snapshot(context.root)
    assert receipt_setup.persist(context).record == context.receipt
    assert first.exists() and second.exists()
    assert request_setup.snapshot(context.root) == before


@pytest.mark.parametrize(
    "damage",
    [
        "absent",
        "staging",
        "unmarked",
        "ambiguous",
        "payload",
        "marker",
        "scheduler-receipt",
    ],
)
@pytest.mark.parametrize("existing_receipt", [False, True])
def test_only_current_valid_committed_bundles_are_acknowledged(
    committed_recovery_bundle, damage, existing_receipt
):
    context = committed_recovery_bundle
    if existing_receipt:
        receipt_setup.persist(context)
    if damage == "absent":
        context.published.rename(context.invocation.path / "retained-bundle")
    elif damage == "staging":
        context.published.rename(context.staging)
        (context.staging / "COMMIT.json").unlink()
    elif damage == "unmarked":
        (context.published / "COMMIT.json").unlink()
    elif damage == "ambiguous":
        context.staging.mkdir()
    elif damage == "payload":
        (context.published / "response.json").write_bytes(b"changed")
    elif damage == "marker":
        (context.published / "COMMIT.json").write_bytes(b"{")
    else:
        (context.invocation.attempt.path / "slurm-receipt.json").write_bytes(b"changed")
    before = request_setup.snapshot(context.root)
    with pytest.raises(RecoveryReceiptIntegrityError):
        receipt_setup.persist(context)
    assert request_setup.snapshot(context.root) == before
    if existing_receipt:
        assert receipt_setup.read(context).record == context.receipt
    else:
        assert not context.audit.exists()


@pytest.mark.parametrize("changed_record", ["manifest", "commit"])
def test_valid_but_different_bundle_metadata_is_a_conflict(
    committed_recovery_bundle, changed_record
):
    context = committed_recovery_bundle
    manifest = context.intent.expected_manifest
    commit = context.intent.expected_commit
    if changed_record == "manifest":
        manifest = manifest.model_copy(
            update={"created_at": request_setup.timestamp(context.run, 4)}
        )
        data = encode_bundle_manifest(manifest)
        (context.published / "payload-manifest.json").write_bytes(data)
        commit = BundleCommit.model_validate(
            {
                **commit.model_dump(),
                "manifest": request_setup.reference("payload-manifest.json", data),
            }
        )
    else:
        commit = commit.model_copy(
            update={"created_at": request_setup.timestamp(context.run, 5)}
        )
    (context.published / "COMMIT.json").write_bytes(encode_bundle_commit(commit))
    before = request_setup.snapshot(context.root)
    with pytest.raises(RecoveryReceiptConflictError) as caught:
        receipt_setup.persist(context)
    assert caught.value.code == "expected_bundle_record_conflict"
    assert request_setup.snapshot(context.root) == before


def test_no_ordinary_bundle_publisher_is_called(committed_recovery_bundle, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("receipt storage must never invoke bundle publication")

    monkeypatch.setattr(bundles, "publish_owned_bundle", forbidden)
    assert (
        receipt_setup.persist(committed_recovery_bundle).record
        == committed_recovery_bundle.receipt
    )


@pytest.mark.parametrize("phase", receipt_setup.PHASES)
def test_publication_faults_report_phase_and_preserve_bundle(
    committed_recovery_bundle, monkeypatch, phase
):
    context = committed_recovery_bundle
    before_bundle = request_setup.snapshot(context.published)
    before_request = context.path.read_bytes()
    original = storage._step

    def fail_at(progress, current_phase, operation, *args):
        if current_phase == phase:
            progress.phase = current_phase
            raise OSError(errno.EIO, request_setup.SECRET)
        return original(progress, current_phase, operation, *args)

    monkeypatch.setattr(storage, "_step", fail_at)
    with pytest.raises(RecoveryReceiptPublicationError) as caught:
        receipt_setup.persist(context)

    error = caught.value
    assert error.phase == phase
    assert error.operation == "publish"
    assert error.errno == errno.EIO
    assert error.receipt_publication_uncertain == (
        receipt_setup.PHASES.index(phase)
        >= receipt_setup.PHASES.index("receipt-rename")
    )
    assert error.bundle_acknowledged == (
        receipt_setup.PHASES.index(phase)
        > receipt_setup.PHASES.index("bundle-verification")
    )
    assert error.preparation_uncertain == (
        receipt_setup.PHASES.index(phase)
        >= receipt_setup.PHASES.index("temporary-create")
    )
    assert request_setup.SECRET not in str(error)
    assert request_setup.SECRET not in json.dumps(error.to_report())
    assert request_setup.snapshot(context.published) == before_bundle
    assert context.path.read_bytes() == before_request

    if receipt_setup.PHASES.index(phase) > receipt_setup.PHASES.index("receipt-rename"):
        assert error.temporary_path is not None
        assert not error.temporary_path.exists()
        assert context.audit.read_bytes() == encode_recovery_receipt(context.receipt)
    else:
        assert not context.audit.exists()
        if error.temporary_path is not None:
            assert error.temporary_path.exists()


def test_final_verification_detects_changed_receipt(
    committed_recovery_bundle, monkeypatch
):
    context = committed_recovery_bundle
    original = storage._step

    def changed(progress, phase, operation, *args):
        if phase == "final-verification":
            context.audit.write_bytes(b"{")
        return original(progress, phase, operation, *args)

    monkeypatch.setattr(storage, "_step", changed)
    with pytest.raises(RecoveryReceiptPublicationError) as caught:
        receipt_setup.persist(context)
    assert caught.value.phase == "final-verification"
    assert caught.value.reason == "invalid_stored_receipt"
    assert caught.value.receipt_publication_uncertain


@pytest.mark.parametrize("operation", ["namespace", "parents", "receipt-read"])
def test_io_errors_are_sanitized(committed_recovery_bundle, monkeypatch, operation):
    context = committed_recovery_bundle
    if operation == "namespace":
        original = requests._absent

        def absent(path):
            if path == context.unit:
                raise OSError(errno.EIO, request_setup.SECRET)
            return original(path)

        monkeypatch.setattr(requests, "_absent", absent)
        action = lambda: receipt_setup.persist(context)
    elif operation == "parents":

        def unavailable(*args, **kwargs):
            raise requests.RecoveryRequestInspectionError(
                context.invocation.record,
                request_setup.RECOVERY_ID,
                context.path,
                "request_unavailable",
                errno=errno.EIO,
            )

        monkeypatch.setattr(requests, "read_owned_recovery_request", unavailable)
        action = lambda: receipt_setup.persist(context)
    else:
        receipt_setup.persist(context)

        def unreadable(path):
            raise OSError(errno.EIO, request_setup.SECRET)

        monkeypatch.setattr(storage, "_read_bytes", unreadable)
        action = lambda: receipt_setup.read(context)

    with pytest.raises(RecoveryReceiptInspectionError) as caught:
        action()
    assert caught.value.errno == errno.EIO
    assert request_setup.SECRET not in str(caught.value)
    assert request_setup.SECRET not in json.dumps(caught.value.to_report())


def test_reports_are_detached(committed_recovery_bundle):
    context = committed_recovery_bundle
    data = encode_recovery_receipt(context.receipt)
    receipt_setup.temporary(context, data, "one")
    receipt_setup.temporary(context, data, "two")
    with pytest.raises(RecoveryReceiptIntegrityError) as caught:
        receipt_setup.persist(context)
    report = caught.value.to_report()
    assert report["temporary_paths"]
    report["temporary_paths"].clear()
    report["identity"]["run_id"] = request_setup.OTHER_ID
    fresh = caught.value.to_report()
    assert len(fresh["temporary_paths"]) == 2
    assert fresh["identity"]["run_id"] == context.run.manifest.run_id


@pytest.mark.parametrize("phase", ["temporary-file-flush", "receipt-rename"])
def test_process_exit_retries_same_identity(committed_recovery_bundle, phase):
    context = committed_recovery_bundle
    expected_file = context.root.parent / "expected-receipt.json"
    expected_file.write_bytes(encode_recovery_receipt(context.receipt))
    before_bundle = request_setup.snapshot(context.published)
    script = """
import os
import sys
from pathlib import Path
from jobflow_gitlab_slurm.persistence.recovery import receipts as storage
from jobflow_gitlab_slurm.persistence.attempts.ownership import owned_invocation
from jobflow_gitlab_slurm.persistence.recovery.records import decode_recovery_receipt

root, run, job, index, attempt, invocation, filename, phase = sys.argv[1:]
receipt = decode_recovery_receipt(Path(filename).read_bytes())
original = storage._step

def exit_after(progress, current_phase, operation, *args):
    result = original(progress, current_phase, operation, *args)
    if current_phase == phase:
        os._exit(73)
    return result

storage._step = exit_after
with owned_invocation(root, run, job, int(index), attempt, invocation) as ownership:
    storage.persist_owned_recovery_receipt(ownership, receipt)
"""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            *(str(value) for value in context.parameters),
            str(expected_file),
            phase,
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 73, result.stderr
    assert receipt_setup.persist(context).record == context.receipt
    assert receipt_setup.read(context).record == context.receipt
    assert context.audit.read_bytes() == expected_file.read_bytes()
    assert request_setup.snapshot(context.published) == before_bundle
    assert tuple(context.recovery_root.iterdir()) == (context.unit,)

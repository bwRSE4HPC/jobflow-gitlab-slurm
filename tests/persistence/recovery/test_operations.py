"""persistence / recovery / test_operations contracts."""

import errno
import json
import os

import pytest

from jobflow_gitlab_slurm.persistence.attempts.ownership import (
    InvocationOwnershipError,
    owned_invocation,
)
from jobflow_gitlab_slurm.persistence.bundles import storage as bundles
from jobflow_gitlab_slurm.persistence.bundles.inspection import BundleInspectionError
from jobflow_gitlab_slurm.persistence.bundles.records import (
    BundleCommit,
    encode_bundle_commit,
    encode_bundle_manifest,
)
from jobflow_gitlab_slurm.persistence.recovery import operations as recovery
from jobflow_gitlab_slurm.persistence.recovery import receipts, requests
from jobflow_gitlab_slurm.persistence.recovery.operations import (
    BundleRecoveryCompletionError,
    BundleRecoveryConflictError,
    BundleRecoveryInspectionError,
    BundleRecoveryIntegrityError,
    recover_owned_bundle,
)
from jobflow_gitlab_slurm.persistence.recovery.records import (
    encode_recovery_receipt,
    encode_recovery_request,
)
from tests.persistence.recovery import _operation_support as operation_setup
from tests.persistence.recovery import _request_support as request_setup


def test_initial_recovery_publishes_request_bundle_and_receipt(
    recoverable_bundle,
):
    context = recoverable_bundle
    before_payloads = operation_setup.payload_snapshot(context)
    assert not context.path.exists()

    handle = operation_setup.recover(context)

    assert handle.path == context.audit
    assert handle.record == context.receipt
    assert context.path.read_bytes() == encode_recovery_request(context.request)
    assert context.audit.read_bytes() == encode_recovery_receipt(context.receipt)
    assert not context.staging.exists()
    assert (context.published / "COMMIT.json").read_bytes() == encode_bundle_commit(
        context.intent.expected_commit
    )
    assert operation_setup.payload_snapshot(context) == before_payloads
    assert tuple(context.recovery_root.iterdir()) == (context.unit,)


@pytest.mark.parametrize(("original", "current"), operation_setup.ALLOWED)
def test_allowed_forward_progress_and_exact_retry(
    recoverable_bundle, original, current
):
    context = recoverable_bundle
    operation_setup.original_state(context, original)
    operation_setup.current_state(context, current)
    before_payloads = operation_setup.payload_snapshot(context)
    original_request = context.path.read_bytes()

    handle = operation_setup.recover(context)
    before_retry = request_setup.snapshot(context.root)

    assert handle.record == context.receipt
    assert operation_setup.recover(context) == handle
    assert request_setup.snapshot(context.root) == before_retry
    assert context.path.read_bytes() == original_request
    assert operation_setup.payload_snapshot(context) == before_payloads
    assert (
        requests.decode_recovery_request(context.path.read_bytes()).observed_status
        == original
    )


@pytest.mark.parametrize(
    "damage",
    ["absent", "ambiguous", "document", "response", "manifest", "scheduler-receipt"],
)
def test_incomplete_or_invalid_results_are_never_reconstructed(
    recoverable_bundle, damage
):
    context = recoverable_bundle
    if damage == "absent":
        context.staging.rename(context.invocation.path / "retained-output")
    elif damage == "ambiguous":
        context.published.mkdir()
    elif damage == "document":
        (context.staging / "job-document.json").unlink()
    elif damage == "response":
        (context.staging / "response.json").write_bytes(b"changed")
    elif damage == "manifest":
        (context.staging / "payload-manifest.json").write_bytes(b"{")
    else:
        (context.invocation.attempt.path / "slurm-receipt.json").write_bytes(b"changed")
    before = request_setup.snapshot(context.root)

    with pytest.raises(BundleRecoveryIntegrityError):
        operation_setup.recover(context)

    assert request_setup.snapshot(context.root) == before
    assert not context.path.exists()
    assert not context.audit.exists()


@pytest.mark.parametrize("changed_record", ["manifest", "commit"])
def test_valid_different_bundle_records_are_conflicts(
    recoverable_bundle, changed_record
):
    context = recoverable_bundle
    operation_setup.original_state(context, "committed")
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

    with pytest.raises(BundleRecoveryConflictError):
        operation_setup.recover(context)

    assert request_setup.snapshot(context.root) == before


@pytest.mark.parametrize("record", ["request", "receipt"])
def test_invalid_caller_models_fail_before_changes(recoverable_bundle, record):
    context = recoverable_bundle
    request = context.request
    receipt = context.receipt
    if record == "request":
        request = request.model_copy(update={"schema_version": 2})
    else:
        receipt = receipt.model_copy(update={"schema_version": 2})
    before = request_setup.snapshot(context.root)

    with pytest.raises(BundleRecoveryIntegrityError) as caught:
        operation_setup.recover(context, request, receipt)

    assert caught.value.code == "invalid_expected_recovery_records"
    assert caught.value.recovery_id is None
    assert request_setup.snapshot(context.root) == before


@pytest.mark.parametrize("record", ["request", "receipt"])
def test_invalid_cross_record_binding_fails_before_changes(recoverable_bundle, record):
    context = recoverable_bundle
    request = context.request
    receipt = context.receipt
    if record == "request":
        request = request.model_copy(update={"intent_id": request_setup.OTHER_ID})
        context.request = request
        receipt = operation_setup.receipt_for(context)
    else:
        receipt = receipt.model_copy(
            update={"created_at": request_setup.timestamp(context.run, 5)}
        )
    before = request_setup.snapshot(context.root)

    with pytest.raises(BundleRecoveryIntegrityError) as caught:
        operation_setup.recover(context, request, receipt)

    assert caught.value.code == "recovery_binding_invalid"
    assert request_setup.snapshot(context.root) == before


@pytest.mark.parametrize("operation", ["inactive", "inherited"])
def test_ownership_context_and_process_guard(recoverable_bundle, operation):
    context = recoverable_bundle
    with owned_invocation(*context.parameters) as ownership:
        if operation == "inherited":
            pid = os.fork()
            if pid == 0:
                try:
                    recover_owned_bundle(
                        ownership, context.request, receipt=context.receipt
                    )
                    os._exit(1)
                except InvocationOwnershipError:
                    os._exit(0)
                finally:
                    os._exit(2)
            _, status = os.waitpid(pid, 0)
            assert os.waitstatus_to_exitcode(status) == 0
    before = request_setup.snapshot(context.root)
    with pytest.raises(InvocationOwnershipError):
        recover_owned_bundle(ownership, context.request, receipt=context.receipt)
    assert request_setup.snapshot(context.root) == before


@pytest.mark.parametrize("record", ["request", "receipt"])
def test_valid_changed_audit_records_are_not_adopted(recoverable_bundle, record):
    context = recoverable_bundle
    operation_setup.retain_request(context)
    if record == "request":
        changed = context.request.model_copy(
            update={"created_at": request_setup.timestamp(context.run, 7)}
        )
        context.path.write_bytes(encode_recovery_request(changed))
    else:
        changed = context.receipt.model_copy(
            update={"created_at": request_setup.timestamp(context.run, 8)}
        )
        context.audit.write_bytes(encode_recovery_receipt(changed))
    before = request_setup.snapshot(context.root)

    with pytest.raises(BundleRecoveryConflictError):
        operation_setup.recover(context)

    assert request_setup.snapshot(context.root) == before


@pytest.mark.parametrize("namespace", ["different-id", "multiple-ids"])
def test_reserved_recovery_identity_is_not_bypassed(recoverable_bundle, namespace):
    context = recoverable_bundle
    operation_setup.retain_request(context)
    if namespace == "different-id":
        request = context.request.model_copy(
            update={"recovery_id": request_setup.OTHER_ID}
        )
        context.request = request
        receipt = operation_setup.receipt_for(context)
        expected = BundleRecoveryConflictError
    else:
        (context.recovery_root / request_setup.OTHER_ID).mkdir()
        request = context.request
        receipt = context.receipt
        expected = BundleRecoveryIntegrityError
    before = request_setup.snapshot(context.root)

    with pytest.raises(expected):
        operation_setup.recover(context, request, receipt)

    assert request_setup.snapshot(context.root) == before


@pytest.mark.parametrize("record", ["request", "receipt"])
def test_exact_temporary_audit_records_are_resumed(recoverable_bundle, record):
    context = recoverable_bundle
    if record == "request":
        context.unit.mkdir(parents=True)
        path = context.unit / ".recovery-request-retained"
        path.write_bytes(encode_recovery_request(context.request))
        inode = path.stat().st_ino
    else:
        operation_setup.original_state(context, "committed")
        path = context.unit / ".recovery-receipt-retained"
        path.write_bytes(encode_recovery_receipt(context.receipt))
        inode = path.stat().st_ino

    assert operation_setup.recover(context).record == context.receipt
    final = context.path if record == "request" else context.audit
    assert final.stat().st_ino == inode
    assert not path.exists()


@pytest.mark.parametrize("final_receipt", [False, True])
def test_receipt_evidence_for_uncommitted_bundle_is_a_hold(
    recoverable_bundle, final_receipt
):
    context = recoverable_bundle
    operation_setup.retain_request(context)
    path = (
        context.audit if final_receipt else context.unit / ".recovery-receipt-retained"
    )
    path.write_bytes(encode_recovery_receipt(context.receipt))
    before = request_setup.snapshot(context.root)

    with pytest.raises(BundleRecoveryIntegrityError) as caught:
        operation_setup.recover(context)

    assert caught.value.code == "audit_completion_with_uncommitted_bundle"
    assert request_setup.snapshot(context.root) == before


@pytest.mark.parametrize("status", ["staging_only", "published_uncommitted"])
def test_exact_temporary_marker_is_reused(recoverable_bundle, monkeypatch, status):
    context = recoverable_bundle
    operation_setup.original_state(context, status)
    candidate = operation_setup.marker_temporary(
        context, encode_bundle_commit(context.intent.expected_commit)
    )
    inode = candidate.stat().st_ino

    def forbidden(*args, **kwargs):
        pytest.fail("exact retained marker must not be regenerated")

    monkeypatch.setattr(recovery, "_new_marker", forbidden)
    assert operation_setup.recover(context).record == context.receipt
    assert not candidate.exists()
    assert (context.published / "COMMIT.json").stat().st_ino == inode


@pytest.mark.parametrize("damage", ["partial", "conflicting", "multiple"])
def test_unmarked_temporary_marker_holds_preserve_evidence(recoverable_bundle, damage):
    context = recoverable_bundle
    operation_setup.original_state(context, "published_uncommitted")
    if damage == "partial":
        operation_setup.marker_temporary(context, b"{")
        expected = BundleRecoveryIntegrityError
    elif damage == "conflicting":
        changed = context.intent.expected_commit.model_copy(
            update={"created_at": request_setup.timestamp(context.run, 5)}
        )
        operation_setup.marker_temporary(context, encode_bundle_commit(changed))
        expected = BundleRecoveryConflictError
    else:
        data = encode_bundle_commit(context.intent.expected_commit)
        operation_setup.marker_temporary(context, data, "one")
        operation_setup.marker_temporary(context, data, "two")
        expected = BundleRecoveryIntegrityError
    before = request_setup.snapshot(context.root)

    with pytest.raises(expected):
        operation_setup.recover(context)

    assert request_setup.snapshot(context.root) == before
    assert not (context.published / "COMMIT.json").exists()


def test_exact_final_marker_preserves_safe_leftovers(recoverable_bundle):
    context = recoverable_bundle
    operation_setup.original_state(context, "committed")
    first = operation_setup.marker_temporary(
        context, b"partial retained evidence", "one"
    )
    second = operation_setup.marker_temporary(
        context, b"different retained evidence", "two"
    )
    before_marker = (context.published / "COMMIT.json").read_bytes()

    assert operation_setup.recover(context).record == context.receipt
    assert first.read_bytes() == b"partial retained evidence"
    assert second.read_bytes() == b"different retained evidence"
    assert (context.published / "COMMIT.json").read_bytes() == before_marker


def test_request_acknowledgment_precedes_every_bundle_mutation(
    recoverable_bundle, monkeypatch
):
    context = recoverable_bundle
    original = recovery._step
    phases = []

    def observed(progress, phase, operation, *args):
        phases.append(phase)
        if phase in ("directory-rename", "marker-create", "marker-rename"):
            assert progress.request_acknowledged
            assert context.path.read_bytes() == encode_recovery_request(context.request)
        if phase == "receipt-completion":
            assert (context.published / "COMMIT.json").read_bytes() == (
                encode_bundle_commit(context.intent.expected_commit)
            )
        return original(progress, phase, operation, *args)

    monkeypatch.setattr(recovery, "_step", observed)
    assert operation_setup.recover(context).record == context.receipt
    assert phases.index("request-acknowledgment") < phases.index("directory-rename")
    assert phases.index("committed-verification") < phases.index("receipt-completion")


def test_no_ordinary_publisher_or_scientific_decoder_is_used(
    recoverable_bundle, monkeypatch
):
    def forbidden(*args, **kwargs):
        pytest.fail("recovery must not invoke ordinary publication")

    monkeypatch.setattr(bundles, "publish_owned_bundle", forbidden)
    assert (
        operation_setup.recover(recoverable_bundle).record == recoverable_bundle.receipt
    )


def test_marker_selection_is_rechecked_before_rename(recoverable_bundle, monkeypatch):
    context = recoverable_bundle
    original = recovery._step
    retained = context.invocation.path / "retained-marker"

    def changed(progress, phase, operation, *args):
        if phase == "marker-ready-check":
            progress.temporary_path.rename(retained)
        return original(progress, phase, operation, *args)

    monkeypatch.setattr(recovery, "_step", changed)
    with pytest.raises(BundleRecoveryCompletionError) as caught:
        operation_setup.recover(context)
    assert caught.value.phase == "marker-ready-check"
    assert caught.value.reason == "temporary_marker_selection_changed"
    assert retained.read_bytes() == encode_bundle_commit(context.intent.expected_commit)
    assert not (context.published / "COMMIT.json").exists()


@pytest.mark.parametrize(
    "failure", ["intent", "inspection", "read", "value", "filesystem"]
)
def test_preflight_errors_are_classified_and_sanitized(
    recoverable_bundle, monkeypatch, failure
):
    context = recoverable_bundle
    if failure == "intent":

        def unavailable(*args):
            raise requests.RecoveryRequestInspectionError(
                context.invocation.record,
                request_setup.RECOVERY_ID,
                context.intent_path,
                "intent_unavailable",
                errno=errno.EIO,
            )

        monkeypatch.setattr(requests, "_intent", unavailable)
        expected = BundleRecoveryInspectionError
    elif failure == "inspection":

        def unavailable(*args):
            raise BundleInspectionError(
                context.invocation.record, context.staging, errno.EIO
            )

        monkeypatch.setattr(recovery, "inspect_owned_bundle", unavailable)
        expected = BundleRecoveryInspectionError
    elif failure in ("read", "value"):

        def unavailable(*args):
            if failure == "read":
                raise OSError(errno.EIO, request_setup.SECRET)
            raise ValueError(request_setup.SECRET)

        monkeypatch.setattr(recovery, "_read_bytes", unavailable)
        expected = (
            BundleRecoveryInspectionError
            if failure == "read"
            else BundleRecoveryIntegrityError
        )
    else:

        def unavailable(*args):
            raise bundles.BundleIntegrityError(
                context.invocation.record,
                context.staging,
                "cross_filesystem_bundle",
            )

        monkeypatch.setattr(bundles, "require_same_filesystem", unavailable)
        expected = BundleRecoveryIntegrityError

    before = request_setup.snapshot(context.root)
    with pytest.raises(expected) as caught:
        operation_setup.recover(context)
    assert request_setup.SECRET not in str(caught.value)
    assert request_setup.SECRET not in json.dumps(caught.value.to_report())
    assert request_setup.snapshot(context.root) == before


def test_receipt_inspection_error_is_translated(recoverable_bundle, monkeypatch):
    context = recoverable_bundle
    operation_setup.original_state(context, "committed")
    context.audit.write_bytes(encode_recovery_receipt(context.receipt))

    def unreadable(*args):
        raise receipts.RecoveryReceiptInspectionError(
            next_ownership,
            request_setup.RECOVERY_ID,
            context.audit,
            "receipt_unavailable",
            errno=errno.EIO,
        )

    with owned_invocation(*context.parameters) as next_ownership:
        monkeypatch.setattr(receipts, "_read", unreadable)
        with pytest.raises(BundleRecoveryInspectionError) as caught:
            recover_owned_bundle(
                next_ownership, context.request, receipt=context.receipt
            )
    assert caught.value.code == "receipt_unavailable"


def test_reports_are_detached(recoverable_bundle):
    context = recoverable_bundle
    operation_setup.original_state(context, "published_uncommitted")
    data = encode_bundle_commit(context.intent.expected_commit)
    operation_setup.marker_temporary(context, data, "one")
    operation_setup.marker_temporary(context, data, "two")
    with pytest.raises(BundleRecoveryIntegrityError) as caught:
        operation_setup.recover(context)
    report = caught.value.to_report()
    assert report["kind"] == "bundle-recovery-error"
    assert report["temporary_paths"]
    report["temporary_paths"].clear()
    report["identity"]["run_id"] = request_setup.OTHER_ID
    fresh = caught.value.to_report()
    assert len(fresh["temporary_paths"]) == 2
    assert fresh["identity"]["run_id"] == context.request.run_id

"""persistence / recovery / test_registration contracts."""

import errno
import os
import subprocess
import sys

import pytest
from pydantic import ValidationError

from jobflow_gitlab_slurm.persistence.attempts.ownership import (
    InvocationOwnershipError,
    owned_invocation,
)
from jobflow_gitlab_slurm.persistence.attempts.records import RecordArtifactReference
from jobflow_gitlab_slurm.persistence.bundles import storage as bundles
from jobflow_gitlab_slurm.persistence.journal import storage as journal
from jobflow_gitlab_slurm.persistence.journal.records import (
    EventRecord,
)
from jobflow_gitlab_slurm.persistence.recovery import operations as recovery
from jobflow_gitlab_slurm.persistence.recovery import receipts, requests
from jobflow_gitlab_slurm.persistence.recovery import registration as association
from jobflow_gitlab_slurm.persistence.recovery.records import (
    encode_recovery_receipt,
    encode_recovery_request,
)
from jobflow_gitlab_slurm.persistence.recovery.registration import (
    EVENT_TYPE,
    RecoveryCompletedPayload,
    RecoveryJournalConflictError,
    RecoveryJournalIncompleteError,
    RecoveryJournalInspectionError,
    RecoveryJournalIntegrityError,
    register_owned_recovery_event,
)
from jobflow_gitlab_slurm.persistence.runs.storage import RunBusyError, locked_run
from tests.persistence.recovery import _registration_support as registration_setup
from tests.persistence.recovery import _request_support as request_setup


def test_register_exact_evidence_and_retry_without_rewriting(
    recovered_bundle_for_registration,
):
    context = recovered_bundle_for_registration
    before = request_setup.snapshot(context.invocation.path)
    flow_path = context.run.path / "flow/payload.json"
    flow_bytes = flow_path.read_bytes()

    first = registration_setup.register(context)
    after_first = request_setup.snapshot(context.root)
    second = registration_setup.register(context)

    assert first == second
    assert first.event_id == context.request.journal_event_id
    assert first.event_type == EVENT_TYPE
    assert first.sequence == 1
    assert journal.read_events(context.root, context.request.run_id).events == (first,)
    assert request_setup.snapshot(context.root) == after_first
    assert request_setup.snapshot(context.invocation.path) == before
    assert flow_path.read_bytes() == flow_bytes

    payload = RecoveryCompletedPayload.model_validate(first.payload())
    assert payload.result == "committed_bundle_verified"
    assert payload.receipt.model_dump() == request_setup.reference(
        context.audit.relative_to(context.run.path).as_posix(),
        context.audit.read_bytes(),
    )
    assert payload.intent == context.request.intent
    assert payload.request == context.receipt.request
    assert payload.bundle_manifest == context.receipt.bundle_manifest
    assert payload.bundle_commit == context.receipt.bundle_commit
    assert "actor" not in first.payload()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("schema_version", 2),
        ("schema_version", True),
        ("kind", "different"),
        ("run_id", "invalid"),
        ("job_uuid", ""),
        ("job_uuid", "different-job"),
        ("job_key", "0" * 64),
        ("job_index", True),
        ("job_index", 0),
        ("job_index", 2),
        ("attempt_id", request_setup.OTHER_ID),
        ("invocation_id", request_setup.OTHER_ID),
        ("recovery_id", request_setup.OTHER_ID),
        ("result", "scientific_success"),
        ("unexpected", "value"),
    ],
)
def test_payload_rejects_invalid_or_unbound_fields(
    recovered_bundle_for_registration, field, value
):
    data = registration_setup.payload_for(recovered_bundle_for_registration).model_dump(
        mode="json"
    )
    data[field] = value
    with pytest.raises(ValidationError):
        RecoveryCompletedPayload.model_validate(data)


@pytest.mark.parametrize("field", registration_setup.REFERENCES)
@pytest.mark.parametrize("damage", ["path", "empty", "nested-instance"])
def test_payload_rejects_invalid_references(
    recovered_bundle_for_registration, field, damage
):
    data = registration_setup.payload_for(recovered_bundle_for_registration).model_dump(
        mode="json"
    )
    if damage == "path":
        data[field]["path"] = "other/record.json"
    elif damage == "empty":
        data[field]["size_bytes"] = 0
    else:
        reference_model = RecordArtifactReference.model_validate(data[field])
        data[field] = reference_model.model_copy(update={"sha256": "invalid"})

    with pytest.raises(ValidationError):
        RecoveryCompletedPayload.model_validate(data)


def test_payload_is_frozen_revalidated_and_detached(recovered_bundle_for_registration):
    payload = registration_setup.payload_for(recovered_bundle_for_registration)
    with pytest.raises(ValidationError):
        payload.result = "different"

    changed = payload.model_copy(update={"schema_version": 2})
    with pytest.raises(ValidationError):
        RecoveryCompletedPayload.model_validate(changed)

    detached = payload.model_dump(mode="json")
    detached["receipt"]["sha256"] = "0" * 64
    assert payload.receipt.sha256 != detached["receipt"]["sha256"]


@pytest.mark.parametrize("record", ["request", "receipt"])
def test_invalid_caller_records_do_not_mutate_files(
    recovered_bundle_for_registration, record
):
    context = recovered_bundle_for_registration
    request = context.request
    receipt = context.receipt
    if record == "request":
        request = request.model_copy(update={"schema_version": 2})
    else:
        receipt = receipt.model_copy(update={"schema_version": 2})
    before = request_setup.snapshot(context.root)

    with pytest.raises(RecoveryJournalIntegrityError) as caught:
        registration_setup.register(context, request, receipt)

    report = registration_setup.assert_private(caught.value)
    assert report["phase"] == "caller-validation"
    assert report["filesystem_acknowledged_in_this_call"] is False
    assert request_setup.snapshot(context.root) == before


@pytest.mark.parametrize("record", ["request", "receipt"])
def test_valid_but_incorrect_caller_binding_is_held(
    recovered_bundle_for_registration, record
):
    context = recovered_bundle_for_registration
    request = context.request
    receipt = context.receipt
    if record == "request":
        request = request.model_copy(update={"intent_id": request_setup.OTHER_ID})
    else:
        receipt = receipt.model_copy(
            update={"created_at": request_setup.timestamp(context.run, 5)}
        )
    before = request_setup.snapshot(context.root)

    with pytest.raises(RecoveryJournalIntegrityError):
        registration_setup.register(context, request, receipt)

    assert request_setup.snapshot(context.root) == before


@pytest.mark.parametrize(
    "damage",
    [
        "missing-request",
        "missing-receipt",
        "temporary-receipt",
        "bad-request",
        "bad-receipt",
        "bad-intent",
        "missing-marker",
        "changed-document",
        "changed-scheduler-receipt",
        "ambiguous-bundle",
    ],
)
def test_missing_or_invalid_evidence_is_not_repaired(
    recovered_bundle_for_registration, damage
):
    context = recovered_bundle_for_registration
    if damage == "missing-request":
        context.path.unlink()
    elif damage == "missing-receipt":
        context.audit.unlink()
    elif damage == "temporary-receipt":
        context.audit.rename(context.unit / ".recovery-receipt-retained")
    elif damage == "bad-request":
        context.path.write_bytes(b"{")
    elif damage == "bad-receipt":
        context.audit.write_bytes(b"{")
    elif damage == "bad-intent":
        context.intent_path.write_bytes(b"{")
    elif damage == "missing-marker":
        (context.published / "COMMIT.json").unlink()
    elif damage == "changed-document":
        (context.published / "job-document.json").write_bytes(b"changed")
    elif damage == "changed-scheduler-receipt":
        (context.invocation.attempt.path / "slurm-receipt.json").write_bytes(b"changed")
    else:
        context.staging.mkdir()
    before = request_setup.snapshot(context.root)

    with pytest.raises(RecoveryJournalIntegrityError):
        registration_setup.register(context)

    assert request_setup.snapshot(context.root) == before
    assert not (context.run.path / "events").exists()


@pytest.mark.parametrize("record", ["request", "receipt"])
def test_valid_different_retained_audit_records_conflict(
    recovered_bundle_for_registration, record
):
    context = recovered_bundle_for_registration
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

    with pytest.raises(RecoveryJournalConflictError) as caught:
        registration_setup.register(context)

    registration_setup.assert_private(caught.value)
    assert request_setup.snapshot(context.root) == before


def test_old_event_does_not_substitute_for_current_evidence(
    recovered_bundle_for_registration,
):
    context = recovered_bundle_for_registration
    first = registration_setup.register(context)
    context.audit.unlink()
    before = request_setup.snapshot(context.root)

    with pytest.raises(RecoveryJournalIntegrityError):
        registration_setup.register(context)

    assert request_setup.snapshot(context.root) == before
    assert journal.read_events(context.root, context.request.run_id).events == (first,)


def test_older_event_retry_preserves_later_head(recovered_bundle_for_registration):
    context = recovered_bundle_for_registration
    first = registration_setup.register(context)
    later = journal.append_event(
        context.root,
        context.request.run_id,
        event_id=request_setup.OTHER_ID,
        event_type="example.note",
        payload={"value": 1},
    )
    before = request_setup.snapshot(context.root)

    assert registration_setup.register(context) == first
    assert request_setup.snapshot(context.root) == before
    assert journal.read_events(context.root, context.request.run_id).events == (
        first,
        later,
    )


@pytest.mark.parametrize("change", ["type", "payload"])
def test_conflicting_recorded_event_input_is_held(
    recovered_bundle_for_registration, change
):
    context = recovered_bundle_for_registration
    payload = registration_setup.payload_for(context).model_dump(mode="json")
    event_type = EVENT_TYPE
    if change == "type":
        event_type = "example.other"
    else:
        payload["result"] = "different"
    journal.append_event(
        context.root,
        context.request.run_id,
        event_id=context.request.journal_event_id,
        event_type=event_type,
        payload=payload,
    )
    before = request_setup.snapshot(context.root)

    with pytest.raises(RecoveryJournalConflictError) as caught:
        registration_setup.register(context)

    report = registration_setup.assert_private(caught.value)
    assert report["code"] == "journal_event_conflict"
    assert report["filesystem_acknowledged_in_this_call"] is True
    assert report["journal_registration_uncertain"] is False
    assert request_setup.snapshot(context.root) == before


def test_exact_pending_tail_is_finished_without_duplicate(
    recovered_bundle_for_registration, monkeypatch
):
    context = recovered_bundle_for_registration
    pending = registration_setup.make_pending(context, monkeypatch)
    original_bytes = registration_setup.event_path(context).read_bytes()
    with pytest.raises(journal.JournalIncompleteError):
        journal.read_events(context.root, context.request.run_id)

    assert registration_setup.register(context) == pending
    assert registration_setup.register(context) == pending
    assert registration_setup.event_path(context).read_bytes() == original_bytes
    assert journal.read_events(context.root, context.request.run_id).events == (
        pending,
    )


def test_unrelated_pending_tail_remains_held(
    recovered_bundle_for_registration, monkeypatch
):
    context = recovered_bundle_for_registration
    pending = registration_setup.make_pending(context, monkeypatch, unrelated=True)
    before = request_setup.snapshot(context.root)

    with pytest.raises(RecoveryJournalIncompleteError) as caught:
        registration_setup.register(context)

    report = registration_setup.assert_private(caught.value)
    assert report["pending_event_id"] == pending.event_id
    assert report["pending_sequence"] == pending.sequence
    assert report["pending_path"] == str(registration_setup.event_path(context))
    assert report["filesystem_acknowledged_in_this_call"] is True
    assert request_setup.snapshot(context.root) == before


@pytest.mark.parametrize("damage", ["head", "event"])
def test_corrupt_journal_is_not_reconstructed(
    recovered_bundle_for_registration, damage
):
    context = recovered_bundle_for_registration
    registration_setup.register(context)
    path = (
        context.run.path / "journal-head.json"
        if damage == "head"
        else registration_setup.event_path(context)
    )
    path.write_bytes(b"{")
    before = request_setup.snapshot(context.root)

    with pytest.raises(RecoveryJournalIntegrityError) as caught:
        registration_setup.register(context)

    assert caught.value.code == "journal_integrity_invalid"
    assert request_setup.snapshot(context.root) == before


def test_payload_io_precedes_run_lock_and_no_recovery_is_called(
    recovered_bundle_for_registration, monkeypatch
):
    context = recovered_bundle_for_registration
    order = []
    original_ack = receipts.persist_owned_recovery_receipt
    original_append = journal.append_event

    def forbidden(*args, **kwargs):
        pytest.fail("registration must not publish or recover a bundle")

    def acknowledge(ownership, receipt):
        ownership.require_active()
        with locked_run(context.root, context.request.run_id):
            order.append("payload-io-outside-run-lock")
        result = original_ack(ownership, receipt)
        order.append("filesystem-acknowledged")
        return result

    def append(*args, **kwargs):
        assert order == [
            "payload-io-outside-run-lock",
            "filesystem-acknowledged",
        ]
        assert kwargs["event_id"] == context.request.journal_event_id
        order.append("journal-registration")
        return original_append(*args, **kwargs)

    monkeypatch.setattr(recovery, "recover_owned_bundle", forbidden)
    monkeypatch.setattr(bundles, "publish_owned_bundle", forbidden)
    monkeypatch.setattr(receipts, "persist_owned_recovery_receipt", acknowledge)
    monkeypatch.setattr(journal, "append_event", append)

    assert registration_setup.register(context).sequence == 1
    assert order[-1] == "journal-registration"


@pytest.mark.parametrize("origin", ["request", "receipt", "os"])
def test_inspection_errors_are_sanitized(
    recovered_bundle_for_registration, monkeypatch, origin
):
    context = recovered_bundle_for_registration

    with owned_invocation(*context.parameters) as ownership:

        def unreadable(*args):
            if origin == "request":
                raise requests.RecoveryRequestInspectionError(
                    ownership.invocation.record,
                    context.request.recovery_id,
                    context.path,
                    "request_unavailable",
                    errno=errno.EIO,
                )
            if origin == "receipt":
                raise receipts.RecoveryReceiptInspectionError(
                    ownership,
                    context.request.recovery_id,
                    context.audit,
                    "receipt_unavailable",
                    errno=errno.EIO,
                )
            raise OSError(errno.EIO, request_setup.SECRET)

        target = requests if origin == "request" else receipts
        attribute = (
            "read_owned_recovery_request"
            if origin == "request"
            else "read_owned_recovery_receipt"
        )
        monkeypatch.setattr(target, attribute, unreadable)
        before = request_setup.snapshot(context.root)
        with pytest.raises(RecoveryJournalInspectionError) as caught:
            register_owned_recovery_event(
                ownership, context.request, receipt=context.receipt
            )

    report = registration_setup.assert_private(caught.value)
    assert report["errno"] == errno.EIO
    assert report["filesystem_acknowledged_in_this_call"] is False
    assert request_setup.snapshot(context.root) == before


def test_invalid_internal_payload_blocks_acknowledgment(
    recovered_bundle_for_registration, monkeypatch
):
    context = recovered_bundle_for_registration

    def invalid(*args):
        raise ValueError(request_setup.SECRET)

    monkeypatch.setattr(association, "_payload", invalid)
    before = request_setup.snapshot(context.root)
    with pytest.raises(RecoveryJournalIntegrityError) as caught:
        registration_setup.register(context)

    assert caught.value.phase == "payload-validation"
    registration_setup.assert_private(caught.value)
    assert request_setup.snapshot(context.root) == before


def test_busy_registration_propagates_without_implicit_retry(
    recovered_bundle_for_registration, monkeypatch
):
    context = recovered_bundle_for_registration
    original = journal.append_event
    calls = []

    with owned_invocation(*context.parameters) as ownership:

        def busy(*args, **kwargs):
            calls.append(kwargs["event_id"])
            with locked_run(context.root, context.request.run_id):
                return original(*args, **kwargs)

        with monkeypatch.context() as patch:
            patch.setattr(journal, "append_event", busy)
            with pytest.raises(RunBusyError):
                register_owned_recovery_event(
                    ownership, context.request, receipt=context.receipt
                )

    assert calls == [context.request.journal_event_id]
    assert not (context.run.path / "events").exists()
    assert registration_setup.register(context).sequence == 1


def test_inactive_and_inherited_ownership_are_rejected(
    recovered_bundle_for_registration,
):
    context = recovered_bundle_for_registration
    before = request_setup.snapshot(context.root)
    with owned_invocation(*context.parameters) as ownership:
        pid = os.fork()
        if pid == 0:
            try:
                register_owned_recovery_event(
                    ownership, context.request, receipt=context.receipt
                )
            except InvocationOwnershipError:
                os._exit(0)
            finally:
                os._exit(2)

    with pytest.raises(InvocationOwnershipError):
        register_owned_recovery_event(
            ownership, context.request, receipt=context.receipt
        )
    assert request_setup.snapshot(context.root) == before


def test_report_is_detached(recovered_bundle_for_registration, monkeypatch):
    context = recovered_bundle_for_registration
    registration_setup.make_pending(context, monkeypatch, unrelated=True)
    with pytest.raises(RecoveryJournalIncompleteError) as caught:
        registration_setup.register(context)
    report = caught.value.to_report()
    report["identity"]["run_id"] = request_setup.OTHER_ID
    report["issues"].append({"code": "invented"})
    fresh = caught.value.to_report()
    assert fresh["identity"]["run_id"] == context.request.run_id
    assert fresh["issues"] == []


def test_competing_process_cannot_append_during_registration(
    recovered_bundle_for_registration, monkeypatch
):
    context = recovered_bundle_for_registration
    original = journal._write_staged
    results = []
    script = """
import sys
from jobflow_gitlab_slurm.persistence.journal.storage import append_event
from jobflow_gitlab_slurm.persistence.runs.storage import RunBusyError

try:
    append_event(
        sys.argv[1], sys.argv[2],
        event_id=sys.argv[3],
        event_type="example.note",
        payload={"value": 1},
    )
except RunBusyError:
    sys.exit(3)
sys.exit(1)
"""

    def contend(path, record):
        if isinstance(record, EventRecord):
            results.append(
                subprocess.run(
                    [
                        sys.executable,
                        "-c",
                        script,
                        str(context.root),
                        context.request.run_id,
                        request_setup.OTHER_ID,
                    ],
                    check=False,
                    capture_output=True,
                    text=True,
                )
            )
        original(path, record)

    monkeypatch.setattr(journal, "_write_staged", contend)
    first = registration_setup.register(context)
    assert len(results) == 1
    assert results[0].returncode == 3, results[0].stderr
    assert journal.read_events(context.root, context.request.run_id).events == (first,)


@pytest.mark.parametrize("record", ["request", "receipt"])
def test_foreign_run_is_rejected_at_registration_boundary(
    recovered_bundle_for_registration, record
):
    context = recovered_bundle_for_registration

    # Structural validity does not establish binding to an enclosing run.
    data = registration_setup.payload_for(context).model_dump(mode="json")
    data["run_id"] = request_setup.OTHER_ID
    assert (
        RecoveryCompletedPayload.model_validate(data).run_id == request_setup.OTHER_ID
    )

    request = context.request
    receipt = context.receipt
    if record == "request":
        request = request.model_copy(update={"run_id": request_setup.OTHER_ID})
    else:
        receipt = receipt.model_copy(update={"run_id": request_setup.OTHER_ID})
    before = request_setup.snapshot(context.root)

    with pytest.raises(RecoveryJournalIntegrityError) as caught:
        registration_setup.register(context, request, receipt)

    report = registration_setup.assert_private(caught.value)
    assert report["phase"] == "evidence-validation"
    assert report["filesystem_acknowledged_in_this_call"] is False
    assert request_setup.snapshot(context.root) == before
    assert not (context.run.path / "events").exists()

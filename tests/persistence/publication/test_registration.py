"""persistence / publication / test_registration contracts."""

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
from jobflow_gitlab_slurm.persistence.bundles import inspection
from jobflow_gitlab_slurm.persistence.bundles import storage as bundles
from jobflow_gitlab_slurm.persistence.bundles.records import (
    BundleArtifactReference,
    encode_bundle_commit,
    encode_bundle_manifest,
)
from jobflow_gitlab_slurm.persistence.journal import storage as journal
from jobflow_gitlab_slurm.persistence.journal.records import (
    EventRecord,
)
from jobflow_gitlab_slurm.persistence.publication import registration as association
from jobflow_gitlab_slurm.persistence.publication import storage as intents
from jobflow_gitlab_slurm.persistence.publication.records import (
    PublicationIntentRecord,
    encode_publication_intent,
)
from jobflow_gitlab_slurm.persistence.publication.registration import (
    EVENT_TYPE,
    PublicationCompletedPayload,
    PublicationJournalConflictError,
    PublicationJournalIncompleteError,
    PublicationJournalInspectionError,
    PublicationJournalIntegrityError,
    register_owned_publication_event,
)
from jobflow_gitlab_slurm.persistence.recovery import operations as recovery
from jobflow_gitlab_slurm.persistence.recovery import receipts
from jobflow_gitlab_slurm.persistence.recovery import registration as recovery_journal
from jobflow_gitlab_slurm.persistence.runs.storage import RunBusyError, locked_run
from tests.persistence.publication import (
    _registration_support as publication_registration_setup,
)
from tests.persistence.recovery import _request_support as request_setup


def test_register_and_retry_preserve_exact_evidence(
    published_bundle_for_registration,
):
    context = published_bundle_for_registration
    before = request_setup.snapshot(context.invocation.path)
    first = publication_registration_setup.register(context)
    after_first = request_setup.snapshot(context.root)
    second = publication_registration_setup.register(context)

    assert first == second
    assert first.event_id == context.intent.intent_id
    assert first.event_type == EVENT_TYPE
    assert first.sequence == 1
    assert request_setup.snapshot(context.root) == after_first
    assert request_setup.snapshot(context.invocation.path) == before
    assert journal.read_events(context.root, context.run.manifest.run_id).events == (
        first,
    )

    payload = PublicationCompletedPayload.model_validate(first.payload())
    prefix = context.invocation.path.relative_to(context.run.path).as_posix()
    assert payload.intent.model_dump() == request_setup.reference(
        f"{prefix}/publication-intent.json",
        encode_publication_intent(context.intent),
    )
    assert payload.bundle_manifest.model_dump() == request_setup.reference(
        f"{prefix}/published/payload-manifest.json",
        encode_bundle_manifest(context.intent.expected_manifest),
    )
    assert payload.bundle_commit.model_dump() == request_setup.reference(
        f"{prefix}/published/COMMIT.json",
        encode_bundle_commit(context.intent.expected_commit),
    )
    assert payload.result == "committed_bundle_verified"
    assert "recovery_id" not in first.payload()


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
        ("intent_id", "invalid"),
        ("result", "scientific_success"),
        ("unexpected", "value"),
    ],
)
def test_payload_rejects_invalid_fields(
    published_bundle_for_registration, field, value
):
    data = publication_registration_setup.payload_for(
        published_bundle_for_registration
    ).model_dump(mode="json")
    data[field] = value
    with pytest.raises(ValidationError):
        PublicationCompletedPayload.model_validate(data)


@pytest.mark.parametrize("field", publication_registration_setup.REFERENCES)
@pytest.mark.parametrize("damage", ["path", "empty", "nested-instance"])
def test_payload_rejects_invalid_references(
    published_bundle_for_registration, field, damage
):
    data = publication_registration_setup.payload_for(
        published_bundle_for_registration
    ).model_dump(mode="json")
    if damage == "path":
        data[field]["path"] = "other/record.json"
    elif damage == "empty":
        data[field]["size_bytes"] = 0
    else:
        model = RecordArtifactReference.model_validate(data[field])
        data[field] = model.model_copy(update={"sha256": "invalid"})

    with pytest.raises(ValidationError):
        PublicationCompletedPayload.model_validate(data)


def test_payload_is_frozen_revalidated_and_detached(
    published_bundle_for_registration,
):
    payload = publication_registration_setup.payload_for(
        published_bundle_for_registration
    )
    with pytest.raises(ValidationError):
        payload.result = "different"
    with pytest.raises(ValidationError):
        PublicationCompletedPayload.model_validate(
            payload.model_copy(update={"schema_version": 2})
        )

    detached = payload.model_dump(mode="json")
    detached["intent"]["sha256"] = "0" * 64
    assert payload.intent.sha256 != detached["intent"]["sha256"]


def test_invalid_caller_record_does_not_mutate(
    published_bundle_for_registration,
):
    context = published_bundle_for_registration
    invalid = context.intent.model_copy(update={"schema_version": 2})
    before = request_setup.snapshot(context.root)
    with pytest.raises(PublicationJournalIntegrityError) as caught:
        publication_registration_setup.register(context, invalid)

    report = publication_registration_setup.assert_private(caught.value)
    assert report["phase"] == "caller-validation"
    assert report["intent_id"] is None
    assert report["event_id"] is None
    assert request_setup.snapshot(context.root) == before


def test_foreign_run_is_rejected_contextually(
    published_bundle_for_registration,
):
    context = published_bundle_for_registration
    data = publication_registration_setup.payload_for(context).model_dump(mode="json")
    data["run_id"] = request_setup.OTHER_ID
    assert (
        PublicationCompletedPayload.model_validate(data).run_id
        == request_setup.OTHER_ID
    )

    manifest = context.intent.expected_manifest.model_copy(
        update={"run_id": request_setup.OTHER_ID}
    )
    commit = context.intent.expected_commit.model_copy(
        update={
            "run_id": request_setup.OTHER_ID,
            "manifest": BundleArtifactReference.model_validate(
                request_setup.reference(
                    "payload-manifest.json", encode_bundle_manifest(manifest)
                )
            ),
        }
    )
    changed = PublicationIntentRecord.model_validate(
        context.intent.model_copy(
            update={"expected_manifest": manifest, "expected_commit": commit}
        )
    )
    before = request_setup.snapshot(context.root)

    with pytest.raises(PublicationJournalIntegrityError) as caught:
        publication_registration_setup.register(context, changed)

    assert caught.value.phase == "intent-validation"
    assert request_setup.snapshot(context.root) == before
    assert not (context.run.path / "events").exists()


@pytest.mark.parametrize("field", ["intent_id", "created_at"])
def test_different_retained_intent_conflicts(published_bundle_for_registration, field):
    context = published_bundle_for_registration
    value = (
        request_setup.OTHER_ID
        if field == "intent_id"
        else request_setup.timestamp(context.run, 8)
    )
    changed = context.intent.model_copy(update={field: value})
    context.intent_path.write_bytes(encode_publication_intent(changed))
    before = request_setup.snapshot(context.root)

    with pytest.raises(PublicationJournalConflictError) as caught:
        publication_registration_setup.register(context)

    publication_registration_setup.assert_private(caught.value)
    assert request_setup.snapshot(context.root) == before


def test_valid_different_marker_conflicts(published_bundle_for_registration):
    context = published_bundle_for_registration
    changed = context.intent.expected_commit.model_copy(
        update={"created_at": request_setup.timestamp(context.run, 8)}
    )
    (context.published / "COMMIT.json").write_bytes(encode_bundle_commit(changed))
    before = request_setup.snapshot(context.root)

    with pytest.raises(PublicationJournalConflictError):
        publication_registration_setup.register(context)

    assert request_setup.snapshot(context.root) == before


@pytest.mark.parametrize(
    "damage",
    [
        "missing-intent",
        "temporary-intent",
        "bad-intent",
        "missing-marker",
        "changed-document",
        "changed-response",
        "changed-scheduler-receipt",
        "ambiguous-bundle",
    ],
)
def test_missing_or_invalid_evidence_is_held(published_bundle_for_registration, damage):
    context = published_bundle_for_registration
    if damage == "missing-intent":
        context.intent_path.unlink()
    elif damage == "temporary-intent":
        context.intent_path.rename(
            context.invocation.path / ".publication-intent-retained"
        )
    elif damage == "bad-intent":
        context.intent_path.write_bytes(b"{")
    elif damage == "missing-marker":
        (context.published / "COMMIT.json").unlink()
    elif damage == "changed-document":
        (context.published / "job-document.json").write_bytes(b"changed")
    elif damage == "changed-response":
        (context.published / "response.json").write_bytes(b"changed")
    elif damage == "changed-scheduler-receipt":
        (context.invocation.attempt.path / "slurm-receipt.json").write_bytes(b"changed")
    else:
        context.staging.mkdir()
    before = request_setup.snapshot(context.root)

    with pytest.raises(PublicationJournalIntegrityError):
        publication_registration_setup.register(context)

    assert request_setup.snapshot(context.root) == before
    assert not (context.run.path / "events").exists()


@pytest.mark.parametrize("state", ["staging-only", "published-uncommitted"])
def test_registration_cannot_enter_publication(recovery_staging, monkeypatch, state):
    context = recovery_staging
    if state == "published-uncommitted":
        context.staging.rename(context.published)

    def forbidden(*args):
        pytest.fail("registration must not publish or acknowledge this state")

    monkeypatch.setattr(intents, "publish_owned_publication_intent", forbidden)
    monkeypatch.setattr(bundles, "publish_owned_bundle", forbidden)
    before = request_setup.snapshot(context.root)

    with pytest.raises(PublicationJournalIntegrityError):
        publication_registration_setup.register(context)

    assert request_setup.snapshot(context.root) == before


def test_missing_intent_cannot_be_created(
    published_bundle_for_registration, monkeypatch
):
    context = published_bundle_for_registration
    context.intent_path.unlink()

    def forbidden(*args):
        pytest.fail("registration must not create a missing intent")

    monkeypatch.setattr(intents, "publish_owned_publication_intent", forbidden)
    before = request_setup.snapshot(context.root)
    with pytest.raises(PublicationJournalIntegrityError):
        publication_registration_setup.register(context)
    assert request_setup.snapshot(context.root) == before


def test_old_event_does_not_replace_current_evidence(
    published_bundle_for_registration,
):
    context = published_bundle_for_registration
    first = publication_registration_setup.register(context)
    (context.published / "response.json").write_bytes(b"changed")
    before = request_setup.snapshot(context.root)

    with pytest.raises(PublicationJournalIntegrityError):
        publication_registration_setup.register(context)

    assert request_setup.snapshot(context.root) == before
    assert journal.read_events(context.root, context.run.manifest.run_id).events == (
        first,
    )


def test_older_event_retry_preserves_later_head(
    published_bundle_for_registration,
):
    context = published_bundle_for_registration
    first = publication_registration_setup.register(context)
    later = journal.append_event(
        context.root,
        context.run.manifest.run_id,
        event_id=request_setup.OTHER_ID,
        event_type="example.note",
        payload={"value": 1},
    )
    before = request_setup.snapshot(context.root)

    assert publication_registration_setup.register(context) == first
    assert request_setup.snapshot(context.root) == before
    assert journal.read_events(context.root, context.run.manifest.run_id).events == (
        first,
        later,
    )


@pytest.mark.parametrize("different", ["type", "payload"])
def test_event_identity_collision_is_held(published_bundle_for_registration, different):
    context = published_bundle_for_registration
    payload = publication_registration_setup.payload_for(context).model_dump(
        mode="json"
    )
    event_type = EVENT_TYPE
    if different == "type":
        event_type = "example.note"
    else:
        payload = {"different": True}
    journal.append_event(
        context.root,
        context.run.manifest.run_id,
        event_id=context.intent.intent_id,
        event_type=event_type,
        payload=payload,
    )
    before = request_setup.snapshot(context.root)

    with pytest.raises(PublicationJournalConflictError) as caught:
        publication_registration_setup.register(context)

    report = publication_registration_setup.assert_private(caught.value)
    assert report["code"] == "journal_event_conflict"
    assert request_setup.snapshot(context.root) == before


def test_matching_pending_tail_is_finished(
    published_bundle_for_registration, monkeypatch
):
    context = published_bundle_for_registration
    pending = publication_registration_setup.make_pending(context, monkeypatch)
    before = request_setup.snapshot(context.invocation.path)

    assert publication_registration_setup.register(context) == pending
    assert publication_registration_setup.register(context) == pending
    assert journal.read_events(context.root, context.run.manifest.run_id).events == (
        pending,
    )
    assert request_setup.snapshot(context.invocation.path) == before


def test_unrelated_pending_tail_stays_held(
    published_bundle_for_registration, monkeypatch
):
    context = published_bundle_for_registration
    pending = publication_registration_setup.make_pending(
        context, monkeypatch, unrelated=True
    )
    before = request_setup.snapshot(context.root)

    with pytest.raises(PublicationJournalIncompleteError) as caught:
        publication_registration_setup.register(context)

    report = publication_registration_setup.assert_private(caught.value)
    assert report["pending_event_id"] == pending.event_id
    assert report["pending_sequence"] == 1
    assert report["pending_path"] == str(
        publication_registration_setup.event_path(context)
    )
    assert request_setup.snapshot(context.root) == before


def test_corrupt_journal_stays_held(published_bundle_for_registration):
    context = published_bundle_for_registration
    publication_registration_setup.register(context)
    publication_registration_setup.event_path(context).write_bytes(b"{")
    before = request_setup.snapshot(context.root)

    with pytest.raises(PublicationJournalIntegrityError) as caught:
        publication_registration_setup.register(context)

    assert (
        publication_registration_setup.assert_private(caught.value)["code"]
        == "journal_integrity_invalid"
    )
    assert request_setup.snapshot(context.root) == before


def test_acknowledgment_order_and_lock_boundary(
    published_bundle_for_registration, monkeypatch
):
    context = published_bundle_for_registration
    order = []
    original_intent = intents.publish_owned_publication_intent
    original_bundle = bundles.publish_owned_bundle
    original_append = journal.append_event

    def acknowledge_intent(ownership, intent):
        with locked_run(context.root, context.run.manifest.run_id):
            order.append("intent")
        assert intents.read_owned_publication_intent(ownership).record == intent
        return original_intent(ownership, intent)

    def acknowledge_bundle(ownership, manifest, commit):
        assert order == ["intent"]
        with locked_run(context.root, context.run.manifest.run_id):
            order.append("bundle")
        assert inspection.inspect_owned_bundle(ownership).status == "committed"
        return original_bundle(ownership, manifest, commit)

    def append(*args, **kwargs):
        assert order == ["intent", "bundle"]
        assert kwargs["event_id"] == context.intent.intent_id
        order.append("journal")
        return original_append(*args, **kwargs)

    def forbidden(*args, **kwargs):
        pytest.fail("registration must not execute recovery")

    monkeypatch.setattr(intents, "publish_owned_publication_intent", acknowledge_intent)
    monkeypatch.setattr(bundles, "publish_owned_bundle", acknowledge_bundle)
    monkeypatch.setattr(journal, "append_event", append)
    monkeypatch.setattr(recovery, "recover_owned_bundle", forbidden)

    assert publication_registration_setup.register(context).sequence == 1
    assert order == ["intent", "bundle", "journal"]


def test_bundle_state_is_rechecked_before_acknowledgment(
    published_bundle_for_registration, monkeypatch
):
    context = published_bundle_for_registration
    original = intents.publish_owned_publication_intent

    def acknowledge(ownership, intent):
        result = original(ownership, intent)
        context.published.rename(context.staging)
        return result

    def forbidden(*args):
        pytest.fail("registration must not publish staging after revalidation")

    monkeypatch.setattr(intents, "publish_owned_publication_intent", acknowledge)
    monkeypatch.setattr(bundles, "publish_owned_bundle", forbidden)

    with pytest.raises(PublicationJournalIntegrityError) as caught:
        publication_registration_setup.register(context)

    assert caught.value.phase == "bundle-acknowledgment"
    assert caught.value.intent_acknowledged is True
    assert caught.value.bundle_acknowledged is False
    assert not (context.published / "COMMIT.json").exists()
    assert (context.staging / "COMMIT.json").exists()
    assert not (context.run.path / "events").exists()


@pytest.mark.parametrize("origin", ["intent", "bundle", "os"])
def test_inspection_errors_are_sanitized(
    published_bundle_for_registration, monkeypatch, origin
):
    context = published_bundle_for_registration

    with owned_invocation(*context.parameters) as ownership:

        def unreadable(*args):
            if origin == "intent":
                raise intents.IntentInspectionError(
                    ownership.invocation.record,
                    context.intent_path,
                    "intent_unavailable",
                    errno.EIO,
                )
            if origin == "bundle":
                raise inspection.BundleInspectionError(
                    ownership.invocation.record,
                    context.invocation.path,
                    errno.EIO,
                )
            raise OSError(errno.EIO, request_setup.SECRET)

        target = inspection if origin == "bundle" else intents
        attribute = (
            "inspect_owned_bundle"
            if origin == "bundle"
            else "read_owned_publication_intent"
        )
        monkeypatch.setattr(target, attribute, unreadable)
        before = request_setup.snapshot(context.root)

        with pytest.raises(PublicationJournalInspectionError) as caught:
            register_owned_publication_event(ownership, context.intent)

    report = publication_registration_setup.assert_private(caught.value)
    assert report["errno"] == errno.EIO
    assert report["intent_acknowledged_in_this_call"] is False
    assert report["bundle_acknowledged_in_this_call"] is False
    assert request_setup.snapshot(context.root) == before


def test_invalid_internal_payload_prevents_acknowledgment(
    published_bundle_for_registration, monkeypatch
):
    context = published_bundle_for_registration

    def invalid(*args):
        raise ValueError(request_setup.SECRET)

    def forbidden(*args):
        pytest.fail("invalid payload must stop before acknowledgment")

    monkeypatch.setattr(association, "_payload", invalid)
    monkeypatch.setattr(intents, "publish_owned_publication_intent", forbidden)
    before = request_setup.snapshot(context.root)

    with pytest.raises(PublicationJournalIntegrityError) as caught:
        publication_registration_setup.register(context)

    assert caught.value.phase == "payload-validation"
    publication_registration_setup.assert_private(caught.value)
    assert request_setup.snapshot(context.root) == before


def test_busy_registration_has_no_implicit_retry(
    published_bundle_for_registration, monkeypatch
):
    context = published_bundle_for_registration
    original = journal.append_event
    calls = []

    with owned_invocation(*context.parameters) as ownership:

        def busy(*args, **kwargs):
            calls.append(kwargs["event_id"])
            with locked_run(context.root, context.run.manifest.run_id):
                return original(*args, **kwargs)

        monkeypatch.setattr(journal, "append_event", busy)
        with pytest.raises(RunBusyError):
            register_owned_publication_event(ownership, context.intent)

    assert calls == [context.intent.intent_id]
    assert not (context.run.path / "events").exists()


def test_inactive_and_inherited_ownership_are_rejected(
    published_bundle_for_registration,
):
    context = published_bundle_for_registration
    before = request_setup.snapshot(context.root)

    with owned_invocation(*context.parameters) as ownership:
        pid = os.fork()
        if pid == 0:
            try:
                register_owned_publication_event(ownership, context.intent)
            except InvocationOwnershipError:
                os._exit(0)
            finally:
                os._exit(2)
        _, status = os.waitpid(pid, 0)
        assert os.waitstatus_to_exitcode(status) == 0

    with pytest.raises(InvocationOwnershipError):
        register_owned_publication_event(ownership, context.intent)

    assert request_setup.snapshot(context.root) == before


def test_reports_are_detached(published_bundle_for_registration, monkeypatch):
    context = published_bundle_for_registration
    publication_registration_setup.make_pending(context, monkeypatch, unrelated=True)
    with pytest.raises(PublicationJournalIncompleteError) as caught:
        publication_registration_setup.register(context)

    report = caught.value.to_report()
    report["identity"]["run_id"] = request_setup.OTHER_ID
    report["issues"].append({"code": "invented"})
    fresh = caught.value.to_report()
    assert fresh["identity"]["run_id"] == context.run.manifest.run_id
    assert fresh["issues"] == []


def test_competing_process_cannot_append_during_registration(
    published_bundle_for_registration, monkeypatch
):
    context = published_bundle_for_registration
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
                        context.run.manifest.run_id,
                        request_setup.OTHER_ID,
                    ],
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
            )
        original(path, record)

    monkeypatch.setattr(journal, "_write_staged", contend)
    first = publication_registration_setup.register(context)
    assert len(results) == 1
    assert results[0].returncode == 3, results[0].stderr
    assert journal.read_events(context.root, context.run.manifest.run_id).events == (
        first,
    )


def test_publication_event_preserves_separate_recovery_audit(committed_recovery_bundle):
    context = committed_recovery_bundle
    with owned_invocation(*context.parameters) as ownership:
        receipts.persist_owned_recovery_receipt(ownership, context.receipt)
        recovery_event = recovery_journal.register_owned_recovery_event(
            ownership,
            context.request,
            receipt=context.receipt,
        )
        before = request_setup.snapshot(context.invocation.path)
        publication_event = register_owned_publication_event(ownership, context.intent)

    assert publication_event.event_id == context.intent.intent_id
    assert publication_event.event_id != recovery_event.event_id
    assert publication_event.sequence == 2
    assert request_setup.snapshot(context.invocation.path) == before
    assert journal.read_events(context.root, context.run.manifest.run_id).events == (
        recovery_event,
        publication_event,
    )
    assert publication_registration_setup.register(context) == publication_event

"""persistence / journal / test_retry contracts."""

import pytest

from jobflow_gitlab_slurm.persistence.journal import storage as journal
from jobflow_gitlab_slurm.persistence.journal.storage import (
    JournalEventConflictError,
    JournalIncompleteError,
    JournalPublicationError,
    read_events,
)
from tests.persistence.journal import _append_support as append_setup


def test_payload_is_detached_and_dictionary_order_does_not_change_retry(append_run):
    payload = {"nested": {"items": [1, 2]}, "label": "é"}
    first = append_setup.append(append_run, payload=payload)
    payload["nested"]["items"].append(3)
    assert first.payload() == {"nested": {"items": [1, 2]}, "label": "é"}

    before = append_setup.snapshot(append_run.root)
    repeated = append_setup.append(
        append_run,
        payload={"label": "é", "nested": {"items": [1, 2]}},
    )
    assert repeated == first
    assert append_setup.snapshot(append_run.root) == before


@pytest.mark.parametrize("retry_id", [append_setup.EVENT_ID, append_setup.SECOND_ID])
def test_matching_retry_preserves_original_record_and_never_rewinds_head(
    append_run, monkeypatch, retry_id
):
    first = append_setup.append(append_run)
    second = append_setup.append(append_run, append_setup.SECOND_ID)
    before = append_setup.snapshot(append_run.root)

    def forbidden(*args, **kwargs):
        raise AssertionError("matching committed retry generated or rewrote metadata")

    monkeypatch.setattr(journal, "_utc_now", forbidden)
    monkeypatch.setattr(journal, "uuid4", forbidden)
    monkeypatch.setattr(journal, "_write_staged", forbidden)

    repeated = append_setup.append(append_run, retry_id)
    assert repeated == (first if retry_id == append_setup.EVENT_ID else second)
    assert read_events(append_run.root, append_setup.RUN_ID).head.last_sequence == 2
    assert append_setup.snapshot(append_run.root) == before


@pytest.mark.parametrize(
    "changes",
    [
        {"event_type": "example.changed"},
        {"payload": {"value": 2}},
        {"payload": {"value": 1.0}},
    ],
)
def test_committed_id_with_changed_input_is_a_conflict(append_run, changes):
    append_setup.append(append_run)
    before = append_setup.snapshot(append_run.root)
    with pytest.raises(JournalEventConflictError) as caught:
        append_setup.append(append_run, **changes)
    assert caught.value.run_id == append_setup.RUN_ID
    assert caught.value.event_id == append_setup.EVENT_ID
    assert caught.value.path == append_setup.event_path(append_run)
    assert append_setup.snapshot(append_run.root) == before


def test_exact_matching_tail_retry_preserves_event_bytes_and_advances_head(
    append_run, monkeypatch
):
    pending = append_setup.make_pending(append_run, monkeypatch)
    original_bytes = append_setup.event_path(append_run).read_bytes()
    recovered = append_setup.append(append_run)

    assert recovered == pending
    assert append_setup.event_path(append_run).read_bytes() == original_bytes
    replay = read_events(append_run.root, append_setup.RUN_ID)
    assert replay.events == (pending,)
    assert replay.head.last_sequence == pending.sequence
    assert replay.head.last_sha256 == pending.sha256


@pytest.mark.parametrize(
    "changes",
    [
        {"event_id": append_setup.SECOND_ID},
        {"event_type": "example.changed"},
        {"payload": {"value": 2}},
    ],
)
def test_nonmatching_pending_tail_retry_remains_held(append_run, monkeypatch, changes):
    pending = append_setup.make_pending(append_run, monkeypatch)
    before = append_setup.snapshot(append_run.root)
    with pytest.raises(JournalIncompleteError) as caught:
        append_setup.append(append_run, **changes)
    assert caught.value.pending_event == pending
    assert append_setup.snapshot(append_run.root) == before


def test_old_committed_retry_cannot_bypass_a_pending_tail(append_run, monkeypatch):
    first = append_setup.append(append_run)
    original = journal.os.replace

    def fail(source, destination):
        raise OSError("head publication held")

    with monkeypatch.context() as patch:
        patch.setattr(journal.os, "replace", fail)
        with pytest.raises(JournalPublicationError):
            append_setup.append(append_run, append_setup.SECOND_ID)
    before = append_setup.snapshot(append_run.root)
    with pytest.raises(JournalIncompleteError):
        append_setup.append(append_run, first.event_id)
    assert append_setup.snapshot(append_run.root) == before

    recovered = append_setup.append(append_run, append_setup.SECOND_ID)
    assert read_events(append_run.root, append_setup.RUN_ID).events == (
        first,
        recovered,
    )
    assert journal.os.replace is original

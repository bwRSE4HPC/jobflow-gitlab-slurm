"""persistence / attempts / test_storage contracts."""

import os
import stat
import sys
from dataclasses import FrozenInstanceError

import pytest
from pydantic import ValidationError

from jobflow_gitlab_slurm.persistence.attempts import storage as attempts
from jobflow_gitlab_slurm.persistence.attempts.definitions import (
    DefinitionIntegrityError,
    publish_job_definition,
)
from jobflow_gitlab_slurm.persistence.attempts.records import (
    job_key,
)
from jobflow_gitlab_slurm.persistence.attempts.storage import (
    AttemptConflictError,
    AttemptIntegrityError,
    AttemptPublicationError,
    read_attempt,
    read_invocation,
    register_invocation,
    reserve_attempt,
)
from jobflow_gitlab_slurm.persistence.runs.storage import locked_run
from tests.persistence.attempts import _attempt_support as attempt_setup


def test_publication_preserves_originals_and_returns_verified_chain(
    attempt_or_invocation,
):
    attempt_run = attempt_or_invocation.run
    originals = {
        path: path.read_bytes()
        for path in attempt_run.path.rglob("*")
        if path.is_file()
    }
    handle = attempt_setup.publish(attempt_or_invocation)

    assert handle.path == attempt_setup.paths(attempt_or_invocation)[1]
    assert handle.record == attempt_or_invocation.record
    assert (handle.path / attempt_or_invocation.filename).read_bytes() == (
        attempt_or_invocation.record.model_dump_json().encode("utf-8")
    )
    assert {path: path.read_bytes() for path in originals} == originals
    linked = handle if attempt_or_invocation.mode == "attempt" else handle.attempt
    assert linked.definition == attempt_run.definition
    assert attempt_setup.read(attempt_or_invocation) == handle
    assert not list(handle.path.parent.glob(".staging-*"))
    assert stat.S_IMODE(handle.path.stat().st_mode) == 0o700
    assert (
        stat.S_IMODE((handle.path / attempt_or_invocation.filename).stat().st_mode)
        == 0o600
    )
    assert not (attempt_run.path / "events").exists()
    assert not (attempt_run.path / "journal-head.json").exists()
    assert "do_not_import_attempt_flow" not in sys.modules
    assert "do_not_import_attempt_job" not in sys.modules
    with pytest.raises(FrozenInstanceError):
        handle.path = attempt_run.path


def test_second_process_reopening(attempt_or_invocation):
    handle = attempt_setup.publish(attempt_or_invocation)
    result = attempt_setup.process(
        """
import sys
from jobflow_gitlab_slurm.persistence.attempts.storage import read_attempt, read_invocation

arguments = (sys.argv[1], sys.argv[2], sys.argv[3], 1, sys.argv[4])
if sys.argv[6] == "attempt":
    handle = read_attempt(*arguments)
else:
    handle = read_invocation(*arguments, sys.argv[5])
print(handle.record.model_dump_json())
""",
        attempt_or_invocation.run.root,
        attempt_setup.RUN_ID,
        attempt_setup.JOB_UUID,
        attempt_setup.ATTEMPT_ID,
        attempt_setup.INVOCATION_ID,
        attempt_or_invocation.mode,
    )
    assert result.returncode == 0, result.stderr
    assert type(handle.record).model_validate_json(result.stdout) == handle.record


def test_reads_and_exact_ack_preserve_extensible_directory(
    attempt_or_invocation, tmp_path
):
    handle = attempt_setup.publish(attempt_or_invocation)
    output = handle.path / "staging"
    output.mkdir()
    (output / "partial-output").write_bytes(b"preserve worker evidence")
    (handle.path / "future-receipt.json").write_bytes(b"opaque future metadata")
    os.mkfifo(handle.path / "unexamined.fifo")
    outside = tmp_path / "outside-output"
    outside.write_bytes(b"do not inspect or modify")
    (handle.path / "unexamined-link").symlink_to(outside)

    before = attempt_setup.snapshot(attempt_or_invocation.run.root)
    assert attempt_setup.read(attempt_or_invocation) == handle
    assert attempt_setup.publish(attempt_or_invocation) == handle
    assert attempt_setup.snapshot(attempt_or_invocation.run.root) == before
    assert outside.read_bytes() == b"do not inspect or modify"


def test_external_artifact_availability_is_not_claimed(attempt_or_invocation):
    handle = attempt_setup.publish(attempt_or_invocation)
    for source in attempt_or_invocation.run.sources[1:]:
        source.unlink()
    attempt_or_invocation.run.payload.unlink()
    before = attempt_setup.snapshot(attempt_or_invocation.run.root)
    assert attempt_setup.read(attempt_or_invocation) == handle
    assert attempt_setup.publish(attempt_or_invocation) == handle
    assert attempt_setup.snapshot(attempt_or_invocation.run.root) == before


def test_changed_valid_timestamp_conflicts_without_mutation(attempt_or_invocation):
    attempt_setup.publish(attempt_or_invocation)
    changed = attempt_or_invocation.record.model_copy(
        update={"created_at": attempt_setup.timestamp(attempt_or_invocation.run, 3)}
    )
    before = attempt_setup.snapshot(attempt_or_invocation.run.root)
    with pytest.raises(AttemptConflictError) as caught:
        attempt_setup.publish(attempt_or_invocation, changed)
    assert caught.value.path == attempt_setup.paths(attempt_or_invocation)[1]
    assert caught.value.attempt_id == attempt_setup.ATTEMPT_ID
    assert attempt_setup.JOB_UUID not in str(caught.value)
    assert attempt_setup.snapshot(attempt_or_invocation.run.root) == before


def test_changed_valid_definition_conflicts(attempt_run):
    original = attempt_setup.attempt_record(attempt_run)
    reserve_attempt(attempt_run.root, original)
    definition = attempt_run.definition.record.model_copy(
        update={
            "definition_id": attempt_setup.SECOND_DEFINITION_ID,
            "payload": attempt_run.definition.record.payload.model_copy(
                update={
                    "path": (
                        f"jobs/{job_key(attempt_setup.JOB_UUID)}/index-1/definitions/"
                        f"{attempt_setup.SECOND_DEFINITION_ID}/job.json"
                    )
                }
            ),
        }
    )
    publish_job_definition(attempt_run.root, definition, attempt_run.payload)
    changed = original.model_copy(
        update={"definition_id": attempt_setup.SECOND_DEFINITION_ID}
    )
    before = attempt_setup.snapshot(attempt_run.root)
    with pytest.raises(AttemptConflictError):
        reserve_attempt(attempt_run.root, changed)
    assert attempt_setup.snapshot(attempt_run.root) == before


@pytest.mark.parametrize(
    "change",
    [
        {"schema_version": 2},
        {"schema_version": True},
        {"job_index": True},
        {"job_key": "0" * 64},
        {"attempt_id": "../outside"},
        {"run_id": "../outside"},
    ],
)
def test_invalid_record_copy_is_rejected_before_writes(attempt_or_invocation, change):
    invalid = attempt_or_invocation.record.model_copy(update=change)
    before = attempt_setup.snapshot(attempt_or_invocation.run.root)
    with pytest.raises(ValidationError):
        attempt_setup.publish(attempt_or_invocation, invalid)
    assert attempt_setup.snapshot(attempt_or_invocation.run.root) == before


def test_invalid_invocation_id_is_rejected_before_writes(attempt_run):
    reserve_attempt(attempt_run.root, attempt_setup.attempt_record(attempt_run))
    invalid = attempt_setup.invocation_record(attempt_run).model_copy(
        update={"invocation_id": "../outside"}
    )
    before = attempt_setup.snapshot(attempt_run.root)
    with pytest.raises(ValidationError):
        register_invocation(attempt_run.root, invalid)
    assert attempt_setup.snapshot(attempt_run.root) == before


@pytest.mark.parametrize(
    "field",
    [
        "definition_sha256",
        "consumer_code_sha256",
        "worker_runtime_sha256",
        "created_at",
    ],
)
def test_attempt_provenance_validation_precedes_writes(attempt_run, field):
    value = (
        attempt_setup.timestamp(attempt_run, -1) if field == "created_at" else "0" * 64
    )
    record = attempt_setup.attempt_record(attempt_run).model_copy(update={field: value})
    before = attempt_setup.snapshot(attempt_run.root)
    with pytest.raises(ValueError):
        reserve_attempt(attempt_run.root, record)
    assert attempt_setup.snapshot(attempt_run.root) == before
    assert not (
        attempt_run.path / "jobs" / job_key(attempt_setup.JOB_UUID) / "index-1/attempts"
    ).exists()


def test_private_pair_validator_rejects_identity_and_definition_mismatch(attempt_run):
    record = attempt_setup.attempt_record(attempt_run)
    different_identity = record.model_copy(
        update={
            "job_uuid": "other-job",
            "job_key": job_key("other-job"),
        }
    )
    with pytest.raises(ValueError, match="run/job identity"):
        attempts._validate_attempt(
            attempt_run.handle, different_identity, attempt_run.definition
        )

    different_definition = record.model_copy(
        update={"definition_id": attempt_setup.SECOND_DEFINITION_ID}
    )
    with pytest.raises(ValueError, match="definition reference"):
        attempts._validate_attempt(
            attempt_run.handle, different_definition, attempt_run.definition
        )


def test_missing_definition_does_not_create_attempt(attempt_run):
    preserved = attempt_run.definition.path.with_name("preserved-definition")
    attempt_run.definition.path.rename(preserved)
    before = attempt_setup.snapshot(attempt_run.root)
    with pytest.raises(DefinitionIntegrityError):
        reserve_attempt(attempt_run.root, attempt_setup.attempt_record(attempt_run))
    assert attempt_setup.snapshot(attempt_run.root) == before


def test_corrupt_definition_does_not_create_attempt(attempt_run):
    (attempt_run.definition.path / "job.json").write_bytes(b"corrupt")
    before = attempt_setup.snapshot(attempt_run.root)
    with pytest.raises(DefinitionIntegrityError):
        reserve_attempt(attempt_run.root, attempt_setup.attempt_record(attempt_run))
    assert attempt_setup.snapshot(attempt_run.root) == before


def test_missing_attempt_is_not_created_by_registration(attempt_run):
    before = attempt_setup.snapshot(attempt_run.root)
    with pytest.raises(AttemptIntegrityError):
        register_invocation(
            attempt_run.root, attempt_setup.invocation_record(attempt_run)
        )
    assert attempt_setup.snapshot(attempt_run.root) == before


def test_invocation_cannot_predate_attempt(attempt_run):
    reserve_attempt(attempt_run.root, attempt_setup.attempt_record(attempt_run))
    record = attempt_setup.invocation_record(attempt_run).model_copy(
        update={"created_at": attempt_setup.timestamp(attempt_run, 0)}
    )
    before = attempt_setup.snapshot(attempt_run.root)
    with pytest.raises(ValueError, match="precedes execution attempt"):
        register_invocation(attempt_run.root, record)
    assert attempt_setup.snapshot(attempt_run.root) == before


def test_distinct_attempts_and_invocations_preserve_all_evidence(attempt_run):
    first = reserve_attempt(attempt_run.root, attempt_setup.attempt_record(attempt_run))
    second = reserve_attempt(
        attempt_run.root,
        attempt_setup.attempt_record(
            attempt_run, attempt_id=attempt_setup.SECOND_ATTEMPT_ID
        ),
    )
    entry_one = register_invocation(
        attempt_run.root, attempt_setup.invocation_record(attempt_run)
    )
    entry_two = register_invocation(
        attempt_run.root,
        attempt_setup.invocation_record(
            attempt_run, invocation_id=attempt_setup.SECOND_INVOCATION_ID
        ),
    )

    assert first.path != second.path
    assert entry_one.path != entry_two.path
    assert entry_one.attempt == entry_two.attempt == first
    assert (
        read_attempt(
            attempt_run.root,
            attempt_setup.RUN_ID,
            attempt_setup.JOB_UUID,
            1,
            attempt_setup.SECOND_ATTEMPT_ID,
        )
        == second
    )
    assert (
        read_invocation(
            attempt_run.root,
            attempt_setup.RUN_ID,
            attempt_setup.JOB_UUID,
            1,
            attempt_setup.ATTEMPT_ID,
            attempt_setup.SECOND_INVOCATION_ID,
        )
        == entry_two
    )
    assert not hasattr(first, "success")
    assert not hasattr(entry_one, "active")


@pytest.mark.parametrize(
    "change",
    [
        {"job_uuid": ""},
        {"job_uuid": None},
        {"job_uuid": "\ud800"},
        {"job_index": 0},
        {"job_index": True},
        {"attempt_id": attempt_setup.ATTEMPT_ID.upper()},
        {"attempt_id": "../outside"},
        {"run_id": "../outside"},
    ],
)
def test_invalid_lookup_is_non_mutating(attempt_or_invocation, change):
    before = attempt_setup.snapshot(attempt_or_invocation.run.root)
    with pytest.raises(ValueError):
        attempt_setup.read(attempt_or_invocation, **change)
    assert attempt_setup.snapshot(attempt_or_invocation.run.root) == before


def test_invalid_invocation_lookup_is_non_mutating(attempt_run):
    before = attempt_setup.snapshot(attempt_run.root)
    with pytest.raises(ValidationError):
        read_invocation(
            attempt_run.root,
            attempt_setup.RUN_ID,
            attempt_setup.JOB_UUID,
            1,
            attempt_setup.ATTEMPT_ID,
            "../outside",
        )
    assert attempt_setup.snapshot(attempt_run.root) == before


def test_absent_record_read_creates_nothing(attempt_or_invocation):
    before = attempt_setup.snapshot(attempt_or_invocation.run.root)
    with pytest.raises(AttemptIntegrityError):
        attempt_setup.read(attempt_or_invocation)
    assert attempt_setup.snapshot(attempt_or_invocation.run.root) == before


@pytest.mark.parametrize("kind", ["missing", "directory", "symlink", "fifo"])
def test_required_metadata_file_is_checked(attempt_or_invocation, tmp_path, kind):
    handle = attempt_setup.publish(attempt_or_invocation)
    path = handle.path / attempt_or_invocation.filename
    original = path.read_bytes()
    path.unlink()
    if kind == "directory":
        path.mkdir()
    elif kind == "symlink":
        target = tmp_path / "preserved-metadata"
        target.write_bytes(original)
        path.symlink_to(target)
    elif kind == "fifo":
        os.mkfifo(path)

    before = attempt_setup.snapshot(attempt_or_invocation.run.root)
    with pytest.raises(AttemptIntegrityError):
        attempt_setup.read(attempt_or_invocation)
    with pytest.raises(AttemptIntegrityError):
        attempt_setup.publish(attempt_or_invocation)
    assert attempt_setup.snapshot(attempt_or_invocation.run.root) == before


@pytest.mark.parametrize(
    "change",
    [
        {"schema_version": 2},
        {"unknown": "rejected"},
        {"run_id": attempt_setup.OTHER_RUN_ID},
        {"created_at": "2000-01-01T00:00:00.000000Z"},
    ],
)
def test_inconsistent_stored_record_is_held(attempt_or_invocation, change):
    handle = attempt_setup.publish(attempt_or_invocation)
    attempt_setup.rewrite(
        handle.path / attempt_or_invocation.filename,
        lambda data: data.update(change),
    )
    with pytest.raises(AttemptIntegrityError):
        attempt_setup.read(attempt_or_invocation)


@pytest.mark.parametrize("field", ["job_uuid", "job_index", "attempt_id"])
def test_valid_record_for_another_location_is_rejected(attempt_or_invocation, field):
    handle = attempt_setup.publish(attempt_or_invocation)

    def mutate(data):
        replacements = {
            "job_uuid": "other-job",
            "job_index": 2,
            "attempt_id": attempt_setup.SECOND_ATTEMPT_ID,
        }
        data[field] = replacements[field]
        data["job_key"] = job_key(data["job_uuid"])

    attempt_setup.rewrite(handle.path / attempt_or_invocation.filename, mutate)
    with pytest.raises(AttemptIntegrityError):
        attempt_setup.read(attempt_or_invocation)


def test_stored_invocation_id_must_match_location(attempt_run):
    reserve_attempt(attempt_run.root, attempt_setup.attempt_record(attempt_run))
    handle = register_invocation(
        attempt_run.root, attempt_setup.invocation_record(attempt_run)
    )
    attempt_setup.rewrite(
        handle.path / "invocation.json",
        lambda data: data.update({"invocation_id": attempt_setup.SECOND_INVOCATION_ID}),
    )
    with pytest.raises(AttemptIntegrityError):
        read_invocation(
            attempt_run.root,
            attempt_setup.RUN_ID,
            attempt_setup.JOB_UUID,
            1,
            attempt_setup.ATTEMPT_ID,
            attempt_setup.INVOCATION_ID,
        )


@pytest.mark.parametrize(
    "field",
    ["definition_sha256", "consumer_code_sha256", "worker_runtime_sha256"],
)
def test_stored_attempt_provenance_is_rechecked(attempt_or_invocation, field):
    attempt_setup.publish(attempt_or_invocation)
    attempt_path = (
        attempt_or_invocation.run.path
        / "jobs"
        / job_key(attempt_setup.JOB_UUID)
        / "index-1/attempts"
        / attempt_setup.ATTEMPT_ID
        / "attempt.json"
    )
    attempt_setup.rewrite(attempt_path, lambda data: data.update({field: "0" * 64}))
    before = attempt_setup.snapshot(attempt_or_invocation.run.root)
    with pytest.raises(AttemptIntegrityError):
        attempt_setup.read(attempt_or_invocation)
    with pytest.raises(AttemptIntegrityError):
        attempt_setup.publish(attempt_or_invocation)
    assert attempt_setup.snapshot(attempt_or_invocation.run.root) == before


def test_definition_payload_corruption_is_rechecked(attempt_or_invocation):
    attempt_setup.publish(attempt_or_invocation)
    (attempt_or_invocation.run.definition.path / "job.json").write_bytes(b"corrupt")
    before = attempt_setup.snapshot(attempt_or_invocation.run.root)
    with pytest.raises(AttemptIntegrityError):
        attempt_setup.read(attempt_or_invocation)
    with pytest.raises((AttemptIntegrityError, DefinitionIntegrityError)):
        attempt_setup.publish(attempt_or_invocation)
    assert attempt_setup.snapshot(attempt_or_invocation.run.root) == before


def test_publication_flush_order(attempt_or_invocation, monkeypatch):
    trace = []
    original_write = attempts._write_record
    original_sync = attempts.definitions._sync_directory
    original_rename = attempts.os.rename

    def write(path, record):
        original_write(path, record)
        trace.append("metadata")

    def sync(path):
        original_sync(path)
        trace.append("stage" if path.name.startswith(".staging-") else path)

    def rename(source, target):
        original_rename(source, target)
        trace.append("rename")

    monkeypatch.setattr(attempts, "_write_record", write)
    monkeypatch.setattr(attempts.definitions, "_sync_directory", sync)
    monkeypatch.setattr(attempts.os, "rename", rename)

    attempt_setup.publish(attempt_or_invocation)
    parents, _ = attempt_setup.paths(attempt_or_invocation)
    assert trace == ["metadata", "stage", "rename", *reversed(parents)]


def test_acknowledgment_flushes_only_metadata_and_directories(
    attempt_or_invocation, monkeypatch
):
    handle = attempt_setup.publish(attempt_or_invocation)
    (handle.path / "opaque-output").write_bytes(b"do not flush as a result")
    before = attempt_setup.snapshot(attempt_or_invocation.run.root)
    trace = []
    original_file = attempts.definitions._sync_file
    original_directory = attempts.definitions._sync_directory

    def sync_file(path):
        original_file(path)
        trace.append(path)

    def sync_directory(path):
        original_directory(path)
        trace.append(path)

    monkeypatch.setattr(attempts.definitions, "_sync_file", sync_file)
    monkeypatch.setattr(attempts.definitions, "_sync_directory", sync_directory)

    assert attempt_setup.publish(attempt_or_invocation) == handle
    parents, _ = attempt_setup.paths(attempt_or_invocation)
    assert trace == [
        handle.path / attempt_or_invocation.filename,
        handle.path,
        *reversed(parents),
    ]
    assert attempt_setup.snapshot(attempt_or_invocation.run.root) == before


@pytest.mark.parametrize("after_rename", [False, True])
def test_uncertain_rename_requires_same_identity_retry(
    attempt_or_invocation, monkeypatch, after_rename
):
    original = attempts.os.rename

    def interrupted(source, target):
        if after_rename:
            original(source, target)
        raise OSError("rename outcome requires inspection")

    with monkeypatch.context() as patch:
        patch.setattr(attempts.os, "rename", interrupted)
        with pytest.raises(AttemptPublicationError) as caught:
            attempt_setup.publish(attempt_or_invocation)

    error = caught.value
    attempt_setup.assert_publication_error(
        error, attempt_or_invocation, phase="rename", uncertain=True
    )
    assert error.path.exists() is after_rename
    assert error.staging_path.exists() is not after_rename
    assert (
        attempt_setup.publish(attempt_or_invocation).record
        == attempt_or_invocation.record
    )
    if not after_rename:
        assert error.staging_path.is_dir()


@pytest.mark.parametrize("operation", ["publish", "read"])
def test_run_lock_excludes_second_process(attempt_or_invocation, operation):
    attempt_setup.publish(attempt_or_invocation)
    record_path = attempt_or_invocation.run.payload.with_name("retained-record.json")
    record_path.write_text(
        attempt_or_invocation.record.model_dump_json(), encoding="utf-8"
    )
    code = """
import sys
from pathlib import Path
from jobflow_gitlab_slurm.persistence.attempts.records import AttemptRecord, InvocationRecord
from jobflow_gitlab_slurm.persistence.attempts.storage import (
    read_attempt, read_invocation, register_invocation, reserve_attempt,
)
from jobflow_gitlab_slurm.persistence.runs.storage import RunBusyError

mode, operation = sys.argv[3], sys.argv[4]
model = AttemptRecord if mode == "attempt" else InvocationRecord
record = model.model_validate_json(Path(sys.argv[2]).read_bytes())
try:
    if operation == "publish":
        function = reserve_attempt if mode == "attempt" else register_invocation
        function(sys.argv[1], record)
    else:
        arguments = (
            sys.argv[1], record.run_id, record.job_uuid,
            record.job_index, record.attempt_id,
        )
        if mode == "attempt":
            read_attempt(*arguments)
        else:
            read_invocation(*arguments, record.invocation_id)
except RunBusyError:
    print("busy")
    sys.exit(17)
print("verified")
"""
    with locked_run(attempt_or_invocation.run.root, attempt_setup.RUN_ID):
        before = attempt_setup.snapshot(attempt_or_invocation.run.root)
        result = attempt_setup.process(
            code,
            attempt_or_invocation.run.root,
            record_path,
            attempt_or_invocation.mode,
            operation,
        )
        assert result.returncode == 17, result.stderr
        assert result.stdout.strip() == "busy"
        assert attempt_setup.snapshot(attempt_or_invocation.run.root) == before

    result = attempt_setup.process(
        code,
        attempt_or_invocation.run.root,
        record_path,
        attempt_or_invocation.mode,
        operation,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "verified"


@pytest.mark.parametrize("after_rename", [False, True])
def test_abrupt_exit_retains_recoverable_metadata_identity(
    attempt_or_invocation, after_rename
):
    record_path = attempt_or_invocation.run.payload.with_name("retained-record.json")
    record_path.write_text(
        attempt_or_invocation.record.model_dump_json(), encoding="utf-8"
    )
    result = attempt_setup.process(
        """
import os
import sys
from pathlib import Path
from jobflow_gitlab_slurm.persistence.attempts import storage as attempt_storage
from jobflow_gitlab_slurm.persistence.attempts.records import AttemptRecord, InvocationRecord

mode = sys.argv[3]
model = AttemptRecord if mode == "attempt" else InvocationRecord
record = model.model_validate_json(Path(sys.argv[2]).read_bytes())
original = attempt_storage.os.rename

def interrupted(source, target):
    if sys.argv[4] == "after":
        original(source, target)
    os._exit(73)

attempt_storage.os.rename = interrupted
function = (
    attempt_storage.reserve_attempt
    if mode == "attempt"
    else attempt_storage.register_invocation
)
function(sys.argv[1], record)
""",
        attempt_or_invocation.run.root,
        record_path,
        attempt_or_invocation.mode,
        "after" if after_rename else "before",
    )
    assert result.returncode == 73, result.stderr
    target = attempt_setup.paths(attempt_or_invocation)[1]
    stages = list(target.parent.glob(".staging-*"))
    assert target.exists() is after_rename
    assert len(stages) == (0 if after_rename else 1)
    if not after_rename:
        assert (stages[0] / attempt_or_invocation.filename).read_bytes() == (
            attempt_or_invocation.record.model_dump_json().encode("utf-8")
        )

    recovered = attempt_setup.publish(attempt_or_invocation)
    assert recovered.record == attempt_or_invocation.record
    assert attempt_setup.read(attempt_or_invocation) == recovered
    assert len(list(target.parent.glob(".staging-*"))) == len(stages)

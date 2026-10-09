"""persistence / attempts / test_definitions contracts."""

import stat
import sys
from dataclasses import FrozenInstanceError, replace

import pytest
from pydantic import ValidationError

from jobflow_gitlab_slurm.persistence.attempts import definitions
from jobflow_gitlab_slurm.persistence.attempts.definitions import (
    DefinitionConflictError,
    DefinitionIntegrityError,
    DefinitionPublicationError,
    read_job_definition,
)
from jobflow_gitlab_slurm.persistence.attempts.records import (
    JobDefinitionRecord,
    job_key,
)
from jobflow_gitlab_slurm.persistence.runs.storage import locked_run
from tests.persistence.attempts import _definition_support as definition_setup


def test_publication_preserves_payload_and_original_run(definition_run):
    original_paths = [
        definition_run.path / "run.json",
        definition_run.path / "flow/original.json",
        definition_run.path / "flow/payload.json",
        definition_run.path / "artifacts/references.json",
    ]
    originals = {path: path.read_bytes() for path in original_paths}
    record = definition_setup.record_for(definition_run)
    handle = definition_setup.publish(definition_run, record)

    assert handle.path == definition_setup.destination(definition_run)
    assert handle.record == record
    assert (handle.path / "job.json").read_bytes() == definition_setup.JOB_BYTES
    assert (handle.path / "definition.json").read_bytes() == (
        record.model_dump_json().encode("utf-8")
    )
    assert {path: path.read_bytes() for path in originals} == originals
    assert {path.name for path in handle.path.iterdir()} == {
        "definition.json",
        "job.json",
    }
    assert not list(handle.path.parent.glob(".staging-*"))
    assert not (definition_run.path / "events").exists()
    assert not (definition_run.path / "journal-head.json").exists()
    assert "do_not_import_definition_flow" not in sys.modules
    assert "do_not_import_definition_job" not in sys.modules

    for path in (definition_run.path / "jobs").rglob("*"):
        expected = 0o700 if path.is_dir() else 0o600
        assert stat.S_IMODE(path.stat().st_mode) == expected

    with pytest.raises(FrozenInstanceError):
        handle.path = definition_run.path
    assert definition_setup.read(definition_run) == handle


def test_reopen_in_second_process(definition_run):
    handle = definition_setup.publish(definition_run)
    result = definition_setup.process(
        """
import sys
from jobflow_gitlab_slurm.persistence.attempts.definitions import read_job_definition

handle = read_job_definition(
    sys.argv[1], sys.argv[2], sys.argv[3], 1, sys.argv[4]
)
print(handle.record.model_dump_json())
""",
        definition_run.root,
        definition_setup.RUN_ID,
        definition_setup.JOB_UUID,
        definition_setup.DEFINITION_ID,
    )
    assert result.returncode == 0, result.stderr
    assert JobDefinitionRecord.model_validate_json(result.stdout) == handle.record


def test_read_is_non_mutating_and_exact_retry_needs_no_source(definition_run):
    handle = definition_setup.publish(definition_run)
    definition_run.payload.unlink()
    before = definition_setup.snapshot(definition_run.root)

    assert definition_setup.read(definition_run) == handle
    assert definition_setup.publish(definition_run) == handle
    assert definition_setup.snapshot(definition_run.root) == before


@pytest.mark.parametrize("identity", ["plain", "é", "e\u0301", "/", "\n", "\0"])
def test_exact_job_identity_uses_hash_locator(definition_run, identity):
    record = definition_setup.record_for(definition_run, job_uuid=identity)
    handle = definition_setup.publish(definition_run, record)
    assert handle.record.job_uuid == identity
    assert handle.path == definition_setup.destination(definition_run, record)
    assert handle.path.parent.parent.parent.name == job_key(identity)
    assert definition_setup.read(definition_run, record) == handle


def test_separate_definitions_share_containers_without_overwrite(definition_run):
    first = definition_setup.publish(definition_run)
    second_record = definition_setup.record_for(
        definition_run, definition_id=definition_setup.SECOND_ID
    )
    second = definition_setup.publish(definition_run, second_record)
    assert first.path != second.path
    assert definition_setup.read(definition_run) == first
    assert definition_setup.read(definition_run, second_record) == second


@pytest.mark.parametrize("field", ["created_at", "digest", "size"])
def test_same_id_changed_record_conflicts_without_mutation(definition_run, field):
    record = definition_setup.record_for(definition_run)
    definition_setup.publish(definition_run, record)
    if field == "created_at":
        changed = record.model_copy(
            update={"created_at": "9999-01-01T00:00:00.000000Z"}
        )
    else:
        reference = record.payload.model_copy(
            update={
                "sha256" if field == "digest" else "size_bytes": (
                    "0" * 64
                    if field == "digest"
                    else len(definition_setup.JOB_BYTES) + 1
                )
            }
        )
        changed = record.model_copy(update={"payload": reference})

    before = definition_setup.snapshot(definition_run.root)
    with pytest.raises(DefinitionConflictError) as caught:
        definition_setup.publish(definition_run, changed)
    assert caught.value.definition_id == definition_setup.DEFINITION_ID
    assert caught.value.path == definition_setup.destination(definition_run)
    assert definition_setup.JOB_UUID not in str(caught.value)
    assert definition_setup.snapshot(definition_run.root) == before


@pytest.mark.parametrize(
    "change",
    [
        {"definition_id": "../outside"},
        {"job_index": True},
        {"schema_version": True},
        {"jobflow_version": "0.4.0"},
        {"job_key": "0" * 64},
    ],
)
def test_invalid_model_copy_fails_before_changes(definition_run, change):
    invalid = definition_setup.record_for(definition_run).model_copy(update=change)
    before = definition_setup.snapshot(definition_run.root)
    with pytest.raises(ValidationError):
        definition_setup.publish(definition_run, invalid)
    assert definition_setup.snapshot(definition_run.root) == before


@pytest.mark.parametrize("origin", ["dynamic_response", "amendment"])
def test_unsupported_storage_origins_are_explicit(definition_run, origin):
    record = definition_setup.record_for(definition_run).model_copy(
        update={"origin": origin}
    )
    before = definition_setup.snapshot(definition_run.root)
    with pytest.raises(ValueError, match="original_flow only"):
        definition_setup.publish(definition_run, record)
    assert definition_setup.snapshot(definition_run.root) == before


@pytest.mark.parametrize("field", ["sha256", "size_bytes"])
def test_wrong_flow_source_fails_before_changes(definition_run, field):
    record = definition_setup.record_for(definition_run)
    value = "0" * 64 if field == "sha256" else len(definition_setup.FLOW_BYTES) + 1
    source = record.source.model_copy(update={field: value})
    record = record.model_copy(update={"source": source})
    before = definition_setup.snapshot(definition_run.root)
    with pytest.raises(ValueError, match="original Flow envelope"):
        definition_setup.publish(definition_run, record)
    assert definition_setup.snapshot(definition_run.root) == before


def test_definition_cannot_predate_run(definition_run):
    record = definition_setup.record_for(definition_run).model_copy(
        update={"created_at": "2000-01-01T00:00:00.000000Z"}
    )
    before = definition_setup.snapshot(definition_run.root)
    with pytest.raises(ValueError, match="precedes run"):
        definition_setup.publish(definition_run, record)
    assert definition_setup.snapshot(definition_run.root) == before


@pytest.mark.parametrize("field", ["sha256", "size_bytes"])
def test_wrong_payload_claim_fails_before_container_creation(definition_run, field):
    record = definition_setup.record_for(definition_run)
    value = "0" * 64 if field == "sha256" else len(definition_setup.JOB_BYTES) + 1
    record = record.model_copy(
        update={"payload": record.payload.model_copy(update={field: value})}
    )
    before = definition_setup.snapshot(definition_run.root)
    with pytest.raises(ValueError):
        definition_setup.publish(definition_run, record)
    assert definition_setup.snapshot(definition_run.root) == before


@pytest.mark.parametrize(
    "change",
    [
        {"job_uuid": ""},
        {"job_uuid": None},
        {"job_uuid": "\ud800"},
        {"job_index": 0},
        {"job_index": True},
        {"definition_id": definition_setup.DEFINITION_ID.upper()},
        {"definition_id": "../outside"},
    ],
)
def test_invalid_lookup_is_non_mutating(definition_run, change):
    values = {
        "job_uuid": definition_setup.JOB_UUID,
        "job_index": 1,
        "definition_id": definition_setup.DEFINITION_ID,
    }
    values.update(change)
    before = definition_setup.snapshot(definition_run.root)
    with pytest.raises(ValueError):
        read_job_definition(definition_run.root, definition_setup.RUN_ID, **values)
    assert definition_setup.snapshot(definition_run.root) == before


def test_missing_definition_read_does_not_create_containers(definition_run):
    before = definition_setup.snapshot(definition_run.root)
    with pytest.raises(DefinitionIntegrityError) as caught:
        definition_setup.read(definition_run)
    assert caught.value.path == definition_setup.destination(definition_run)
    assert definition_setup.snapshot(definition_run.root) == before


def test_missing_run_is_not_created(definition_run):
    before = definition_setup.snapshot(definition_run.root)
    with pytest.raises(FileNotFoundError):
        read_job_definition(
            definition_run.root,
            definition_setup.OTHER_RUN_ID,
            definition_setup.JOB_UUID,
            1,
            definition_setup.DEFINITION_ID,
        )
    assert definition_setup.snapshot(definition_run.root) == before


def test_unexpected_published_entry_is_held(definition_run):
    handle = definition_setup.publish(definition_run)
    (handle.path / "extra").write_bytes(b"preserve")
    before = definition_setup.snapshot(definition_run.root)
    with pytest.raises(DefinitionIntegrityError):
        definition_setup.publish(definition_run)
    assert definition_setup.snapshot(definition_run.root) == before


@pytest.mark.parametrize(
    "data",
    [
        b"{",
        b"\xff",
        b"[]",
        b'{"schema_version":1,"schema_version":1}',
        b'{"schema_version":NaN}',
        b'{"schema_version":Infinity}',
        b'{"schema_version":-Infinity}',
    ],
)
def test_invalid_metadata_json_is_held(definition_run, data):
    handle = definition_setup.publish(definition_run)
    (handle.path / "definition.json").write_bytes(data)
    before = definition_setup.snapshot(definition_run.root)
    with pytest.raises(DefinitionIntegrityError):
        definition_setup.read(definition_run)
    assert definition_setup.snapshot(definition_run.root) == before


@pytest.mark.parametrize(
    "change",
    [
        {"schema_version": 2},
        {"unknown": "rejected"},
        {"run_id": definition_setup.OTHER_RUN_ID},
        {"origin": "dynamic_response"},
        {"created_at": "2000-01-01T00:00:00.000000Z"},
    ],
)
def test_invalid_stored_metadata_is_held(definition_run, change):
    handle = definition_setup.publish(definition_run)
    definition_setup.rewrite_metadata(
        handle.path / "definition.json",
        lambda data: data.update(change),
    )
    with pytest.raises(DefinitionIntegrityError):
        definition_setup.read(definition_run)


def test_stored_flow_binding_is_checked(definition_run):
    handle = definition_setup.publish(definition_run)

    def mutate(data):
        data["source"]["sha256"] = "0" * 64

    definition_setup.rewrite_metadata(handle.path / "definition.json", mutate)
    with pytest.raises(DefinitionIntegrityError):
        definition_setup.read(definition_run)


@pytest.mark.parametrize("field", ["job_uuid", "job_index", "definition_id"])
def test_valid_metadata_for_another_location_is_rejected(definition_run, field):
    handle = definition_setup.publish(definition_run)

    def mutate(data):
        replacements = {
            "job_uuid": "different-job",
            "job_index": 2,
            "definition_id": definition_setup.SECOND_ID,
        }
        data[field] = replacements[field]
        data["job_key"] = job_key(data["job_uuid"])
        data["payload"]["path"] = (
            f"jobs/{data['job_key']}/index-{data['job_index']}/definitions/"
            f"{data['definition_id']}/job.json"
        )

    definition_setup.rewrite_metadata(handle.path / "definition.json", mutate)
    with pytest.raises(DefinitionIntegrityError):
        definition_setup.read(definition_run)


def test_payload_corruption_is_held(definition_run):
    handle = definition_setup.publish(definition_run)
    (handle.path / "job.json").write_bytes(b"corrupted")
    before = definition_setup.snapshot(definition_run.root)
    with pytest.raises(DefinitionIntegrityError):
        definition_setup.read(definition_run)
    with pytest.raises(DefinitionIntegrityError):
        definition_setup.publish(definition_run)
    assert definition_setup.snapshot(definition_run.root) == before


def test_stored_payload_size_is_verified(definition_run):
    handle = definition_setup.publish(definition_run)

    def mutate(data):
        data["payload"]["size_bytes"] += 1

    definition_setup.rewrite_metadata(handle.path / "definition.json", mutate)
    with pytest.raises(DefinitionIntegrityError):
        definition_setup.read(definition_run)


def test_publication_flush_order(definition_run, monkeypatch):
    trace = []
    original_write = definitions._write_metadata
    original_stage = definitions.stage_verified_artifact
    original_sync = definitions._sync_directory
    original_rename = definitions.os.rename

    def write(path, record):
        original_write(path, record)
        trace.append("metadata")

    def stage(*arguments):
        result = original_stage(*arguments)
        trace.append("payload")
        return result

    def sync(path):
        original_sync(path)
        trace.append("stage" if path.name.startswith(".staging-") else path)

    def rename(source, target):
        original_rename(source, target)
        trace.append("rename")

    monkeypatch.setattr(definitions, "_write_metadata", write)
    monkeypatch.setattr(definitions, "stage_verified_artifact", stage)
    monkeypatch.setattr(definitions, "_sync_directory", sync)
    monkeypatch.setattr(definitions.os, "rename", rename)

    definition_setup.publish(definition_run)
    parents, _ = definitions._paths(
        definition_run.handle, definition_setup.record_for(definition_run)
    )
    assert trace == [
        "metadata",
        "payload",
        "stage",
        "rename",
        *reversed(parents),
    ]


def test_acknowledgment_reflushes_without_rewriting(definition_run, monkeypatch):
    handle = definition_setup.publish(definition_run)
    before = definition_setup.snapshot(definition_run.root)
    trace = []
    original_file = definitions._sync_file
    original_directory = definitions._sync_directory

    def sync_file(path):
        original_file(path)
        trace.append(path)

    def sync_directory(path):
        original_directory(path)
        trace.append(path)

    monkeypatch.setattr(definitions, "_sync_file", sync_file)
    monkeypatch.setattr(definitions, "_sync_directory", sync_directory)
    assert definition_setup.publish(definition_run) == handle
    parents, _ = definitions._paths(
        definition_run.handle, definition_setup.record_for(definition_run)
    )
    assert trace == [
        handle.path / "definition.json",
        handle.path / "job.json",
        handle.path,
        *reversed(parents),
    ]
    assert definition_setup.snapshot(definition_run.root) == before


@pytest.mark.parametrize("after_rename", [False, True])
def test_uncertain_rename_retry_uses_same_identity(
    definition_run, monkeypatch, after_rename
):
    original = definitions.os.rename

    def interrupted(source, target):
        if after_rename:
            original(source, target)
        raise OSError("rename outcome requires inspection")

    with monkeypatch.context() as patch:
        patch.setattr(definitions.os, "rename", interrupted)
        with pytest.raises(DefinitionPublicationError) as caught:
            definition_setup.publish(definition_run)

    error = caught.value
    definition_setup.assert_publication_error(
        error, definition_run, phase="rename", uncertain=True
    )
    assert error.path.exists() is after_rename
    assert error.staging_path.exists() is not after_rename

    assert definition_setup.publish(
        definition_run
    ).record == definition_setup.record_for(definition_run)
    if not after_rename:
        assert error.staging_path.is_dir()


def test_source_change_between_verification_and_staging_is_rejected(
    definition_run, monkeypatch
):
    original = definitions.stage_verified_artifact

    def changed(source, target, digest):
        definition_run.payload.write_bytes(b"changed after preflight")
        return original(source, target, digest)

    monkeypatch.setattr(definitions, "stage_verified_artifact", changed)
    with pytest.raises(DefinitionPublicationError) as caught:
        definition_setup.publish(definition_run)
    definition_setup.assert_publication_error(
        caught.value, definition_run, phase="payload"
    )
    assert isinstance(caught.value.__cause__, ValueError)
    assert caught.value.staging_path.is_dir()
    assert (caught.value.staging_path / "definition.json").is_file()
    assert not (caught.value.staging_path / "job.json").exists()


def test_staged_payload_size_is_checked_independently(definition_run, monkeypatch):
    original = definitions.stage_verified_artifact

    def wrong_size(*arguments):
        result = original(*arguments)
        return replace(result, size_bytes=result.size_bytes + 1)

    monkeypatch.setattr(definitions, "stage_verified_artifact", wrong_size)
    with pytest.raises(DefinitionPublicationError) as caught:
        definition_setup.publish(definition_run)
    definition_setup.assert_publication_error(
        caught.value, definition_run, phase="payload"
    )
    assert isinstance(caught.value.__cause__, ValueError)
    assert (
        caught.value.staging_path / "job.json"
    ).read_bytes() == definition_setup.JOB_BYTES


def test_existing_non_directory_cannot_be_ensured(tmp_path):
    path = tmp_path / "container"
    path.write_bytes(b"preserve")
    with pytest.raises(ValueError, match="real directory"):
        definitions._ensure_directory(path)
    assert path.read_bytes() == b"preserve"


@pytest.mark.parametrize("operation", ["publish", "read"])
def test_run_lock_excludes_second_process(definition_run, operation):
    record = definition_setup.record_for(definition_run)
    definition_setup.publish(definition_run, record)
    record_path = definition_run.payload.with_name("retained-record.json")
    record_path.write_text(record.model_dump_json(), encoding="utf-8")
    code = """
import sys
from pathlib import Path
from jobflow_gitlab_slurm.persistence.attempts.records import JobDefinitionRecord
from jobflow_gitlab_slurm.persistence.attempts.definitions import (
    publish_job_definition, read_job_definition,
)
from jobflow_gitlab_slurm.persistence.runs.storage import RunBusyError

record = JobDefinitionRecord.model_validate_json(Path(sys.argv[2]).read_bytes())
try:
    if sys.argv[4] == "publish":
        publish_job_definition(sys.argv[1], record, sys.argv[3])
    else:
        read_job_definition(
            sys.argv[1], record.run_id, record.job_uuid,
            record.job_index, record.definition_id,
        )
except RunBusyError:
    print("busy")
    sys.exit(17)
print("verified")
"""
    with locked_run(definition_run.root, definition_setup.RUN_ID):
        before = definition_setup.snapshot(definition_run.root)
        result = definition_setup.process(
            code, definition_run.root, record_path, definition_run.payload, operation
        )
        assert result.returncode == 17, result.stderr
        assert result.stdout.strip() == "busy"
        assert definition_setup.snapshot(definition_run.root) == before

    result = definition_setup.process(
        code, definition_run.root, record_path, definition_run.payload, operation
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "verified"
    assert not list(
        definition_setup.destination(definition_run).parent.glob(".staging-*")
    )


@pytest.mark.parametrize("after_rename", [False, True])
def test_abrupt_process_exit_preserves_recoverable_identity(
    definition_run, after_rename
):
    record = definition_setup.record_for(definition_run)
    record_path = definition_run.payload.with_name("retained-record.json")
    record_path.write_text(record.model_dump_json(), encoding="utf-8")
    result = definition_setup.process(
        """
import os
import sys
from pathlib import Path
from jobflow_gitlab_slurm.persistence.attempts import definitions as definition_storage
from jobflow_gitlab_slurm.persistence.attempts.records import JobDefinitionRecord

record = JobDefinitionRecord.model_validate_json(Path(sys.argv[2]).read_bytes())
original = definition_storage.os.rename

def interrupted(source, target):
    if sys.argv[4] == "after":
        original(source, target)
    os._exit(73)

definition_storage.os.rename = interrupted
definition_storage.publish_job_definition(sys.argv[1], record, sys.argv[3])
""",
        definition_run.root,
        record_path,
        definition_run.payload,
        "after" if after_rename else "before",
    )
    assert result.returncode == 73, result.stderr
    target = definition_setup.destination(definition_run)
    stages = list(target.parent.glob(".staging-*"))
    assert target.exists() is after_rename
    assert len(stages) == (0 if after_rename else 1)
    if not after_rename:
        assert (stages[0] / "job.json").read_bytes() == definition_setup.JOB_BYTES

    recovered = definition_setup.publish(definition_run, record)
    assert recovered.record == record
    assert definition_setup.read(definition_run) == recovered
    assert len(list(target.parent.glob(".staging-*"))) == len(stages)

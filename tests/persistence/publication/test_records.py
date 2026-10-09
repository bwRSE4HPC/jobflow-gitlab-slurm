"""Offline publication expectations, canonical codecs, and parent binding."""

import builtins
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

from jobflow_gitlab_slurm.persistence.attempts.records import (
    AttemptRecord,
    InvocationRecord,
    JobDefinitionRecord,
    job_key,
)
from jobflow_gitlab_slurm.persistence.bundles.records import (
    BundleCommit,
    BundleManifest,
    encode_bundle_commit,
    encode_bundle_manifest,
)
from jobflow_gitlab_slurm.persistence.publication.records import (
    PublicationIntentRecord,
    decode_publication_intent,
    encode_publication_intent,
    validate_publication_intent,
)

RUN_ID = "00000000-0000-4000-8000-000000000001"

DEFINITION_ID = "aaaaaaaa-0000-4000-8000-000000000002"

ATTEMPT_ID = "00000000-0000-4000-8000-000000000003"

INVOCATION_ID = "00000000-0000-4000-8000-000000000004"

INTENT_ID = "aaaaaaaa-0000-4000-8000-000000000005"

OTHER_ID = "00000000-0000-4000-8000-000000000099"

JOB_UUID = "../../opaque-job-é"

DEFINITION_TIME = "2026-10-08T12:00:00.000000Z"

ATTEMPT_TIME = "2026-10-08T12:00:01.000000Z"

INVOCATION_TIME = "2026-10-08T12:00:02.000000Z"

MANIFEST_TIME = "2026-10-08T12:00:03.000000Z"

COMMIT_TIME = "2026-10-08T12:00:04.000000Z"

INTENT_TIME = "2026-10-08T12:00:05.000000Z"


def canonical(document):
    return json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def reference(path, digest="a" * 64, size=12):
    return {"path": path, "sha256": digest, "size_bytes": size}


@pytest.fixture
def records():
    key = job_key(JOB_UUID)
    common = {
        "schema_version": 1,
        "run_id": RUN_ID,
        "job_uuid": JOB_UUID,
        "job_index": 1,
        "job_key": key,
    }
    definition = JobDefinitionRecord.model_validate(
        {
            **common,
            "kind": "job-definition",
            "created_at": DEFINITION_TIME,
            "definition_id": DEFINITION_ID,
            "jobflow_version": "0.3.1",
            "payload": reference(
                f"jobs/{key}/index-1/definitions/{DEFINITION_ID}/job.json",
                digest="b" * 64,
            ),
            "origin": "original_flow",
            "source": reference("flow/payload.json"),
        }
    )
    attempt = AttemptRecord.model_validate(
        {
            **common,
            "kind": "execution-attempt",
            "created_at": ATTEMPT_TIME,
            "attempt_id": ATTEMPT_ID,
            "definition_id": DEFINITION_ID,
            "definition_sha256": "b" * 64,
            "consumer_code_sha256": "c" * 64,
            "worker_runtime_sha256": "d" * 64,
        }
    )
    invocation = InvocationRecord.model_validate(
        {
            **common,
            "kind": "worker-invocation",
            "created_at": INVOCATION_TIME,
            "attempt_id": ATTEMPT_ID,
            "invocation_id": INVOCATION_ID,
        }
    )
    bundle_identity = {
        **common,
        "attempt_id": ATTEMPT_ID,
        "invocation_id": INVOCATION_ID,
    }
    manifest = BundleManifest.model_validate(
        {
            **bundle_identity,
            "kind": "bundle-manifest",
            "created_at": MANIFEST_TIME,
            "definition_id": DEFINITION_ID,
            "definition_sha256": "b" * 64,
            "consumer_code_sha256": "c" * 64,
            "worker_runtime_sha256": "d" * 64,
            "jobflow_version": "0.3.1",
            "scheduler_receipt": reference(
                f"jobs/{key}/index-1/attempts/{ATTEMPT_ID}/slurm-receipt.json",
                digest="e" * 64,
            ),
            "document": reference("job-document.json"),
            "response": reference("response.json"),
            "files": (reference("files/output.txt"),),
            "additional_data": (
                {
                    "store_name": "results",
                    "blob_uuid": "opaque-blob",
                    "artifact": reference("data/blob.json"),
                },
            ),
        }
    )
    manifest_bytes = canonical(manifest.model_dump(mode="json"))
    commit = BundleCommit.model_validate(
        {
            **bundle_identity,
            "kind": "bundle-commit",
            "created_at": COMMIT_TIME,
            "manifest": reference(
                "payload-manifest.json",
                digest=hashlib.sha256(manifest_bytes).hexdigest(),
                size=len(manifest_bytes),
            ),
        }
    )
    intent = PublicationIntentRecord(
        schema_version=1,
        kind="publication-intent",
        intent_id=INTENT_ID,
        created_at=INTENT_TIME,
        expected_manifest=manifest,
        expected_commit=commit,
    )
    return definition, attempt, invocation, intent


@pytest.fixture
def intent(records):
    return records[3]


def test_canonical_round_trip_and_parent_binding(records, intent):
    encoded = encode_publication_intent(intent)
    assert encoded == canonical(intent.model_dump(mode="json"))
    assert not encoded.endswith(b"\n")
    assert "é".encode() in encoded
    assert b"\\u00e9" not in encoded
    assert decode_publication_intent(encoded) == intent
    assert validate_publication_intent(*records) is None

    loaded = decode_publication_intent(encoded)
    assert loaded.intent_id == INTENT_ID
    assert loaded.created_at == INTENT_TIME
    assert loaded.expected_commit.created_at == COMMIT_TIME
    assert encode_bundle_manifest(loaded.expected_manifest) == (
        encode_bundle_manifest(intent.expected_manifest)
    )
    assert encode_bundle_commit(loaded.expected_commit) == (
        encode_bundle_commit(intent.expected_commit)
    )


def test_no_redundant_identity_or_outcome_fields(intent):
    assert set(intent.model_dump()) == {
        "schema_version",
        "kind",
        "intent_id",
        "created_at",
        "expected_manifest",
        "expected_commit",
    }
    assert intent.expected_manifest.job_uuid == JOB_UUID


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema_version", 0),
        ("schema_version", 2),
        ("schema_version", True),
        ("schema_version", "1"),
        ("schema_version", 1.0),
        ("kind", "other"),
        ("intent_id", "invalid"),
        ("intent_id", INTENT_ID.upper()),
        ("intent_id", "00000000-0000-1000-8000-000000000001"),
        ("intent_id", 1),
        ("created_at", "invalid"),
        ("created_at", "2026-10-08T12:00:05Z"),
        ("created_at", "2026-10-08T12:00:05.000000+00:00"),
        ("created_at", "2026-10-08T14:00:05.000000+02:00"),
        ("created_at", None),
        ("unexpected", True),
        ("run_id", RUN_ID),
        ("success", True),
        ("expected_manifest", None),
        ("expected_commit", None),
    ],
)
def test_invalid_fields(intent, field, value):
    data = intent.model_dump()
    data[field] = value
    with pytest.raises(ValidationError):
        PublicationIntentRecord.model_validate(data)


@pytest.mark.parametrize(
    "field",
    [
        "schema_version",
        "kind",
        "intent_id",
        "created_at",
        "expected_manifest",
        "expected_commit",
    ],
)
def test_every_field_is_required(intent, field):
    data = intent.model_dump()
    del data[field]
    with pytest.raises(ValidationError):
        PublicationIntentRecord.model_validate(data)


def test_nested_records_are_detached_and_frozen(intent):
    data = intent.model_dump()
    rebuilt = PublicationIntentRecord.model_validate(data)
    data["expected_manifest"]["document"]["sha256"] = "f" * 64
    data["expected_commit"]["manifest"]["size_bytes"] += 1
    assert rebuilt.expected_manifest.document.sha256 == "a" * 64
    assert rebuilt.expected_commit.manifest == intent.expected_commit.manifest

    dumped = rebuilt.model_dump()
    dumped["expected_manifest"]["files"][0]["size_bytes"] = 999
    assert rebuilt.expected_manifest.files[0].size_bytes == 12

    with pytest.raises(ValidationError, match="frozen"):
        rebuilt.created_at = COMMIT_TIME
    with pytest.raises(ValidationError, match="frozen"):
        rebuilt.expected_manifest.created_at = COMMIT_TIME
    with pytest.raises(ValidationError, match="frozen"):
        rebuilt.expected_commit.created_at = INTENT_TIME
    with pytest.raises(ValidationError, match="frozen"):
        rebuilt.expected_manifest.document.size_bytes = 999
    with pytest.raises(ValidationError, match="frozen"):
        rebuilt.expected_manifest.additional_data[0].store_name = "changed"


@pytest.mark.parametrize(
    "field,value",
    [
        ("run_id", OTHER_ID),
        ("job_uuid", "different-job"),
        ("job_index", 2),
        ("attempt_id", OTHER_ID),
        ("invocation_id", OTHER_ID),
    ],
)
def test_manifest_commit_identity_mismatch(intent, field, value):
    data = intent.model_dump()
    data["expected_commit"][field] = value
    if field == "job_uuid":
        data["expected_commit"]["job_key"] = job_key(value)
    with pytest.raises(ValidationError, match="identity mismatch"):
        PublicationIntentRecord.model_validate(data)


@pytest.mark.parametrize("field", ["size_bytes", "sha256"])
def test_commit_must_pin_exact_canonical_manifest(intent, field):
    data = intent.model_dump()
    reference_data = data["expected_commit"]["manifest"]
    if field == "size_bytes":
        reference_data[field] += 1
    else:
        reference_data[field] = "f" * 64
    with pytest.raises(ValidationError, match="manifest"):
        PublicationIntentRecord.model_validate(data)


@pytest.mark.parametrize(
    "change,message",
    [
        ("early-intent", "intent creation"),
        ("early-marker", "marker creation"),
    ],
)
def test_timestamp_ordering(intent, change, message):
    data = intent.model_dump()
    if change == "early-intent":
        data["created_at"] = INVOCATION_TIME
    else:
        data["expected_commit"]["created_at"] = INVOCATION_TIME
    with pytest.raises(ValidationError, match=message):
        PublicationIntentRecord.model_validate(data)


def test_equal_times_are_valid_and_retained(intent):
    data = intent.model_dump()
    data["created_at"] = MANIFEST_TIME
    data["expected_commit"]["created_at"] = MANIFEST_TIME
    record = PublicationIntentRecord.model_validate(data)
    assert decode_publication_intent(encode_publication_intent(record)) == record


def test_expected_marker_can_precede_intent_creation(intent):
    assert intent.expected_commit.created_at < intent.created_at
    assert decode_publication_intent(encode_publication_intent(intent)) == intent


@pytest.mark.parametrize(
    "field,value",
    [
        ("definition_id", OTHER_ID),
        ("definition_sha256", "f" * 64),
        ("consumer_code_sha256", "f" * 64),
        ("worker_runtime_sha256", "f" * 64),
    ],
)
def test_parent_provenance_mismatch(records, field, value):
    definition, attempt, invocation, intent = records
    data = intent.model_dump()
    data["expected_manifest"][field] = value
    manifest = BundleManifest.model_validate(data["expected_manifest"])
    encoded = canonical(manifest.model_dump(mode="json"))
    data["expected_commit"]["manifest"] = reference(
        "payload-manifest.json",
        digest=hashlib.sha256(encoded).hexdigest(),
        size=len(encoded),
    )
    changed = PublicationIntentRecord.model_validate(data)
    with pytest.raises(ValueError, match=field):
        validate_publication_intent(definition, attempt, invocation, changed)


def test_parent_invocation_identity_mismatch(records):
    definition, attempt, invocation, intent = records
    changed = invocation.model_copy(update={"invocation_id": OTHER_ID})
    with pytest.raises(ValueError, match="identity mismatch"):
        validate_publication_intent(definition, attempt, changed, intent)


def test_manifest_must_not_precede_parent_invocation(records):
    definition, attempt, invocation, intent = records
    changed = invocation.model_copy(update={"created_at": INTENT_TIME})
    with pytest.raises(ValueError, match="manifest creation"):
        validate_publication_intent(definition, attempt, changed, intent)


@pytest.mark.parametrize("position", [0, 1, 2, 3])
def test_parent_validator_revalidates_unsafe_copies(records, position):
    changed = list(records)
    changed[position] = changed[position].model_copy(update={"schema_version": 2})
    with pytest.raises(ValidationError):
        validate_publication_intent(*changed)


@pytest.mark.parametrize("operation", ["encode", "validate"])
@pytest.mark.parametrize("target", ["outer", "manifest", "commit", "artifact"])
def test_operations_revalidate_nested_unsafe_models(records, intent, operation, target):
    if target == "outer":
        changed = intent.model_copy(update={"schema_version": 2})
    elif target == "manifest":
        manifest = intent.expected_manifest.model_copy(update={"schema_version": 2})
        changed = intent.model_copy(update={"expected_manifest": manifest})
    elif target == "commit":
        commit = intent.expected_commit.model_copy(update={"schema_version": 2})
        changed = intent.model_copy(update={"expected_commit": commit})
    else:
        artifact = intent.expected_manifest.document.model_copy(
            update={"size_bytes": 0}
        )
        manifest = intent.expected_manifest.model_copy(update={"document": artifact})
        changed = intent.model_copy(update={"expected_manifest": manifest})

    with pytest.raises(ValidationError):
        if operation == "encode":
            encode_publication_intent(changed)
        else:
            validate_publication_intent(*records[:3], changed)


@pytest.mark.parametrize(
    "data",
    [
        b"",
        b"{",
        b"\xff",
        b"null",
        b"[]",
        b'{"schema_version":NaN}',
        b'{"schema_version":Infinity}',
        b'{"schema_version":-Infinity}',
        b'{"schema_version":1,"schema_version":1}',
    ],
)
def test_decoder_rejects_invalid_json(data):
    with pytest.raises(ValueError):
        decode_publication_intent(data)


@pytest.mark.parametrize("data", ["{}", bytearray(b"{}"), None, 1])
def test_decoder_requires_bytes(data):
    with pytest.raises(ValueError, match="must be bytes"):
        decode_publication_intent(data)


@pytest.mark.parametrize(
    "change",
    ["newline", "pretty", "escaped-unicode", "key-order"],
)
def test_decoder_rejects_noncanonical_bytes(intent, change):
    document = intent.model_dump(mode="json")
    if change == "newline":
        encoded = encode_publication_intent(intent) + b"\n"
    elif change == "pretty":
        encoded = json.dumps(document, indent=2, ensure_ascii=False).encode()
    elif change == "escaped-unicode":
        encoded = json.dumps(
            document,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode()
    else:
        encoded = json.dumps(
            document,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode()
    with pytest.raises(ValueError, match="canonical"):
        decode_publication_intent(encoded)


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema_version", True),
        ("schema_version", 2),
        ("kind", "other"),
        ("unexpected", True),
        ("@module", "untrusted.module"),
    ],
)
def test_decoder_validates_schema_without_object_decoding(intent, field, value):
    data = intent.model_dump(mode="json")
    data[field] = value
    with pytest.raises(ValidationError):
        decode_publication_intent(canonical(data))


def test_decoder_rejects_nested_duplicate_keys(intent):
    encoded = encode_publication_intent(intent)
    changed = encoded.replace(
        b'"path":"job-document.json"',
        b'"path":"job-document.json","path":"job-document.json"',
        1,
    )
    assert changed != encoded
    with pytest.raises(ValueError, match="duplicate JSON key"):
        decode_publication_intent(changed)


def test_decoder_rejects_excessive_nesting():
    depth = sys.getrecursionlimit() + 100
    encoded = b"[" * depth + b"0" + b"]" * depth
    with pytest.raises(ValueError, match="nesting|recursion"):
        decode_publication_intent(encoded)


def test_decoder_translates_parser_recursion_error(intent, monkeypatch):
    encoded = encode_publication_intent(intent)

    def fail_loads(*args, **kwargs):
        raise RecursionError("injected parser overflow")

    monkeypatch.setattr(
        "jobflow_gitlab_slurm.persistence.bundles.records.json.loads",
        fail_loads,
    )
    with pytest.raises(ValueError, match="JSON nesting exceeds supported depth"):
        decode_publication_intent(encoded)


def test_operations_do_not_open_files(records, intent, monkeypatch):
    def forbid_open(*args, **kwargs):
        raise AssertionError("publication record operations must not open files")

    monkeypatch.setattr(builtins, "open", forbid_open)
    monkeypatch.setattr(os, "open", forbid_open)
    monkeypatch.setattr(Path, "open", forbid_open)

    encoded = encode_publication_intent(intent)
    assert decode_publication_intent(encoded) == intent
    assert validate_publication_intent(*records) is None


def test_fresh_import_does_not_load_scientific_or_storage_layers():
    script = """
import builtins
import importlib

original_import = builtins.__import__
blocked = {"jobflow", "monty"}

def guarded_import(name, *args, **kwargs):
    if name.split(".", 1)[0] in blocked:
        raise AssertionError(f"unexpected scientific import: {name}")
    return original_import(name, *args, **kwargs)

builtins.__import__ = guarded_import
importlib.import_module("jobflow_gitlab_slurm.persistence.publication.records")
"""
    environment = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
    completed = subprocess.run(
        [sys.executable, "-I", "-c", script],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr

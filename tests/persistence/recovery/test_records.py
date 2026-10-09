"""Offline recovery provenance, strict codecs, and exact parent binding."""

import builtins
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

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
)
from jobflow_gitlab_slurm.persistence.publication.records import PublicationIntentRecord
from jobflow_gitlab_slurm.persistence.recovery.records import (
    RecoveryActor,
    RecoveryReceiptRecord,
    RecoveryRequestRecord,
    decode_recovery_receipt,
    decode_recovery_request,
    encode_recovery_receipt,
    encode_recovery_request,
    validate_recovery_receipt,
    validate_recovery_request,
)

RUN_ID = "00000000-0000-4000-8000-000000000001"

DEFINITION_ID = "aaaaaaaa-0000-4000-8000-000000000002"

ATTEMPT_ID = "00000000-0000-4000-8000-000000000003"

INVOCATION_ID = "00000000-0000-4000-8000-000000000004"

INTENT_ID = "aaaaaaaa-0000-4000-8000-000000000005"

RECOVERY_ID = "bbbbbbbb-0000-4000-8000-000000000006"

EVENT_ID = "cccccccc-0000-4000-8000-000000000007"

OTHER_ID = "00000000-0000-4000-8000-000000000099"

JOB_UUID = "../../opaque-recovery-job-é"

CODECS = (
    (
        "request_record",
        RecoveryRequestRecord,
        encode_recovery_request,
        decode_recovery_request,
    ),
    (
        "receipt_record",
        RecoveryReceiptRecord,
        encode_recovery_receipt,
        decode_recovery_receipt,
    ),
)


def canonical(document):
    return json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def reference(path, data=b"opaque"):
    return {
        "path": path,
        "sha256": hashlib.sha256(data).hexdigest(),
        "size_bytes": len(data),
    }


def invocation_path(document):
    return (
        f"jobs/{document['job_key']}/index-{document['job_index']}/"
        f"attempts/{document['attempt_id']}/"
        f"invocations/{document['invocation_id']}"
    )


def rebind_paths(document):
    prefix = invocation_path(document)
    if document["kind"] == "bundle-recovery-request":
        document["intent"]["path"] = f"{prefix}/publication-intent.json"
    else:
        document["request"]["path"] = (
            f"{prefix}/recoveries/{document['recovery_id']}/request.json"
        )
        document["bundle_manifest"]["path"] = (
            f"{prefix}/published/payload-manifest.json"
        )
        document["bundle_commit"]["path"] = f"{prefix}/published/COMMIT.json"
    return document


@pytest.fixture
def chain():
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
            "created_at": "2026-10-08T12:00:00.000000Z",
            "definition_id": DEFINITION_ID,
            "jobflow_version": "0.3.1",
            "payload": reference(
                f"jobs/{key}/index-1/definitions/{DEFINITION_ID}/job.json"
            ),
            "origin": "original_flow",
            "source": reference("flow/payload.json"),
        }
    )
    attempt = AttemptRecord.model_validate(
        {
            **common,
            "kind": "execution-attempt",
            "created_at": "2026-10-08T12:00:01.000000Z",
            "attempt_id": ATTEMPT_ID,
            "definition_id": DEFINITION_ID,
            "definition_sha256": definition.payload.sha256,
            "consumer_code_sha256": "c" * 64,
            "worker_runtime_sha256": "d" * 64,
        }
    )
    invocation = InvocationRecord.model_validate(
        {
            **common,
            "kind": "worker-invocation",
            "created_at": "2026-10-08T12:00:02.000000Z",
            "attempt_id": ATTEMPT_ID,
            "invocation_id": INVOCATION_ID,
        }
    )
    qualified = {
        **common,
        "attempt_id": ATTEMPT_ID,
        "invocation_id": INVOCATION_ID,
    }
    prefix = invocation_path(qualified)
    manifest = BundleManifest.model_validate(
        {
            **qualified,
            "kind": "bundle-manifest",
            "created_at": "2026-10-08T12:00:03.000000Z",
            "definition_id": DEFINITION_ID,
            "definition_sha256": definition.payload.sha256,
            "consumer_code_sha256": attempt.consumer_code_sha256,
            "worker_runtime_sha256": attempt.worker_runtime_sha256,
            "jobflow_version": "0.3.1",
            "scheduler_receipt": reference(
                f"jobs/{key}/index-1/attempts/{ATTEMPT_ID}/slurm-receipt.json"
            ),
            "document": reference("job-document.json"),
            "response": reference("response.json"),
            "files": (),
            "additional_data": (),
        }
    )
    commit = BundleCommit.model_validate(
        {
            **qualified,
            "kind": "bundle-commit",
            "created_at": "2026-10-08T12:00:04.000000Z",
            "manifest": reference(
                "payload-manifest.json",
                canonical(manifest.model_dump(mode="json")),
            ),
        }
    )
    intent = PublicationIntentRecord(
        schema_version=1,
        kind="publication-intent",
        intent_id=INTENT_ID,
        created_at="2026-10-08T12:00:05.000000Z",
        expected_manifest=manifest,
        expected_commit=commit,
    )
    request_record = RecoveryRequestRecord.model_validate(
        {
            **qualified,
            "kind": "bundle-recovery-request",
            "created_at": "2026-10-08T12:00:06.000000Z",
            "recovery_id": RECOVERY_ID,
            "journal_event_id": EVENT_ID,
            "actor": {
                "kind": "operator",
                "identifier": "operator-1",
            },
            "intent_id": INTENT_ID,
            "intent": reference(
                f"{prefix}/publication-intent.json",
                canonical(intent.model_dump(mode="json")),
            ),
            "observed_status": "staging_only",
            "action": "publish_staging",
        }
    )
    receipt_record = RecoveryReceiptRecord.model_validate(
        {
            **qualified,
            "kind": "bundle-recovery-receipt",
            "created_at": "2026-10-08T12:00:07.000000Z",
            "recovery_id": RECOVERY_ID,
            "request": reference(
                f"{prefix}/recoveries/{RECOVERY_ID}/request.json",
                canonical(request_record.model_dump(mode="json")),
            ),
            "bundle_manifest": reference(
                f"{prefix}/published/payload-manifest.json",
                canonical(manifest.model_dump(mode="json")),
            ),
            "bundle_commit": reference(
                f"{prefix}/published/COMMIT.json",
                canonical(commit.model_dump(mode="json")),
            ),
            "result": "committed_bundle_verified",
        }
    )
    return SimpleNamespace(
        definition=definition,
        attempt=attempt,
        invocation=invocation,
        intent=intent,
        request_record=request_record,
        receipt_record=receipt_record,
    )


def validate_case(chain, case_name, record):
    parents = (
        chain.definition,
        chain.attempt,
        chain.invocation,
        chain.intent,
    )
    if case_name == "request_record":
        return validate_recovery_request(*parents, record)
    return validate_recovery_receipt(
        *parents,
        chain.request_record,
        receipt=record,
    )


@pytest.mark.parametrize("case_name,record_type,encode,decode", CODECS)
def test_canonical_round_trip(chain, case_name, record_type, encode, decode):
    record = getattr(chain, case_name)
    encoded = encode(record)
    assert encoded == canonical(record.model_dump(mode="json"))
    assert not encoded.endswith(b"\n")
    assert "é".encode() in encoded
    assert b"\\u00e9" not in encoded
    loaded = decode(encoded)
    assert isinstance(loaded, record_type)
    assert loaded == record
    assert validate_case(chain, case_name, loaded) is None


@pytest.mark.parametrize("case_name,record_type,encode,decode", CODECS)
def test_all_fields_required(chain, case_name, record_type, encode, decode):
    document = getattr(chain, case_name).model_dump()
    for field in document:
        incomplete = {key: value for key, value in document.items() if key != field}
        with pytest.raises(ValidationError):
            record_type.model_validate(incomplete)


@pytest.mark.parametrize("case_name,record_type,encode,decode", CODECS)
@pytest.mark.parametrize(
    "field,value",
    [
        ("schema_version", 0),
        ("schema_version", 2),
        ("schema_version", True),
        ("schema_version", "1"),
        ("schema_version", 1.0),
        ("kind", "other"),
        ("run_id", "invalid"),
        ("created_at", "2026-10-08T12:00:06Z"),
        ("job_uuid", ""),
        ("job_uuid", b"opaque"),
        ("job_uuid", "\ud800"),
        ("job_index", 0),
        ("job_index", True),
        ("job_index", "1"),
        ("job_key", "f" * 64),
        ("attempt_id", "invalid"),
        ("invocation_id", "invalid"),
        ("recovery_id", "invalid"),
        ("unknown", "forbidden"),
    ],
)
def test_strict_common_fields(
    chain, case_name, record_type, encode, decode, field, value
):
    document = getattr(chain, case_name).model_dump()
    document[field] = value
    with pytest.raises(ValidationError):
        record_type.model_validate(document)


@pytest.mark.parametrize(
    "identifier",
    [
        "",
        ".operator",
        "_operator",
        "-operator",
        "with space",
        "operator@example",
        "../operator",
        "operator/1",
        "opérateur",
        "operator\n",
        "operator\0",
        "x" * 129,
    ],
)
def test_invalid_actor_identifier(identifier):
    with pytest.raises(ValidationError):
        RecoveryActor(kind="operator", identifier=identifier)


@pytest.mark.parametrize("kind", ["operator", "controller"])
def test_actor_bounds_and_immutability(kind):
    actor = RecoveryActor(kind=kind, identifier="a" + "_.-" * 42 + "z")
    assert len(actor.identifier) == 128
    with pytest.raises(ValidationError):
        actor.identifier = "replacement"


@pytest.mark.parametrize(
    "document",
    [
        {},
        {"kind": "operator"},
        {"identifier": "operator-1"},
        {"kind": "administrator", "identifier": "operator-1"},
        {"kind": "operator", "identifier": 1},
        {"kind": "operator", "identifier": "operator-1", "token": "forbidden"},
    ],
)
def test_strict_actor_fields(document):
    with pytest.raises(ValidationError):
        RecoveryActor.model_validate(document)


@pytest.mark.parametrize(
    "observed_status",
    ["staging_only", "published_uncommitted", "committed"],
)
@pytest.mark.parametrize(
    "action",
    ["publish_staging", "complete_marker", "acknowledge_commit"],
)
def test_observation_action_mapping(chain, observed_status, action):
    document = chain.request_record.model_dump()
    document.update(observed_status=observed_status, action=action)
    expected = {
        "staging_only": "publish_staging",
        "published_uncommitted": "complete_marker",
        "committed": "acknowledge_commit",
    }
    if action == expected[observed_status]:
        record = RecoveryRequestRecord.model_validate(document)
        assert validate_case(chain, "request_record", record) is None
    else:
        with pytest.raises(ValidationError, match="original observation"):
            RecoveryRequestRecord.model_validate(document)


@pytest.mark.parametrize(
    "field,value",
    [
        ("journal_event_id", "invalid"),
        ("intent_id", "invalid"),
        ("observed_status", "absent"),
        ("observed_status", "ambiguous"),
        ("observed_status", "invalid"),
        ("action", "rerun"),
    ],
)
def test_request_only_fields(chain, field, value):
    document = chain.request_record.model_dump()
    document[field] = value
    with pytest.raises(ValidationError):
        RecoveryRequestRecord.model_validate(document)


def test_receipt_cannot_claim_scientific_success(chain):
    document = chain.receipt_record.model_dump()
    document["result"] = "successful"
    with pytest.raises(ValidationError):
        RecoveryReceiptRecord.model_validate(document)


@pytest.mark.parametrize(
    "case_name,field",
    [
        ("request_record", "intent"),
        ("receipt_record", "request"),
        ("receipt_record", "bundle_manifest"),
        ("receipt_record", "bundle_commit"),
    ],
)
@pytest.mark.parametrize(
    "update",
    [
        {"path": "other/record.json"},
        {"path": "../record.json"},
        {"path": "/record.json"},
        {"path": "other\\record.json"},
        {"size_bytes": 0},
        {"size_bytes": True},
        {"sha256": "invalid"},
        {"extra": "forbidden"},
    ],
)
def test_strict_reference_fields(chain, case_name, field, update):
    record = getattr(chain, case_name)
    document = record.model_dump()
    document[field].update(update)
    with pytest.raises(ValidationError):
        type(record).model_validate(document)


@pytest.mark.parametrize("case_name", ["request_record", "receipt_record"])
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
def test_cross_record_identity_binding(chain, case_name, field, value):
    original = getattr(chain, case_name)
    document = original.model_dump()
    document[field] = value
    if field == "job_uuid":
        document["job_key"] = job_key(value)
    changed = type(original).model_validate(rebind_paths(document))
    with pytest.raises(ValueError, match="identity mismatch"):
        validate_case(chain, case_name, changed)


def test_request_intent_identity_binding(chain):
    changed = chain.request_record.model_copy(update={"intent_id": OTHER_ID})
    with pytest.raises(ValueError, match="intent_id mismatch"):
        validate_case(chain, "request_record", changed)


def test_receipt_recovery_identity_binding(chain):
    document = chain.receipt_record.model_dump()
    document["recovery_id"] = OTHER_ID
    changed = RecoveryReceiptRecord.model_validate(rebind_paths(document))
    with pytest.raises(ValueError, match="recovery_id mismatch"):
        validate_case(chain, "receipt_record", changed)


@pytest.mark.parametrize(
    "case_name,created_at",
    [
        ("request_record", "2026-10-08T12:00:04.000000Z"),
        ("receipt_record", "2026-10-08T12:00:05.000000Z"),
    ],
)
def test_timestamp_binding(chain, case_name, created_at):
    changed = getattr(chain, case_name).model_copy(update={"created_at": created_at})
    with pytest.raises(ValueError, match="creation precedes"):
        validate_case(chain, case_name, changed)


@pytest.mark.parametrize(
    "case_name,field",
    [
        ("request_record", "intent"),
        ("receipt_record", "request"),
        ("receipt_record", "bundle_manifest"),
        ("receipt_record", "bundle_commit"),
    ],
)
@pytest.mark.parametrize("attribute", ["sha256", "size_bytes"])
def test_exact_canonical_reference_binding(chain, case_name, field, attribute):
    original = getattr(chain, case_name)
    document = original.model_dump()
    if attribute == "sha256":
        document[field][attribute] = "f" * 64
    else:
        document[field][attribute] += 1
    changed = type(original).model_validate(document)
    with pytest.raises(ValueError, match="mismatch"):
        validate_case(chain, case_name, changed)


def test_parent_runtime_binding(chain):
    changed_attempt = chain.attempt.model_copy(
        update={"worker_runtime_sha256": "f" * 64}
    )
    with pytest.raises(ValueError):
        validate_recovery_receipt(
            chain.definition,
            changed_attempt,
            chain.invocation,
            chain.intent,
            chain.request_record,
            receipt=chain.receipt_record,
        )


@pytest.mark.parametrize(
    "parent",
    ["definition", "attempt", "invocation", "intent"],
)
def test_parent_instances_are_revalidated(chain, parent):
    setattr(
        chain,
        parent,
        getattr(chain, parent).model_copy(update={"schema_version": 2}),
    )
    with pytest.raises(ValidationError):
        validate_case(chain, "receipt_record", chain.receipt_record)


@pytest.mark.parametrize("case_name,record_type,encode,decode", CODECS)
def test_records_frozen_and_unsafe_outer_copies_rejected(
    chain, case_name, record_type, encode, decode
):
    record = getattr(chain, case_name)
    with pytest.raises(ValidationError):
        record.created_at = "2026-10-08T12:00:08.000000Z"

    unsafe = record.model_copy(update={"schema_version": 2})
    with pytest.raises(ValidationError):
        encode(unsafe)
    with pytest.raises(ValidationError):
        validate_case(chain, case_name, unsafe)


@pytest.mark.parametrize(
    "case_name,field",
    [
        ("request_record", "intent"),
        ("receipt_record", "request"),
        ("receipt_record", "bundle_manifest"),
        ("receipt_record", "bundle_commit"),
    ],
)
def test_nested_reference_instances_revalidated(chain, case_name, field):
    record = getattr(chain, case_name)
    unsafe_reference = getattr(record, field).model_copy(update={"path": "../escape"})
    unsafe = record.model_copy(update={field: unsafe_reference})
    encode = (
        encode_recovery_request
        if case_name == "request_record"
        else encode_recovery_receipt
    )
    with pytest.raises(ValidationError):
        encode(unsafe)
    with pytest.raises(ValidationError):
        validate_case(chain, case_name, unsafe)


def test_nested_actor_instance_revalidated(chain):
    actor = chain.request_record.actor.model_copy(update={"identifier": "../operator"})
    unsafe = chain.request_record.model_copy(update={"actor": actor})
    with pytest.raises(ValidationError):
        encode_recovery_request(unsafe)
    with pytest.raises(ValidationError):
        validate_case(chain, "request_record", unsafe)


@pytest.mark.parametrize("case_name,record_type,encode,decode", CODECS)
@pytest.mark.parametrize(
    "data",
    [
        b"",
        b"{",
        b"\xff",
        b"[]",
        b"null",
        b'{"value":NaN}',
        b'{"value":Infinity}',
        b'{"value":-Infinity}',
        b'{"schema_version":1,"schema_version":1}',
        b'{"actor":{"kind":"operator","kind":"controller"}}',
    ],
)
def test_malformed_or_nonfinite_json(
    chain, case_name, record_type, encode, decode, data
):
    with pytest.raises(ValueError):
        decode(data)


@pytest.mark.parametrize("case_name,record_type,encode,decode", CODECS)
def test_decoder_requires_bytes(chain, case_name, record_type, encode, decode):
    encoded = encode(getattr(chain, case_name))
    for wrong_type in (encoded.decode(), bytearray(encoded), memoryview(encoded)):
        with pytest.raises(ValueError, match="must be bytes"):
            decode(wrong_type)


@pytest.mark.parametrize("case_name,record_type,encode,decode", CODECS)
def test_valid_but_noncanonical_json_rejected(
    chain, case_name, record_type, encode, decode
):
    record = getattr(chain, case_name)
    document = record.model_dump(mode="json")
    variants = (
        encode(record) + b"\n",
        b" " + encode(record),
        json.dumps(document, indent=2, ensure_ascii=False).encode(),
        json.dumps(
            document,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode(),
    )
    for data in variants:
        with pytest.raises(ValueError, match="canonical"):
            decode(data)


@pytest.mark.parametrize("case_name,record_type,encode,decode", CODECS)
def test_decoder_rejects_unknown_fields_and_schemas(
    chain, case_name, record_type, encode, decode
):
    document = getattr(chain, case_name).model_dump(mode="json")
    for update in (
        {"schema_version": 2},
        {"@module": "untrusted.consumer"},
        {"scientific_success": True},
    ):
        with pytest.raises(ValidationError):
            decode(canonical({**document, **update}))


def test_codecs_and_binding_do_not_open_files(chain, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("pure recovery records must not open files")

    monkeypatch.setattr(builtins, "open", forbidden)
    monkeypatch.setattr(os, "open", forbidden)
    monkeypatch.setattr(Path, "open", forbidden)

    request_record = decode_recovery_request(
        encode_recovery_request(chain.request_record)
    )
    receipt_record = decode_recovery_receipt(
        encode_recovery_receipt(chain.receipt_record)
    )
    assert (
        validate_recovery_receipt(
            chain.definition,
            chain.attempt,
            chain.invocation,
            chain.intent,
            request_record,
            receipt=receipt_record,
        )
        is None
    )


def test_import_does_not_load_jobflow_or_monty():
    script = """
import builtins
import sys

original_import = builtins.__import__

def guarded(name, *args, **kwargs):
    if name.split(".")[0] in {"jobflow", "monty"}:
        raise AssertionError(f"unexpected scientific import: {name}")
    return original_import(name, *args, **kwargs)

builtins.__import__ = guarded
import jobflow_gitlab_slurm.persistence.recovery.records
assert "jobflow" not in sys.modules
assert "monty" not in sys.modules
"""
    result = subprocess.run(
        [sys.executable, "-I", "-B", "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr

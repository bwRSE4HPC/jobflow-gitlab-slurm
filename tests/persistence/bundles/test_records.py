"""Offline bundle metadata, canonical bytes, inventories, and marker binding."""

import hashlib
import json
import sys

import pytest
from pydantic import ValidationError

from jobflow_gitlab_slurm.persistence.attempts.records import (
    AttemptRecord,
    InvocationRecord,
    JobDefinitionRecord,
    job_key,
)
from jobflow_gitlab_slurm.persistence.bundles.records import (
    AdditionalDataReference,
    BundleArtifactReference,
    BundleCommit,
    BundleManifest,
    decode_bundle_commit,
    decode_bundle_manifest,
    encode_bundle_commit,
    encode_bundle_manifest,
    validate_bundle_records,
)

RUN_ID = "00000000-0000-4000-8000-000000000001"

DEFINITION_ID = "aaaaaaaa-0000-4000-8000-000000000002"

ATTEMPT_ID = "00000000-0000-4000-8000-000000000003"

INVOCATION_ID = "00000000-0000-4000-8000-000000000004"

OTHER_ID = "00000000-0000-4000-8000-000000000099"

JOB_UUID = "../../job-é"

DEFINITION_TIME = "2026-10-08T12:00:00.000000Z"

ATTEMPT_TIME = "2026-10-08T12:00:01.000000Z"

INVOCATION_TIME = "2026-10-08T12:00:02.000000Z"

MANIFEST_TIME = "2026-10-08T12:00:03.000000Z"

COMMIT_TIME = "2026-10-08T12:00:04.000000Z"


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


def identity():
    return {
        "schema_version": 1,
        "run_id": RUN_ID,
        "job_uuid": JOB_UUID,
        "job_index": 1,
        "job_key": job_key(JOB_UUID),
        "attempt_id": ATTEMPT_ID,
        "invocation_id": INVOCATION_ID,
    }


def receipt_path(data):
    return (
        f"jobs/{data['job_key']}/index-{data['job_index']}/attempts/"
        f"{data['attempt_id']}/slurm-receipt.json"
    )


def data_reference(path, store="results", blob="blob-a"):
    return {
        "store_name": store,
        "blob_uuid": blob,
        "artifact": reference(path),
    }


def manifest_data():
    data = identity()
    data.update(
        {
            "kind": "bundle-manifest",
            "created_at": MANIFEST_TIME,
            "definition_id": DEFINITION_ID,
            "definition_sha256": "b" * 64,
            "consumer_code_sha256": "c" * 64,
            "worker_runtime_sha256": "d" * 64,
            "jobflow_version": "0.3.1",
            "scheduler_receipt": reference(receipt_path(data), digest="e" * 64),
            "document": reference("job-document.json"),
            "response": reference("response.json"),
            "files": (
                reference("files/a.txt", size=0),
                reference("files/sub/b.txt"),
            ),
            "additional_data": (
                data_reference("data/one.json"),
                data_reference("data/two.json", blob="blob-b"),
            ),
        }
    )
    return data


def sample_manifest(**changes):
    data = manifest_data()
    data.update(changes)
    return BundleManifest.model_validate(data)


def commit_data(manifest=None):
    if manifest is None:
        manifest = sample_manifest()
    encoded = canonical(manifest.model_dump(mode="json"))
    data = identity()
    data.update(
        {
            "kind": "bundle-commit",
            "created_at": COMMIT_TIME,
            "manifest": reference(
                "payload-manifest.json",
                digest=hashlib.sha256(encoded).hexdigest(),
                size=len(encoded),
            ),
        }
    )
    return data


def sample_commit(manifest=None, **changes):
    data = commit_data(manifest)
    data.update(changes)
    return BundleCommit.model_validate(data)


def linked_records():
    base = identity()
    common = {
        key: base[key]
        for key in ("schema_version", "run_id", "job_uuid", "job_index", "job_key")
    }
    definition = JobDefinitionRecord.model_validate(
        {
            **common,
            "kind": "job-definition",
            "created_at": DEFINITION_TIME,
            "definition_id": DEFINITION_ID,
            "jobflow_version": "0.3.1",
            "payload": reference(
                f"jobs/{base['job_key']}/index-1/definitions/{DEFINITION_ID}/job.json",
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
    manifest = sample_manifest()
    return definition, attempt, invocation, manifest, sample_commit(manifest)


CASES = (
    (
        BundleManifest,
        manifest_data,
        encode_bundle_manifest,
        decode_bundle_manifest,
    ),
    (
        BundleCommit,
        commit_data,
        encode_bundle_commit,
        decode_bundle_commit,
    ),
)


@pytest.mark.parametrize("model,factory,encoder,decoder", CASES)
def test_canonical_round_trip(model, factory, encoder, decoder):
    record = model.model_validate(factory())
    encoded = encoder(record)
    assert encoded == canonical(record.model_dump(mode="json"))
    assert not encoded.endswith(b"\n")
    assert "é".encode() in encoded
    assert b"\\u00e9" not in encoded
    assert decoder(encoded) == record
    assert model.model_validate_json(record.model_dump_json()) == record
    with pytest.raises(ValidationError, match="frozen"):
        record.created_at = COMMIT_TIME


def test_complete_metadata_binding_uses_independent_digest_and_size():
    records = linked_records()
    manifest, commit = records[3:]
    encoded = canonical(manifest.model_dump(mode="json"))
    assert commit.manifest.sha256 == hashlib.sha256(encoded).hexdigest()
    assert commit.manifest.size_bytes == len(encoded)
    assert validate_bundle_records(*records) is None
    assert "success" not in manifest.model_dump()
    assert "success" not in commit.model_dump()


def test_empty_inventories_are_explicit_and_valid():
    manifest = sample_manifest(files=(), additional_data=())
    assert manifest.files == ()
    assert manifest.additional_data == ()
    assert decode_bundle_manifest(encode_bundle_manifest(manifest)) == manifest


def test_null_output_is_not_a_missing_document():
    payload = b'{"output":null}'
    manifest = sample_manifest(
        document=reference(
            "job-document.json",
            digest=hashlib.sha256(payload).hexdigest(),
            size=len(payload),
        )
    )
    assert manifest.document.size_bytes > 0


def test_nested_inventory_models_are_detached_and_frozen():
    data = manifest_data()
    manifest = BundleManifest.model_validate(data)
    data["files"][0]["sha256"] = "f" * 64
    data["additional_data"][0]["artifact"]["size_bytes"] = 999
    assert manifest.files[0].sha256 == "a" * 64
    assert manifest.additional_data[0].artifact.size_bytes == 12
    assert isinstance(manifest.files, tuple)
    assert isinstance(manifest.additional_data, tuple)

    dumped = manifest.model_dump(mode="json")
    dumped["files"][0]["size_bytes"] = 999
    assert manifest.files[0].size_bytes == 0

    with pytest.raises(ValidationError, match="frozen"):
        manifest.files[0].size_bytes = 1
    with pytest.raises(ValidationError, match="frozen"):
        manifest.additional_data[0].store_name = "changed"


@pytest.mark.parametrize("model,factory,encoder,decoder", CASES)
@pytest.mark.parametrize(
    "field,value",
    [
        ("schema_version", 0),
        ("schema_version", 2),
        ("schema_version", True),
        ("schema_version", "1"),
        ("kind", "other"),
        ("run_id", "invalid"),
        ("attempt_id", DEFINITION_ID.upper()),
        ("invocation_id", "12345678-1234-1234-8123-123456789abc"),
        ("created_at", "invalid"),
        ("created_at", "2026-10-08T12:00:00Z"),
        ("created_at", "2026-10-08T12:00:00.000000+00:00"),
        ("job_uuid", ""),
        ("job_uuid", 1),
        ("job_uuid", "\ud800"),
        ("job_index", 0),
        ("job_index", True),
        ("job_index", "1"),
        ("job_key", "f" * 64),
        ("job_key", "F" * 64),
        ("success", True),
    ],
)
def test_invalid_common_fields(model, factory, encoder, decoder, field, value):
    data = factory()
    data[field] = value
    with pytest.raises(ValidationError):
        model.model_validate(data)


@pytest.mark.parametrize("model,factory,encoder,decoder", CASES)
def test_every_record_field_is_required(model, factory, encoder, decoder):
    values = factory()
    for field in values:
        data = factory()
        del data[field]
        with pytest.raises(ValidationError):
            model.model_validate(data)


@pytest.mark.parametrize("field", ["document", "response"])
@pytest.mark.parametrize("change", ["wrong-path", "zero-size", "missing"])
def test_required_payload_references(field, change):
    data = manifest_data()
    if change == "wrong-path":
        data[field]["path"] = "files/wrong.json"
    elif change == "zero-size":
        data[field]["size_bytes"] = 0
    else:
        del data[field]
    with pytest.raises(ValidationError):
        BundleManifest.model_validate(data)


@pytest.mark.parametrize("change", ["wrong-path", "zero-size"])
def test_scheduler_reference_is_bound_to_attempt(change):
    data = manifest_data()
    if change == "wrong-path":
        data["scheduler_receipt"]["path"] = "other/slurm-receipt.json"
    else:
        data["scheduler_receipt"]["size_bytes"] = 0
    with pytest.raises(ValidationError, match="scheduler receipt"):
        BundleManifest.model_validate(data)


@pytest.mark.parametrize("change", ["wrong-path", "zero-size"])
def test_marker_reference_role(change):
    data = commit_data()
    if change == "wrong-path":
        data["manifest"]["path"] = "other.json"
    else:
        data["manifest"]["size_bytes"] = 0
    with pytest.raises(ValidationError, match="manifest"):
        BundleCommit.model_validate(data)


@pytest.mark.parametrize(
    "path",
    ["", "/absolute", "../x", "files/./x", "files//x", "files/", "x\\y", "x\0y"],
)
def test_bundle_reference_uses_safe_lexical_paths(path):
    with pytest.raises(ValidationError):
        BundleArtifactReference.model_validate(reference(path))


@pytest.mark.parametrize(
    "path", ["data/x", "files", "COMMIT.json", "payload-manifest.json"]
)
def test_application_namespace(path):
    with pytest.raises(ValidationError, match="below files/"):
        sample_manifest(files=(reference(path),))


@pytest.mark.parametrize("path", ["files/x", "data", "COMMIT.json"])
def test_additional_data_namespace(path):
    with pytest.raises(ValidationError, match="below data/"):
        AdditionalDataReference.model_validate(data_reference(path))


@pytest.mark.parametrize("field", ["store_name", "blob_uuid"])
@pytest.mark.parametrize("bad", ["", None, 1, "\ud800"])
def test_additional_data_identity_validation(field, bad):
    data = data_reference("data/x")
    data[field] = bad
    with pytest.raises(ValidationError):
        AdditionalDataReference.model_validate(data)


def test_additional_data_preserves_exact_identity_strings():
    value = AdditionalDataReference.model_validate(
        data_reference("data/x", store="../é", blob="/CUSTOM-BLOB")
    )
    assert value.store_name == "../é"
    assert value.blob_uuid == "/CUSTOM-BLOB"
    assert AdditionalDataReference.model_validate_json(value.model_dump_json()) == value


@pytest.mark.parametrize("field", ["files", "additional_data"])
def test_python_inventory_requires_tuple(field):
    data = manifest_data()
    data[field] = list(data[field])
    with pytest.raises(ValidationError):
        BundleManifest.model_validate(data)


@pytest.mark.parametrize("field", ["files", "additional_data"])
def test_inventory_order_is_not_silently_normalized(field):
    data = manifest_data()
    data[field] = tuple(reversed(data[field]))
    with pytest.raises(ValidationError, match="ordered by path"):
        BundleManifest.model_validate(data)


@pytest.mark.parametrize("field", ["files", "additional_data"])
def test_duplicate_payload_paths(field):
    data = manifest_data()
    if field == "files":
        data[field] = (reference("files/a"), reference("files/a"))
    else:
        data[field] = (
            data_reference("data/a", blob="first"),
            data_reference("data/a", blob="second"),
        )
    with pytest.raises(ValidationError, match="duplicate declared payload"):
        BundleManifest.model_validate(data)


@pytest.mark.parametrize("field", ["files", "additional_data"])
def test_file_ancestor_conflicts(field):
    data = manifest_data()
    if field == "files":
        data[field] = (reference("files/a"), reference("files/a/b"))
    else:
        data[field] = (
            data_reference("data/a", blob="first"),
            data_reference("data/a/b", blob="second"),
        )
    with pytest.raises(ValidationError, match="ancestor"):
        BundleManifest.model_validate(data)


def test_similar_names_are_not_ancestor_conflicts():
    manifest = sample_manifest(
        files=(reference("files/a"), reference("files/ab")),
    )
    assert len(manifest.files) == 2


def test_duplicate_store_blob_identity():
    with pytest.raises(ValidationError, match="store/blob identity"):
        sample_manifest(
            additional_data=(
                data_reference("data/a", store="results", blob="same"),
                data_reference("data/b", store="results", blob="same"),
            )
        )


def test_same_blob_name_in_different_stores_is_valid():
    manifest = sample_manifest(
        additional_data=(
            data_reference("data/a", store="first", blob="same"),
            data_reference("data/b", store="second", blob="same"),
        )
    )
    assert len(manifest.additional_data) == 2


@pytest.mark.parametrize(
    "field,value",
    [
        ("definition_id", "invalid"),
        ("definition_sha256", "short"),
        ("consumer_code_sha256", "G" * 64),
        ("worker_runtime_sha256", None),
        ("jobflow_version", "0.3.0"),
    ],
)
def test_manifest_specific_fields(field, value):
    data = manifest_data()
    data[field] = value
    with pytest.raises(ValidationError):
        BundleManifest.model_validate(data)


@pytest.mark.parametrize("model,factory,encoder,decoder", CASES)
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
def test_decoders_reject_invalid_json(model, factory, encoder, decoder, data):
    with pytest.raises(ValueError):
        decoder(data)


@pytest.mark.parametrize("model,factory,encoder,decoder", CASES)
@pytest.mark.parametrize("data", ["{}", bytearray(b"{}"), None, 1])
def test_decoders_require_bytes(model, factory, encoder, decoder, data):
    with pytest.raises(ValueError, match="must be bytes"):
        decoder(data)


@pytest.mark.parametrize("model,factory,encoder,decoder", CASES)
def test_decoders_reject_excessive_nesting(model, factory, encoder, decoder):
    depth = sys.getrecursionlimit() + 100
    data = b"[" * depth + b"0" + b"]" * depth
    with pytest.raises(
        ValueError,
        match=r"JSON nesting exceeds supported depth|recursion limit exceeded",
    ):
        decoder(data)


@pytest.mark.parametrize("model,factory,encoder,decoder", CASES)
def test_decoders_translate_parser_recursion_error(
    model, factory, encoder, decoder, monkeypatch
):
    encoded = encoder(model.model_validate(factory()))
    failure = RecursionError("injected JSON parser overflow")

    def fail_loads(*args, **kwargs):
        raise failure

    monkeypatch.setattr(
        "jobflow_gitlab_slurm.persistence.bundles.records.json.loads",
        fail_loads,
    )

    with pytest.raises(
        ValueError,
        match=r"^JSON nesting exceeds supported depth$",
    ) as caught:
        decoder(encoded)

    assert caught.value.__cause__ is failure


@pytest.mark.parametrize("model,factory,encoder,decoder", CASES)
@pytest.mark.parametrize(
    "change", ["newline", "pretty", "escaped-unicode", "key-order"]
)
def test_noncanonical_bytes(model, factory, encoder, decoder, change):
    record = model.model_validate(factory())
    document = record.model_dump(mode="json")
    if change == "newline":
        encoded = encoder(record) + b"\n"
    elif change == "pretty":
        encoded = json.dumps(document, indent=2, ensure_ascii=False).encode()
    elif change == "escaped-unicode":
        encoded = json.dumps(
            document, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode()
    else:
        encoded = json.dumps(
            document, separators=(",", ":"), ensure_ascii=False
        ).encode()
    with pytest.raises(ValueError, match="canonical"):
        decoder(encoded)


@pytest.mark.parametrize("model,factory,encoder,decoder", CASES)
@pytest.mark.parametrize(
    "field,value",
    [("schema_version", 2), ("schema_version", True), ("success", True)],
)
def test_decoders_validate_record_schema(
    model, factory, encoder, decoder, field, value
):
    record = model.model_validate(factory())
    data = record.model_dump(mode="json")
    data[field] = value
    with pytest.raises(ValidationError):
        decoder(canonical(data))


def test_nested_duplicate_json_key_is_rejected():
    encoded = encode_bundle_manifest(sample_manifest())
    duplicate = encoded.replace(
        b'"path":"job-document.json"',
        b'"path":"job-document.json","path":"job-document.json"',
        1,
    )
    assert duplicate != encoded
    with pytest.raises(ValueError, match="duplicate JSON key"):
        decode_bundle_manifest(duplicate)


@pytest.mark.parametrize("position", [3, 4])
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
def test_cross_record_identity_conflicts(position, field, value):
    records = list(linked_records())
    data = records[position].model_dump()
    data[field] = value
    if field == "job_uuid":
        data["job_key"] = job_key(value)
    if position == 3:
        data["scheduler_receipt"]["path"] = receipt_path(data)
    records[position] = type(records[position]).model_validate(data)
    with pytest.raises(ValueError, match="identity mismatch"):
        validate_bundle_records(*records)


@pytest.mark.parametrize(
    "field,value",
    [
        ("definition_id", OTHER_ID),
        ("definition_sha256", "f" * 64),
        ("consumer_code_sha256", "f" * 64),
        ("worker_runtime_sha256", "f" * 64),
    ],
)
def test_cross_record_definition_and_runtime_conflicts(field, value):
    records = list(linked_records())
    data = records[3].model_dump()
    data[field] = value
    records[3] = BundleManifest.model_validate(data)
    with pytest.raises(ValueError, match=field):
        validate_bundle_records(*records)


@pytest.mark.parametrize(
    "position,value,message",
    [
        (3, ATTEMPT_TIME, "manifest creation"),
        (4, INVOCATION_TIME, "marker creation"),
    ],
)
def test_cross_record_time_conflicts(position, value, message):
    records = list(linked_records())
    data = records[position].model_dump()
    data["created_at"] = value
    records[position] = type(records[position]).model_validate(data)
    with pytest.raises(ValueError, match=message):
        validate_bundle_records(*records)


def test_equal_invocation_manifest_marker_times():
    records = list(linked_records())
    records[3] = sample_manifest(created_at=INVOCATION_TIME)
    records[4] = sample_commit(records[3], created_at=INVOCATION_TIME)
    assert validate_bundle_records(*records) is None


@pytest.mark.parametrize("field", ["size_bytes", "sha256"])
def test_marker_must_bind_exact_manifest_bytes(field):
    records = list(linked_records())
    data = records[4].model_dump()
    if field == "size_bytes":
        data["manifest"][field] += 1
    else:
        data["manifest"][field] = "f" * 64
    records[4] = BundleCommit.model_validate(data)
    with pytest.raises(ValueError, match="manifest"):
        validate_bundle_records(*records)


@pytest.mark.parametrize("position", [0, 1, 2, 3, 4])
def test_cross_record_validator_revalidates_unsafe_copies(position):
    records = list(linked_records())
    records[position] = records[position].model_copy(update={"schema_version": 2})
    with pytest.raises(ValidationError):
        validate_bundle_records(*records)


@pytest.mark.parametrize("model,factory,encoder,decoder", CASES)
def test_encoder_revalidates_unsafe_record(model, factory, encoder, decoder):
    record = model.model_validate(factory())
    unsafe = record.model_copy(update={"job_key": "f" * 64})
    with pytest.raises(ValidationError, match="job_key"):
        encoder(unsafe)


def test_encoder_revalidates_nested_manifest_reference():
    manifest = sample_manifest()
    unsafe_document = manifest.document.model_copy(update={"size_bytes": 0})
    unsafe = manifest.model_copy(update={"document": unsafe_document})
    with pytest.raises(ValidationError, match="nonempty"):
        encode_bundle_manifest(unsafe)


def test_encoder_revalidates_nested_marker_reference():
    commit = sample_commit()
    unsafe_reference = commit.manifest.model_copy(update={"path": "../manifest.json"})
    unsafe = commit.model_copy(update={"manifest": unsafe_reference})
    with pytest.raises(ValidationError):
        encode_bundle_commit(unsafe)


def test_manifest_revalidates_additional_data_instances():
    manifest = sample_manifest()
    entry = manifest.additional_data[0]
    unsafe_artifact = entry.artifact.model_copy(update={"path": "files/blob"})
    unsafe_entry = entry.model_copy(update={"artifact": unsafe_artifact})
    unsafe = manifest.model_copy(update={"additional_data": (unsafe_entry,)})
    with pytest.raises(ValidationError, match="below data/"):
        encode_bundle_manifest(unsafe)

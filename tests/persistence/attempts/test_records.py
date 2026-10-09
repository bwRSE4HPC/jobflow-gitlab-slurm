"""Offline identity, provenance, immutability, and cross-record validation."""

import hashlib

import pytest
from pydantic import ValidationError

from jobflow_gitlab_slurm.persistence.attempts.records import (
    AttemptRecord,
    InvocationRecord,
    JobDefinitionRecord,
    RecordArtifactReference,
    job_key,
    validate_attempt_records,
)

RUN_ID = "00000000-0000-4000-8000-000000000001"

DEFINITION_ID = "aaaaaaaa-0000-4000-8000-000000000002"

ATTEMPT_ID = "00000000-0000-4000-8000-000000000003"

INVOCATION_ID = "00000000-0000-4000-8000-000000000004"

OTHER_ID = "00000000-0000-4000-8000-000000000099"

JOB_UUID = "example-job"

DEFINITION_CREATED = "2026-10-08T12:00:00.000000Z"

ATTEMPT_CREATED = "2026-10-08T12:00:01.000000Z"

INVOCATION_CREATED = "2026-10-08T12:00:02.000000Z"


def reference_data(path="flow/payload.json", digest="a" * 64, size=12):
    return {"path": path, "sha256": digest, "size_bytes": size}


def common_data(job_uuid=JOB_UUID, job_index=1):
    return {
        "schema_version": 1,
        "run_id": RUN_ID,
        "job_uuid": job_uuid,
        "job_index": job_index,
        "job_key": job_key(job_uuid),
    }


def definition_data(job_uuid=JOB_UUID, job_index=1):
    data = common_data(job_uuid, job_index)
    data.update(
        {
            "kind": "job-definition",
            "created_at": DEFINITION_CREATED,
            "definition_id": DEFINITION_ID,
            "jobflow_version": "0.3.1",
            "payload": reference_data(
                f"jobs/{data['job_key']}/index-{job_index}/definitions/"
                f"{DEFINITION_ID}/job.json",
                digest="b" * 64,
            ),
            "origin": "original_flow",
            "source": reference_data(),
        }
    )
    return data


def attempt_data(job_uuid=JOB_UUID, job_index=1):
    data = common_data(job_uuid, job_index)
    data.update(
        {
            "kind": "execution-attempt",
            "created_at": ATTEMPT_CREATED,
            "attempt_id": ATTEMPT_ID,
            "definition_id": DEFINITION_ID,
            "definition_sha256": "b" * 64,
            "consumer_code_sha256": "c" * 64,
            "worker_runtime_sha256": "d" * 64,
        }
    )
    return data


def invocation_data(job_uuid=JOB_UUID, job_index=1):
    data = common_data(job_uuid, job_index)
    data.update(
        {
            "kind": "worker-invocation",
            "created_at": INVOCATION_CREATED,
            "invocation_id": INVOCATION_ID,
            "attempt_id": ATTEMPT_ID,
        }
    )
    return data


MODELS = (
    (JobDefinitionRecord, definition_data),
    (AttemptRecord, attempt_data),
    (InvocationRecord, invocation_data),
)


def linked_records(job_uuid=JOB_UUID, job_index=1):
    return (
        JobDefinitionRecord.model_validate(definition_data(job_uuid, job_index)),
        AttemptRecord.model_validate(attempt_data(job_uuid, job_index)),
        InvocationRecord.model_validate(invocation_data(job_uuid, job_index)),
    )


@pytest.mark.parametrize(
    "identity",
    [
        "12345678-1234-1234-8123-123456789abc",
        DEFINITION_ID,
        DEFINITION_ID.upper(),
        "custom-job",
        "../../outside",
        "/",
        "é",
        "e\u0301",
        " ",
        "\n",
        "\0",
    ],
)
def test_job_ids_are_preserved_with_safe_hash_locators(identity):
    expected = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    assert job_key(identity) == expected
    assert len(expected) == 64
    assert all(character in "0123456789abcdef" for character in expected)

    records = linked_records(identity)
    assert validate_attempt_records(*records) is None
    for record in records:
        assert record.job_uuid == identity
        assert record.job_key == expected
        assert type(record).model_validate_json(record.model_dump_json()) == record

    assert records[0].payload.path.startswith(f"jobs/{expected}/")


def test_job_keys_do_not_normalize_case_or_unicode():
    assert job_key(DEFINITION_ID) != job_key(DEFINITION_ID.upper())
    assert job_key("é") != job_key("e\u0301")


@pytest.mark.parametrize("bad", ["", None, 1, True, b"job", [], {}])
def test_job_key_rejects_empty_or_non_string_ids(bad):
    with pytest.raises(ValueError, match="nonempty string"):
        job_key(bad)


def test_job_key_rejects_unencodable_identity():
    with pytest.raises(UnicodeEncodeError):
        job_key("\ud800")


@pytest.mark.parametrize("model,factory", MODELS)
def test_models_round_trip_and_are_frozen(model, factory):
    record = model.model_validate(factory())
    assert model.model_validate_json(record.model_dump_json()) == record
    with pytest.raises(ValidationError, match="frozen"):
        record.created_at = INVOCATION_CREATED


def test_nested_references_are_detached_and_frozen():
    data = definition_data()
    record = JobDefinitionRecord.model_validate(data)
    data["payload"]["sha256"] = "f" * 64
    data["source"]["size_bytes"] = 999
    assert record.payload.sha256 == "b" * 64
    assert record.source.size_bytes == 12

    dumped = record.model_dump()
    dumped["payload"]["size_bytes"] = 999
    assert record.payload.size_bytes == 12

    with pytest.raises(ValidationError, match="frozen"):
        record.payload.size_bytes = 999


@pytest.mark.parametrize("model,factory", MODELS)
@pytest.mark.parametrize(
    "field,value",
    [
        ("schema_version", 0),
        ("schema_version", 2),
        ("schema_version", True),
        ("schema_version", "1"),
        ("kind", "other-record"),
        ("run_id", "invalid"),
        ("created_at", "invalid"),
        ("created_at", "2026-10-08T12:00:00"),
        ("created_at", "2026-10-08T14:00:00.000000+02:00"),
        ("created_at", "2026-10-08T12:00:00Z"),
        ("created_at", "2026-10-08T12:00:00.000000+00:00"),
        ("job_uuid", ""),
        ("job_uuid", None),
        ("job_uuid", 42),
        ("job_uuid", "\ud800"),
        ("job_index", 0),
        ("job_index", -1),
        ("job_index", True),
        ("job_index", "1"),
        ("job_index", 1.0),
        ("job_key", "f" * 64),
        ("job_key", "F" * 64),
        ("job_key", "short"),
        ("unexpected", True),
    ],
)
def test_invalid_common_fields(model, factory, field, value):
    data = factory()
    data[field] = value
    with pytest.raises(ValidationError):
        model.model_validate(data)


@pytest.mark.parametrize("model,factory", MODELS)
@pytest.mark.parametrize(
    "field",
    [
        "schema_version",
        "kind",
        "run_id",
        "created_at",
        "job_uuid",
        "job_index",
        "job_key",
    ],
)
def test_required_common_fields(model, factory, field):
    data = factory()
    del data[field]
    with pytest.raises(ValidationError):
        model.model_validate(data)


@pytest.mark.parametrize(
    "model,factory,field",
    [
        (JobDefinitionRecord, definition_data, "definition_id"),
        (AttemptRecord, attempt_data, "definition_id"),
        (AttemptRecord, attempt_data, "attempt_id"),
        (InvocationRecord, invocation_data, "attempt_id"),
        (InvocationRecord, invocation_data, "invocation_id"),
    ],
)
@pytest.mark.parametrize(
    "bad",
    [
        "invalid",
        DEFINITION_ID.upper(),
        "12345678-1234-1234-8123-123456789abc",
        "00000000-0000-4000-0000-000000000001",
        None,
        1,
    ],
)
def test_backend_identity_fields_require_canonical_uuid4(model, factory, field, bad):
    data = factory()
    data[field] = bad
    with pytest.raises(ValidationError):
        model.model_validate(data)


@pytest.mark.parametrize(
    "path",
    [
        "",
        "/absolute",
        ".",
        "..",
        "./file",
        "../file",
        "dir/./file",
        "dir/../file",
        "dir//file",
        "dir/",
        "dir\\file",
        "dir/\0file",
        "dir/\ud800",
    ],
)
def test_unsafe_artifact_paths(path):
    with pytest.raises(ValidationError):
        RecordArtifactReference.model_validate(reference_data(path))


@pytest.mark.parametrize("path", ["file.json", "flow/payload.json", "data/é.json"])
@pytest.mark.parametrize("size", [0, 12])
def test_artifact_references_round_trip_without_normalization(path, size):
    reference = RecordArtifactReference.model_validate(reference_data(path, size=size))
    assert reference.path == path
    assert reference.size_bytes == size
    assert (
        RecordArtifactReference.model_validate_json(reference.model_dump_json())
        == reference
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("path", None),
        ("path", 1),
        ("sha256", "A" * 64),
        ("sha256", "g" * 64),
        ("sha256", "a" * 63),
        ("sha256", None),
        ("size_bytes", -1),
        ("size_bytes", True),
        ("size_bytes", "12"),
        ("size_bytes", 12.0),
        ("unexpected", True),
    ],
)
def test_invalid_artifact_reference_fields(field, value):
    data = reference_data()
    data[field] = value
    with pytest.raises(ValidationError):
        RecordArtifactReference.model_validate(data)


@pytest.mark.parametrize("field", ["path", "sha256", "size_bytes"])
def test_artifact_reference_requires_every_field(field):
    data = reference_data()
    del data[field]
    with pytest.raises(ValidationError):
        RecordArtifactReference.model_validate(data)


@pytest.mark.parametrize(
    "path",
    [
        "job.json",
        f"jobs/{'f' * 64}/index-1/definitions/{DEFINITION_ID}/job.json",
        f"jobs/{job_key(JOB_UUID)}/index-2/definitions/{DEFINITION_ID}/job.json",
        f"jobs/{job_key(JOB_UUID)}/index-1/definitions/{OTHER_ID}/job.json",
        f"jobs/{job_key(JOB_UUID)}/index-1/definitions/{DEFINITION_ID}/other.json",
    ],
)
def test_definition_payload_path_must_match_identity(path):
    data = definition_data()
    data["payload"]["path"] = path
    with pytest.raises(ValidationError, match="payload path"):
        JobDefinitionRecord.model_validate(data)


def test_original_flow_source_path_is_fixed():
    data = definition_data()
    data["source"]["path"] = "flow/other.json"
    with pytest.raises(ValidationError, match="original_flow source"):
        JobDefinitionRecord.model_validate(data)


@pytest.mark.parametrize("origin", ["dynamic_response", "amendment"])
def test_other_origins_preserve_source_without_semantic_approval(origin):
    data = definition_data()
    data["origin"] = origin
    data["source"]["path"] = "evidence/source.json"
    record = JobDefinitionRecord.model_validate(data)
    assert record.origin == origin
    assert record.source.path == "evidence/source.json"


@pytest.mark.parametrize(
    "field,value",
    [
        ("jobflow_version", "0.3.0"),
        ("jobflow_version", None),
        ("origin", "rerun"),
        ("origin", "ORIGINAL_FLOW"),
        ("origin", None),
    ],
)
def test_unsupported_definition_fields(field, value):
    data = definition_data()
    data[field] = value
    with pytest.raises(ValidationError):
        JobDefinitionRecord.model_validate(data)


@pytest.mark.parametrize(
    "field",
    ["definition_sha256", "consumer_code_sha256", "worker_runtime_sha256"],
)
@pytest.mark.parametrize("bad", ["short", "G" * 64, None, 1])
def test_attempt_digest_fields(field, bad):
    data = attempt_data()
    data[field] = bad
    with pytest.raises(ValidationError):
        AttemptRecord.model_validate(data)


@pytest.mark.parametrize(
    "model,factory,fields",
    [
        (
            JobDefinitionRecord,
            definition_data,
            [
                "definition_id",
                "jobflow_version",
                "payload",
                "origin",
                "source",
            ],
        ),
        (
            AttemptRecord,
            attempt_data,
            [
                "attempt_id",
                "definition_id",
                "definition_sha256",
                "consumer_code_sha256",
                "worker_runtime_sha256",
            ],
        ),
        (
            InvocationRecord,
            invocation_data,
            ["invocation_id", "attempt_id"],
        ),
    ],
)
def test_required_record_specific_fields(model, factory, fields):
    for field in fields:
        data = factory()
        del data[field]
        with pytest.raises(ValidationError):
            model.model_validate(data)


@pytest.mark.parametrize("index", [1, 2, 100])
def test_linked_records_accept_positive_job_indices(index):
    assert validate_attempt_records(*linked_records(job_index=index)) is None


def test_equal_creation_times_are_permitted():
    records = tuple(
        model.model_validate({**factory(), "created_at": DEFINITION_CREATED})
        for model, factory in MODELS
    )
    assert validate_attempt_records(*records) is None


@pytest.mark.parametrize("position", [1, 2])
@pytest.mark.parametrize("change", ["run_id", "job_uuid", "job_index"])
def test_cross_record_job_identity_conflicts(position, change):
    records = list(linked_records())
    data = records[position].model_dump()
    if change == "run_id":
        data["run_id"] = OTHER_ID
    elif change == "job_uuid":
        data["job_uuid"] = "another-job"
        data["job_key"] = job_key(data["job_uuid"])
    else:
        data["job_index"] = 2
    records[position] = type(records[position]).model_validate(data)
    with pytest.raises(ValueError, match="run/job identity"):
        validate_attempt_records(*records)


@pytest.mark.parametrize(
    "position,field,value,message",
    [
        (1, "definition_id", OTHER_ID, "definition_id"),
        (1, "definition_sha256", "f" * 64, "definition_sha256"),
        (2, "attempt_id", OTHER_ID, "attempt_id"),
        (1, "created_at", "2026-10-08T11:59:59.000000Z", "attempt creation"),
        (2, "created_at", DEFINITION_CREATED, "invocation creation"),
    ],
)
def test_cross_record_link_and_time_conflicts(position, field, value, message):
    records = list(linked_records())
    data = records[position].model_dump()
    data[field] = value
    records[position] = type(records[position]).model_validate(data)
    with pytest.raises(ValueError, match=message):
        validate_attempt_records(*records)


@pytest.mark.parametrize("position", [0, 1, 2])
def test_cross_record_validator_revalidates_unsafe_model_copies(position):
    records = list(linked_records())
    records[position] = records[position].model_copy(update={"job_key": "f" * 64})
    with pytest.raises(ValidationError, match="job_key"):
        validate_attempt_records(*records)


def test_cross_record_validator_revalidates_nested_reference():
    definition, attempt, invocation = linked_records()
    unsafe_payload = definition.payload.model_copy(update={"path": "../job.json"})
    unsafe_definition = definition.model_copy(update={"payload": unsafe_payload})
    with pytest.raises(ValidationError, match="run-relative"):
        validate_attempt_records(unsafe_definition, attempt, invocation)


@pytest.mark.parametrize("field", ["payload", "source"])
def test_definition_revalidates_reference_instances(field):
    data = definition_data()
    reference = RecordArtifactReference.model_validate(data[field])
    data[field] = reference.model_copy(update={"size_bytes": -1})
    with pytest.raises(ValidationError):
        JobDefinitionRecord.model_validate(data)


def test_separate_invocations_preserve_one_attempt_without_selecting_a_result():
    definition, attempt, first = linked_records()
    second_data = invocation_data()
    second_data["invocation_id"] = OTHER_ID
    second = InvocationRecord.model_validate(second_data)

    assert validate_attempt_records(definition, attempt, first) is None
    assert validate_attempt_records(definition, attempt, second) is None
    assert first.invocation_id != second.invocation_id
    assert first.attempt_id == second.attempt_id
    assert "execution_state" not in first.model_dump()


def test_new_attempt_keeps_job_identity_and_index():
    definition, first_attempt, first_invocation = linked_records()
    new_attempt_data = attempt_data()
    new_attempt_data["attempt_id"] = OTHER_ID
    second_attempt = AttemptRecord.model_validate(new_attempt_data)

    new_invocation_data = invocation_data()
    new_invocation_data["attempt_id"] = OTHER_ID
    new_invocation_data["invocation_id"] = DEFINITION_ID
    second_invocation = InvocationRecord.model_validate(new_invocation_data)

    assert validate_attempt_records(definition, first_attempt, first_invocation) is None
    assert (
        validate_attempt_records(definition, second_attempt, second_invocation) is None
    )
    assert first_attempt.attempt_id != second_attempt.attempt_id
    assert first_attempt.job_uuid == second_attempt.job_uuid
    assert first_attempt.job_index == second_attempt.job_index

"""Canonical event/head metadata, strict JSON boundaries, and checksums."""

import hashlib
import json
import sys
from decimal import Decimal

import pytest
from pydantic import ValidationError

from jobflow_gitlab_slurm.persistence.journal.records import (
    MAX_SEQUENCE,
    EventRecord,
    JournalHead,
    canonical_payload,
    decode_event,
    decode_head,
    encode_record,
    make_event,
    make_head,
)

RUN_ID = "00000000-0000-4000-8000-000000000001"

EVENT_ID = "00000000-0000-4000-8000-000000000002"

CREATED = "2026-10-07T12:00:00.000000Z"

UPDATED = "2026-10-07T13:00:00.000000Z"


def canonical_bytes(document):
    return json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def resign(document):
    body = {key: value for key, value in document.items() if key != "sha256"}
    return {
        **body,
        "sha256": hashlib.sha256(canonical_bytes(body)).hexdigest(),
    }


def sample_event(**changes):
    values = {
        "run_id": RUN_ID,
        "event_id": EVENT_ID,
        "sequence": 1,
        "created_at": CREATED,
        "event_type": "example.note",
        "payload": {"message": "é", "items": [1, True, None]},
        "previous_sha256": None,
    }
    values.update(changes)
    return make_event(**values)


def sample_head(**changes):
    values = {
        "run_id": RUN_ID,
        "updated_at": UPDATED,
        "last_sequence": 0,
        "last_sha256": None,
    }
    values.update(changes)
    return make_head(**values)


def sample_record(kind):
    return sample_event() if kind == "event" else sample_head()


def model_for(kind):
    return EventRecord if kind == "event" else JournalHead


def decoder_for(kind):
    return decode_event if kind == "event" else decode_head


@pytest.mark.parametrize("kind", ["event", "head"])
def test_canonical_round_trip_and_independent_checksum(kind):
    record = sample_record(kind)
    body = record.model_dump(mode="json", exclude={"sha256"})
    expected = hashlib.sha256(canonical_bytes(body)).hexdigest()
    assert record.sha256 == expected
    encoded = encode_record(record)
    assert encoded == canonical_bytes(record.model_dump(mode="json"))
    assert not encoded.endswith(b"\n")
    assert decoder_for(kind)(encoded) == record
    with pytest.raises(ValidationError, match="frozen"):
        record.sha256 = "0" * 64


def test_payload_encoding_and_detachment():
    shared = {"value": [1, 2]}
    payload = {
        "z": None,
        "a": [shared, shared, {}, [], False, -3, 1.25, "é"],
    }
    event = sample_event(payload=payload)
    original = event.payload_json
    assert original == canonical_bytes(payload).decode("utf-8")
    assert "é" in original
    assert "\\u00e9" not in original

    shared["value"].append(3)
    payload["new"] = "changed"
    assert event.payload_json == original

    detached = event.payload()
    detached["a"][0]["value"].append(99)
    assert event.payload() == json.loads(original)
    assert detached != event.payload()


def test_key_order_is_normalized_but_array_order_is_preserved():
    first = {"z": [1, 2], "a": {"y": 2, "x": 1}}
    second = {"a": {"x": 1, "y": 2}, "z": [1, 2]}
    assert canonical_payload(first) == canonical_payload(second)
    assert sample_event(payload=first).sha256 == sample_event(payload=second).sha256
    assert canonical_payload({"z": [1, 2]}) != canonical_payload({"z": [2, 1]})


def test_integer_and_float_payload_identities_differ():
    assert canonical_payload({"value": 1}) != canonical_payload({"value": 1.0})
    assert (
        sample_event(payload={"value": 1}).sha256
        != sample_event(payload={"value": 1.0}).sha256
    )


def test_monty_shaped_payload_is_plain_data():
    payload = {
        "@module": "never_import_this_module",
        "@class": "NeverConstruct",
        "value": 42,
    }
    event = decode_event(encode_record(sample_event(payload=payload)))
    assert event.payload() == payload
    assert "never_import_this_module" not in sys.modules


@pytest.mark.parametrize("payload", [None, [], (), "text", 1, True])
def test_payload_root_must_be_plain_object(payload):
    with pytest.raises(ValueError, match="plain JSON object"):
        canonical_payload(payload)


@pytest.mark.parametrize("key", [1, True, None, ("tuple",)])
def test_payload_keys_must_be_strings(key):
    with pytest.raises(ValueError, match="keys must be strings"):
        canonical_payload({key: "value"})


@pytest.mark.parametrize(
    "value",
    [
        b"bytes",
        (1, 2),
        {1, 2},
        Decimal("1.25"),
        complex(1, 2),
        object(),
    ],
)
def test_non_json_python_values_are_rejected(value):
    with pytest.raises(ValueError, match="unsupported JSON value type"):
        canonical_payload({"value": value})


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_nonfinite_numbers_are_rejected(value):
    with pytest.raises(ValueError, match="finite"):
        canonical_payload({"value": value})


@pytest.mark.parametrize("kind", ["dict", "list"])
def test_cyclic_payloads_are_rejected(kind):
    if kind == "dict":
        payload = {}
        payload["self"] = payload
    else:
        value = []
        value.append(value)
        payload = {"value": value}
    with pytest.raises(ValueError, match="cyclic"):
        canonical_payload(payload)


def test_excessive_python_payload_depth_is_rejected():
    value = None
    for _ in range(sys.getrecursionlimit() + 10):
        value = [value]
    with pytest.raises(ValueError, match="nesting"):
        canonical_payload({"value": value})


def test_excessive_encoded_json_depth_is_rejected():
    depth = sys.getrecursionlimit() + 10
    data = b"[" * depth + b"0" + b"]" * depth
    with pytest.raises(ValueError, match="nesting"):
        decode_event(data)


def test_invalid_unicode_payload_is_rejected():
    with pytest.raises(UnicodeError):
        canonical_payload({"value": "\ud800"})


@pytest.mark.parametrize(
    "text",
    [
        "null",
        "[]",
        '{"x":1,"x":2}',
        '{"x":NaN}',
        '{"x":Infinity}',
        '{"x":-Infinity}',
        '{"x":1e9999}',
        '{ "x": 1 }',
        '{"x":1}\n',
        '{"value":"\\u00e9"}',
        "{",
    ],
)
def test_invalid_or_noncanonical_payload_text_is_rejected(text):
    values = sample_event().model_dump(mode="json")
    values["payload_json"] = text
    with pytest.raises(ValueError):
        EventRecord.model_validate(resign(values))


@pytest.mark.parametrize(
    "event_type",
    ["", "UpperCase", "bad space", "bad/name", ".leading", "x\n", "x;command"],
)
def test_invalid_event_type_is_rejected(event_type):
    with pytest.raises(ValueError, match="event_type"):
        sample_event(event_type=event_type)


@pytest.mark.parametrize("event_type", ["note", "job.submitted", "a-b_c.1"])
def test_valid_event_type_identifiers(event_type):
    assert sample_event(event_type=event_type).event_type == event_type


@pytest.mark.parametrize(
    ("sequence", "previous"),
    [(1, "a" * 64), (2, None)],
)
def test_event_predecessor_shape(sequence, previous):
    with pytest.raises(ValueError, match="predecessor"):
        sample_event(sequence=sequence, previous_sha256=previous)


@pytest.mark.parametrize("sequence", [2, MAX_SEQUENCE])
def test_later_event_metadata_accepts_checksum_without_proving_history(sequence):
    event = sample_event(sequence=sequence, previous_sha256="a" * 64)
    assert event.previous_sha256 == "a" * 64
    assert decode_event(encode_record(event)) == event


@pytest.mark.parametrize(
    ("sequence", "digest"),
    [(0, "a" * 64), (1, None)],
)
def test_head_tail_shape(sequence, digest):
    with pytest.raises(ValueError, match="head"):
        sample_head(last_sequence=sequence, last_sha256=digest)


@pytest.mark.parametrize("sequence", [1, MAX_SEQUENCE])
def test_positive_head_metadata_without_proving_event_exists(sequence):
    head = sample_head(last_sequence=sequence, last_sha256="b" * 64)
    assert head.last_sha256 == "b" * 64
    assert decode_head(encode_record(head)) == head


@pytest.mark.parametrize("kind", ["event", "head"])
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("schema_version", 2),
        ("schema_version", True),
        ("schema_version", "1"),
        ("schema_version", 1.0),
        ("kind", "wrong"),
        ("run_id", "not-a-uuid"),
        ("run_id", "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"),
        ("unexpected", "value"),
    ],
)
def test_common_record_fields_are_strict(kind, field, value):
    values = sample_record(kind).model_dump(mode="json")
    values[field] = value
    with pytest.raises(ValueError):
        model_for(kind).model_validate(resign(values))


@pytest.mark.parametrize(
    ("kind", "field", "value"),
    [
        ("event", "sequence", 0),
        ("event", "sequence", MAX_SEQUENCE + 1),
        ("event", "sequence", True),
        ("event", "sequence", "1"),
        ("event", "event_id", "not-a-uuid"),
        ("event", "created_at", "2026-10-07T12:00:00Z"),
        ("event", "created_at", "2026-10-07T12:00:00.000000"),
        ("event", "created_at", "2026-10-07T12:00:00.000000+01:00"),
        ("event", "event_type", 1),
        ("event", "payload_json", {}),
        ("event", "previous_sha256", "A" * 64),
        ("head", "last_sequence", -1),
        ("head", "last_sequence", MAX_SEQUENCE + 1),
        ("head", "last_sequence", True),
        ("head", "last_sequence", "0"),
        ("head", "updated_at", "invalid"),
        ("head", "last_sha256", "A" * 64),
    ],
)
def test_specific_record_fields_are_strict(kind, field, value):
    values = sample_record(kind).model_dump(mode="json")
    values[field] = value
    with pytest.raises(ValueError):
        model_for(kind).model_validate(resign(values))


@pytest.mark.parametrize("kind", ["event", "head"])
@pytest.mark.parametrize("field", ["schema_version", "kind", "run_id", "sha256"])
def test_required_common_fields(kind, field):
    values = sample_record(kind).model_dump(mode="json")
    del values[field]
    with pytest.raises(ValidationError):
        model_for(kind).model_validate(values)


@pytest.mark.parametrize(
    ("kind", "field"),
    [
        ("event", "event_id"),
        ("event", "sequence"),
        ("event", "created_at"),
        ("event", "event_type"),
        ("event", "payload_json"),
        ("event", "previous_sha256"),
        ("head", "updated_at"),
        ("head", "last_sequence"),
        ("head", "last_sha256"),
    ],
)
def test_required_specific_fields(kind, field):
    values = sample_record(kind).model_dump(mode="json")
    del values[field]
    with pytest.raises(ValidationError):
        model_for(kind).model_validate(resign(values))


@pytest.mark.parametrize("kind", ["event", "head"])
@pytest.mark.parametrize("digest", ["0" * 64, "A" * 64, "a" * 63])
def test_invalid_or_incorrect_record_checksum(kind, digest):
    values = sample_record(kind).model_dump(mode="json")
    values["sha256"] = digest
    with pytest.raises(ValueError):
        model_for(kind).model_validate(values)


@pytest.mark.parametrize("kind", ["event", "head"])
def test_changed_body_requires_new_checksum(kind):
    values = sample_record(kind).model_dump(mode="json")
    field = "created_at" if kind == "event" else "updated_at"
    values[field] = "2026-10-07T14:00:00.000000Z"
    with pytest.raises(ValueError, match="checksum mismatch"):
        model_for(kind).model_validate(values)


@pytest.mark.parametrize("kind", ["event", "head"])
def test_encoder_revalidates_unsafe_model_copy(kind):
    record = sample_record(kind)
    unsafe = record.model_copy(update={"sha256": "0" * 64})
    with pytest.raises(ValueError, match="checksum mismatch"):
        encode_record(unsafe)


@pytest.mark.parametrize("value", [None, {}, object()])
def test_encoder_requires_supported_record_model(value):
    with pytest.raises(ValueError, match="EventRecord or JournalHead"):
        encode_record(value)


@pytest.mark.parametrize("kind", ["event", "head"])
@pytest.mark.parametrize(
    "data",
    [
        b"{",
        b"\xff",
        b"[]",
        b"null",
        b"NaN",
        b'{"x":Infinity}',
        b'{"x":-Infinity}',
        b'{"x":1e9999}',
        b'{"x":1,"x":2}',
    ],
)
def test_decoder_rejects_invalid_record_json(kind, data):
    with pytest.raises(ValueError):
        decoder_for(kind)(data)


@pytest.mark.parametrize("kind", ["event", "head"])
@pytest.mark.parametrize("data", ["{}", bytearray(b"{}"), None])
def test_decoder_requires_bytes(kind, data):
    with pytest.raises(ValueError, match="must be bytes"):
        decoder_for(kind)(data)


@pytest.mark.parametrize("kind", ["event", "head"])
@pytest.mark.parametrize("style", ["pretty", "newline", "leading-space", "key-order"])
def test_decoder_rejects_noncanonical_record_bytes(kind, style):
    record = sample_record(kind)
    document = record.model_dump(mode="json")
    canonical = encode_record(record)
    if style == "pretty":
        data = json.dumps(document, indent=2, ensure_ascii=False).encode("utf-8")
    elif style == "newline":
        data = canonical + b"\n"
    elif style == "leading-space":
        data = b" " + canonical
    else:
        reversed_document = dict(reversed(tuple(document.items())))
        data = json.dumps(
            reversed_document,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    assert data != canonical
    with pytest.raises(ValueError, match="canonical event-json-v1"):
        decoder_for(kind)(data)


def test_decoder_rejects_alternate_unicode_escape_encoding():
    event = sample_event()
    data = json.dumps(
        event.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    assert data != encode_record(event)
    with pytest.raises(ValueError, match="canonical event-json-v1"):
        decode_event(data)


def test_event_and_head_discriminators_are_not_interchangeable():
    with pytest.raises(ValueError):
        decode_event(encode_record(sample_head()))
    with pytest.raises(ValueError):
        decode_head(encode_record(sample_event()))

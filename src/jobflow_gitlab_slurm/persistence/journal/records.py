"""Strict event/head metadata and canonical encoding; no journal I/O."""

import hashlib
import json
import math
import re
from typing import Any, Literal, Self, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from jobflow_gitlab_slurm.config.request import Digest
from jobflow_gitlab_slurm.persistence.runs.records import RunId, UtcTimestamp

MAX_SEQUENCE = 999999999999


def _validate_json(value: object, ancestors: set[int]) -> None:
    if value is None or type(value) in {str, bool, int}:
        return

    if type(value) is float:
        if not math.isfinite(cast(float, value)):
            raise ValueError("JSON numbers must be finite")
        return

    if type(value) not in {dict, list}:
        raise ValueError(f"unsupported JSON value type: {type(value).__name__}")

    identity = id(value)
    if identity in ancestors:
        raise ValueError("cyclic JSON payload")
    ancestors.add(identity)
    try:
        if isinstance(value, dict):
            if any(type(key) is not str for key in value):
                raise ValueError("JSON object keys must be strings")
            for item in value.values():
                _validate_json(item, ancestors)
        else:
            for item in cast(list[object], value):
                _validate_json(item, ancestors)
    finally:
        ancestors.remove(identity)


def _canonical_bytes(document: object) -> bytes:
    try:
        _validate_json(document, set())
        return json.dumps(
            document,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except RecursionError as error:
        raise ValueError("JSON nesting exceeds supported depth") from error


def canonical_payload(payload: object) -> str:
    """Detach a plain JSON-object payload into canonical immutable text."""
    if type(payload) is not dict:
        raise ValueError("event payload must be a plain JSON object")
    return _canonical_bytes(payload).decode("utf-8")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValueError(f"nonfinite JSON constant: {value}")


def _parse_json(data: bytes) -> Any:
    if type(data) is not bytes:
        raise ValueError("encoded record input must be bytes")
    try:
        document = json.loads(
            data.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
        _validate_json(document, set())
    except RecursionError as error:
        raise ValueError("JSON nesting exceeds supported depth") from error
    return document


def _checked_payload_text(value: str) -> str:
    payload = _parse_json(value.encode("utf-8"))
    if canonical_payload(payload) != value:
        raise ValueError("payload_json must use canonical event-json-v1 encoding")
    return value


def _checksum(body: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_bytes(body)).hexdigest()


class _ChecksummedRecord(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        strict=True,
        frozen=True,
        revalidate_instances="always",
    )

    schema_version: int
    run_id: RunId
    sha256: Digest

    @field_validator("schema_version")
    @classmethod
    def check_version(cls, value: int) -> int:
        if value != 1:
            raise ValueError("unsupported event/head schema_version")
        return value

    @model_validator(mode="after")
    def check_checksum(self) -> Self:
        body = self.model_dump(mode="json", exclude={"sha256"})
        if _checksum(body) != self.sha256:
            raise ValueError("record checksum mismatch")
        return self


class EventRecord(_ChecksummedRecord):
    """Immutable event metadata, not evidence that its head was committed."""

    kind: Literal["run-event"]
    event_id: RunId
    sequence: int = Field(ge=1, le=MAX_SEQUENCE)
    created_at: UtcTimestamp
    event_type: str
    payload_json: str
    previous_sha256: Digest | None

    @field_validator("event_type")
    @classmethod
    def check_event_type(cls, value: str) -> str:
        if re.fullmatch(r"[a-z][a-z0-9_.-]*", value) is None:
            raise ValueError("invalid event_type identifier")
        return value

    @field_validator("payload_json")
    @classmethod
    def check_payload_json(cls, value: str) -> str:
        return _checked_payload_text(value)

    @model_validator(mode="after")
    def check_predecessor_shape(self) -> Self:
        if (self.sequence == 1) != (self.previous_sha256 is None):
            raise ValueError(
                "sequence 1 requires null previous_sha256; "
                "later sequences require a predecessor checksum"
            )
        return self

    def payload(self) -> dict[str, object]:
        """Return detached data, without importing or constructing Python objects."""
        text = _checked_payload_text(self.payload_json)
        return cast(dict[str, object], _parse_json(text.encode("utf-8")))


class JournalHead(_ChecksummedRecord):
    """Validated anchor metadata; this model does not advance an on-disk head."""

    kind: Literal["journal-head"]
    updated_at: UtcTimestamp
    last_sequence: int = Field(ge=0, le=MAX_SEQUENCE)
    last_sha256: Digest | None

    @model_validator(mode="after")
    def check_tail_shape(self) -> Self:
        if (self.last_sequence == 0) != (self.last_sha256 is None):
            raise ValueError(
                "zero head requires null last_sha256; "
                "positive heads require an event checksum"
            )
        return self


def make_event(
    *,
    run_id: str,
    event_id: str,
    sequence: int,
    created_at: str,
    event_type: str,
    payload: dict[str, object],
    previous_sha256: str | None,
) -> EventRecord:
    """Build metadata with caller-supplied identity/time and computed checksum.

    The future journal layer assigns time and sequence. This factory does not
    verify that a predecessor exists or that a matching head has been committed.
    """
    body = {
        "schema_version": 1,
        "kind": "run-event",
        "run_id": run_id,
        "event_id": event_id,
        "sequence": sequence,
        "created_at": created_at,
        "event_type": event_type,
        "payload_json": canonical_payload(payload),
        "previous_sha256": previous_sha256,
    }
    return EventRecord.model_validate({**body, "sha256": _checksum(body)})


def make_head(
    *,
    run_id: str,
    updated_at: str,
    last_sequence: int,
    last_sha256: str | None,
) -> JournalHead:
    """Build anchor metadata; no timestamp generation or filesystem mutation."""
    body = {
        "schema_version": 1,
        "kind": "journal-head",
        "run_id": run_id,
        "updated_at": updated_at,
        "last_sequence": last_sequence,
        "last_sha256": last_sha256,
    }
    return JournalHead.model_validate({**body, "sha256": _checksum(body)})


def encode_record(record: EventRecord | JournalHead) -> bytes:
    """Revalidate and encode an event/head using canonical event-json-v1."""
    if type(record) not in {EventRecord, JournalHead}:
        raise ValueError("expected an EventRecord or JournalHead")
    validated = type(record).model_validate(record.model_dump(mode="json"))
    return _canonical_bytes(validated.model_dump(mode="json"))


def _decode_record[T: _ChecksummedRecord](
    data: bytes,
    model: type[T],
) -> T:
    document = _parse_json(data)
    record = model.model_validate(document)
    if _canonical_bytes(record.model_dump(mode="json")) != data:
        raise ValueError("record bytes must use canonical event-json-v1 encoding")
    return record


def decode_event(data: bytes) -> EventRecord:
    """Validate canonical event bytes without importing workflow code."""
    return _decode_record(data, EventRecord)


def decode_head(data: bytes) -> JournalHead:
    """Validate canonical head bytes without checking journal history."""
    return _decode_record(data, JournalHead)

# Event journal contract v1

Status (2026-10-08): **journal contract accepted; models, read-only replay,
append, and explicit matching-tail recovery installed and locally user-verified**.
The user reports all requested local gates pass, including 100% combined
statement/branch coverage and packaging. Successful final test counts were not
supplied; these successful reruns were not independently repeated by the
assistant. No [002-durable-run-state](../002-durable-run-state.md) remote CI or
live filesystem result is recorded.
This is a persistence foundation, not a scheduler, budget/amendment interface,
or scientific-state projection.

## Operations and ownership

The public operations are `read_events` and `append_event`. Both
operate on an existing run through its existing persistent per-run lock.
They do not allocate workspaces, replace run identities, modify the original
request/Flow, import workflow code, or contact Slurm. Concurrent access raises
the existing `RunBusyError`; there is no implicit wait/retry loop.

The user approved the contention-handling boundary: the storage/journal primitive
tries once, while the future backend controller/reconciler catches contention,
reports that run as deferred, and continues with other runs. A later scheduled
invocation retries the same run/operation identities. This is the backend
manager's responsibility, not jobflow core's. Busy is distinct from integrity
failure (hold for inspection) and uncertain publication (explicit same-identity
reconciliation). The controller behavior itself remains unimplemented.

The first user-applied implementation slice provides `event_records.py` and
`test_event_records.py`: strict event/head models, canonical JSON validation,
checksums, detached payload access, and encode/decode helpers. These files are
installed and locally user-verified, including the isolated wheel import check.
The final test count was not supplied. This slice generates no timestamps, writes no
files, acquires no locks, and neither appends events nor advances anchors.
The installed journal layer assigns timestamps and sequences and enforces the
cross-record publication/recovery protocol below.

Read-only journal replay is implemented in `journal.py` with `test_journal.py`
and a wheel import entry. Its local verification gates are user-reported
passing. The reader returns an immutable snapshot
of committed events and the head under `locked_run`; busy, integrity, and
incomplete-publication outcomes stay distinct. It does not initialize, append,
flush, repair, or truncate a journal. A returned snapshot
does not retain the lock or establish a cross-host durability guarantee.

Append and explicit same-identity recovery were approved on 2026-10-07 and
are installed in `journal.py`, with tests in `test_journal_append.py`.
The user reports all local verification gates pass.
The implementation keeps validation/lock errors distinct
from event-ID conflicts and publication failures. Publication diagnostics retain
phase, intended sequence, staging/published paths, operation, and conservative
uncertainty. Committed retries reflush without rewriting the head; only an exact
matching pending-tail retry can advance it. No CLI, scheduler, budget, amendment,
or scientific execution changes are included in the append/recovery slice.

For append, the caller supplies and retains one lowercase UUIDv4 `event_id`,
an `event_type`, and a JSON-object payload. The target `run_id` and runs root
are explicit API arguments. Generate the event ID once per intended operation,
not once per pipeline retry. The backend supplies sequence number, creation
timestamp, predecessor hash, and record checksum only for a new event.

Payloads are ordinary JSON data: string-keyed objects, arrays, strings,
integers, finite floats, booleans, and null. Reject Python objects, non-string
keys, nonfinite values, and custom/Monty decoding. Decimal quantities use
strings; later domain validators enforce their specific meaning. Payloads must
not carry credentials, licensed data, or scientific output blobs.

The journal validates transport structure, not event-domain meaning.
`event_type` uses `^[a-z][a-z0-9_.-]*$`. A future state reducer must separately
recognize and validate applicable event types; an unfamiliar type must not be
treated as permission to schedule, amend, or spend compute time.

## Record fields

Each event record is a strict, frozen version-one model. Unknown fields
and unsupported schemas are rejected; integer fields do not accept booleans.

| Field | Contract |
| --- | --- |
| `schema_version` | Required integer `1`. |
| `kind` | Required literal `run-event`. |
| `run_id` | Required lowercase UUIDv4; must match the opened run. |
| `event_id` | Required caller-retained lowercase UUIDv4; unique within a run. |
| `sequence` | Integer from `1` through `999999999999`; ordering is by sequence, not clock time. |
| `created_at` | Backend-generated UTC `YYYY-MM-DDTHH:MM:SS.ffffffZ`; retained unchanged on retries. |
| `event_type` | Identifier described above. |
| `payload_json` | Immutable string containing canonical JSON-object text; accessors return detached decoded data. |
| `previous_sha256` | Required null for sequence 1; otherwise the predecessor's checksum. |
| `sha256` | Lowercase SHA-256 of canonical record-body bytes, excluding this field. |

Using a canonical string for payload storage avoids mutable nested aliases,
as with the existing site snapshot. Dictionary key insertion order is ignored;
array order and JSON number representation are preserved. For example `1` and
`1.0` remain different payload identities. Payload validation/encoding detaches
the input before publication; later caller mutation cannot alter stored bytes.

The proposed encoding, `event-json-v1`, uses UTF-8 bytes from:

```python
json.dumps(
    document,
    sort_keys=True,
    separators=(",", ":"),
    ensure_ascii=False,
    allow_nan=False,
).encode("utf-8")
```

Use this encoding for payload text, the record body excluding `sha256`, and
the complete record including `sha256`. No trailing newline is
added. Published bytes must match the canonical complete-record encoding;
duplicate keys, invalid UTF-8, and nonfinite JSON constants are rejected.
This is a package-specific encoding, not an RFC canonical-JSON claim.

## Layout and publication

```text
<run-root>/
  journal-head.json                mutable tail anchor, outside events/
  .staging-journal-head-<random>    interrupted anchor-write evidence
  events/
    000000000001.json
    000000000002.json
    .staging-<event-id>-<random>.json   unpublished creation evidence
```

Read-only replay creates nothing. A run with neither a head nor an events
directory has no initialized journal and replays as empty. A valid zero head
with no events directory or an empty events directory also replays as empty.
An existing events directory without a valid head is an integrity error, even
if empty: do not silently derive or regenerate a missing anchor from the files.

Append initializes a zero head and flushes it before creating the private
events directory, under the existing run lock. This supports existing runs
without changing their manifest. Symlinked or non-directory journal paths and
unsafe anchor/event file types are invalid. Reject unexpected entries within
events/. Ignore only its reserved `.staging-*` namespace; those files are
evidence, not journal entries. Filename and sequence must agree.

### Anchor fields

The strict version-one head model carries `schema_version: 1`, kind
`journal-head`, matching `run_id`, canonical UTC `updated_at`, `last_sequence`,
`last_sha256`, and its own `sha256`. `last_sequence` is an integer from zero
through `999999999999`; zero requires null `last_sha256`, while a positive
sequence requires a lowercase event checksum. Hash the canonical head body
excluding its own `sha256`, and store canonical complete head bytes using the
same encoding rules as event records. The head checksum detects accidental
modification, not deliberate rewriting. The head is a mutable commit anchor,
not an additional append-only event.

Before append or replay, validate the complete visible history under the lock:
contiguous sequence numbers beginning at 1, unique event IDs, matching run
identity, valid self-checksums, and the predecessor checksum chain. A gap,
conflict, corrupt record, or unsupported schema blocks append; do not skip,
truncate, rewrite, or reconstruct history automatically. Also validate the
head checksum, run identity, and the event it anchors. A head ahead of visible
history, a missing anchored event, or a different checksum is an integrity
failure. Loss of an events directory with a positive head is not an empty run.

### Append ordering and acknowledgment

1. Validate journal and anchor under the run lock; check retry/conflict rules.
2. Write a new event to an exclusive private staging file, validate and flush
   it, then rename it to the next sequence filename on the same filesystem.
   Refuse an existing event destination; never replace an event.
3. Flush the events directory so the event is persisted before advancing head.
4. Write and flush a complete replacement head through an exclusive staging
   file in the run root. Atomically replace `journal-head.json` and flush the
   run directory. An ordinary append advances the head by exactly one event.
5. Acknowledge success only after these flushes succeed.

All writers
must cooperate through the existing run lock in a trusted root. Refuse an
existing event destination; replacement is allowed only for the mutable head.
As with run creation,
this is not an adversarial filesystem sandbox and live filesystem capabilities
remain an independent site gate.

This revises the earlier unanchored proposal: an event file's rename makes it
visible but not yet committed for replay. The logical commit point is the
atomic head replacement that anchors it; durable acknowledgment additionally
requires the directory flush. Two files are not one filesystem transaction,
so intermediate states are handled explicitly below. Successful renames consume
their own staging names; failed/interrupted staging is retained without
automatic cleanup. No other mutable index is authoritative.

### Replay and intermediate states

| Observed state | Required behavior |
| --- | --- |
| Full valid chain ends exactly at the valid head | Replay the committed events. |
| Exactly one valid event follows the anchored prefix | Raise an incomplete-publication diagnostic; do not silently ignore it or expose it as committed. Only an explicit matching append retry may finalize the head. |
| More than one event follows the head | Hold as an integrity/recovery problem; normal one-at-a-time append cannot produce this state. No automatic adoption. |
| Head is missing/invalid with an existing events directory, ahead of history, or disagrees with an anchored event | Hold as an integrity error; preserve evidence. |

Replay is fail-closed while publication is incomplete. It must not return a
seemingly ready prefix that could let a controller proceed around the pending
decision. The diagnostic identifies the anchor, committed prefix length,
pending event identity/path when available, and the required explicit action.
Journal-aware default inspect/list reporting is installed and locally
user-verified. The extension checks metadata and journal
under one lock, exposes separate journal findings without event payloads,
and uses nonzero exits for incomplete/invalid journals. No independent
assistant rerun, remote CI, or live filesystem result is claimed for this
reporting slice. A future controller
must still reconcile applicable domain state before scheduling.

## Retry and uncertain publication

For a fully committed valid journal, an existing `event_id` with the same event type and
canonical payload returns the original event. Compare semantic input only:
the original sequence, timestamp, predecessor hash, and checksum remain intact.
The event may occur anywhere in history, not only at the tail. Reusing the
same ID with different input raises a conflict; no new record is appended.

If exactly one valid unanchored tail exists, an explicit append retry whose
event ID/type/canonical payload matches that tail may reflush its existing
record and events directory, publish a head anchoring it, and flush the run
directory. It returns the original event without changing its timestamp,
sequence, checksum, or payload. Different input/identity cannot adopt the tail;
new appends and retries of older events remain held until this is resolved.

For a repeated append of an already committed matching event, reflush that
record, the current head, and containing directories before acknowledging
success. Do not rewind or rewrite the head to an older retried event. Read-only
replay does not perform this durability acknowledgment.

A publication failure reports run ID, event ID, intended sequence when known,
event/anchor staging and published paths, publication phase, original cause,
and whether publication is uncertain. At/after a publication boundary,
inspect/replay and explicitly retry with the same
event ID and input. Do not infer failure means the event is absent. Failure
before publication preserves evidence too; staging never participates in replay.
No outcome authorizes a scientific rerun or a new event identity automatically.

## Integrity limits and acceptance checks

Checksums detect accidental record modification and broken internal links;
they are not signatures or protection against a writer able to rewrite history.
The run-local head detects removal of anchored tail events while the head
survives. It does not detect coordinated rollback/deletion of both history and
head, or loss of the whole run workspace. An external witness is a later
integration feature; the head must not be described as independent storage.
The anchor detects missing data but does not recreate it. No crash-durability or cross-host
guarantee is claimed until the actual filesystem passes its live gates.

Acceptance tests must cover:

- Empty replay of an existing run without filesystem writes.
- Exact canonical bytes/checksums, detached payloads, strict schemas, and
  rejection of unsupported JSON values and unsafe files/paths.
- Ordered cross-process replay; original request, Flow bytes, and IDs unchanged.
- Idempotent retries, including an older event after later appends, and explicit
  conflict rejection when input changes under the same ID.
- Gaps, duplicate IDs, foreign-run records, filename mismatches, corrupt hashes,
  and predecessor mismatch blocking reads/appends without modifying evidence.
- Strict head fields and checksum, missing/mismatching/ahead anchors, anchored
  suffix deletion, and a deleted events directory with a positive head.
- Exactly one unanchored event holding replay/new appends until a matching
  explicit retry; incompatible input or multiple unanchored events fail closed.
- Older committed-event retries never rewinding the head; read-only replay
  neither initializing nor repairing a head.
- Separate-process lock contention and concurrent-writer exclusion.
- Injected writes/flush/rename failures and abrupt process exit before/after
  event publication and head replacement; retry acknowledges the same event
  without duplication. Test every intermediate state and final flush failure.
- Local lint/formatting, 100% statement/branch coverage, build, and isolated
  wheel imports. See the progress record for actual results and remaining gates.

CLI event editing, typed domain transitions, budget top-ups, amendments, worker
result publication, Slurm intent/receipt handling, scheduling, and active-run
projections are deliberately deferred.

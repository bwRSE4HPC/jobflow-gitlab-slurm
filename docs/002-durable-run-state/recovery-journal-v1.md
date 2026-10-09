# Recovery-to-journal association v1

Status (2026-10-08): **bounded slice installed; local acceptance gate
user-reported passing**.
Explicit bundle recovery is installed with its local gate user-reported passing.
Read-only inspection confirms the association module, tests, and wheel import
are installed. This slice belongs to
[002-durable-run-state](../002-durable-run-state.md).

## Purpose and boundary

Associate an existing exact recovery request and completion receipt with the
anchored run journal. Filesystem completion and journal publication are separate
transactions. A missing journal event is pending registration, not evidence that
the calculation or recovery did not happen.

Start with recovery registration: its request already retains `journal_event_id`.
Ordinary publication has no separately named event-identity field in its current
intent schema. The [next approved slice](publication-journal-v1.md) reserves the existing
immutable intent ID as its sole completion-event identity, without changing that
schema. Its installation/gates remain pending; never generate a fresh retry event ID.

## Approved files and interface

- New `src/jobflow_gitlab_slurm/persistence/recovery/registration.py`.
- New `tests/persistence/recovery/`.
- One import addition in `ci/check-wheel.sh` and documentation updates.

Approved API:

```python
register_owned_recovery_event(ownership, request, *, receipt) -> EventRecord
```

Require active originating-process invocation ownership. Caller records retain
the exact IDs, bytes, timestamps, and actor metadata used by recovery. Generate
no new recovery/event identity. Do not call the bundle recovery operation or
ordinary publisher. Do not execute jobflow, parse scientific results, contact
Slurm, select results, release descendants, or amend definitions.

## Approved event-domain payload

Event type: `bundle.recovery_completed`. Use the request's `journal_event_id`.
The strict frozen `RecoveryCompletedPayload` has literal kind
`bundle-recovery-completed`; references are named `intent`, `request`, `receipt`,
`bundle_manifest`, and `bundle_commit`. It is domain metadata, not a new event/head
storage format or state reducer.
A strict version-one JSON-object payload binds:

- Qualified run/job/index/attempt/invocation identity and `recovery_id`.
- Exact run-relative path, SHA-256, and size references to retained intent,
  recovery request, recovery receipt, published manifest, and commit marker.
- Literal filesystem outcome `committed_bundle_verified`.

Construct payload only from revalidated exact records and storage evidence.
Keep actor provenance in the referenced immutable request rather than copying
its body. Store no scientific blobs, credentials, environment dumps, hostnames,
or controller-specific configuration. Version and reject unknown fields at the
domain boundary without changing the general event/head schema.
The payload model checks structure, UUID syntax, job-key binding, and qualified
run-relative paths. Those paths do not identify the enclosing run independently;
registration validates run identity against the owned invocation and retained
parent records before journal mutation. Standalone model validity does not
establish that contextual binding.

This event asserts verified filesystem recovery completion and audit association,
not scheduler success, scientific validity, or permission to continue a flow.
The later state reducer must independently validate applicable execution policy.

## Ordering and locks

1. Require active invocation ownership and validate caller records and their
   complete parent/intent binding before any journal mutation.
2. Require the exact existing final request and receipt. Missing final evidence
   is a hold; registration must not manufacture a receipt or finish recovery.
3. Validate the deterministic typed payload before acknowledgment. Revalidate
   and durably acknowledge the exact existing receipt through installed
   receipt persistence, including its full bundle/request byte checks. Perform
   this potentially large payload I/O outside the run lock, under invocation
   ownership. This operation must not create a marker or repair payloads.
4. Serialize that validated payload and invoke existing
   `append_event` with the retained event ID. It acquires the run lock briefly
   while invocation ownership remains active; preserve invocation-before-run
   lock order and never reacquire the invocation lock.
5. Return the original/new event only after existing journal durability
   acknowledgment succeeds. No independent association index is authoritative.

Concurrent run-lock contention remains fail-fast. The future manager defers and
retries the same operation; no implicit wait or retry loop belongs in this API.
Writer quiescence and trusted-root assumptions remain inherited prerequisites,
not conclusions established by actor metadata or lock possession.

## Restart and failure behavior

| Evidence | Required behavior |
| --- | --- |
| Valid exact final audit and committed bundle; event absent | Register the retained event ID without repeating recovery or calculation. |
| Same event ID/type/payload already anchored, even before later events | Reacknowledge and return the original record; no duplicate or head rewind. |
| Exactly matching unanchored event tail | Explicitly finalize that tail through existing matching append retry. |
| Unrelated pending tail, corrupt journal, or conflicting event input | Hold; preserve audit, bundle, event, and anchor evidence. |
| Missing/corrupt/conflicting audit or bundle evidence, even if event exists | Hold; an old event does not substitute for present byte validation. |
| Journal failure after filesystem acknowledgment | Report registration as uncertain where appropriate; preserve the same event identity for inspection/retry. |

Provide sanitized detached diagnostics identifying run/job/attempt/invocation/
recovery/event, relevant paths, phase, available errno, and separate filesystem
acknowledgment and journal-registration uncertainty. Preserve busy and integrity
distinctions. Do not expose consumer record bodies or raw chained causes through
the new API's public report. No error authorizes a scientific rerun or fresh ID.
Acknowledgment and uncertainty fields describe this call; false does not prove
that an earlier registration or filesystem mutation is absent. Run-lock busy and
inactive/inherited ownership retain their existing exception classifications.

## Acceptance and subsequent work

Verify strict domain payload fields and deterministic references; full exact
binding; inactive/inherited ownership; missing/conflicting/unsafe evidence;
acknowledgment-before-append ordering; committed and pending-tail retries;
older-event retries; conflicting/unrelated journal holds; and cross-process
contention and abrupt termination around event/head publication. Assert that
bundle/audit bytes, identities, and timestamps remain unchanged and no payload
producer or scientific code is called. Require full local formatting/lint,
focused/full tests, 100% combined coverage, build, and isolated-wheel gates.

Ordinary-publication association and lifecycle tests are now locally accepted
on user-reported evidence. [002-durable-run-state](../002-durable-run-state.md)
remote CI and Stage 2 close-out remain pending. Live cross-host durability and
scheduler/writer exclusion remain independent deployment gates.

Source/tests and the wheel import are installed through user application.
On 2026-10-08, the user reports verification passes, completing this slice's
local acceptance gate. This report applies to the supplied quality, focused/full
test, coverage, and packaging sequence. No successful final test count was
supplied, and no independent assistant rerun of those final checks is claimed.
Ordinary publication, CLI, retained-audit schemas, dependency manifests, and
journal APIs remain unchanged. Remote CI and live verification remain pending.

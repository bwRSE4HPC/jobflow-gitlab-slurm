# Ordinary-publication-to-journal association v1

Status (2026-10-08): **installed; local verification gate user-reported passing**.
No successful final test count or command transcript was supplied; no independent
assistant rerun is claimed. Cycle-wide close-out and remote/live gates remain pending.
This bounded persistence slice follows locally accepted
[recovery registration](recovery-journal-v1.md) within
[002-durable-run-state](../002-durable-run-state.md).
[Progress](../progress.md) owns current acceptance evidence.

## Purpose and identity decision

Associate an exact existing publication intent and committed opaque bundle with
the run journal. A missing event means pending registration, not permission to
repeat a calculation or reconstruct missing results.

Approved event type: `bundle.publication_completed`.
Reserve the durable `PublicationIntentRecord.intent_id` as this event's
`event_id`. One immutable intent describes one expected commit; therefore it
has exactly one publication-completion event. This is an explicit protocol
mapping, not a new random ID allocated during registration.

The ID already satisfies the journal's lowercase UUIDv4 contract. Reusing it
avoids altering existing intent/event schemas, adding another persistence
transaction, or retroactively rewriting stored expectations. Other event
producers must not use that intent ID for another event. A conflicting journal
event stays held; do not escape conflict with a fresh ID.

Alternatives: a separate durable registration record adds another crash boundary;
an explicit new intent field requires a version/migration policy. Neither is
required for this one-completion-event-per-intent mapping. UUIDv5 derivation
would not satisfy the existing UUIDv4 event contract.

## Approved files and interface

- New `src/jobflow_gitlab_slurm/persistence/publication/registration.py`.
- New `tests/persistence/publication/`.
- One import in `ci/check-wheel.sh`, plus documentation updates.

Approved API:

```python
register_owned_publication_event(ownership, intent) -> EventRecord
```

Include a strict frozen `PublicationCompletedPayload` with version 1, literal
kind `bundle-publication-completed`, qualified run/job/index/attempt/invocation
identity, `intent_id`, and exact run-relative path/digest/size references named
`intent`, `bundle_manifest`, and `bundle_commit`.
Use literal result `committed_bundle_verified`.
Check contextual run/parent binding at registration, not merely UUID syntax
in the standalone payload model.

No dependency, CLI, existing publisher, retained record, or general journal
schema changes are included. Reuse existing package-local primitives without a
general persistence refactor.

## Preconditions, ordering, and locks

1. Require active originating-process invocation ownership. Revalidate the
   supplied intent and complete definition/attempt/invocation binding.
2. Require an already existing final canonical intent identical to that supplied.
   Missing intent is a hold; never create it retroactively during registration.
3. Require an already committed, valid bundle identical to the expected
   manifest and marker. Reject staging-only, unmarked, missing, invalid,
   conflicting, or ambiguous evidence before invoking an acknowledgment path.
4. Re-establish durability of the existing intent and committed bundle through
   exact acknowledgment only. No new payload, marker, intent, or recovery receipt
   may be created. The installed publisher can also publish staging, so a
   committed-only preflight is mandatory before reusing its acknowledgment branch.
   This relies on continued invocation ownership and cooperating writers.
5. Construct/revalidate deterministic domain metadata and append using
   `event_id=intent.intent_id`. Journal locking is brief and follows the existing
   invocation-before-run order. Large payload I/O remains outside the run lock.
6. Return only after journal durability acknowledgment succeeds.

The event asserts current committed filesystem evidence under a retained
expectation, not which process first completed publication. It is not a
recovery audit receipt and must not substitute for recovery request/receipt/event
requirements. A bundle completed through recovery may also have this event;
the separate recovery audit is preserved and no recovery completion is inferred
or manufactured. Future reconciliation must check all applicable evidence.

No scientific decoding/execution, payload production, recovery action,
scheduler query, result selection, descendant release, or automatic retry loop
belongs to this API.

## Restart and diagnostics

Exact existing anchored events are durably reacknowledged, including after later
events. An exactly matching unanchored tail uses the installed append retry.
Unrelated tails, event-ID/type/payload conflicts, corrupt history, and unavailable
or changed filesystem evidence remain held without overwrite or cleanup.

Report sanitized qualified identities, intent/event ID, relevant paths,
phase/errno, and separate intent/bundle acknowledgment and journal-registration
uncertainty. Preserve existing busy and inactive/inherited ownership exceptions.
Hints require evidence preservation and same-identity inspection/retry;
uncertainty never authorizes a scientific rerun or replacement intent.

## Acceptance checks and remaining boundaries

Test strict payload validation and contextual binding; stable intent-to-event
identity; missing/changed/unsafe intent or bundle evidence; committed-only
preflight; acknowledgment-before-append ordering; intent/bundle flush faults;
same-event and older-event retries; matching pending tails; conflicts/unrelated
tails; separate-process contention and abrupt termination across event/head
publication; and coexistence with retained recovery audits.

Prove no invocation ever enters staging publication or recovery, and that
bundle/audit bytes, IDs, timestamps, and lock inodes are preserved. Pass existing
formatting/lint, syntax/CLI, full tests, agreed 100% combined coverage, build,
isolated-wheel verification, and whitespace gates after user application.

Whole-lifecycle tests are installed with their local gate user-reported passing;
the approved producer-staging boundary keeps preparation/enforcement in Stage 3.
Remote CI, cross-host durability, scheduler/writer
exclusion, workers, and jobflow semantics are separate gates.

Approval authorizes supplying source/tests and verification commands for user
application, not assistant source/test/CI edits or verification execution.

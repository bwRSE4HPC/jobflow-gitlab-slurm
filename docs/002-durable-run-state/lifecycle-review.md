# Persistence lifecycle review

Status (2026-10-09): **test-only slice installed; producer-staging boundary
approved; assistant-executed full local gate passed**, including all 12 lifecycle
cases within the 2,324-test suite. See the
[cycle review record](review-and-verification.md) for commands and coverage.
Cycle-wide close-out and remote/live gates remain pending.
The [cycle overview](../002-durable-run-state.md) owns scope and acceptance;
[progress](../progress.md) owns the checkpoint.

## Inspected evidence

The existing definition and attempt/invocation tests construct runs through the
installed storage APIs and exercise separate-process reopening, contention,
publication faults, and abrupt exit. Bundle tests construct the parent chain
through those APIs, then prepare inert staging directly. Publication-journal
tests start with a retained intent and an already committed bundle; their
separate-process interruption cases span acknowledgment and event/head writes.
Recovery-journal tests start with completed recovery evidence and exercise
registration restart separately. Coexistence of publication and recovery events
is tested.

The initial review identified substantial component and partial-composition
checks but no dedicated empty-root lifecycle scenario. The installed
`tests/integration/test_persistence_lifecycle.py` now supplies that cross-process composition
coverage: metadata hand-offs, ordinary publication, explicit recovery, journal
registration, and holds for changed/missing evidence. This behavioral coverage
is distinct from statement/branch coverage. Initial acceptance was user-reported;
the 2026-10-09 assistant full-suite rerun now verifies these installed cases too.

## Approved producer-staging boundary

Keep payload preparation in Stage 3: it requires the worker's jobflow document,
Response, file, and additional-store semantics. Stage 2 continues to consume
complete opaque staging. Do not add a production staging builder or orchestration
API solely to close this cycle.

The worker integration must own the invocation while preparing payloads,
constructing exact inventories/expected records, durably acknowledging the
retained intent, publishing the bundle, and registering completion. Failed or
uncertain intent acknowledgment must prevent publication. Preparation of bytes
does not authorize execution or establish scientific completion.

The installed low-level publisher does not itself require an intent. Its
composition with intent persistence is a future producer responsibility, not
an already enforced property. A lifecycle test can demonstrate correct
composition but cannot enforce it for all callers.

Interrupted incomplete staging remains held: Stage 2 neither regenerates
missing results nor relaunches calculations. Stage 3 must distinguish any
approved parse-only reconstruction from scientific re-execution; Stage 4 must
establish scheduler/writer exclusion before controller recovery. Missing
completion events alone never authorize a rerun.

## Approved bounded test slice

Affected code: new `tests/integration/test_persistence_lifecycle.py` only. A test-local
fixture/helper may live in that file; avoid a suite-wide fixture refactor.
Update documentation as the slice progresses. No source, schema, CLI,
dependency, lockfile, wheel-check, or CI changes are proposed.

Use inert opaque bytes and caller-retained records/IDs/timestamps. Bootstrap
from an empty trusted POSIX root. Build/reopen the run, original-Flow definition,
attempt, invocation, and ownership through installed APIs. Start fresh processes
at selected hand-offs, passing paths and retained inputs rather than live
handles. Explicit test preparation of staging/opaque scheduler-receipt bytes is
not a worker or Slurm implementation.

| Scenario | Required observation |
| --- | --- |
| Metadata hand-offs | Fresh processes reopen exact run/definition/attempt/invocation identities and provenance; original payload bytes remain unchanged. |
| Ordinary publication | Persist/acknowledge intent before publishing prepared staging; register exactly one event using the retained intent ID. |
| Interrupted intact staging or published-unmarked bundle | Ordinary registration holds; an explicitly supplied same-identity recovery request/receipt completes only the permitted forward steps. |
| Committed bundle without completion event, or matching unanchored tail | Resume registration without payload production, calculation execution, or new IDs; retain exact existing event bytes where present. |
| Recovery receipt before journal registration | Reopen retained audit evidence, register its stable event, and preserve separate ordinary-publication provenance on subsequent registration. |
| Changed or missing retained evidence | Report the appropriate hold without replacing metadata, adopting new expectations, or discarding failure evidence. |

Assert exact bytes, digests, identities, timestamps, persistent lock inodes,
inventory validity, and anchored event uniqueness/order. Preserve abandoned
temporary evidence. Bound subprocess waits and surface sanitized failure
diagnostics. Reuse existing codecs/APIs; use fault injection only where an
otherwise inaccessible publication hand-off must be exposed.

## Acceptance and limits

Pass focused lifecycle tests and the existing full local quality, agreed 100%
combined coverage, CLI/syntax, packaging, and whitespace gates. The original
proposal did not authorize assistant execution; the user's subsequent cycle
review/verification request did, and the 2026-10-09 local gate passed.

These tests exercise local process restart and API composition, not power loss,
cross-host filesystem behavior, scheduler success, authorization, jobflow
semantics, or descendant eligibility. Cycle-specific remote CI and the final
user-directed diff/close-out review remain pending after this slice.

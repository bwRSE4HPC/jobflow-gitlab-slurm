# Recovery audit v1

Status (2026-10-08): **models/codecs installed; full local acceptance gate
user-reported passing**.
Owned intent persistence has passed its full local gate on user-reported
evidence. This document records the approved bounded models/codecs slice and
the ordering constraints for later audit storage and explicit bundle recovery.
Complete source/tests and the wheel-import insertion are installed through user
application. Only documentation is edited by the assistant. No recovery execution,
new filesystem operation, or Stage 2 close-out is authorized by this approval.

## Purpose and authority boundary

Publication intent retains what bytes were expected. A recovery request records
who requested an explicit operation, which intent it uses, and what was observed.
A separate receipt records verified filesystem completion. Do not mutate a
request from pending to successful or infer completion from its existence.

These records are provenance under the trusted-root/cooperating-writer model,
not authenticated authorization. Actor labels are caller-supplied assertions.
Terminal scheduler evidence, orphan-writer exclusion, and scientific/result
eligibility belong to later worker/reconciler integration. No record here
attests that the scientific calculation succeeded or authorizes a rerun.

## Approved first slice: strict records and codecs only

New module: `src/jobflow_gitlab_slurm/persistence/recovery/records.py`.
New tests: `tests/persistence/recovery/test_records.py`.
One module import in `ci/check-wheel.sh` after user application.

All models are strict and frozen, reject unknown fields, and revalidate nested
instances. Records require integer `schema_version: 1`, canonical UTC timestamps,
exact jobflow job strings, their validated SHA-256 locators, and lowercase
UUIDv4 backend identities. Generate no IDs or timestamps in model/codec code.

### Actor metadata

Approved `RecoveryActor` fields:

- `kind`: literal `operator` or `controller`.
- `identifier`: bounded opaque ASCII label, matching
  `^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`.

Use an appropriate pseudonymous or deployment-local label. Do not automatically
capture usernames, hostnames, environment dumps, runner tokens, email addresses,
or private infrastructure addresses. An actor label is not proof of identity.

### Recovery request

Approved `RecoveryRequestRecord` fields:

- `schema_version`, literal `kind: bundle-recovery-request`, `created_at`.
- Qualified identity: `run_id`, `job_uuid`, `job_index`, `job_key`,
  `attempt_id`, and `invocation_id`.
- Caller-retained `recovery_id` and `journal_event_id`, both UUIDv4. Allocate
  once per intended operation; retain them unchanged across process retries.
- `actor`: strict `RecoveryActor`.
- `intent_id`: the retained publication intent's identity.
- `intent`: strict run-relative `RecordArtifactReference`, pinning exact
  canonical intent bytes by path, SHA-256, and size.
- `observed_status` and `action`, constrained to the mapping below.

| Original verified observation | Requested action |
| --- | --- |
| `staging_only` | `publish_staging` |
| `published_uncommitted` | `complete_marker` |
| `committed` | `acknowledge_commit` |

These are declarations until an I/O operation verifies actual bundle evidence
and byte integrity against intent. Absent, partial, invalid, unsafe, or ambiguous
evidence must not be represented as a recoverable observation. The original
observation/action are immutable; do not rewrite them as recovery progresses.

### Completion receipt

Approved `RecoveryReceiptRecord` fields:

- `schema_version`, literal `kind: bundle-recovery-receipt`, `created_at`.
- The same qualified identity and `recovery_id` as its request.
- `request`: strict run-relative reference to the exact canonical request bytes.
- `bundle_manifest` and `bundle_commit`: strict run-relative references to
  the expected published manifest and marker bytes.
- Literal `result: committed_bundle_verified`.

This result describes verified and durably acknowledged filesystem evidence,
not scheduler or scientific success. It does not claim that the receipt writer
performed the original marker rename: an uncertain prior operation may already
have completed it. The original marker is never regenerated with a new timestamp.

### Pure binding and canonical bytes

Use deterministic compact UTF-8 JSON with sorted keys, unescaped Unicode,
finite-only values, and no trailing newline. Decoders reject duplicate keys,
noncanonical bytes, unsupported schemas, and unsafe/incorrect reference paths.
No Monty decoding or consumer imports.

Pure cross-record validators must check:

- Request/receipt identities agree with the definition, attempt, invocation,
  and intent; reuse the installed intent and parent validators.
- Intent ID, exact canonical intent hash/size, and its identity-derived path
  agree with the request. Request creation does not precede intent creation.
- Receipt references the request's exact canonical bytes and qualified recovery
  path. Receipt creation does not precede request creation.
- Published manifest/marker paths and their byte hash/size agree with the
  intent's unchanged nested records.

Metadata timestamp ordering is not proof of physical publication ordering.
Actual record presence, durability, integrity, or authority is not established
by these pure validators.

Supplied APIs:

- `encode_recovery_request(record)` / `decode_recovery_request(data)`.
- `encode_recovery_receipt(record)` / `decode_recovery_receipt(data)`.
- `validate_recovery_request(definition, attempt, invocation, intent, request)`.
- `validate_recovery_receipt(definition, attempt, invocation, intent, request,
  *, receipt)`; also validates the complete request/parent chain.

Request references must resolve lexically to the qualified invocation's
`publication-intent.json`. Receipt references must resolve lexically to
`recoveries/<recovery-id>/request.json`, `published/payload-manifest.json`,
and `published/COMMIT.json`. Each referenced record must be nonempty; actual
bytes are pinned by the cross-record validators, not by filesystem access.

## Layout implemented in separate storage slices

```text
<invocation-directory>/
  publication-intent.json
  recoveries/
    <recovery-id>/
      request.json              immutable before any recovery mutation
      receipt.json              immutable after verified durable completion
  staging/
  published/
```

References are run-relative and identity-derived. Neither recovery IDs nor
job UUID strings supplied as arbitrary paths can escape this layout.
Reserved private staging names and filesystem APIs are defined by the separate
[request](recovery-request-storage-v1.md) and
[receipt](recovery-receipt-storage-v1.md) storage contracts, not these pure models.

## Ordering and retry behavior implemented separately

Under active invocation ownership, with brief run-lock access only when needed:

1. Reopen/validate retained intent and inspect all bundle evidence/bytes. The
   recovery caller must separately satisfy its applicable authorization and
   writer-exclusion requirements.
2. Persist and durably acknowledge the immutable request before recovery
   mutation. A failed or uncertain request acknowledgment forbids proceeding.
3. Perform only the exact requested completion, or reconcile matching forward
   progress from an earlier execution of the same recovery request. Never
   parse missing results, run a calculation, or regenerate expected metadata.
4. Revalidate the full committed bundle and durably acknowledge it. Only then
   persist the completion receipt. An uncertain receipt acknowledgment requires
   inspection/retry of the same recovery identity.
5. Associate verified receipt evidence with the journal through the retained
   `journal_event_id` and exact retry semantics in the installed
   [recovery-journal slice](recovery-journal-v1.md). Journal
   registration is not atomic with bundle or receipt publication.

| Crash window | Required conservative interpretation |
| --- | --- |
| Request publication/acknowledgment incomplete | Inspect/retry the same request; do not begin recovery mutation. |
| Durable request, bundle still incomplete | Retain the request; revalidate evidence before any explicit retry. |
| Bundle committed, receipt absent | Revalidate exact expected commit, durably acknowledge it, and complete the same request's audit; no scientific rerun. |
| Receipt exists, journal registration absent | Verify/reacknowledge audit evidence and retry the retained event identity; do not start another recovery or calculation. |

Permitted forward observations are staging-only to published-uncommitted to
committed, and published-uncommitted to committed. Skipping an intermediate
observation is permitted only when full exact expected evidence verifies.
Original committed evidence remains committed. Regression, changed metadata,
or partial/corrupt/ambiguous evidence is held rather than explained away.

Missing receipt means pending or uncertain audit completion, not proof that
no mutation happened. Preserve earlier request and temporary evidence. Do not
use a fresh recovery ID to bypass an unresolved prior request. Competing
pending requests are prevented by the installed one-recovery-ID-per-invocation
storage rule; a process-scoped lock alone cannot resolve outstanding durable
requests. Complete scheduler/scientific failure-report persistence remains
later worker/reconciler work.

Journal holds and unknown scheduler outcomes stay distinct from filesystem
completion. A valid receipt cannot override either or release dependent jobs.

## First-slice risks and acceptance

Approved scope: two models plus actor metadata, canonical codecs, pure
binding validators, offline tests, wheel import, and documentation only.
No filesystem I/O, locks, recovery operation, CLI, journal append, scheduler
query, dependency changes, or edits to existing publisher/storage APIs.

Tests must cover strict fields and versions; actor limits; all state/action
pairs; required fields and immutability; exact identity/path/hash/size and
timestamp binding; unsafe copied nested models; canonical round trips;
malformed/duplicate/nonfinite/noncanonical JSON; and no scientific imports
or file operations. Pass existing full local quality, 100% combined coverage,
build, and isolated-wheel gates after user application.

Primary risks: circular recovery trust, confusing provenance with permission,
claiming completion before durable evidence, rejecting valid forward progress,
and escaping uncertain work by changing identity. The first models-only slice
defines those boundaries without claiming a working recovery protocol.
Read-only inspection confirms source/tests and the wheel import are installed.
At the user's explicit request, the assistant executed formatting checks, lint,
shell syntax, focused/full tests, coverage, build, and the existing isolated-wheel
check on 2026-10-08. Formatting, lint, shell syntax, build, and wheel verification
passed, and full-suite statement/branch coverage reached 100%. The user
subsequently reports the complete local verification gate passes. No successful
final test count was supplied, and no assistant rerun is claimed for that final
gate. No source/test/CI edits were made by the assistant. Remote CI and live
durability remain unverified for this slice.

The [request-only storage slice](recovery-request-storage-v1.md) is installed;
its full local acceptance gate is user-reported passing. Its additional
operation-identity/storage invariants are separate from the installed models.
This approval does not extend to completion receipts or recovery execution.

The subsequent [completion-receipt storage slice](recovery-receipt-storage-v1.md)
is installed with its local acceptance gate user-reported passing. It verifies
and acknowledges already committed bundles; it does not repair them.

The subsequent [explicit bundle recovery slice](bundle-recovery-v1.md) is installed
with its local gate user-reported passing. The separate
[recovery-to-journal association](recovery-journal-v1.md) is installed with its
local acceptance gate user-reported passing. Cycle scope is recorded in
[002-durable-run-state](../002-durable-run-state.md); [progress](../progress.md) owns
the current checkpoint.

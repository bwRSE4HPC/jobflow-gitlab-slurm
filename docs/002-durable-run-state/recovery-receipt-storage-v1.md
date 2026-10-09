# Recovery-receipt storage v1

Status (2026-10-08): **bounded slice installed; full local acceptance gate
user-reported passing**.
Recovery-request storage is installed with its full local acceptance gate
user-reported passing. Receipt models/codecs already exist. This approved slice adds
owned immutable receipt storage, not recovery execution or scientific success.

## Scope and files

Approved files:

- New `src/jobflow_gitlab_slurm/persistence/recovery/receipts.py`.
- New `tests/persistence/recovery/`.
- A narrowly scoped namespace extension in `recovery_storage.py`, with
  regression coverage in `tests/persistence/recovery/`.
- One module import in `ci/check-wheel.sh` and relevant documentation updates.

Keep the current record schemas, bundle publisher APIs, dependencies, CLI,
run/journal formats, and ownership/lock ordering unchanged. Do not refactor
unrelated storage modules or introduce a general persistence abstraction.

## Approved APIs

```python
read_owned_recovery_receipt(ownership, recovery_id) -> RecoveryReceiptHandle
persist_owned_recovery_receipt(ownership, receipt) -> RecoveryReceiptHandle
```

The immutable handle contains the final path, validated receipt record,
SHA-256, and byte count. Both APIs require active `InvocationOwnership` in
its originating process/context. Do not acquire the invocation lock again or
hold the run lock during bundle verification and flushing.

Readback performs no writes, flushing, or scientific payload inspection.
It validates the canonical receipt and complete retained request/intent/parent
chain. A readable receipt is historical audit metadata, not proof of current
bundle integrity, durable acknowledgment in this process, or result eligibility.
Missing final receipt means absent or unfinished audit completion, not proof
that bundle mutation or scientific execution did not happen.

Persistence returns only after revalidating and durably acknowledging the
exact committed bundle and immutable audit evidence. A model or stale handle
is insufficient. It never creates a missing commit marker or publishes staging.

## Namespace compatibility

Use the existing reserved recovery ID:

```text
recoveries/<recovery-id>/
  request.json
  receipt.json
  .recovery-request-<opaque-suffix>
  .recovery-receipt-<opaque-suffix>
```

The installed request scanner recognizes safe regular `receipt.json` and
nonempty receipt-temporary names alongside request evidence.
Request readback continues to validate request/intent
metadata only; recognizing a receipt filename must not attest receipt content
or completion. Receipt readback/persistence provide that separate validation.
Symlinks, special files, unsafe directories, unknown entries, and multiple or
different recovery IDs remain held. Receipt operations require a final valid
request; receipt-only evidence must not manufacture one.

The single-recovery-ID-per-invocation invariant remains unchanged after
completion. No operation allocates IDs, timestamps, or replacement metadata.

## Persistence protocol

Under active invocation ownership:

1. Revalidate caller metadata and reopen the retained request and intent.
   Use the installed pure receipt validator to verify full identity, timestamp,
   path, and canonical-byte binding. Validate any existing receipt or temporary
   candidate before attempting publication. Conflicting records are never replaced.
2. Reacknowledge the exact existing request; do not depend on a handle produced
   by an earlier process. Preserve its actor, original observation/action, and
   journal event ID.
3. Inspect the complete bundle and require `committed` with valid content.
   Require the exact intent-pinned manifest and commit marker, not merely valid
   metadata with the same identity. Hold absent, staging-only, unmarked,
   ambiguous, incomplete, corrupt, or differing evidence.
4. Durably acknowledge the verified payloads, manifest, existing marker,
   referenced scheduler-receipt bytes, and affected directory entries using
   existing safe-descriptor and synchronization mechanisms. Reinspect and
   verify exact committed evidence after flushing. Do not invoke ordinary bundle
   publication as an unchecked acknowledgment shortcut or change bundle bytes.
5. Publish canonical receipt bytes through a private same-filesystem temporary
   file: flush bytes and temporary directory entry, rename to an absent final
   name, flush the final file and recovery parent-directory chain, then reopen
   and verify exact bytes. No receipt publication precedes successful bundle
   acknowledgment.

An exact existing final receipt still requires current bundle verification
and all acknowledgment checks before persistence succeeds. Preserve its original
bytes and timestamp. A matching sole temporary receipt may be resumed only
after the same prerequisite checks. Partial, conflicting, unsafe, or multiple
temporary candidates without a final receipt hold publication. Safe temporary
evidence alongside an exact final receipt is retained, not automatically deleted.

The trusted-root/cooperating-writer boundary remains unchanged. Process ownership
does not establish scheduler termination or exclude unmanaged/orphan writers;
those are separate requirements of the later recovery/reconciler operation.

## Diagnostics and uncertainty

Provide distinct missing, integrity, conflict, inspection, and publication
errors with detached sanitized reports. Include qualified identities, recovery
ID, evidence locations, operation/phase, errno when available, and preparation
and final-receipt publication uncertainty. Do not expose record bodies or raw
exceptions. Distinguish bundle acknowledgment failure from receipt publication
failure; observing a final filename alone never acknowledges completion.

Retain all evidence after failure. Hints direct the caller to inspect/retry the
same recovery ID and exact records, not delete evidence, select another ID,
release dependent jobs, repair a bundle, or relaunch a calculation. An uncertain
receipt acknowledgment does not undo a committed bundle or completed calculation.

## Acceptance and remaining boundaries

Tests must cover read-only absence, request namespace compatibility, full
record binding, inactive/inherited ownership, filesystem entry safety,
single-ID enforcement, exact temporary reuse and immutable conflicts. Prove
that only exact valid committed bundles permit publication and that every
acknowledgment/publication fault leaves evidence and truthful uncertainty.
Exercise restart/same-ID retry and separate-process ownership with real files.
Verify reads perform no writes/flushes, and persistence never publishes staging,
creates a marker, executes consumer code, or mutates journal state.

Require formatting, lint, focused/full tests, agreed 100% combined statement/
branch coverage, build, and isolated-wheel verification. Source/tests, the request
namespace extension, and the wheel import are installed, as confirmed by inspection.
On 2026-10-08, explicitly requested assistant checks passed formatting, shell
syntax, build, and isolated-wheel verification; full-suite execution reached
100% combined statement/branch coverage. The user subsequently reports local
verification passes, completing this slice's local acceptance. No successful
final test count was supplied, and no assistant rerun is claimed for that final
gate. No assistant source/test/CI edits were made.
Cross-host durability, remote CI, and live infrastructure remain separate gates.

The subsequent [explicit bundle recovery slice](bundle-recovery-v1.md) is
installed with its local gate user-reported passing. Journal association is a
separate slice in [002-durable-run-state](../002-durable-run-state.md);
see [progress](../progress.md) for current acceptance and remaining work.
A receipt attests filesystem completion only; scheduler
success, jobflow semantics, and descendant eligibility remain later work.

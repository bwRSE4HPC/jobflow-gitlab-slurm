# Explicit bundle recovery v1

Status (2026-10-08): **bounded slice installed; local acceptance gate passed
on recorded assistant checks and user-reported successful verification**.
Publication, retained intent, recovery-request storage, and completion-receipt
storage have passed their local gates on recorded/user-reported evidence.
This approved slice completes intact filesystem publication only. It does not execute
jobflow, parse scientific output, query Slurm, or establish result eligibility.

## Scope and files

This slice belongs to cycle
[002-durable-run-state](../002-durable-run-state.md), with:

- New `src/jobflow_gitlab_slurm/persistence/recovery/operations.py`.
- New `tests/persistence/recovery/`.
- One module import in `ci/check-wheel.sh` and relevant documentation updates.

Reuse the installed ownership, inspection, strict records/codecs, intent,
request/receipt storage, and package-local synchronization mechanisms. Keep
record schemas, ordinary publisher APIs, dependencies, CLI, and journal formats
unchanged. Do not introduce a general storage refactor or payload producer.

## Approved interface and caller responsibility

```python
recover_owned_bundle(ownership, request, *, receipt) -> RecoveryReceiptHandle
```

The caller retains exact request and intended receipt records, including all
IDs, timestamps, actor metadata, original observation/action, and journal event
identity across retries. Expected bundle records come from the retained
publication intent, not from adopting whatever files happen to be present.
No operation allocates identities or regenerates a marker timestamp.

Require active invocation ownership in its originating process/context. Do
not reacquire the invocation lock or hold the run lock during payload I/O.
Busy ownership remains fail-fast; manager scheduling/retry policy is later work.

The caller must independently establish that the relevant producer and unmanaged/
orphan writers are quiescent and that recovery is explicitly authorized. The
low-level storage API assumes a trusted root and cooperating writers; a request,
actor label, lock, or caller boolean is not authenticated authorization or proof
of scheduler termination. Live controller use remains gated on later scheduler/
worker integration and cross-host filesystem verification. Offline tests use
inert evidence without a live producer.

## Validation and allowed forward progress

Before bundle mutation, strictly validate caller records and their parent/intent
binding, inspect audit and bundle evidence, and durably persist/reacknowledge the
exact request using installed request storage. Failed or uncertain request
acknowledgment forbids proceeding. Reinspect current bundle evidence under the
same ownership after request acknowledgment and before selecting an action.

| Original immutable request observation | Permitted current exact valid evidence | Completion |
| --- | --- | --- |
| `staging_only` | Staging only; published without marker; committed | Finish remaining publication steps, or acknowledge matching completed progress. |
| `published_uncommitted` | Published without marker; committed | Finish the marker, or acknowledge matching completed progress. |
| `committed` | Committed only | Acknowledge without changing bundle bytes. |

The original observation/action remain unchanged. Never rewrite an earlier
request to explain current progress. Regression, absence, both directories,
partial/corrupt/unsafe inventory, mismatched identities, or valid but different
manifest/marker bytes are held. Check full declared payload and scheduler-receipt
hashes/sizes; scheduler-receipt bytes remain opaque.

Validate existing audit evidence before bundle mutation. Malformed/conflicting
receipts or uncertain partial audit files cannot be bypassed by repairing the
bundle or selecting another ID. Retained receipt evidence coupled with a bundle
that is no longer committed is a hold, not permission to repeat publication.
Missing receipt alone does not prove that recovery mutation has not happened.

## Completion protocol

For exact valid staging-only evidence:

1. Verify the complete inventory, exact retained manifest, safe filesystem
   entries, and same-filesystem rename boundary.
2. Flush payloads, manifest, referenced scheduler receipt, and affected
   directories, retaining all scientific/result bytes unchanged.
3. Recheck that the published destination is absent, rename staging to published,
   and flush the renamed directory and its invocation parent.
4. Reverify exact valid published-uncommitted evidence, then complete the marker
   through the protocol below.

For exact valid published-uncommitted evidence:

1. Reverify and acknowledge intact payload/manifest/scheduler-receipt evidence.
2. Publish only the retained intent's exact canonical commit-marker bytes through
   a safe same-filesystem private temporary file and absent final destination.
   Flush temporary bytes and their directory entry before rename; flush the final
   marker and affected directories afterward.
3. Verify the complete committed bundle. Do not reconstruct a missing document,
   Response, payload, or scientific output.

For exact valid committed evidence, do not rewrite files, rename directories,
create a new marker, or update timestamps. Complete/reacknowledge the retained
receipt through installed receipt persistence, which independently revalidates
and flushes the committed bundle and request before acknowledging audit completion.
Return its validated receipt handle only after that operation succeeds.

## Retained temporary markers

The ordinary publisher can leave invocation-local `.bundle-commit-*` files.
The current bundle inspector does not classify these surrounding temporary
entries; the recovery operation must inspect them separately without following
symlinks or opening special files. Do not alter ordinary inspection behavior.

With no final marker, a single safe, same-filesystem temporary file whose bytes
exactly match the retained expected marker may be reused. When no temporary
exists, a new file may be created with those same expected bytes. Partial, conflicting,
malformed, unsafe, or multiple temporary candidates hold completion; never choose
arbitrarily, overwrite, delete, or replace them with another candidate.

With an exact verified final marker, safe leftover temporary evidence is retained
without adoption or cleanup. Unsafe temporary entries remain a hold. Renaming
the selected exact candidate consumes its old pathname; it is not evidence loss.

## Errors and crash windows

Provide distinct integrity, conflict, inspection, and completion/publication
errors with sanitized detached reports. Include qualified run/job/attempt/
invocation/recovery identity, phase, relevant audit/staging/published/temporary
paths, available errno, and separate uncertainty about bundle publication and
audit completion. Do not expose record bodies or raw consumer exceptions.

| Interruption | Required restart interpretation |
| --- | --- |
| Request acknowledgment incomplete | Retry/inspect the same request; no bundle mutation authorized. |
| Durable request; staging rename incomplete | Inspect both names; accept only exact valid permitted forward evidence. |
| Published evidence intact; marker unfinished | Inspect retained temporary marker evidence and complete only an eligible exact candidate or absent candidate. |
| Marker visible; flush/receipt incomplete | Reverify/reacknowledge the exact committed bundle and finish the same receipt; do not rerun. |
| Receipt acknowledged; journal event absent | Preserve completed filesystem evidence; journal association is a later separate operation. |

Failures preserve evidence and give same-identity inspection/retry hints. Neither
an exception nor missing audit metadata proves that a mutation did not happen.
Unrecoverable missing/corrupt payloads require operator inspection and a later
separately authorized parse-only repair or new calculation, not this operation.
No recovery outcome releases dependent jobs or overrides scheduler/journal holds.

## Acceptance and remaining gates

Test all allowed original/current observation pairs and rejected regressions;
exact identity/intent/audit binding; absent/partial/corrupt/ambiguous evidence;
temporary-marker reuse/holds; no overwrite/cleanup; inactive/inherited ownership;
and same-ID retries after I/O errors and separate-process termination. Verify
request acknowledgment precedes the first bundle mutation, receipt acknowledgment
follows committed verification, and scientific bytes/IDs/timestamps are preserved.

Inject interruptions before and after directory rename, marker rename and
durability acknowledgments, and receipt completion. Prove repeated recovery
does not repeat a calculation or manufacture missing jobflow results. Pass the
agreed formatting/lint, focused/full tests, 100% combined coverage, build, and
isolated-wheel gates. Read-only inspection confirms source, tests, and the wheel
import are installed. At the user's explicit request on 2026-10-08, assistant-run
formatting, lint, shell syntax, CLI help, whitespace, build, and isolated-wheel
checks passed, and full-suite execution reached 100% statement and branch coverage.
The user subsequently reports verification passed, completing this slice's local
acceptance gate. No successful final test count was supplied and no independent
assistant rerun is claimed for that final gate. No assistant source/test/CI edits
were made.

Recovery-journal association, ordinary-publication association, and lifecycle
tests are now locally accepted on user-reported evidence.
[002-durable-run-state](../002-durable-run-state.md) remote CI and Stage 2
close-out remain pending. Live cross-host locking/durability and
scheduler/writer-exclusion verification remain deployment gates.

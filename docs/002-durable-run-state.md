# Cycle 002: durable run state

Branch: `002-durable-run-state`.
Predecessor: [001-jobflow-compatible-package](001-jobflow-compatible-package.md),
squash-merged through PR #1 as `ded57b3`.
Status (2026-10-09): implementation slices locally accepted; assistant cycle
review and full local working-tree gate passed. No blocking code finding was
identified within the reviewed scope. User documentation review, committed
candidate identification, remote CI, and explicit close-out/merge approval remain
pending. See the [review record](002-durable-run-state/review-and-verification.md).

This document collects cycle scope, interfaces, mechanisms, and acceptance
criteria. The [overall plan](implementation-plan.md) owns stage ordering;
[progress](progress.md) owns the current checkpoint and verification evidence.
Refined slice documents and versioned contracts live in
[002-durable-run-state/](002-durable-run-state/). The contracts linked below
own their respective formats and behavior.
Do not duplicate minor test/debugging iterations as implementation slices.

## Scope and boundaries

Build the offline filesystem persistence foundation before worker execution:
retain exact original Flow and job-definition bytes, immutable qualified
metadata, anchored events, invocation ownership, opaque result-bundle evidence,
and explicit same-identity publication recovery.

No consumer code execution, scientific parsing, Slurm submission, active-result
selection, budget scheduling, or GitLab reconciliation belongs to this cycle.
The persistence boundary was reviewed on 2026-10-07 and approved on 2026-10-08.
JobStore execution semantics and payload preparation/producer enforcement belong
to Stage 3. This boundary clarification and the test-only whole-lifecycle slice
were approved on 2026-10-08; see the
[lifecycle review](002-durable-run-state/lifecycle-review.md). No additional
production staging API is planned for Stage 2.

## Slice and contract map

- Artifact verification and run creation/reopening:
  [artifacts](002-durable-run-state/artifact-contract-v1.md), [run metadata](002-durable-run-state/record-contract-v1.md),
  and [run-state persistence](002-durable-run-state/run-state-v1.md).
- Anchored event reading, append/recovery, and journal-aware inspection:
  [event journal](002-durable-run-state/event-contract-v1.md).
- Qualified definition/attempt/invocation metadata and immutable storage:
  [records](002-durable-run-state/attempt-record-contract-v1.md),
  [definition storage](002-durable-run-state/definition-storage-v1.md), and
  [attempt storage](002-durable-run-state/attempt-storage-v1.md).
- Invocation ownership, opaque bundle validation, and ordinary publication:
  [ownership](002-durable-run-state/bundle-storage-v1.md), [bundle records](002-durable-run-state/bundle-record-contract-v1.md),
  [inspection](002-durable-run-state/bundle-inspection-v1.md), and
  [publication](002-durable-run-state/bundle-publication-v1.md).
- Retained publication expectations and immutable recovery provenance:
  [intent](002-durable-run-state/publication-intent-v1.md),
  [intent storage](002-durable-run-state/publication-intent-storage-v1.md),
  [audit records](002-durable-run-state/recovery-audit-v1.md),
  [request storage](002-durable-run-state/recovery-request-storage-v1.md), and
  [receipt storage](002-durable-run-state/recovery-receipt-storage-v1.md).
- Intact expected publication recovery:
  [explicit recovery](002-durable-run-state/bundle-recovery-v1.md).
- Association of completed recovery with a stable journal event:
  [recovery registration](002-durable-run-state/recovery-journal-v1.md).
- Ordinary-publication event identity and registration:
  [publication registration](002-durable-run-state/publication-journal-v1.md).
  Installed; local acceptance gate user-reported passing.
- Cycle-wide code/documentation review and executed local gate:
  [review and verification](002-durable-run-state/review-and-verification.md).
- Whole-lifecycle composition and producer-staging boundary review:
  [review and approved test slice](002-durable-run-state/lifecycle-review.md).
  Test slice installed; local gate user-reported passing.

## Implementation slices and acceptance evidence

Local slice gates comprise tests with the agreed 100% combined statement/branch
coverage gate, Ruff formatting/lint, applicable CLI help and shell syntax,
wheel/sdist build, isolated-wheel verification, and diff whitespace checks.
CI reports coverage but does not enforce a 100% threshold. Counts identify
historical checkpoints, not the current suite size. User-reported final reruns
are not claimed as independently repeated assistant checks. The subsequent
2026-10-09 cycle-wide assistant rerun passed 2,324 tests with 100% statement and
branch coverage, quality checks, and packaging. Its evidence is recorded
separately in the review record; historical slice counts below are unchanged.

| Slice | Implemented behavior | Verification evidence |
| --- | --- | --- |
| Artifact helpers | Streaming exact-byte verification and private staging; no implicit Flow reserialization. | Installed and user-reported local gates pass at the 112-test checkpoint. |
| Run metadata models | Strict manifest/Flow envelope models and deterministic `site-json-v1` snapshots. | Installed and user-reported local gates pass at the 166-test checkpoint. |
| POSIX run storage | Atomic initial run publication, persistent per-run locks, reopening, optional external-artifact verification, and conservative interrupted-publication diagnostics. | User-reported local gates pass at the 248-test checkpoint; tests include reopening, contention, injected publication failures, and abrupt process exit. |
| CLI and directory discovery | `create-run`, `inspect-run`, and `list-runs`; explicit metadata classifications and selection through `request.site_id`. | Installed and user-reported local gates pass; successful final test count not supplied. |
| Event/head models | Strict immutable records, canonical JSON encoding, checksums, and detached payload access. | Installed and user-reported local gates pass; successful final test count not supplied. |
| Journal reader | Locked, non-mutating replay of history and anchor; distinct busy, integrity, and incomplete-publication outcomes. | Installed and user-reported local gates pass; successful final test count not supplied. |
| Journal append/recovery | Event-before-head publication, stable-ID retry acknowledgment, and explicit exact matching-tail recovery. | Installed and user-reported local gates pass; successful final test count not supplied. |
| Journal-aware reporting | Common locked inspection, separate metadata/journal findings, sanitized diagnostics, and exit precedence: invalid (2), incomplete (4), busy (3), otherwise 0. | Installed and user-reported local gates pass; successful final test count not supplied. |
| Identity/provenance models | Exact jobflow string identities, SHA-256 filesystem locators, strict definition/attempt/invocation records, and pure cross-record validation. | Installed and user-reported local gates pass on 2026-10-08; successful final test count not supplied. |
| Bundle models/codecs | Immutable inventories, explicit path namespaces, canonical manifest/marker codecs, and metadata-only cross-record validation. | Installed and user-reported full local acceptance gate passes on 2026-10-08, including the agreed 100% combined coverage gate and packaging. Successful final test count not supplied; no independent assistant rerun. |
| Job-definition storage | Atomic opaque payload/metadata publication under the run lock, exact same-ID acknowledgment, read-only reopening, and retained interrupted-publication evidence; original-Flow origin only. | Installed and user-reported supplied local verification passes on 2026-10-08, including the agreed 100% combined coverage gate and packaging. Successful final test count not supplied; no assistant-executed verification. |
| Attempt/invocation storage | Immutable metadata publication/reopening with linked definition/request validation, exact retry acknowledgment, and retained publication evidence; no scheduling lease or execution authorization. | Assistant-executed checks on 2026-10-08: 152 focused tests and 1,267 full-suite tests pass with 100% statement and branch coverage; locked sync, formatting, syntax, CLI help, build, isolated wheel verification, and whitespace checks pass. The user subsequently reports the final quality checks and full local gate passed; no assistant rerun of the final checks. |
| Invocation ownership | Persistent lock provisioning, nonblocking per-invocation exclusion, invocation-before-run metadata validation, and process/context lifetime guards; no worker or bundle mutation. | Assistant-executed checks on 2026-10-08: 62 focused and 1,329 full-suite tests pass with 100% statement and branch coverage; locked sync, formatting, syntax, CLI help, build, isolated wheel verification, and whitespace checks pass. The user subsequently reports the final verification gate passed, completing local acceptance; no assistant rerun of the final checks. |
| Read-only bundle inspection | Owned evidence classification, canonical metadata/provenance checks, exhaustive inventories, streamed payload/receipt byte checks, and sanitized detached reports; no mutation, scientific decoding, or result selection. | Installed and user-reported full local acceptance gate passes on 2026-10-08, including the agreed 100% combined coverage gate and packaging. Successful final test count not supplied; no assistant-executed verification. |
| Bundle publication | Owned publication of complete opaque staging and durable acknowledgment of an exact committed bundle; interrupted published-but-unmarked evidence requires explicit recovery. | Installed and user-reported full local acceptance gate passes on 2026-10-08, including the agreed 100% combined coverage gate and packaging. Successful final test count not supplied; no assistant-executed verification. |
| Publication-intent models/codecs | Immutable complete expected manifest/commit records, stable intent identity/timestamp, canonical encoding, and pure parent binding; no persistence or recovery action. | Installed and user-reported full local acceptance gate passes on 2026-10-08, including the agreed 100% combined coverage gate and packaging. Successful final test count not supplied; no assistant-executed verification. |
| Publication-intent storage | Owned immutable intent publication/readback, exact durable acknowledgment, distinct missing/conflict/integrity/I/O outcomes, retained interrupted-write evidence, and refusal of retroactive creation after a published path exists. | Installed and user-reported full local acceptance gate passes on 2026-10-08, including the agreed 100% combined coverage gate and packaging. Successful final test count not supplied; no assistant-executed verification. |
| Recovery-audit models/codecs | Immutable actor/request/receipt records, retained recovery and journal event identities, canonical codecs, and pure expected-bundle binding. | Installed; full local gate user-reported passing on 2026-10-08. Requested assistant checks passed formatting/lint, syntax, build and wheel verification; coverage reached 100%. No successful final test count or final assistant rerun recorded. |
| Recovery-request storage | Owned immutable request publication/readback, same-ID acknowledgment, one recovery ID per invocation, and retained uncertain evidence. | Installed; full local gate user-reported passing on 2026-10-08. Requested assistant checks passed formatting, syntax, build and wheel verification; coverage reached 100%. No successful final test count or final assistant rerun recorded. |
| Completion-receipt storage | Owned receipt publication/readback, request-scanner compatibility, and acknowledgment of already committed bundles only. | Installed; full local gate user-reported passing on 2026-10-08. Requested assistant checks passed formatting, syntax, build and wheel verification; coverage reached 100%. No successful final test count or final assistant rerun recorded. |
| Explicit bundle recovery | Exact intact expected publication, request-before-mutation ordering, same-ID forward progress, temporary-marker handling, and receipt completion. | Installed; full local gate user-reported passing on 2026-10-08. Requested assistant checks passed formatting/lint, syntax, CLI help, whitespace, build and wheel verification; coverage reached 100%. No successful final test count or final assistant rerun recorded. |
| Recovery-journal association | Registration of an existing recovery request/receipt using its retained event identity; no calculation, new receipt, or scheduler eligibility. | Installed; full local acceptance gate user-reported passing on 2026-10-08. No successful final test count supplied or independent assistant rerun of the final checks. |
| Ordinary-publication-journal association | Committed-only preflight, exact intent/bundle acknowledgment before journal registration, and one completion event using the retained intent ID; no publication from staging, recovery action, or scheduler eligibility. | Installed; local verification gate user-reported passing on 2026-10-08. No successful final test count or command transcript supplied; no independent assistant rerun. |
| Persistence lifecycle tests | Empty-root setup and fresh-process metadata hand-offs, ordinary publication, explicit same-identity recovery, journal registration/retry, and evidence holds; inert opaque payloads only. | Installed; local verification gate user-reported passing on 2026-10-08. No successful final test count or command transcript supplied; no independent assistant rerun. |

The installed-wheel gate verifies package imports, dependency compatibility,
and CLI help outside the checkout, not execution compatibility or HPC readiness.
The [progress record](progress.md) summarizes the current outstanding gates.

## Persistence behavior and boundaries

### Original run creation and reopening

The API requires a caller-retained UUIDv4, an existing trusted POSIX root,
and cooperating writers. Persistent `.locks/<run-id>.lock` files coordinate
creation and reopening across publication. Exact Flow bytes and validated
metadata are staged privately, flushed, and published by same-filesystem
rename. Failed staging is retained. Errors at/after the rename boundary report
uncertain publication and require inspection of the same ID rather than
creation of a fresh run.

The approved [artifact contract](002-durable-run-state/artifact-contract-v1.md) separates unchanged
`flow/payload.json` bytes from the versioned `flow/original.json` envelope.
The request's serialized-flow digest identifies payload bytes, not a
decode/re-encode result or a later-generated run ID. External consumer-code
and runtime references are recorded rather than copied into every run.
Reopening verifies their bytes only when explicitly requested; verification
will be required before execution.

The CLI configuration validators check digest syntax, not artifact contents;
artifact helpers and run creation perform actual byte verification. Budget
serialization emits plain decimal strings without precision loss; exponent
notation remains rejected in YAML/JSON input.

### Anchored journal and inspection

The approved [event contract](002-durable-run-state/event-contract-v1.md) uses a run-local head
outside `events/`, event-before-head flush ordering, and head replacement as
the logical commit point. Replay fails closed for an unanchored tail. Only an
explicit matching append retry can finish that tail; inconsistent anchors
and multiple unanchored events remain held. Reads never initialize, repair,
flush, or truncate the journal.

Inspection validates metadata and journal under one existing run lock, without
exposing event payload bodies or chained decoder causes. Reports describe
metadata/artifact integrity and journal holds, not scientific status:
`execution_state` remains `not_evaluated`. Root-wide discovery is non-atomic.
Known `.locks` and `.staging-*` entries are excluded; unexpected published
entries are reported rather than silently ignored.

Primitive contention raises `RunBusyError`. The approved future manager policy
is to defer a busy run, process others, and retry in a later invocation using
stable identities. This infrastructure policy belongs to the backend manager,
not jobflow core. Integrity and uncertain-publication failures remain distinct;
no implicit scientific rerun is authorized.

### Persistence scope review (2026-10-07)

The user approved the reviewed boundary on 2026-10-08:

- Stage 2 supplies durable identities, records, bundle storage, explicit
  interrupted-publication recovery, and read-only inspection.
- Stage 3 supplies the production `JobStore` adapter, worker, output-reference
  resolution, full `Response` semantics, and file/additional-store hand-off.
- Stage 4 supplies scheduler evidence, result eligibility/selection,
  reconciliation, submission recovery, and compute-budget policy.

The [attempt record contract](002-durable-run-state/attempt-record-contract-v1.md) and
[bundle record contract](002-durable-run-state/bundle-record-contract-v1.md) are approved. Their
installed models distinguish metadata validity, actual artifact integrity,
jobflow semantic validity, scheduler success, and selected result. Model
construction or a completion marker alone does not authorize execution.
Job-definition publication/reopening is installed for original-Flow inputs.
Attempt/invocation storage is installed with its full local acceptance gate
passed. Bundle publication and explicit recovery are installed with their full local
acceptance gates user-reported passing. Recovery-journal association is also
installed with its full local acceptance gate user-reported passing.
Ordinary-publication-journal association is installed with its local acceptance
gate user-reported passing; journal completion is not result eligibility.

## Mechanisms and risks

Preserve exact bytes and identities across restarts. A typed record is not proof
of filesystem durability, writer quiescence, authorization, or scientific success.
Use retained expectations rather than adopting interrupted evidence as its own
authority. Coordinate cooperating writers with persistent lock inodes; keep
invocation-before-run lock ordering and perform large payload I/O outside the
run lock. Contention fails fast; manager retry policy belongs to later stages.

Publish through private same-filesystem temporary evidence and explicit flush
boundaries. Preserve uncertain evidence and distinguish absence, corruption,
conflict, incomplete publication, and I/O uncertainty. Filesystem completion,
audit completion, and journal anchoring are separate transactions. Missing
registration must never be treated as permission to repeat a calculation.

These are trusted-root/cooperating-writer mechanisms, not an adversarial
filesystem sandbox or authenticated actor system. Local process interruption
tests do not establish cross-host visibility or power-loss durability.

## Pre-close-out structural refactoring

On 2026-10-09 the user adopted the refined behavior-preserving source and test
organization. The [package organization](002-durable-run-state/package-organization.md)
owns the accepted scope: separate run foundations and read-only queries,
record-model ownership, explicit internal/dependency contracts, bounded
filesystem-helper extraction, architecture checks, and responsibility-based
test organization. The user then explicitly authorized autonomous source, test,
configuration, and documentation edits and local verification for this refactor.
The hierarchy, model relocation, bounded filesystem primitives, lock-aware
internal interfaces, behavioral test splits, scoped fixtures/support, and offline
architecture checks are now implemented. Old flat imports are removed without
wrappers; CLI and persisted formats remain unchanged. The fresh gate passes
2,353 tests with 100% statement/branch coverage, encoding comparisons, and
installed-wheel checks. See [progress](progress.md) and the
[verification record](002-durable-run-state/review-and-verification.md).
The earlier gate remains historical evidence for the prior layout; no Git or
remote operation was authorized or performed.

## Close-out summary and Stage 3 hand-off

The implementation delivers the offline persistence foundation: exact opaque
Flow/job-definition bytes, immutable qualified metadata, run discovery and
journal-aware inspection, persistent locks, bundle validation/publication,
retained expectations, explicit same-identity recovery with audit provenance,
and stable publication/recovery event registration. Cross-process lifecycle
tests cover composition alongside the individual failure/restart tests.
All installed slices have locally accepted gates on the evidence recorded above.
The user reported cycle review complete on 2026-10-08. The assistant's code and
documentation review and complete local working-tree gate passed on 2026-10-09.
Neither result authorizes merging or establishes live deployment support.

Consequential boundaries retained for the next cycle:

- Stage 3 must interpret jobflow documents/Responses, supply the production
  `JobStore` adapter and worker, resolve references, and handle dynamic/stop
  semantics, file/additional-store hand-off, and approved amendments.
- The worker must own its invocation while preparing complete opaque staging
  and enforce durable intent acknowledgment before publication. The low-level
  publisher does not enforce intent presence by itself.
- A committed bundle and anchored event are integrity evidence, not scientific
  success, scheduler success, selected-result eligibility, or permission to
  release descendants. Stage 4 supplies scheduler evidence and reconciliation.
- Missing completion evidence never authorizes automatic calculation replay.
  Cross-host visibility/locking and power-loss durability remain live gates;
  coordinated journal/head rollback requires an independent witness.

This hand-off identifies existing roadmap obligations, not approval to begin
Stage 3 implementation on the current branch.

## Remaining acceptance and integration

1. User review of this close-out documentation and the current checkpoint.
2. Confirm the exact merge candidate passes formatting/lint, applicable
   syntax/CLI checks, full offline tests, agreed 100% combined statement/branch
   coverage, distribution build, isolated-wheel verification, and whitespace checks.
3. User-directed commit/push and pull request; record passing cycle-specific
   `Offline checks` CI evidence with its run URL and commit.
4. Explicit close-out approval and user-directed squash-merge. Record the merge
   reference, then plan Stage 3 from updated `main` on a separately agreed branch.

Cross-host locking, durability, and scheduler/writer exclusion remain Stage 6
deployment gates. Local close-out must retain those limitations explicitly.
No branch, merge, remote operation, or new code slice is authorized by this document.

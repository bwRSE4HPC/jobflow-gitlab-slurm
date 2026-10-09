# Read-only bundle inspection v1

Status (2026-10-08): **installed; full local acceptance gate user-reported
passing**. Read-only inspection confirms the module, tests, and wheel-check
import entry are present. The user reports the supplied verification gate
passed, including the agreed 100% combined statement/branch coverage and
packaging checks. The successful final test count was not supplied; no
independent assistant execution is claimed.
Invocation ownership is installed and its local acceptance gate has passed.
This slice follows the [bundle-storage contract](bundle-storage-v1.md) and
installed [bundle models/codecs](bundle-record-contract-v1.md).

## Approved scope and API

Installed module: `src/jobflow_gitlab_slurm/persistence/bundles/inspection.py`.

- `inspect_invocation_bundle(runs_root, run_id, job_uuid, job_index, attempt_id,
  invocation_id)` acquires the existing invocation ownership context and returns
  a frozen `BundleInspection`.
- `inspect_owned_bundle(ownership)` consumes an already-active
  `InvocationOwnership`, checks its lifecycle guard, and inspects without
  reacquiring the invocation lock. This supports later publication/recovery
  code without nested locking.
- `BundleInspection.to_report()` returns a detached JSON-compatible report
  with identity, evidence paths/presence, classification, integrity findings,
  sanitized issue codes, and inspection/recovery hints. It includes no payload
  bodies or raw decoder exception messages.

No CLI change, dependency/lockfile change, existing record-schema change,
worker, JobStore adapter, scheduler lookup, publication, repair, or journal
mutation is included. No existing source module needs a public API refactor.
The approved serializers/models remain the metadata authority.

## Locking and read-only boundary

The public wrapper uses `owned_invocation`: invocation lock first, then brief
run-locked parent metadata validation. Payload I/O runs with invocation ownership
but without the run lock. The owned helper requires a live originating-process
context and does not reacquire either lock. Preconditions retain their existing
error types: expired ownership, missing/unsafe lock, invalid parent metadata,
run contention, or invocation contention are not successful bundle reports.

Reads never provision a lock, create directories, call fsync, rename, repair,
delete evidence, choose a result, or rerun a job. Retained reports are observations
made under ownership, not future integrity guarantees or execution capabilities.
The trusted-root/cooperating-writer assumption and cross-host verification gate
remain unchanged. Ownership does not establish scheduler termination or exclude
unmanaged application subprocesses.

## Evidence classification

Reports retain staging/published/marker presence separately from the outcome.
The approved classifications are:

| Outcome | Meaning |
| --- | --- |
| `absent` | A valid, owned invocation contains neither staging nor published output. This says nothing about whether execution occurred elsewhere. |
| `staging_only` | Only staging exists. Report its content integrity independently; no staged content is committed, even when all declared payloads verify. |
| `published_uncommitted` | Only published exists, without a marker. Report content integrity independently; do not infer readiness or automatically finish publication. |
| `committed` | Only published exists and its canonical marker, manifest, full identity/provenance chain, receipt bytes, inventory, and declared payload bytes verify. This is filesystem integrity, not scientific success or eligibility. |
| `ambiguous` | Staging and published both exist. Retain both paths, do not choose either, and leave content verification unevaluated in this first slice. |
| `invalid` | Observed unsafe layout, malformed/mismatched metadata, undeclared inventory, or inconsistent bytes. Preserve evidence and name the failed check. |

Content integrity is separately `not_evaluated`, `incomplete`, `valid`, or
`invalid`. Missing manifest/payload/receipt in an unmarked unit is incomplete;
malformed metadata, wrong hashes, unsafe entries, or contradictory metadata are
invalid. A marker with missing required evidence is invalid, not a successful
commit. Marker presence in staging is also invalid: the accepted protocol
publishes it only after the directory rename.

Confirmed unsafe top-level staging/published entries take precedence over
ordinary presence classifications. When both are real directories, classify
ambiguity without blessing either inventory. Evidence presence alone never
implies integrity. Permission failures and unexpected I/O errors raise a
sanitized `BundleInspectionError`; they are not reported as absent or proof of
corruption. Busy conditions continue to propagate their existing exceptions.

Always report jobflow semantic state, scheduler state, and result eligibility
as `not_evaluated`. No classification authorizes resubmission, finalization,
parse-only repair, or descendant execution.

## Validation rules

1. Inspect entries with lstat/no-follow semantics. Reject symlink directories,
   symlink files, FIFOs, devices, sockets, and non-directory path components.
   Do not block opening a special file. No filename-based exclusions silently
   ignore unexpected entries within the bundle.
2. Decode manifest and marker using the installed strict canonical codecs.
   Never Monty-decode document/Response/data payloads, import consumer callables,
   or reserialize opaque bytes.
3. Bind a manifest to the owned invocation, its attempt/definition, pinned
   code/runtime digests, jobflow version, and nondecreasing timestamps. With
   no marker, validate these bindings directly without inventing a temporary
   commit record. With a marker, use `validate_bundle_records` and compare
   against the actual canonical manifest bytes.
4. With a manifest available, require exhaustive inventory: required document
   and Response, declared files/data, backend manifest, and published marker
   when present. Only necessary directory ancestors are allowed. Unexpected
   files or directories, including abandoned temporary publication files,
   are held rather than ignored or removed. Without a manifest, inspect safe
   structure and report incomplete content; do not claim inventory completeness.
5. Stream SHA-256 and size checks for declared payloads. Check both values,
   including zero-length declared optional artifacts. Parent directories must
   be separately checked; final-component no-follow alone is insufficient.
6. Verify the fixed run-relative scheduler receipt's bytes and safe path using
   the manifest reference. Do not parse its scheduler schema or infer Slurm
   acceptance/terminal state; those belong to Stage 4.

Diagnostic issues use stable codes and safely represented paths, not scientific
file contents, raw malformed metadata, credentials, or chained decoder input.
Hints say preserve/inspect or defer on contention; potential recovery is subject
to later validated evidence and authorization, never offered as already proven.

Streaming bounds payload-memory usage, not the size of decoded backend metadata
or inventories. Large-inventory scaling and metadata-size policy are separate
concerns; no performance measurement or cross-host guarantee is claimed here.

## Approved files and acceptance gate

- New `src/jobflow_gitlab_slurm/persistence/bundles/inspection.py`.
- New `tests/persistence/bundles/` using inert payload/receipt bytes.
- One module import in `ci/check-wheel.sh`.
- Documentation recording approval, installation, and verification evidence.

Acceptance includes every presence/integrity classification; null-output-like
opaque bytes and empty optional inventories; definition/attempt/invocation
identity, digest, and timestamp mismatches; noncanonical or malformed metadata;
missing/altered document, Response, receipt, files, and additional data;
undeclared entries and unsafe file/directory types; nested path safety; I/O
failure classification; same-invocation contention; independent-process reads;
expired/inherited ownership rejection; run-lock availability during payload
verification; no fsync or filesystem mutation; no consumer import/execution;
and sanitized detached reporting. Pass existing quality, 100% statement/branch
coverage, build, and wheel gates after user application.

Only documentation was changed by the assistant; source/tests and the wheel
import entry were applied by the user. Local acceptance is complete on the
user's report, not an assistant rerun. Publication/finalization/recovery,
jobflow semantics, scheduler eligibility, remote CI, and live filesystem gates
remain separate reviewed work. This slice does not close Stage 2.

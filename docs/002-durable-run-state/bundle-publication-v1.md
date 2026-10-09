# Bundle publication v1

Status (2026-10-08): **approved, installed, and locally user-verified**.
Read-only inspection confirms the module, tests, and wheel-check import are
present. The user reports the full supplied local gate passed, including the
agreed 100% combined coverage and packaging checks. No final test count was
supplied; no independent assistant rerun is claimed. No source,
test, or CI file is edited by the assistant and no checks are executed for
this slice.
Invocation ownership and read-only bundle inspection are installed with passing
local acceptance gates. This slice belongs to cycle
[002-durable-run-state](../002-durable-run-state.md) and refines the publication
portion of [bundle storage v1](bundle-storage-v1.md).
Explicit interrupted-publication recovery is a separate locally accepted slice;
see [its contract](bundle-recovery-v1.md).

## Bounded first publication slice

Implement publication of an already-prepared, complete staging directory, and
durable acknowledgment of an exact existing committed bundle. Do not implement
payload preparation, scientific execution, parse-only repair, scheduler lookup,
result selection, journal registration, or interrupted-publication recovery.

The later producer must hold invocation ownership while preparing its payloads
and canonical manifest. This publication API neither produces those payloads
nor infers that missing staging means a calculation never ran. Offline tests
construct inert staging evidence directly, as the installed inspection tests do.

Approved API in `src/jobflow_gitlab_slurm/persistence/bundles/storage.py`:

- `publish_owned_bundle(ownership, expected_manifest, expected_commit)` consumes
  active `InvocationOwnership`, revalidates both supplied models and their full
  parent binding, then publishes or acknowledges exact matching evidence.
- Return a frozen `BundleHandle` carrying the published path and validated
  manifest/commit records. It retains no lock and establishes neither future
  byte integrity nor scientific/scheduler success.

The caller retains the complete expected records, including IDs and timestamps.
The operation does not regenerate a marker timestamp on retry. Later controller
recovery will need independently retained expected records and an approved audit
contract; merely reading a staged manifest does not establish recovery authority.

No new dependencies, lockfile changes, CLI commands, existing record-schema
changes, or public refactor of existing modules are proposed. Internal reuse of
installed codecs, byte verification, ownership, and inspection is package-local.

## Ownership and validation

Call `ownership.require_active()` before any filesystem change. Do not reacquire
the invocation lock or hold the run lock during payload validation or flushes.
The installed ownership context has already validated parent metadata under
the invocation-before-run lock order.

Before any mutation:

1. Revalidate the caller's manifest and commit through their strict models and
   `validate_bundle_records`, including identity, provenance, digest, size,
   supported jobflow version, and timestamps.
2. Use owned inspection to determine actual staging/published presence and
   content integrity. Retain existing contention, ownership, parent-metadata,
   and unavailable-I/O exception classifications.
3. Require exact canonical manifest bytes matching the expected manifest.
   For acknowledgment, also require exact canonical marker bytes matching the
   expected commit. A valid but different record is a conflict, not a retry.
4. Require exhaustive inventory, safe regular files/directories, actual
   payload and receipt sizes/hashes, and the same-filesystem rename boundary.
   Receipt bytes remain opaque; no Slurm association is inferred.

All checks operate within the accepted trusted-root/cooperating-writer model.
Path checks plus ownership prevent replacement by cooperating backend writers;
they are not protection against an unmanaged or adversarial writer racing a
pathname operation. Filesystem support for locking and durability remains a
live verification gate.

## State-dependent behavior

| Owned observation | Proposed publication behavior |
| --- | --- |
| Complete, valid staging only, with exact expected manifest and no marker | Validate, flush, rename to an absent published destination, then publish the expected marker last. |
| Exact valid committed bundle, without staging | Revalidate and re-establish required flushes; acknowledge without rewriting or replacing metadata/payloads. |
| Valid published bundle without its marker | Raise a recovery-required outcome without mutation. A separately approved explicit recovery operation must finish it. |
| Staging/published evidence is incomplete, invalid, unsafe, or absent | Hold evidence; reject publication. Never reconstruct missing payloads or start a calculation. |
| Both staging and published exist | Reject ambiguity; preserve both. Never choose, merge, or delete either unit. |
| Valid observed records differ from retained expected records | Raise conflict; preserve all evidence. |

An intact staging-only unit may also be left by an interruption. Its filesystem
state does not reveal its history. Calling this low-level publication API is an
explicit caller action, not automatic controller recovery. The future manager
must distinguish worker publication from authorized controller finalization.

## Publication and durable acknowledgment

The proposed phase sequence is:

1. Verify complete staging and exact expected records under ownership.
2. Flush every declared payload and the canonical manifest. Verify and flush the
   referenced receipt and its parent directory; do not rewrite the receipt.
   Flush necessary staging directories from children to parents, including
   staging and its invocation parent.
3. Recheck the published destination is absent. Rename staging to published on
   the same filesystem, then flush the published directory and invocation
   directory. Never replace an existing destination.
4. Create the expected canonical marker in an exclusively created temporary
   regular file outside the bundle inventory, within the same invocation and
   filesystem. Flush the file and its containing directory. Under ownership,
   require `published/COMMIT.json` to be absent and atomically rename the
   temporary file to that path.
5. Flush the published and invocation directories, verify the final bundle,
   and only then acknowledge completion.

Temporary marker files use a reserved invocation-local prefix and do not become
declared application artifacts. Failures retain remaining temporary evidence;
this operation never adopts or cleans up old temporary files. A successful
rename consumes only the current operation's temporary pathname.

Destination-absence checks rely on cooperating writers honoring the invocation
lock; they are not a claim that ordinary POSIX rename provides adversarial
no-replace semantics. An unexpected cross-filesystem boundary must fail rather
than fall back to a non-atomic directory copy.

Exact committed acknowledgment revalidates all records and payload/receipt
bytes and repeats the necessary file/directory flushes, including the existing
marker. It does not change IDs, timestamps, contents, or lock inodes.

## Failure reporting and recovery boundary

Proposed error categories:

- `BundleIntegrityError`: absent, incomplete, unsafe, inconsistent, or ambiguous
  evidence prevents publication; include stable reason codes and evidence paths.
- `BundleConflictError`: valid evidence differs from retained expected records.
- `BundleRecoveryRequiredError`: published evidence is intact but unmarked;
  inspection is available, while recovery remains a separate explicit action.
- `BundlePublicationError`: an I/O failure during mutation or durable
  acknowledgment; include full qualified identity, operation, phase, staging/
  published/temporary paths, and conservative publication uncertainty.

Before attempted directory rename, a new completion marker has not been
published by this operation, but staging may still exist after failed flushes.
From attempted rename onward, do not assume that an exception means publication
did not occur. Acknowledgment failures of an existing committed bundle also
retain uncertainty. Hard process termination may leave no exception report;
the next caller must inspect the same identity.

Diagnostics must not expose scientific payloads, raw malformed metadata, or
credential-bearing exception text. Hints instruct preservation and same-identity
inspection. They do not authorize a new calculation, cleanup, dependent jobs,
or automatic finalization. A visible marker before successful final flushes is
not, by itself, a durability acknowledgment.

The later recovery slice must specify independently retained expected records,
recovery provenance, crash-safe audit publication, and the boundary with journal
registration. Controller use also requires terminal scheduler evidence and
orphan-writer exclusion, which belong to worker/reconciler integration. No
recovery record or scheduler-success claim is introduced by this proposal.

## Files, risks, and acceptance

Approved implementation files:

- New `src/jobflow_gitlab_slurm/persistence/bundles/storage.py`.
- New `tests/persistence/bundles/` using inert payloads and receipts.
- One module import in `ci/check-wheel.sh`.
- Documentation recording approval, installation, evidence, and next checkpoint.

Acceptance covers caller-model rejection before writes; exact manifest/marker
binding; complete publication and second-process inspection; exact committed
acknowledgment without rewriting; changed-record conflicts; unchanged held
evidence; same-invocation contention; ownership lifetime checks; run-lock
availability during payload I/O; safe paths and exhaustive inventories; flush
and rename ordering; cross-filesystem rejection; failure injection at every
publication/acknowledgment phase; abrupt process termination around rename and
marker publication; retained temporary evidence; no consumer imports or
scientific execution; and sanitized uncertainty reports.

Pass the existing full local quality gate, including 100% combined statement/
branch coverage, build, and isolated wheel verification after user application.
Local crash tests establish observed process-interruption behavior, not actual
power-loss durability or cross-host guarantees. Remote CI and live filesystem
verification remain separate gates.

Only documentation is changed by the assistant. The supplied source/tests/import
entry are installed and the local acceptance gate is user-reported passing. Approval
authorizes copy/paste delivery for the publication-only slice, not assistant
code edits, live recovery actions, or Stage 2 close-out. Explicit recovery and
its audit contract were subsequently approved and installed in separate slices;
see the [cycle overview](../002-durable-run-state.md). Their presence does not
extend this publisher's API to automatic recovery or enforce producer intent
ordering.

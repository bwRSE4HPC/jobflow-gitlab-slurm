# Bundle storage v1: ownership and locking review

Status (2026-10-08): **ownership/locking primitive installed; full local
acceptance gate passed**. Tests, coverage, and packaging were verified by the
assistant; the user reports the final verification gate passed.
Attempt/invocation metadata storage has passed its local acceptance gate.
Read-only bundle inspection is also installed with its full local acceptance
gate user-reported passing; see [bundle inspection v1](bundle-inspection-v1.md).
Bundle publication source, tests, and wheel-check import are installed, with
its full local acceptance gate user-reported passing. Explicit recovery is also
installed with its local gate user-reported passing; see
[bundle recovery v1](bundle-recovery-v1.md).
This contract refines the installed [bundle models](bundle-record-contract-v1.md)
and accepted [recovery policy](../decisions/0002-attempt-publication-and-recovery.md).
It introduces no execution or scheduler-success claim.

## Ownership and publication boundary

An invocation owns one output staging directory and at most one published
bundle. The future worker holds exclusive invocation-level ownership before
writing any backend-managed output and until publication or exit. Scientific
execution and large output I/O must not hold the existing per-run lock.

Worker registration alone is not ownership. A second cooperating process with
the same invocation identity must receive a busy outcome, never concurrently
write the same staging directory or start another calculation there. Separate
invocation IDs retain separate evidence; they do not select a winning result
or bypass the future one-active-attempt scheduling policy.

Invocation-local lock and bundle layout; opaque bundle inspection/publication
and explicit recovery are installed in their separate slices. Worker production
of staged payloads remains Stage 3 work:

```text
invocations/<invocation_id>/
  invocation.json                 implemented immutable metadata
  .bundle.lock                    implemented persistent invocation lock
  staging/                        opaque producer-prepared output staging
  published/                      complete or interrupted publication
    payload-manifest.json
    job-document.json
    response.json
    files/...
    data/...
    COMMIT.json
```

The lock is outside both staging and published directories. Never unlink or
replace its inode while the run is active: otherwise independent processes
could lock different inodes under the same pathname. Missing lock files are
not silently created by read-only inspection or interpreted as no active writer.

## Approved lock lifecycle and ordering

1. Explicitly provision the persistent regular lock file for an already-valid
   invocation under the existing run lock. Flush its file and directory entry
   before acknowledging provisioning. Repeated provisioning preserves the
   inode; unsafe paths, incompatible parents, and special files are rejected.
   Provisioning acquires no invocation lock and creates no output staging.
2. Release the run lock before trying the invocation lock. Open the existing
   regular file without following symlinks and acquire exclusive nonblocking
   `flock`. Return an invocation-specific busy error on contention.
3. While holding the invocation lock, briefly acquire the run lock to revalidate
   the invocation/attempt/definition chain. Return a frozen ownership handle
   bound to the full run/job/index/attempt/invocation identity. Release the run
   lock before yielding ownership to the caller.
4. Keep the invocation lock until the ownership context exits; release it on
   exceptions and process termination through descriptor closure. Never delete
   the lock file. Busy/error paths release any locks already acquired.

When both locks are needed, the order is **invocation, then run**. No operation
may hold the run lock while acquiring an invocation lock. No implicit waiting,
retries, lock stealing, or nested public run-lock acquisition is permitted.
Metadata provisioning is a separate step, not an exception allowing inverted
lock acquisition. Future publication APIs must consume existing ownership
rather than reacquire their own invocation lock recursively.

Implemented APIs in `invocation_locking.py`:

- `provision_invocation_lock(runs_root, run_id, job_uuid, job_index, attempt_id,
  invocation_id)` returns a frozen `InvocationLockHandle` after explicit
  provisioning/flushes; the handle conveys no ownership.
- `owned_invocation` with the same arguments is a context manager yielding
  frozen `InvocationOwnership`. It opens an existing lock, acquires it, and
  revalidates metadata under the run lock before yielding.
- `InvocationOwnership.require_active()` rejects retained handles after context
  exit and handles inherited into another process. This is a lifecycle guard
  for cooperating callers, not an authenticated capability or security sandbox.

Invocation contention raises `InvocationBusyError`; existing run contention
continues to raise `RunBusyError`. Missing/unsafe/inaccessible lock state raises
`InvocationLockIntegrityError`. Failed provisioning flushes raise
`InvocationLockProvisionError` with identity, phase, path, and uncertainty;
preserve the lock inode and explicitly retry the same identity. Parent metadata
errors retain their existing classifications. No automatic retry is performed.

The lock coordinates cooperating backend processes; it is not scheduler
evidence, authorization, a heartbeat, or an adversarial security boundary.
Acquiring it does not prove that an orphaned scientific subprocess has stopped
writing. Stage 3 must specify subprocess supervision and descriptor inheritance;
Stage 4 must establish terminal scheduler evidence and the site's visibility
delay before controller finalization. Cross-host behavior remains a live gate.

## Subsequent inspection and publication slices

Installed read-only bundle inspection uses the existing invocation lock and brief
metadata validation, then streams payload verification without holding the run
lock. Reads must not provision locks, flush files, repair, or select results.
Reports should distinguish absent, staging-only, published-without-marker,
committed, invalid, and busy evidence, not collapse these into scientific status.

Before publication, verify the full record chain, canonical manifest/marker
encoding, exact declared payload sizes/hashes, and the run-relative scheduler
receipt's bytes. Receipt-schema validation and actual scheduler association
remain later scheduler work. Document/Response/additional-data payloads stay
opaque; Stage 3 supplies their jobflow semantic validation.

The bundle inventory should be exhaustive: reject undeclared files, symlinks,
special files, and unsupported directories within the publication unit.
Only declared payloads, manifest, marker when applicable, and necessary
directory ancestors are allowed. Keep diagnostic logs and unrelated scientific
scratch outside this unit rather than silently discarding them.

The intended protocol remains: validate and flush payloads plus manifest,
rename staging to an absent published destination on the same filesystem,
flush directory entries, then publish `COMMIT.json` last and durably acknowledge
it. An existing marker/manifest is never replaced with changed content.
Same-record acknowledgment revalidates the entire bundle and re-establishes
required flushes. Publication is necessary, not sufficient, for result eligibility.

Explicit recovery requires retained expected manifest/marker records and
exclusive ownership. It may finish intact staging or a valid published unit
missing its marker, or acknowledge an exact committed unit. It must preserve
ambiguous, conflicting, corrupt, or partial evidence and reject two competing
staging/published units rather than silently choose one. It never reconstructs
missing Response/document bytes, parses application output, or executes a job.
Recovery evidence and journal registration require their own reviewed contract;
this proposal does not authorize a controller recovery action from a bare lock.

## Implementation sequence and acceptance

1. **Locally verified invocation ownership primitive**: `invocation_locking.py`, tests, wheel
   import entry, and documentation. Explicit lock provisioning and an ownership
   context only; no bundle files, worker, receipt schema, CLI, or scheduler.
2. **Locally verified read-only bundle inspection**: stream exact-byte validation and classify
   evidence under ownership; no repair or publication. Module/tests and the
   wheel-check import entry are installed; the full local acceptance gate is
   user-reported passing. No assistant verification rerun is claimed.
3. **Later reviewed bundle publication and explicit recovery**: APIs, durable phase
   diagnostics, exact retry identity, and preserved recovery provenance.
   The [approved publication-only slice](bundle-publication-v1.md) is installed:
   source, tests, and wheel-check import are present, with its full local
   acceptance gate user-reported passing. Explicit recovery and its audit
   contract remain subsequent review work.

First-slice checks: malformed identities fail before writes; missing invocation
is never created; unsafe containers/lock files are rejected; provisioning is
inode-stable and crash-retry safe; ownership is nonblocking; same-invocation
processes exclude each other while distinct invocations can coexist; metadata
is revalidated under the correct lock order; exceptions and abrupt process
exit release ownership; contention/error paths leak no descriptors or locks;
no scientific payload is imported or executed. Pass existing quality, 100%
statement/branch coverage, build, and wheel gates after user application.

Main risks: inode replacement, inverted lock ordering, duplicated ownership,
mistaking an acquired lock for scheduler termination, and treating a commit
marker as jobflow success. The first slice intentionally does not claim that
it solves worker lifecycle, scheduler eligibility, or controller recovery.
Module, tests, and the wheel import entry are installed. On the user's explicit
verification request, the assistant ran 62 focused tests and the full 1,329-test
suite successfully, with 100% statement and branch coverage. Locked sync,
formatting checks, shell syntax, CLI help, wheel/sdist build, isolated wheel
verification, and diff whitespace checks passed. The user subsequently reports
the final verification gate passed, completing local acceptance. The assistant
did not repeat those final checks. No source, tests, or CI definitions were
edited by the assistant.
Remote CI, cross-host locking, worker lifecycle, and explicit recovery I/O
remain pending. Bundle publication's full local gate is user-reported passing;
no assistant verification rerun is claimed.

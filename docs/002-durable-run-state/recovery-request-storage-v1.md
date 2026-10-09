# Recovery-request storage v1

Status (2026-10-08): **request-only slice installed; full local acceptance
gate user-reported passing**.
Recovery-audit models/codecs are installed with their full local acceptance
gate user-reported passing. This approved slice adds only owned immutable request
persistence and readback. It does not introduce completion-receipt storage,
bundle recovery, journal registration, scheduler policy, or calculation execution.

## Purpose and bounded scope

An explicit recovery needs a durable request before it changes bundle evidence.
A process lock alone cannot identify unfinished work after its owner exits.
Retain the request identity and exact bytes so the next process can inspect and
retry the same operation rather than create an unrelated operation.

Approved files:

- `src/jobflow_gitlab_slurm/persistence/recovery/requests.py`.
- `tests/persistence/recovery/`.
- One module import in `ci/check-wheel.sh`.
- Relevant contract/progress documentation.

Do not change existing model schemas, intent storage, bundle publisher APIs,
dependencies, CLI commands, or run/journal formats in this slice.

## Approved APIs

```python
read_owned_recovery_request(ownership, recovery_id) -> RecoveryRequestHandle
persist_owned_recovery_request(ownership, request) -> RecoveryRequestHandle
```

`RecoveryRequestHandle` is an immutable observation containing the final path,
validated `RecoveryRequestRecord`, SHA-256, and byte count. Readback performs
no writes or flushing. Successful persistence acknowledges exact request bytes
and their directory entries; neither handle proves permission to recover,
current bundle eligibility, or successful scientific execution.

Both APIs consume active `InvocationOwnership`, enforce its originating
process/context guard, and derive paths from validated identities. Do not
reacquire the invocation lock or hold the run lock during metadata I/O.
Concurrency remains fail-fast at the existing ownership boundary; manager-level
retry policy is later work.

## Layout and operation identity

```text
<invocation-directory>/
  publication-intent.json
  recoveries/
    <recovery-id>/
      request.json
      .recovery-request-<opaque-suffix>  retained temporary evidence, if present
```

The approved storage invariant reserves **one recovery ID per invocation**, including after
eventual completion. An invocation represents one immutable expected bundle;
verified completion does not require a second recovery identity. A new scientific
attempt is a separate, later explicitly authorized operation, not an audit reset.

This is an additional approved storage invariant, not a claim that the installed
models enforce it. Under invocation ownership:

- An absent namespace permits initial publication of the caller-retained ID.
- A sole existing directory for that ID permits inspection/exact retry.
- A different ID blocks creation, even if its request is missing or completion
  evidence later exists. Report the existing ID and inspection location.
- Multiple ID directories are ambiguous; malformed names, symlinks, special
  entries, or unrecognized files are held as unsafe evidence.
- An existing empty ID directory is an interrupted preparation, not evidence
  that recovery mutation happened. Retry remains restricted to that ID.

Never silently select an ID by directory order, delete earlier evidence, or
allocate IDs/timestamps inside storage. The caller retains the complete request
and both `recovery_id` and `journal_event_id` across retries.

## Validation and state-dependent behavior

Before new filesystem changes, revalidate caller metadata and the qualified
definition/attempt/invocation chain, reopen the installed publication intent,
and verify the request's exact intent reference against retained bytes.

Before creating a new final request, use owned bundle inspection to verify valid
content and that the declared observation matches actual evidence. Verify the
exact expected manifest; if already committed, also verify the exact expected
marker. Absent, partial, invalid, unsafe, or ambiguous evidence forbids a new
request. This is read-only preflight, not bundle mutation or durable completion.

For an existing final request, require exact canonical-byte agreement, including
actor, time, original observation/action, and event identity. A differing valid
record is a conflict. Never replace it or update its observation after forward
progress. Exact retry may reacknowledge an existing request without requiring
the bundle to remain in the original state; the explicit recovery API separately
inspect current evidence and reject regressions or corruption.

Readback validates the retained intent and full parent binding, but neither
creates a missing namespace nor changes bundle state. Missing request reports
absence of final audit metadata, not absence of scientific execution or recovery
mutation. Preserve temporary and surrounding evidence.

## Publication and uncertainty

Create required directories only under the owned invocation. Validate existing
directories and final/temporary files without following symlinks or opening
special files. Use the established safe-descriptor and directory-sync helpers.
The trusted-root/cooperating-writer contract remains unchanged; this is not an
adversarial no-replace filesystem security boundary.

For initial publication: write canonical bytes to a same-filesystem private
temporary regular file, flush it and its directory entry, publish the immutable
final name, flush the final file and the complete newly affected parent-directory
chain, then reread and verify exact bytes. Do not replace an existing final file.

An exact existing final request must be revalidated and durably reacknowledged
before successful persistence returns. Merely observing a final name is not
equivalent to acknowledging it.

Temporary evidence is never automatically removed. If a final request is absent:

- An empty matching ID directory permits same-ID publication.
- A single safe temporary file with exactly the retained canonical bytes may be
  reused and durably published under the same ID after all new-request checks.
- Partial, conflicting, malformed, unsafe, or multiple temporary candidates hold
  publication for inspection. Do not regenerate metadata, choose an arbitrary
  candidate, bypass the hold with another ID, or trigger scientific execution.

If an exact final request exists, retained private temporary evidence must still
be safe; its presence alone is not a second published request. It is preserved,
not silently cleaned up. It cannot authorize a different request or operation.

Report failing operation/phase, relevant identity and paths, retained temporary
location, errno when available, and whether final request publication is uncertain.
Set uncertainty conservatively at a final-name publication attempt or failed
acknowledgment of an existing final request. Directory/preparation uncertainty
must also be reported without implying that bundle mutation occurred.

A failed or uncertain request acknowledgment never authorizes subsequent bundle
mutation. Retry/inspect the same identity. Failures must retain evidence and
provide useful sanitized hints rather than dumping request bodies or exception
causes that could expose consumer details.

## Separate completion boundary

The separately reviewed [completion-receipt storage slice](recovery-receipt-storage-v1.md)
is installed with its local gate user-reported passing. It verifies and durably
acknowledges the exact committed bundle before recording filesystem completion;
a caller-supplied receipt model or stale handle is insufficient. It does not call
the ordinary bundle publisher as an unchecked acknowledgment shortcut.

The installed [explicit recovery operation](bundle-recovery-v1.md) consumes an
acknowledged request and reconciles permitted forward progress without relaunching
a calculation. Its caller must establish authorization and orphan-writer
quiescence separately. Request, bundle, receipt, and journal
publication are separate durability boundaries, not one atomic transaction.

## Acceptance and remaining risks

Offline tests must cover validation before writes, read-only absence handling,
safe directory/file types, wrong ownership context/process, exact intent binding,
valid bundle preflight, same-ID exact retry, original-observation preservation,
different/multiple/empty ID directories, temporary-file holds and exact reuse,
immutable conflicts, and injected failures across each publication/flush phase.
Use real temporary files and separate-process ownership checks where relevant.

Pass formatting, lint, full tests, agreed 100% combined coverage, build, and the
isolated-wheel gate. Source/tests and the wheel import entry are installed.
On 2026-10-08, explicitly requested assistant checks passed formatting, shell
syntax, build, and isolated-wheel verification; full-suite execution reached
100% combined statement/branch coverage. The user subsequently reports the full
local acceptance gate passed. No successful final test count was supplied, and
no assistant rerun is claimed for that final gate.
Cross-host locks, filesystem durability, and power-loss behavior remain
live deployment gates; local success cannot establish them.

Primary risks: fresh-ID bypass, false durable acknowledgment, accepting a declared
observation without checking bytes, destroying failure evidence, and mixing request
storage with bundle repair. Approval covers this request-only slice;
receipt storage and explicit recovery remain separately reviewed work.

Completion-receipt storage is covered by the separate slice linked above.
The subsequent [explicit bundle recovery slice](bundle-recovery-v1.md) is
installed with its local gate user-reported passing. Journal association is a
separate slice in [002-durable-run-state](../002-durable-run-state.md);
see [progress](../progress.md) for current acceptance and remaining work.

Supplied implementation diagnostics distinguish missing, integrity, conflict,
inspection, and publication errors. Publication reports separately retain
preparation uncertainty and final-request publication uncertainty, plus operation,
phase, original recovery identity, and any known temporary path. A failed
acknowledgment never grants permission to mutate the bundle.

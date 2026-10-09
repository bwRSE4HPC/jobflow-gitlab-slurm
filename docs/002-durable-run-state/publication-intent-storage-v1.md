# Publication-intent storage v1

Status (2026-10-08): **approved, installed, and locally user-verified**.
Source, tests, and the wheel-check import are present. The user reports the
full supplied verification gate passed, including the agreed 100% combined
coverage and packaging checks. No successful final test count was supplied;
no independent assistant rerun is claimed.
The intent models/codecs and bundle publisher have passed their local gates
on user-reported evidence. This approved slice persists expected records;
it does not implement recovery, scientific execution, or Stage 2 close-out.

## Scope and layout

Retain one immutable intent for each existing invocation:

```text
<invocation-directory>/
  invocation.json
  .bundle.lock
  publication-intent.json         independent expected-record metadata
  .publication-intent-<random>     retained interrupted-write evidence
  staging/                        existing bundle-publication boundary
  published/                      existing bundle-publication boundary
```

The intent is outside both bundle inventories. It contains the complete
expected manifest/commit records, not copied scientific payloads. Neither
missing invocations nor missing ownership locks are created by this API.

## Approved API and ownership

New module: `src/jobflow_gitlab_slurm/persistence/publication/storage.py`.

- `publish_owned_publication_intent(ownership, intent)` publishes a new intent
  or durably acknowledges an exact existing one.
- `read_owned_publication_intent(ownership)` validates and returns an existing
  intent without writing, flushing, initializing, or repairing anything.
- Both return a frozen `PublicationIntentHandle` containing path, validated
  record, and exact canonical-byte SHA-256 and size. It retains no ownership
  and is not an authorization or indefinite integrity guarantee.

Call `ownership.require_active()` before I/O. Revalidate supplied/stored
metadata against the definition, attempt, and invocation already verified by
the ownership context. Use the existing invocation-before-run lock order;
these operations consume ownership rather than reacquire either lock. No run
lock is held during intent I/O.

The caller supplies and retains all IDs/timestamps. No identity or timestamp
is generated or changed during retry. An exact existing intent means exact
canonical bytes, including its intent ID, timestamp, and both nested records.

## New publication and durable acknowledgment

1. Revalidate caller models and parent binding before filesystem changes.
   Require a real invocation directory and safe regular metadata files.
2. If an intent exists, decode and validate it before comparing canonical
   bytes. An intact but different record is a conflict; invalid evidence is
   an integrity hold. Neither case authorizes replacement.
3. If no intent exists, require the `published` pathname to be absent, including
   dangling symlinks and other unsafe entries. Do not attach newly generated
   expectations retroactively to already-published or interrupted publication.
   Staging may exist: persisting caller-supplied expectations does not validate
   its payloads or authorize their publication.
4. Create an exclusive private temporary regular file in the invocation
   directory, write exact canonical bytes, flush the file and directory, and
   verify the same-filesystem rename boundary. Recheck destination absence
   under ownership; atomically rename to `publication-intent.json`.
5. Flush the final file and invocation directory, reopen/revalidate the exact
   bytes, and only then acknowledge. Ordinary bundle publication must not
   follow a failed or uncertain acknowledgment.

Exact existing acknowledgment revalidates the record, repeats necessary file/
directory flushes, and verifies the expected bytes without rewriting anything.
It remains possible after bundle publication; it does not finalize the bundle.
Read-only reopening performs no flush and makes no durability acknowledgment.

Ordinary POSIX rename plus destination checks relies on cooperating writers
honoring the invocation lock; it is not adversarial no-replace protection.
No non-atomic copy fallback is allowed. Preserve remaining temporary files
on failure; never adopt or delete old temporary evidence automatically. An
explicit same-record retry may use a fresh temporary file if the final intent
is absent and the new-publication preconditions still hold.

## Failure classification and safety

Keep missing-intent, integrity, valid-record conflict, unavailable I/O, and
publication/acknowledgment failure distinguishable. Missing intent means
expected evidence is unavailable, not that a calculation never ran.
Inactive ownership retains its existing error classification.

Mutation diagnostics include qualified invocation identity, operation, phase,
final/temporary paths, stable reason, errno when available, and conservative
intent-publication uncertainty. Before attempted rename, no final intent was
published by this operation. From attempted rename onward, or during existing
acknowledgment, publication/durability can be uncertain. Report intent
uncertainty separately from scientific execution or bundle publication.

Suppress raw malformed metadata, payload contents, and decoder cause text.
Hints direct same-identity inspection and evidence preservation; no cleanup,
scientific rerun, recovery finalization, or dependent scheduling is authorized.

## Integration boundary

The installed low-level `publish_owned_bundle` remains unchanged and does not
yet enforce presence of an intent. A later reviewed producer integration must
persist and durably acknowledge intent before calling that publisher, using
the retained nested records. This slice provides the storage primitive, not
an enforced end-to-end publication workflow.

The separately approved [audit](recovery-audit-v1.md) and
[explicit recovery](bundle-recovery-v1.md) slices are installed; their APIs do
not authenticate recovery actors or establish scheduler/writer exclusion. An
intent alone neither attests terminal Slurm state nor excludes orphan writers.
Do not infer safe controller finalization merely from ownership acquisition.

## Acceptance and affected files

Approved changes:

- New `publication_storage.py` and `tests/persistence/publication/`.
- One import in `ci/check-wheel.sh`.
- Documentation recording installation, gate evidence, and next checkpoint.

Tests cover validation before writes; canonical parent binding; safe path/file
types; missing-intent reporting without mutation; exact same-record retry;
different ID/timestamp/nested-record conflicts; refusal of retroactive creation;
temporary-evidence preservation; unchanged bundle payloads/marker and lock
inode; inactive ownership; separate-process reopening and contention; run-lock
availability; descriptor cleanup; failures at write/fsync/rename/reopen phases;
and abrupt process exits around rename. No scientific imports or execution.

The user reports the existing full local quality, 100% combined coverage,
build, and isolated-wheel gates passed after user application. No assistant
verification execution accompanied this slice. Remote CI, cross-host filesystem
behavior, and power-loss durability remain separate gates.

Complete source/tests and the wheel-import insertion were supplied for user
copy/paste and are now installed. Only documentation is edited by the assistant;
no source/test/CI changes or verification execution are performed. Approval
does not extend to recovery execution, producer enforcement, or journal changes.

The models/codecs scope of the [recovery-audit contract](recovery-audit-v1.md)
is installed with its full local acceptance gate user-reported passing,
including stable retry identity and exact retained-record binding.
Request/receipt storage and explicit recovery are separately implemented slices
in [002-durable-run-state](../002-durable-run-state.md). Their approvals do not
extend this intent-storage API. See [progress](../progress.md) for the current
checkpoint rather than treating this contract as a scheduling plan.

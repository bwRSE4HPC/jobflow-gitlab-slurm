# Publication intent v1

Status (2026-10-08): **models/codecs-only slice approved, installed, and locally
user-verified**. Source, tests, and the wheel-check import are present. The user
reports the full supplied verification gate passed, including the agreed
100% combined coverage and packaging checks. No successful final test count
was supplied; no independent assistant rerun is claimed.
Bundle publication has passed its full local gate on user-reported evidence.
This document records the immutable expected-record model/codec slice within
[002-durable-run-state](../002-durable-run-state.md).
It does not approve recovery execution, new filesystem operations, or Stage 2
close-out.

## Why retain an intent

The installed publisher accepts caller-retained expected manifest and commit
records. In-memory retention is insufficient after that caller exits. A later
process must not derive its expected records solely from the interrupted
bundle it is trying to validate: that would turn observed evidence into its
own recovery authority.

Retain both complete expected records separately from `staging/` and
`published/`, before ordinary publication begins. A hash alone cannot reconstruct
the expected commit's identity and timestamp. Reuse the existing canonical
bundle codecs rather than regenerating a timestamp during recovery.

This separation supports accidental-corruption detection and conservative
recovery under the trusted-root/cooperating-writer model. It is not an external
witness, authenticated authorization, or protection against coordinated
rewriting of the workspace. Timestamps do not prove publication ordering.

## Approved first slice: models and codecs only

New module: `src/jobflow_gitlab_slurm/persistence/publication/records.py`.
New tests: `tests/persistence/publication/test_records.py`.
Its isolated-wheel import is installed in `ci/check-wheel.sh`.

Approved frozen, strict `PublicationIntentRecord` fields:

| Field | Meaning |
| --- | --- |
| `schema_version` | Required integer `1`; reject booleans and unsupported versions. |
| `kind` | Required literal `publication-intent`. |
| `intent_id` | Caller-retained lowercase UUIDv4; unchanged on retries. |
| `created_at` | Caller-retained canonical UTC timestamp; unchanged on retries. |
| `expected_manifest` | Complete strict `BundleManifest`. |
| `expected_commit` | Complete strict `BundleCommit`, including its original timestamp. |

Derive qualified run/job/attempt/invocation identity from the nested manifest;
do not maintain a second set of top-level identity fields. Validate that both
nested records share that identity, their timestamps are consistent, and the
commit pins the exact size and SHA-256 of canonical manifest bytes. Intent
creation must not precede manifest creation. The intended commit timestamp
need not be later than intent creation: its value is retained metadata, not
proof that the physical commit already happened.

Approved operations:

- `encode_publication_intent(record)`: revalidate and emit deterministic UTF-8
  JSON bytes, using the existing sorted-key, compact, finite-only convention.
- `decode_publication_intent(data)`: reject malformed, duplicate-key,
  noncanonical, nonfinite, unsupported, or unknown-field metadata; no Monty
  decoding or consumer imports.
- `validate_publication_intent(definition, attempt, invocation, intent)`:
  revalidate records and use the installed bundle cross-record validation to
  enforce parent identity, definition/code/runtime pins, and timestamp binding.

No payload I/O, lock acquisition, timestamp/identity generation, directory
creation, journal mutation, scheduler query, or jobflow execution belongs in
this slice. Model validity does not prove that an intent was durably persisted,
that payloads are intact, or that recovery is authorized.

## Persistence sequence and integration boundary

1. **Intent persistence:** installed invocation-local
   `publication-intent.json`, outside both bundle inventories. Publish once
   under active invocation ownership, durably acknowledge before beginning
   ordinary bundle publication, and reject conflicting replacements. Decide
   the integration with the existing low-level publisher without silently
   changing its approved interface. A missing intent holds recovery; do not
   synthesize one from interrupted evidence automatically.
   The separate [intent-storage slice](publication-intent-storage-v1.md) is
   installed with its full local gate user-reported passing. Producer enforcement
   remains Stage 3 work; recovery/audit primitives are now installed separately.
2. **Recovery audit:** persist a stable, immutable recovery request before
   mutation, recording the expected intent digest, actor/operation identity,
   observed evidence, and intended action. Publish a separate completion
   receipt after bundle validation and durable acknowledgment. A request
   without a receipt means recovery is incomplete or uncertain, not absent.
   Schemas, retry identities, safe diagnostics, and failure ordering are
   specified in the installed [audit contract](recovery-audit-v1.md),
   [request storage](recovery-request-storage-v1.md), and
   [receipt storage](recovery-receipt-storage-v1.md) slices.
3. **Explicit recovery:** under ownership, revalidate retained expectations
   and all payload/receipt bytes. Finish intact staging, finish a valid
   published-but-unmarked unit, or acknowledge an exact existing commit.
   Reject partial, corrupt, conflicting, unsafe, or ambiguous evidence;
   never merge units or reconstruct missing document/Response bytes. Only an
   exact, uniquely selected temporary marker matching retained expectations
   may be reused; arbitrary or ambiguous temporary evidence is not adopted.
   Preserve original and recovery evidence. The
   [explicit recovery slice](bundle-recovery-v1.md) implements this primitive.
4. **Journal association:** register verified recovery evidence separately
   using a retained event identity. A bundle/audit receipt and the journal
   cannot be assumed to commit atomically. The installed
   [recovery registration](recovery-journal-v1.md) and
   [publication registration](publication-journal-v1.md) slices retain exact
   event identities. Missing registration never authorizes a calculation;
   scheduler reconciliation and execution-state projection remain later work.

Controller recovery additionally needs terminal scheduler evidence and
orphan-writer exclusion from later worker/reconciler integration. Merely
acquiring an invocation lock does not establish either condition. The Stage 2
primitive operates on explicitly supplied inert evidence; it does not implement
automatic controller finalization or scientific-result eligibility.

## First-slice acceptance and risks

Test strict field/version/UUID/timestamp validation; exact nested identity and
manifest digest/size binding; parent provenance mismatch; immutability;
canonical round trips; duplicate keys and nonfinite values; stable retained
timestamps/IDs; and no scientific imports or filesystem side effects.
The user reports the existing full local quality, 100% combined coverage,
build, and isolated-wheel gates passed after user application. The assistant
did not execute verification for this slice. Remote CI and live filesystem
verification remain separate gates.

Complete source/test files and the wheel-import insertion were supplied for
user copy/paste and are now installed. Only documentation was edited by the
assistant. Intent persistence followed as a separately approved, installed,
and locally user-verified slice. Audit storage, explicit recovery, and journal
association subsequently followed as separately approved installed slices.
Producer integration remains Stage 3 work.

The [recovery-audit contract](recovery-audit-v1.md) details the approved
models-only slice and later ordering constraints. Source/tests and the wheel
import are installed; its full local acceptance gate is user-reported passing.
Their storage/recovery APIs were separately approved and installed; live
recovery actions still require explicit operational authorization.

Primary risks are circular recovery trust, regenerating expected marker bytes,
confusing a persisted intent with permission to execute/recover, and treating
metadata integrity as scheduler or scientific success. This slice addresses
the record boundary only. Intent/audit persistence and explicit recovery are
installed; worker producer ordering, scheduler-aware recovery, and live
verification remain separate later gates.

# Package and test organization

Status (2026-10-09): the user adopted the refined source and test organization
and explicitly authorized autonomous implementation and verification in the
existing checkout. The hierarchy and test splits are now implemented; final
verification evidence is recorded in [progress](../progress.md). Scope includes separate
run foundations and read-only queries, record-model ownership, explicit internal
interfaces/dependency rules, bounded filesystem-helper extraction, architecture
checks, and responsibility-based test splits/support and collection configuration.
This organization supersedes the earlier layouts. The task-specific authorization
replaces guided delivery for this refactor only; it does not authorize Git,
remote, package-publication, or HPC operations.

## Design objectives

- Make responsibility and dependency direction discoverable from qualified paths.
- Keep persistence formats and safety behavior unchanged while improving ownership.
- Locate tests by the contract they verify, not by their current source file or
  whether their mechanics happen to involve a subprocess.
- Share explicit setup and observation mechanics without hiding important state.
- Establish inexpensive offline checks against architectural regression as the
  worker and scheduler layers are added later.

## Source hierarchy and names

Keep `__init__.py` and `cli.py` at the package root. Group `site.py`,
`request.py`, `loader.py`, and `registry.py` under `config/`. Group the offline
persistence implementation under `persistence/`:

```text
src/jobflow_gitlab_slurm/
├── __init__.py
├── cli.py
├── config/
│   ├── site.py
│   ├── request.py
│   ├── loader.py
│   └── registry.py
└── persistence/
    ├── _filesystem.py
    ├── artifacts.py
    ├── runs/
    │   ├── records.py
    │   └── storage.py
    ├── queries/
    │   ├── inspection.py
    │   └── discovery.py
    ├── attempts/
    │   ├── records.py
    │   ├── definitions.py
    │   ├── storage.py
    │   └── ownership.py
    ├── journal/
    │   ├── records.py
    │   └── storage.py
    ├── bundles/
    │   ├── records.py
    │   ├── inspection.py
    │   └── storage.py
    ├── publication/
    │   ├── records.py
    │   ├── storage.py
    │   └── registration.py
    └── recovery/
        ├── records.py
        ├── requests.py
        ├── receipts.py
        ├── operations.py
        └── registration.py
```

Domain directories have minimal `__init__.py` files, omitted from the drawing.
The following mapping records the moves from the pre-refactor flat package:

| Directory | Module mapping from the former package root |
| --- | --- |
| `persistence/` | `artifacts.py`: keep name; add the bounded internal `_filesystem.py` extraction described below. |
| `persistence/runs/` | `records.py`, `storage.py`: keep names; move `ExternalArtifact` and `ExternalArtifacts` from storage into run records. |
| `persistence/queries/` | `discovery.py`, `inspection.py`: keep names; separate higher-level read-only queries from run foundations. |
| `persistence/attempts/` | `attempt_records.py` → `records.py`; `definition_storage.py` → `definitions.py`; `attempt_storage.py` → `storage.py`; `invocation_locking.py` → `ownership.py`. |
| `persistence/journal/` | `event_records.py` → `records.py`; `journal.py` → `storage.py`. |
| `persistence/bundles/` | `bundle_records.py` → `records.py`; `bundle_inspection.py` → `inspection.py`; `bundle_storage.py` → `storage.py`. |
| `persistence/publication/` | `publication_records.py` → `records.py`; `publication_storage.py` → `storage.py`; `publication_journal.py` → `registration.py`. |
| `persistence/recovery/` | `recovery_records.py` → `records.py`; `recovery_storage.py` → `requests.py`; `recovery_receipt_storage.py` → `receipts.py`; `bundle_recovery.py` → `operations.py`; `recovery_journal.py` → `registration.py`. |

### Run foundation and query ownership

`runs/records.py` owns run metadata and external-artifact validation/encoding,
without filesystem operations. The pure `ExternalArtifact` and
`ExternalArtifacts` models moved unchanged from the former `storage.py`: fields,
field order, validators, strictness, and encoded bytes are retained. Keep `RunHandle`
and operational storage errors with `runs/storage.py`, alongside creation,
reopening, and run locking. A dataclass is not automatically a record schema.

`queries/` owns read-only views over run and journal state. Its higher-level
dependency direction is visible in the path rather than hidden alongside the
foundation. Query operations must not repair state, publish evidence, or
schedule execution. The hierarchy does not add a new query API or behavior.

### Internal filesystem ownership

`persistence/_filesystem.py` is the narrowly scoped internal module for
shared descriptor and synchronization mechanics. Inspection found bundle
the pre-refactor inspection/storage and publication/recovery code importing
`_regular_descriptor`, `_sync_file`, or `_sync_directory` from definition
storage. Run storage and definition storage also contained matching directory
synchronization implementations. These primitives should not be owned by a
job-definition feature.

The extraction covers only mechanics with established equivalent semantics: the
regular-file descriptor lifetime and file/directory synchronization functions.
Use clearly named internal functions; the private module name distinguishes
them from the consumer API. Keep path qualification, run/invocation identity,
domain validation, recovery selection, and publication sequencing in their
domains. Preserve descriptor flags, cleanup behavior, exception classification,
existing diagnostic behavior, and flush order. Where callers need different
diagnostics, use a thin domain adapter rather than silently normalizing errors.

Do not turn this into a universal transaction helper or merge superficially
similar readers/writers with different safety semantics. Other private-helper
dependencies must be inventoried: same-domain cooperation can remain explicit;
cross-domain calls need a named internal interface or recorded justification,
not an accidental import of a feature's private implementation.

This extraction is implemented with focused descriptor/flush tests; the fresh
full gate includes the existing failure matrices. Domain sequencing and errors
remain separate from the shared primitives.

Use minimal package initializers and explicit imports with descriptive module
aliases where needed. Do not add empty packages for future worker, scheduler,
or controller implementations. The selected migration policy is a clean break
from the old flat Python import paths, without compatibility wrappers. Inventory
and update documented/consumed imports before moving modules; this policy does
not authorize changes to persisted formats or CLI commands.

### Deliberate internal interfaces

Inventory cross-domain private-helper calls and assign each shared operation
an owner and an explicit package-internal contract. Same-domain cooperation
does not require promotion to a public consumer API. Cross-domain cooperation
must not depend accidentally on unrelated feature implementation details.

In particular, run inspection uses `_read_events_locked` while already
holding the run lock. Preserve that ownership arrangement: its internal reader
contract must state that the caller holds the lock, the reader does not reacquire
it, the operation is read-only, and contextual failure classifications remain
unchanged. Substituting a lock-acquiring public reader would not be a mechanical
refactor.

Publication and recovery may share deliberately owned bundle durability
operations. Preserve their sequencing, flush order, and domain error hierarchy;
do not replace those protocols with a generic transaction framework. Filesystem
mechanics belong in `_filesystem.py`; identity checks, contextual errors, and
publication/recovery decisions remain in their domains. Extract additional
modules only when the inventory establishes a cohesive responsibility, not
merely to avoid a private name.

### Dependency rules

An arrow below means “may depend on”; it is not execution order. The table
owns the detailed rules, including the separate read-only query domain.

```text
cli -> config and persistence public/internal entry points

recovery ------> publication ------> bundles ------> attempts
    |                |                                  |
    +--> journal <---+                                  |
             |                                          |
             +--------------> run foundation <----------+
                                  |
                                  +--> config
                                  +--> filesystem / artifacts

queries/inspection -> runs + journal
queries/discovery  -> queries/inspection + runs + config
```

| Area | Permitted internal dependencies | Forbidden reverse dependency |
| --- | --- | --- |
| `config/` | Configuration models, loaders, and registry within the domain | No persistence, CLI, execution, or scheduler code. |
| `_filesystem.py` | Standard library only | No domain models, run storage, publication, or recovery. |
| `artifacts.py` | Narrow filesystem primitives where semantics match | No run lifecycle or orchestration. |
| Run foundation: `runs/` | Config, artifact/filesystem primitives, run records | No journal, attempts, bundles, publication, recovery, or queries. |
| `attempts/` | Run foundation, config, primitives, definition/attempt/invocation modules in their domain | No bundle publication/recovery or journal registration. |
| Journal core | Run foundation, config, primitives, journal records | No publication or recovery registration. |
| `bundles/` | Attempts/ownership, lower-level records, artifact/filesystem primitives | No publication intents or recovery operations. |
| `publication/` | Bundles, attempts/ownership, journal core for registration, and lower foundations | No recovery implementation. |
| `recovery/` | Publication, bundles, attempts/ownership, journal core, and lower foundations | No CLI or future controller/scheduler dependency. |
| `queries/` | Runs, journal core, query modules, and config as required | No publication, repair, or scheduling side effects. Foundation modules must not import queries. |
| `cli.py` | Config and persistence entry points | No other package module imports CLI implementation. |

Within each domain, record models depend only on config and appropriate
lower-level record schemas, not storage or orchestration implementations.
Keep internal module dependencies acyclic. Test-only fault injection can target
private functions without making those functions a consumer API.

Add a small offline architecture test using standard-library AST inspection
to resolve internal import edges, reject cycles/forbidden directions, and reject
test-to-test imports. Cover its edge-resolution rules so module aliases and
relative imports do not bypass the check. No additional dependency-analysis
package is required. Static checks cannot establish runtime import behavior or
detect every dynamic import; explicit package imports and installed-wheel
checks remain separate gates. Cross-boundary exceptions require rationale,
not a broad allowlist that makes the rule ineffective.

## Accepted test hierarchy

Optimize for a developer locating a responsibility or failure scenario, not
for minimizing file edits. Mirror the source domains, separate component API
checks from CLI behavior and lifecycle integration, and split files where
they currently combine distinct contracts. Do not force a split by line count
alone or create a file for every function.

```text
tests/
├── __init__.py
├── conftest.py
├── support/
│   ├── __init__.py
│   ├── configuration.py
│   ├── runs.py
│   ├── inspection.py
│   ├── filesystem.py
│   └── processes.py
├── architecture/
│   └── test_dependencies.py
├── config/
│   ├── test_site.py
│   ├── test_request.py
│   ├── test_loader.py
│   └── test_registry.py
├── persistence/
│   ├── test_filesystem.py
│   ├── test_artifacts.py
│   ├── runs/
│   │   ├── test_records.py
│   │   ├── test_creation.py
│   │   ├── test_reopening.py
│   │   ├── test_integrity.py
│   │   ├── test_locking.py
│   │   └── test_failures.py
│   ├── queries/
│   │   ├── test_inspection.py
│   │   └── test_discovery.py
│   ├── attempts/       # records, definitions, storage, ownership, failure cases
│   ├── journal/        # records, replay, integrity, append, retries, I/O failures
│   ├── bundles/        # records, inspection, publication, I/O failures
│   ├── publication/    # records, intents, registration, I/O failures
│   └── recovery/       # records, requests, receipts, operations, registration
├── cli/
│   ├── test_validation.py
│   ├── test_create_run.py
│   ├── test_inspect_run.py
│   ├── test_list_runs.py
│   └── test_entrypoint.py
└── integration/
    ├── conftest.py
    ├── test_recovery_operations_handoff.py
    ├── test_publication_registration_handoff.py
    ├── test_recovery_registration_handoff.py
    └── test_persistence_lifecycle.py
```

All test directories become Python packages with minimal `__init__.py` files
(omitted from most of the drawing). Their qualified paths permit repeated
filenames such as `test_records.py` without flattening test-module identity.
Explicit support imports use `tests.support`, not bare sibling-module names.
Retain pytest's current default import mode; add an explicit `testpaths =
["tests"]` setting in `pyproject.toml` to define collection scope. Test packages
are not part of the runtime wheel. Retain the `src` application layout and the
independent installed-wheel gate.
[pytest package/import guidance](https://docs.pytest.org/en/stable/explanation/goodpractices.html#choosing-an-import-mode).

Use short role-based names inside domains: `test_records.py`,
`test_inspection.py`, `test_storage.py`, `test_registration.py`, and additional
behavioral names where a split helps, such as `test_retry.py` or
`test_publication_failures.py`. Exact leaf filenames and case assignments must
be inventoried before moving tests; the drawing is not an exhaustive allocation
of every existing case.

### Responsibility splits

| Existing file/family | Accepted separation |
| --- | --- |
| Site loader and run request tests | Site/request models and matching in `config/`; direct YAML loading/rejection in `test_loader.py`; CLI exit codes, messages, and entrypoint behavior in `cli/`. Split compound API/CLI checks into explicit tests while retaining all assertions. |
| Discovery and inspection CLI tests | API ordering, validation, locking, report integrity, and read-only behavior in `persistence/queries/test_discovery.py` and `test_inspection.py`; command-specific output/exit behavior in `cli/`. |
| Run record and storage tests | Record schemas, including external-artifact models, and creation, reopening, integrity, locking, and interrupted/I/O-failure behavior in separate files under `persistence/runs/`. |
| Journal tests | Record encoding, replay/integrity, append/retry, and publication failure contracts in their own focused files. |
| Definition/attempt/bundle/publication/recovery storage families | Separate ordinary contract and retry checks from filesystem guards and I/O-failure matrices where they form distinct substantial groups. Keep parametrized matrices intact rather than scattering individual variants. |
| Subprocess cases embedded in component files | Classify by the primary contract. A single run/journal/ownership API's process exclusion or interrupted retry stays with that component. Scenarios composing independently owned stages, publication plus explicit recovery, or recovery plus completion registration belong under `integration/`. Preserve exact fault boundaries and assertions. |
| Whole persistence lifecycle | Retain its composition-focused test file; extract only support that is shared and does not obscure the staged scenario. |

Component tests may use real files, child processes, and fault injection.
`integration/` identifies composition of separately owned contracts across
handoffs, not every use of a subprocess or a real filesystem. CLI tests target
command parsing, messages, structured output, exit precedence, and read-only
behavior; they may invoke real persistence APIs rather than mocking away the
contract. All tests remain offline. Directory placement is not permission to
skip integration in the default CI run.

Test names should describe expected behavior or the failure boundary, not
implementation helper names. Keep assertions next to the scenario they explain.
Do not separate a fault matrix from its safety outcome just to produce shorter
files. The listed integration filenames are responsibility boundaries; create
them only for actual grouped cases, not as future placeholders.

### Fixture and support responsibilities

Pre-refactor inspection found imports from `test_recovery_storage.py` and
`test_recovery_receipt_storage.py` in other collected test modules. Eliminate
all test-to-test imports. Move genuinely shared setup into fixtures at the
nearest common ancestor and explicit support modules:

- Root `conftest.py`: minimal shared factories only; no automatic run creation,
  global environment mutation, service access, or hidden scientific execution.
- Shared recovery staging and committed-bundle fixtures are explicitly named
  and registered at the root, the common ancestor of persistence and integration
  tests. Their builders remain in recovery support; no fixture is autouse.
  Tests explicitly request the state and domain preparation remains visible.
- Domain `conftest.py` files where needed: specialized journal, publication,
  or recovery scenarios. Do not create one merely for symmetry.
- `support/configuration.py`: explicit synthetic site/request input builders.
- `support/runs.py`: inert artifact/run input preparation with caller-selected
  identities and paths; no publication/recovery scenario orchestration.
- `support/inspection.py`: explicit run preparation shared by query/API and CLI
  inspection tests; no automatic repair or test-module imports.
- `support/filesystem.py`: named observations with explicit semantics. Preserve
  distinctions between metadata, payload, inode, timestamp, symlink, and staging
  evidence; do not weaken assertions by replacing them with a generic snapshot.
- `support/processes.py`: explicit child execution, environment construction,
  exit-status capture, and diagnostics. No implicit retries or evidence cleanup.

Keep fixtures function-scoped and preserve parametrization/lifetime behavior.
Extract common mechanics, but keep failure injection and expected safety
outcomes visible in the tests. Importable helpers must not depend on collected
test modules or application-private assumptions that the tests are supposed
to challenge.

For stateful cases, the test should visibly choose its run/job/attempt/invocation
identity, acquire ownership, arrange the incomplete/committed state, and invoke
the operation under test. A domain fixture may provide preparation primitives,
but must not hide recovery authorization or expected side effects in a generic
`case` fixture. Names should identify provided state, not promise universal
validity. Moving a fixture must not accidentally share a mutable context across
tests or release an ownership context before the assertion that needs it.

Avoid a miscellaneous helper collection. Add further domain builders only when
multiple tests need the same established setup contract; otherwise retain local
preparation. Tests never import each other or `conftest.py` directly. Shared
support may import production models for setup, but assertions about exact bytes
or integrity verdicts must not be derived solely from the same implementation
under test. Keep independent expected values and negative-case evidence.

## Ordered implementation parts

These are implementation parts of the same bounded pre-close-out refactor, not additional
execution features. The user subsequently authorized autonomous implementation
and verification of all parts below, without intermediate user actions.

1. Inventory imports/private helpers, documented or consumed Python interfaces,
   original test cases/parametrizations, and persisted encodings. Record exact
   dependency rules and the case-to-file allocation before bulk movement.
2. Extract the bounded filesystem primitives with focused behavior/failure tests.
   Review unchanged flags, diagnostics, descriptor cleanup, and flush sequencing.
3. Apply the approved source moves/names, relocate external-artifact models,
   and establish explicit lock-aware/internal interfaces. Update imports,
   qualified module aliases, and installed-wheel checks. Establish the
   dependency checks.
4. Extract shared test setup, split API/CLI and distinct contract groups, and
   relocate/rename tests. Preserve compound coverage when separating assertions;
   retain component process tests with the component they verify.
5. Apply test packaging/collection scope, review the dependency and original-case
   maps, update current documentation, and repeat the full acceptance gate.

Helpers used only to implement architecture checks belong in that test area,
not runtime source. The check should validate deliberately invalid graph
examples as well as the actual repository; a successful run alone must not
establish that its rule implementation can catch a violation.

## Migration boundary and acceptance

Update source/test imports, monkeypatch targets, import statements embedded in
subprocess code, installed-wheel module checks, and current documentation links.
Several subprocess helpers currently use `Path(module.__file__).parents[1]` as
their import root. Derive that root from the top-level package instead of the
depth of a relocated leaf module. Keep lifecycle subprocess execution working
after its test file moves.

Keep this a behavior-preserving structural refactor, not an algorithm rewrite.
Do not weaken existing assertions, change fixture scope or failure boundaries,
or modify scientific payloads, CLI behavior, record encodings, schema fields,
event types, or persisted paths. Keep dependency versions unchanged. The
accepted collection setting and test packaging are not already-applied
configuration changes.

Inventory the original collected cases/parametrizations and map them to their
new locations and splits. Exact collection-count equality is not sufficient
and need not hold when compound tests are separated or direct API assertions
are added: account for original behavior and fault cases, not merely totals.
Confirm no cross-test imports remain and repeated basenames collect correctly.
Require dependency checks to pass without unreviewed exceptions, and verify
tests/support are excluded from the installed runtime distribution. Confirm
the clean import-path migration is reflected in documented consumers and
installed-wheel imports, without old-path wrappers or initializer re-exports
that mask dependency cycles. Review lock ownership and internal error contracts
at every adjusted cross-domain call.
Repeat formatting/lint, full tests,
the agreed 100% statement/branch coverage gate, CLI checks, distribution build,
and isolated-wheel verification; compare representative record encodings and
record fresh verification inputs. Historical cycle-review evidence does not
verify the new layout. Remote CI remains required for the final candidate.

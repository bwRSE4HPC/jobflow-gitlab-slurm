# jobflow-gitlab-slurm

A proposed jobflow manager that uses short-lived GitLab CI controller jobs to
submit and reconcile Slurm workers. It is intended for general jobflow
workflows; VASP and atomate2 are downstream demonstrations, not dependencies
of this package.

Cycle [001-jobflow-compatible-package](docs/001-jobflow-compatible-package.md)
delivered the installable offline validation foundation and is merged.
Cycle [002-durable-run-state](docs/002-durable-run-state.md) supplies offline
persistence and is awaiting final integration gates. Implemented functionality:

- Strict site/run-request validation and directory-based site discovery.
- Exact-byte artifact checks, pinned site snapshots, and POSIX run creation/reopening.
- Immutable original-Flow job definitions and qualified attempt/invocation storage.
- Persistent locks, anchored event replay/append, and journal-aware read-only inspection.
- Opaque bundle inspection/publication, retained intents, and explicit same-identity
  recovery with audit records and stable completion-event registration.
- Offline failure, contention, and cross-process lifecycle tests.

All cycle 002 slices have locally accepted verification gates on recorded evidence.
Assistant cycle review and the full local working-tree verification gate passed
on 2026-10-09. User documentation review, committed candidate identification,
remote CI, and explicit close-out/merge approval remain pending.
See [progress](docs/progress.md) for the current checkpoint and
the cycle overview for slice-specific evidence and historical test counts.

There is **no execution manager or supported HPC deployment yet**. The production
`JobStore` adapter, worker, producer enforcement, jobflow execution semantics,
Slurm reconciliation/submission, and GitLab orchestration remain later stages.
Filesystem integrity and completion events do not establish scientific or
scheduler success or result eligibility. Live cross-host verification remains
a separate deployment gate. Design examples describe intended interfaces unless
explicitly marked implemented.

## Offline use and development

Use Python 3.13 and uv. From the repository root:

```bash
uv sync --locked --dev
uv run --locked jobflow-gitlab-slurm --help
```

With your own consumer site and request files (these paths are illustrative):

```bash
uv run --locked jobflow-gitlab-slurm validate-site hpc/sites/example-cluster.yaml
uv run --locked jobflow-gitlab-slurm validate-request request.yaml hpc/sites/example-cluster.yaml
```

These commands validate configuration only; they do not verify artifact bytes,
contact Slurm, or submit a job. See [HPC binding](docs/hpc-binding.md) and
[run-request v1](docs/001-jobflow-compatible-package/run-request-v1.md) for the file contracts.

`create-run`, `inspect-run`, and `list-runs` are also available; see their
`--help` and the [run-state CLI contract](docs/002-durable-run-state/run-state-v1.md#clidiscovery-slice-local-checkpoint-verified).
Creation selects a site from `request.site_id` and verifies supplied artifact
bytes. Inspection/listing validate metadata, stored Flow bytes, and the journal
under the same per-run lock; external-artifact verification is optional.
Journal findings are reported separately: uninitialized, valid, incomplete,
invalid, or not checked. Exit precedence is invalid (2), incomplete (4), busy
(3), otherwise success (0). Inspection performs no repair. These commands do not import
workflow callables or submit jobs. Discovery classifications are not scientific
execution states.

Before submitting changes:

```bash
uv run --locked ruff format --check src tests
uv run --locked ruff check src tests
uv run --locked pytest -q --cov=jobflow_gitlab_slurm --cov-branch --cov-report=term-missing
uv build --no-sources
bash ci/check-wheel.sh
```

GitHub Actions runs offline checks on pushes, pull requests, and manual
dispatch. Its distribution artifacts are test outputs, not a published
package release. See the [testing strategy](docs/testing-strategy.md) for
coverage policy and the separately authorized live HPC gates.

## Contributing

Contributions are welcome through GitHub issues and pull requests. Read the
[contribution guide](CONTRIBUTING.md) for development setup, required checks,
design-change review, and handling sensitive information.

## Repository organization

Runtime code lives under `src/jobflow_gitlab_slurm/`: `config/` owns site and
request validation; `persistence/` separates run foundations, attempts, journal,
bundles, publication, recovery, and read-only queries. The root `cli.py` composes
these interfaces. Package initializers are inert; old flat Python import paths
have been removed without compatibility wrappers. CLI and persisted formats are
unchanged.

Tests mirror these domains, with separate `cli/`, `integration/`, and
`architecture/` checks and explicit fixture/support modules. See
[package organization](docs/002-durable-run-state/package-organization.md) for
ownership, dependency rules, and the migration mapping.

## Design documents

Cycle overviews remain directly under `docs/`; refined slice documents and
cycle-owned contracts live in folders with the same cycle name:
[001-jobflow-compatible-package/](docs/001-jobflow-compatible-package/) and
[002-durable-run-state/](docs/002-durable-run-state/). Cross-cycle guides and
numbered design decisions retain their existing locations.

- [Architecture](docs/architecture.md): component boundaries, durable state,
  and recovery model.
- [Run-state v1 design](docs/002-durable-run-state/run-state-v1.md): accepted filesystem layout,
  identity, publication, and failure-report contract; opaque persistence,
  bundle publication, and explicit recovery implemented; worker integration
  and reconciliation pending.
- [Run-request v1](docs/001-jobflow-compatible-package/run-request-v1.md): accepted workflow, runtime,
  resource, and budget contract; offline file validation implemented,
  execution pending.
- [Artifact contract v1](docs/002-durable-run-state/artifact-contract-v1.md): approved separation of
  unchanged Flow payload bytes from the versioned original-flow envelope;
  byte-verification/private-staging helpers and envelope models implemented;
  initial POSIX run publication implemented; live filesystem verification pending.
- [Record contract v1](docs/002-durable-run-state/record-contract-v1.md): approved manifest/envelope
  models and deterministic site encoding implemented and user-verified.
- [Event contract v1](docs/002-durable-run-state/event-contract-v1.md): accepted journal scope and
  run-local anchor revision, with publication/recovery rules specified;
  event/head models installed and locally user-verified; read-only replay
  locally user-verified; append/recovery installed and locally user-verified.
- [HPC binding](docs/hpc-binding.md): the proposed user-facing site file,
  prerequisites, configuration ownership, and verification gates.
- [Jobflow compatibility](docs/jobflow-compatibility.md): behavior the manager
  must preserve and the tests that demonstrate it.
- [Testing strategy](docs/testing-strategy.md): upstream-aligned pytest
  expectations and additional durability, recovery, and HPC verification.
- [Implementation plan](docs/implementation-plan.md): staged delivery and
  acceptance criteria.
- [Progress record](docs/progress.md): observed state, unresolved checks, and
  the next guided checkpoint.
- [Cycle 001: jobflow-compatible package](docs/001-jobflow-compatible-package.md):
  completed offline foundation, acceptance evidence, and hand-off.
- [Cycle 002: durable run state](docs/002-durable-run-state.md): current cycle's
  scope, implementation slices, mechanisms, risks, and close-out gates.
- [Decision 0001](docs/decisions/0001-public-package-private-hpc-consumers.md):
  separation of public package development from site-specific HPC consumers.
- [Decision 0002](docs/decisions/0002-attempt-publication-and-recovery.md):
  accepted first-slice publication, recovery, and user-facing failure-report policy.
- [Decision 0003](docs/decisions/0003-failed-job-amendments.md): explicit,
  audited corrections to failed, uncommitted jobs without rerunning ancestors.

The public repository must not contain site credentials, proprietary inputs,
licensed binaries, or site-specific runner configuration. Live HPC integration
belongs in an access-controlled downstream consumer project.

Licensed under the [MIT License](LICENSE).

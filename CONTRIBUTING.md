# Contributing

Contributions are welcome through GitHub issues and pull requests.

This project develops an application-neutral jobflow execution backend using
short-lived GitLab controllers and Slurm workers. VASP, atomate2, and
site-specific HPC configuration belong in downstream consumer projects.

## Current scope

The offline foundation is implemented: site/run-request validation and discovery,
exact-byte artifact verification, immutable run/definition/attempt/invocation
storage, persistent ownership, anchored journal operations, read-only inspection,
opaque bundle publication, retained intents, explicit recovery/audit, and stable
completion-event registration. Cross-process lifecycle tests complement the
component failure/restart tests.

Cycle 002 slices are locally accepted and its review is user-reported complete;
the committed candidate matches the local verification gate and remote
`Offline checks` is user-reported passing. Final documentation review and
close-out/merge approval remain. See [progress](docs/progress.md) and the
[cycle overview](docs/002-durable-run-state.md) for evidence rather than treating
historical counts as the current suite size.

The production `JobStore` adapter, worker/producer enforcement, jobflow execution
semantics, scheduler-aware result selection, Slurm submission/reconciliation,
and GitLab orchestration remain planned. Local verification is not live HPC
support, cross-host durability, or scientific/scheduler success.

Before starting a significant feature, review:

- [Architecture](docs/architecture.md)
- [Implementation plan](docs/implementation-plan.md)
- [Jobflow compatibility contract](docs/jobflow-compatibility.md)
- [Testing strategy](docs/testing-strategy.md)
- [Progress record](docs/progress.md)

Open an issue before changing accepted contracts, persistence formats,
recovery behavior, or public interfaces. Clearly distinguish implemented
functionality from proposed behavior.

## Development setup

Requirements:

- Python 3.13
- uv
- Git

The initial compatibility target is `jobflow==0.3.1`.

From the repository root:

```bash
uv sync --locked --dev
uv run --locked jobflow-gitlab-slurm --help
```

Use a cycle branch based on current `main` after the previous cycle has merged.
Name it `XXX-branch-name`, with a sequential, zero-padded three-digit number,
for example `002-durable-run-state`. Create its matching document at
`docs/XXX-branch-name.md` when planning begins. Place refined slice and contract
documents in `docs/XXX-branch-name/`, using the same cycle stem. Keep each major
stage on its own branch and implement it in small reviewed slices; avoid unrelated formatting
or dependency updates.

Change dependencies in `pyproject.toml`, then regenerate the lockfile with
`uv lock`. Commit both files when dependencies change.

## Code and documentation

- Keep the backend application-neutral and independent of KIT infrastructure.
- Use type annotations and clear public API docstrings.
- Follow existing code conventions and Ruff formatting.
- Update documentation when behavior, interfaces, or limitations change.
- Preserve versioned contracts; reject unsupported versions explicitly.
- Do not introduce implicit scientific reruns or unsafe submission retries.

Follow the [package organization](docs/002-durable-run-state/package-organization.md):
record models must not depend on storage operations, queries must not repair
state, and package initializers remain inert. Tests mirror component ownership;
cross-contract composition belongs under integration. Import reusable setup
from explicit support modules, never from collected test modules. The original
flat Python imports have been intentionally removed; use the qualified domain
paths.

Discuss changes affecting identities, locking, publication, amendments, or
recovery before implementation. Record significant design decisions in
`docs/decisions/`.

Documentation responsibilities:

- [Implementation plan](docs/implementation-plan.md): concise roadmap, major
  stages, dependencies, and acceptance gates.
- [Progress](docs/progress.md): current implemented state, verification evidence,
  pending gates, blockers, and next action.
- `docs/XXX-branch-name.md`: branch-matching cycle overview, scope, slice map,
  risks, and acceptance checks. Current and completed cycles are linked from
  the roadmap.
- `docs/XXX-branch-name/`: refined implementation-slice documentation and
  cycle-owned contracts. Keep existing descriptive/versioned filenames inside
  the folder; link them from the cycle overview.
- Versioned contracts (`*-v1.md`) and numbered design decisions in
  `docs/decisions/`: authoritative interface/format and decision records, with
  independent naming schemes; cycle numbers do not replace their versions or IDs.

Cross-link these documents instead of duplicating status. Record implementation
slices and significant decisions, not minor test-fix iterations. Distinguish
proposed, approved, installed, locally verified, CI-verified, and live-verified
behavior. Preserve existing contributor edits and keep private site details out
of public documentation.

## Tests

Every feature needs tests. Every bug fix needs a regression test that
demonstrates the original failure.

Public tests must run without Slurm access, GitLab credentials, licensed
software, or site-specific configuration. Dependency installation may use
the network; offline tests must not require external services.

For execution-related features:

- Use tiny application-neutral jobs and importable fixture modules.
- Test observable behavior against the supported jobflow version.
- Include invalid-input, replay, restart, and failure cases where relevant.
- Test concurrency and publication boundaries with real temporary files and
  separate processes, not mocks alone.
- Verify that incomplete results cannot release dependent jobs.
- Verify that ambiguous submissions cannot cause duplicate calculations.

See the [testing strategy](docs/testing-strategy.md) for detailed gates.

## Local checks

Run before opening or updating a pull request:

```bash
uv sync --locked --dev
bash -n ci/check-wheel.sh
uv run --locked ruff format --check src tests
uv run --locked ruff check src tests

mkdir -p ci-reports
uv run --locked pytest -q \
  --cov=jobflow_gitlab_slurm \
  --cov-branch \
  --cov-report=term-missing \
  --cov-report=xml:ci-reports/coverage.xml \
  --junitxml=ci-reports/junit.xml

uv build --no-sources
bash ci/check-wheel.sh
```

To apply formatting:

```bash
uv run --locked ruff format src tests
```

The wheel check expects exactly one wheel in `dist/`. It verifies installation
outside the source checkout and retains its temporary environment for inspection.

Coverage is reported, but CI does not enforce a percentage threshold.
Cover safety-critical behavior directly; a high percentage alone does not
prove execution correctness. Type checking is not yet configured.

## Pull requests

Describe:

- The problem and intended behavior.
- Changed interfaces or contract clauses.
- Tests added and checks performed.
- Documentation changes.
- Remaining limitations and deliberately deferred verification.

Before closing a cycle, review the diff, pass its agreed verification gates,
and document completed scope, remaining limitations, and the next checkpoint.
Use the hosting platform's squash-merge option to integrate one coherent cycle
commit; do not rewrite shared branch history. Begin the next cycle from updated
`main` only after the merge.

GitHub Actions must pass before merging. Public CI verifies offline behavior
and packaging; it does not establish live HPC support.

Live HPC tests require explicit authorization and a bounded allocation in an
access-controlled downstream consumer. Report sanitized results separately.

## Bug reports

Include a minimal reproducible example, expected and actual behavior,
package/Python/jobflow versions, and relevant sanitized diagnostics.

For backend execution failures, include available run/job/attempt identities,
scheduler outcomes, and recovery steps already attempted. Do not delete or
overwrite evidence merely to retry.

## Sensitive information

Never include credentials, tokens, licensed binaries, pseudopotentials,
proprietary inputs/results, or private infrastructure configuration in
source, issues, logs, or public artifacts.

Do not report a security vulnerability in a public issue. Contact the
maintainers privately through an available non-public channel; a dedicated
security-reporting process has not yet been established.

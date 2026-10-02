# Contributing

Contributions are welcome through GitHub issues and pull requests.

This project develops an application-neutral jobflow execution backend using
short-lived GitLab controllers and Slurm workers. VASP, atomate2, and
site-specific HPC configuration belong in downstream consumer projects.

## Current scope

Offline site/run-request validation, site discovery, and packaging checks
are implemented. Durable state, worker execution, reconciliation, and Slurm
submission remain planned.

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

Use a feature branch based on current `main`. Keep changes focused and avoid
unrelated formatting or dependency updates.

Change dependencies in `pyproject.toml`, then regenerate the lockfile with
`uv lock`. Commit both files when dependencies change.

## Code and documentation

- Keep the backend application-neutral and independent of KIT infrastructure.
- Use type annotations and clear public API docstrings.
- Follow existing code conventions and Ruff formatting.
- Update documentation when behavior, interfaces, or limitations change.
- Preserve versioned contracts; reject unsupported versions explicitly.
- Do not introduce implicit scientific reruns or unsafe submission retries.

Discuss changes affecting identities, locking, publication, amendments, or
recovery before implementation. Record significant design decisions in
`docs/decisions/`.

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
# HPC binding: proposed user-facing interface

Status: **design specification; offline site and run-request validation
commands and directory-based site registry implemented**. Live HPC checks
remain unimplemented. This is the primary setup guide for a downstream
consumer. Its objective is small site files, an explicit list of bootstrap settings that
cannot live in them, and observable pass/fail checks.

## What the operator must have

| Prerequisite | How to verify it |
| --- | --- |
| An access-controlled GitLab consumer project and a trusted controller runner | A job on a protected ref runs under the intended HPC identity; untrusted refs cannot use the runner. |
| Slurm access from that runner identity | `sbatch`, `squeue`, `sacct`, and `scancel` are available; the identity is allowed to charge the chosen account and use the partition. |
| Shared run storage | Controller and compute node can read/write the **same absolute path**; locking and atomic rename work on that filesystem. |
| A usable compute runtime | The worker package and workflow code are pinned and available in the job allocation, natively or through Apptainer. Required applications, licenses, and data remain the consumer's responsibility. |
| A bounded smoke-test allocation | A short test job is permitted in the selected account and partition. |

The first provider will use an existing shared POSIX `runs_root`. Creating
that directory and setting its retention policy are operator tasks. A provider
for per-run expiring workspaces is a later integration point; it must expose
the same create/resolve/list/expiry operations and cannot be silently treated
as a permanent POSIX root.

## Site files

A site YAML file lives in the **consumer** repository, never in this public
package. All values below are examples, not defaults or KIT settings:

```yaml
schema_version: 1
site_id: example-cluster
slurm:
  cluster_name: example-slurm-cluster
  account: example-allocation
  partitions: [short, batch]
  default_partition: short
storage:
  provider: posix
  runs_root: /shared/example/jobflow-runs
worker:
  launch_mode: apptainer
  apptainer_command: apptainer
```

For multiple clusters, place one file per site in a dedicated consumer
directory, for example `hpc/sites/cluster-a.yaml` and
`hpc/sites/cluster-b.yaml`. The filename stem must equal the file's `site_id`.
The in-memory registry rejects a symlinked root and validates every immediate
entry: only regular files with the exact `.yaml` suffix are accepted, not
`.yml` files, symlinks, subdirectories, or other entries. It also rejects
duplicate IDs and empty directories. It selects by exact ID and returns a
deep copy of the selected site:

```python
from jobflow_gitlab_slurm.config.registry import SiteRegistry

registry = SiteRegistry.from_directory("hpc/sites")
site = registry.select("cluster-a")
```

No default cluster is inferred. The current CLI still validates an explicitly
selected site file. When durable run creation is implemented, it must pin the
selected site's normalized snapshot and digest; a later file edit must not
change an active run. Discovery assumes a trusted configuration directory;
it does not implement an atomic filesystem snapshot.

| Field | Required contract |
| --- | --- |
| `schema_version` | Integer; initially `1`. Unknown versions fail closed. |
| `site_id` | Stable, non-secret identifier for manifests and diagnostics. |
| `slurm.cluster_name` | Expected Slurm cluster identity, checked where Slurm exposes it and confirmed in the smoke job. |
| `slurm.account` | Account to charge. Must be authorized for the runner identity. |
| `slurm.partitions` | Nonempty list of partitions the consumer permits. |
| `slurm.default_partition` | One member of `slurm.partitions`. |
| `storage.provider` | `posix` in the first release; additional providers require installed adapters. |
| `storage.runs_root` | Canonical absolute POSIX path below `/`; no traversal, redundant separators, trailing slash, or control characters. Offline validation checks syntax only; doctor/smoke must check that it exists and is writable by controller and workers at the same path. Required for `posix`. |
| `worker.launch_mode` | `apptainer` initially; a native mode can be added with its own checks. |
| `worker.apptainer_command` | Executable name or absolute path on compute nodes. Required for `apptainer`. |

Collect `slurm.cluster_name` from Slurm configuration or the HPC operator,
`slurm.account` from the allocation granted to the runner identity, and
`slurm.partitions` from the partitions that identity may actually use.
Commands such as `scontrol show config` and `sinfo` may help, but some sites
restrict them; lack of read access is not evidence that a value is valid.
Provision `storage.runs_root` under the site's storage policy and check it
from both the controller and a compute allocation. Confirm the Apptainer
command on a compute node, not only on the controller.

The site file does **not** contain secrets, GitLab tokens, VASP module names,
container credentials, or a particular workflow. A run request provides the
selected partition, bounded resources (nodes, tasks, CPUs per task, memory,
walltime), immutable workflow/runtime identifiers, and optional notification
address. Application-specific module setup, additional bind mounts, and
runtime-image staging belong to the consumer's pinned worker launch plan.
The offline validator checks site identity and permitted partition now; the
future manager must repeat these checks against the pinned binding before
submission.
There is no arbitrary shell command interpolation from untrusted YAML.

If storage expires, the provider must expose the expiry and the run must
report that it is no longer resumable; the manager must not silently create a
new workspace under an old run ID. Operators must choose retention sufficient
for the calculation and inspection period.

## Two unavoidable bootstrap settings

GitLab selects a runner **before** checking out site files, so the protected
controller runner's tag must be configured in the consumer's CI pipeline (or
a protected CI variable used by its `tags` field). The pipeline schedule and
its owner are also GitLab project settings. The current CLI receives a site
file path after checkout; the registry API accepts a directory. Do not
duplicate account, partition, or storage settings in CI variables merely to
route the runner.

Keep credentials in GitLab's protected, masked settings or an approved secret
service, not in the site file or logs. A shell executor can run arbitrary
commands with its runner identity; only trusted protected code may reach it.
Public pull requests must not run on the HPC controller.

## Verification gates

Except for `validate-site` and `validate-request`, the names below describe
intended CLI capabilities, **not commands available today**. Each gate must
report the field or capability that failed and stop before advancing to a more
expensive check.

1. **Validate (offline):** parse the versioned schema, reject unknown keys,
   missing fields, unsafe paths, and invalid partition choices. When a run
   request is supplied, reject nonpositive resources and disallowed partitions.
   No Slurm or GitLab access is required.
2. **Doctor (non-submitting):** on the selected controller runner, confirm the
   expected identity, Slurm executables, cluster/account/partition visibility
   where permitted, and shared-directory read/write/lock/rename. Use
   `sbatch --test-only` to validate a generated minimal script without
   submitting it. Report `not verifiable` separately from `passed`: the
   compute-node runtime cannot be confirmed until smoke, and a successful
   `--test-only` is not proof of execution.
3. **Smoke (submitting, explicitly opted in):** submit one bounded, non-VASP
   worker job through the exact runtime path planned for production. Record a
   Slurm job ID, observe a terminal state through accounting, and verify a
   completion document and output file in the shared workspace. This consumes
   a small allocation; never perform it as an implicit validation step.
4. **Recovery:** rerun reconciliation on that completed run and confirm zero
   duplicate submissions. Interrupt a test controller after submission and
   confirm that its next invocation recovers the same Slurm job ID. Inject an
   incomplete worker attempt and verify that the status report names its
   evidence paths, blocked descendants, and safe next action without
   automatically rerunning the calculation.

Only after all four gates pass should the consumer submit scientific jobs.
The live gates belong in an access-controlled GitLab consumer project; public
CI runs offline tests with simulated Slurm and GitLab behavior.

Relevant upstream specifications: [Slurm `sbatch`](https://slurm.schedmd.com/sbatch.html),
[Slurm accounting](https://slurm.schedmd.com/sacct.html),
[GitLab scheduled pipelines](https://docs.gitlab.com/ci/pipelines/schedules/),
and [GitLab runner security](https://docs.gitlab.com/runner/executors/shell/).

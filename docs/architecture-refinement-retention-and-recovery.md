# Architecture refinement: retention, integrity observation, and recovery

This document addresses the following gaps:

- Expiring workspaces: loss of run state and scientific output despite successful publication.
- Missing-run discovery: workspace scanning cannot report a run whose entire workspace disappeared.
- Coordinated rollback: a valid journal prefix and its matching head can replace newer history undetected locally.
- Backup coverage: reviewed-output archival alone does not protect active, expensive workflows.
- Checkpoint consistency: copying a changing run does not necessarily produce a recoverable snapshot.
- Restore safety: retained files do not establish that external jobs or side effects can safely be repeated.
- Scientific redirection: continuing from an intermediate result must preserve history rather than erase it.
- Operational visibility: users need explicit integrity, retention, recovery, and data-loss information.

Status: architecture proposal recorded on 2026-10-09, following the cycle 002
review. This document separates the outstanding concerns and possible
extensions; it does not approve or implement a persistence service, observer,
backup, restore, or derived-run mechanism. It does not expand cycle 002's
close-out scope. See the [current architecture](architecture.md),
[implementation plan](implementation-plan.md), and
[review record](002-durable-run-state/review-and-verification.md).

## 1. Current boundary and feasibility categories

Cycle 002 implements local persistence and integrity primitives, including an
anchored journal, immutable metadata, bundle publication, and explicit
same-identity publication recovery. It does not implement a production worker,
Slurm reconciler, GitLab controller, or the extensions below. Local verification
does not establish cross-host filesystem or power-loss behavior.

The downstream HPC use case has expiring run workspaces. Publication durability
within such a workspace and retention of that workspace are different
properties: a correctly flushed and committed file can still be removed when
the workspace expires. Workspace extension is not the proposed mitigation.

| Category | Scope | Feasibility and approval boundary |
| --- | --- | --- |
| Current foundation | Local metadata, journal, bundle integrity, explicit publication recovery | Implemented and locally verified; live filesystem guarantees remain a deployment gate. |
| Feasible with existing infrastructure, deferred | Small run catalog and witness in an HPC user home/project area outside expiring workspaces | Plausible, but storage lifetime, permissions, quotas, and failure isolation need confirmation. Explicitly deferred until a persistence mechanism addressing workspace expiry is approved. |
| Additional storage services, beyond current setup | Verified operational backups, scientific archives, and restoration from retained copies | Require an approved persistent destination and access/retention arrangements. LSDF and bwDataArchive cannot currently be involved in this setup; alternatives could satisfy the same interfaces. |
| Backend/consumer extensions | Observer, consistent checkpoint protocol, restore reconciliation, derived runs, pipeline controls | Require separately approved implementation. Some can operate with existing services while source data exists, but cannot close the retention gap without retained copies. |

“Outside the workspace” establishes a separate lifecycle, not necessarily an
independent failure or security domain. A user project directory on the same
filesystem and under the same identity may be lost or modified together with
the run. Its existence is not evidence of guaranteed retention or backup.

The following is the downstream service boundary, not a claim that the planned
backend execution integration is already installed:

```text
GitLab / short-lived controller      Slurm / scientific worker
              |                              |
              +------- orchestration --------+
                                             |
                                      expiring workspace
                                      - run metadata
                                      - journal + head
                                      - results and files
                                             |
                                       expiry / deletion
                                             |
                                      state and bytes lost

GitLab logs/artifacts: diagnostics, not an authoritative retained copy.
```

## 2. Workspace expiry and missing-run discovery

### Concern

Discovery that scans only existing workspaces cannot distinguish “no run was
created” from “the run and its workspace disappeared.” Checksums also cannot
recover missing files. This is a retention failure, not necessarily a defect
in the publication protocol.

### Mitigations

- Retain a small catalog outside the expiring workspace: expected run identity,
  storage location, lifecycle, known expiry, and latest protection reference.
- Observe availability and expiry before expiration; report the unprotected
  interval and the destination of any verified retained copy.
- Protect active runs through operational checkpoints to approved persistent
  storage, rather than waiting for final scientific review.
- Make retention limits explicit. Never silently allocate a replacement empty
  workspace and treat it as the vanished run.

The catalog supports detection; the checkpoint supports recovery. If only the
catalog survives, the observer can report a lost run but cannot resume it.
If neither survives, reliable missing-run detection requires another retained
record of expected runs.

**Feasibility:** a small catalog in an existing user home/project area appears
practical but is deferred under the approval boundary above. Protection of full
run contents is not available from the current expiring-workspace setup alone.
Expiry notifications reduce risk; they do not guarantee retention.

## 3. Coordinated rollback and the independent witness

### Concern

The local journal anchor detects a history/head mismatch and loss relative to
the retained head. It cannot detect replacement of both journal and head by a
matching older version. An accidental restoration of an older directory is a
realistic cause; malicious modification is a separate threat model.

### Mitigation

Retain observations outside the workspace: run identity, journal sequence and
hash, observation identity/time, and any verified checkpoint reference. The
observer verifies that current history extends the previously witnessed prefix.
Checking only that the sequence increased is insufficient: a different history
can have the same or a greater sequence.

An older sequence, changed witnessed hash, or incompatible prefix produces a
hold and an evidence report. Do not automatically lower the witness to match
the workspace or reinterpret the rollback as a user-requested restart.

```text
                     proposed lightweight observer
                        /                     \
                read and verify           read / append
                      /                         \
            expiring run workspace       user home/project area
            journal + local head         expected-run catalog
            committed bundles            witnessed sequence/hash
                                         verified-copy references

            different lifecycle          no bulk scientific backup
                         \                 /
                         compare witnessed prefix
                              |
                       report / hold on mismatch
```

### Residual limits

- Progress not yet witnessed can disappear without being detected as rollback.
- A jointly restored or deleted workspace and witness defeats this protection.
- Same-account storage is not tamper-proof. Stronger guarantees need separately
  protected, retained observations outside the shared failure/security domain.
- Signatures authenticate a checkpoint but alone do not prevent replay of an
  older, valid signed checkpoint; retained independent knowledge is still needed.
- A witness detects divergence or loss; it does not recreate missing output.

**Feasibility:** a lifecycle-independent witness is plausible with an approved
project-directory mechanism, but remains deferred. Stronger independence needs
additional storage/access guarantees beyond the current setup. An observer
without such a witness can still check local integrity, but cannot close this gap.

## 4. Operational backup versus scientific archival

Operational protection and scientific archival have different purposes:

| Mechanism | Purpose | Proposed trigger |
| --- | --- | --- |
| Operational checkpoint | Recover active execution state and retained completed work | Automatic at agreed boundaries, with an explicit protection policy. |
| Scientific archive | Preserve a user-reviewed scientific record and provenance | User-triggered selection after inspection. |

Manual archival alone leaves active workflows exposed before review. A proposed
initial protection boundary is a completed calculation plus its committed
backend state. Frequency, acceptable loss window, and whether missing protection
warns or pauses further scheduling are decisions still requiring approval.
The backend cannot recover unfinished application state that was never written;
application-specific restart files need their own retention contract.

Every checkpoint needs an inventory of its exact cut and completeness:

- Run/request/site snapshots, definitions, provenance, and exact journal/head boundary.
- Committed output documents, full jobflow responses, required files, and any
  additional-store data needed to resolve references.
- Submission and recovery evidence needed for later scheduler reconciliation.
- Artifact identities, sizes/hashes, creation information, and verification outcome.
- Runtime/dependency references and an assessment of their continued availability.

A runtime digest identifies an image; it does not keep that image available
after registry cleanup. Licensed executables and external data may impose
separate access constraints. Classify checkpoints as resumable or evidence-only;
missing execution dependencies must not be concealed by a successful file copy.

### Candidate KIT services, not current dependencies

LSDF Online Storage is a candidate for retained operational data, subject to an
approved allocation, capacity, access method, and protection policy. Its service
role is online storage for scientific data; this does not establish access or
a backup contract for this project.
[KIT LSDF service description](https://www.scc.kit.edu/en/11843.php).

bwDataArchive is a candidate for packaged, scientifically reviewed material.
KIT describes it as long-term tape storage, not a working area for frequently
changing data. Its documentation states that it does not provide versioning or
snapshots, so application-managed checkpoint identities and retention remain
necessary.
[KIT bwDataArchive service description](https://www.scc.kit.edu/en/services/bwdataarchive.php).

Neither service is available to this integration at present. No automatic
connection, credentials, retention guarantee, or restore performance is assumed.
An approved alternative persistent destination could fill either role; the
portable package should not require a particular KIT service.

## 5. Consistent checkpoints and verified protection

A recursive copy of a changing run is not a consistent checkpoint. The run lock
does not by itself stop independent invocation writers. An initial protocol
should pause new scheduling, establish writer quiescence, and capture a defined
consistent cut. A later design could copy immutable committed bundles
incrementally and bind them to an exact journal boundary.

Transfer into a staging destination, verify the inventory and hashes there,
then publish a completed checkpoint reference. A successful transfer command
alone is not a verified backup. Retain the original evidence; backup completion
does not authorize deletion or workspace release.

Report the last verified protection boundary and work since that boundary.
The recovery-point objective describes tolerated loss; the recovery-time
objective describes tolerated restoration time. Both need an agreed policy,
and restore timing needs measurements. No present performance or loss-window
guarantee follows from the local persistence tests.

```text
                         proposed extension

expiring workspace -- consistent cut + verified transfer --> persistent store
 active execution                                           operational copies
                                                                   |
                                                         manual scientific review
                                                                   |
                                                          retained archive package

persistent copy / archive -- staged restore + verification --> fresh workspace
                                                             initially held
                                                                   |
                                                       reconcile external execution
                                                                   |
                                                         explicit resume decision
```

**Feasibility:** checkpoint mechanics can be developed offline, but protection
and restoration require an approved retained destination. Cross-host locking,
visibility, flushing, interrupted transfers, and destination behavior require
live verification; they are not closed by the current local gate.

## 6. Restore without duplicate execution

Restoration should be user-authorized into a fresh location, initially held.
Validate inventory, identities, schemas, references, and runtime availability
before allowing scheduling. Do not overwrite an existing run, silently rewrite
immutable storage bindings, or delete old ownership evidence. Disaster recovery
can preserve logical run identity, but rebinding its storage location requires
an explicit future contract.

An older checkpoint may omit jobs that Slurm accepted or completed after the
checkpoint. Reconcile stable submission identities with scheduler evidence;
ambiguous outcomes require operator resolution, not blind resubmission.
Historical accounting and compute-budget consumption also need reconciliation.
Restoring files does not undo consumed compute hours or external side effects.

If required output is genuinely lost, distinguish explicit scientific
recomputation from recovery of existing results. A witness can establish that
progress is missing without supplying the bytes necessary to reconstruct it.

**Feasibility:** restore validation can be designed now. Actual loss recovery
depends on retained complete copies; safe resumption also depends on the planned
worker, scheduler reconciliation, and live integration gates.

## 7. Scientific redirection without history rollback

Intentional rollback of journal/head is not an appropriate continuation API.
Keep the original run and later evidence intact. A proposed derived-run mechanism
creates a new run identity from a selected verified checkpoint, records source
lineage, and supplies corrected continuation definitions.

Reuse requires an explicit eligible dependency closure and compatible output
references/contracts, not arbitrary copying of selected files. Availability of
referenced runtime and scientific data must be checked. Active source jobs need
an explicit disposition; creating a derived run does not cancel them.

The accepted [failed-job amendment](decisions/0003-failed-job-amendments.md) is
a narrower, still-unimplemented mechanism: correct an eligible failed,
uncommitted job without modifying the original flow or already consumed results.
Changes affecting completed work or an established downstream branch call for
a derived run, not an expanded amendment or erased history.

**Feasibility:** lineage and continuation semantics can be developed without
new storage services while source data remains accessible. They do not solve
workspace expiry. Long-lived reuse needs retained source data or a self-contained
checkpoint. Jobflow reference and dynamic-response compatibility require tests
in a separately approved implementation cycle.

## 8. Proposed pipeline responsibilities and user reporting

| Operation | Responsibility and execution boundary |
| --- | --- |
| Observe | Scheduled, lightweight controller check of expected runs, integrity, expiry, witness continuity, and protection status; no scientific execution. |
| Protect | Approved automatic/manual checkpoint; bulk hashing, packaging, and transfer on a site-approved transfer worker or bounded allocation. |
| Archive | Manual selection of a scientifically reviewed checkpoint, with retained inventory and provenance. |
| Restore | Manual staged restore into a fresh, held location; report missing dependencies and ambiguous external execution. |
| Derive | Manual continuation from eligible retained results into a new run with explicit lineage and impact review. |
| Resume | Explicit authorization after integrity and scheduler reconciliation; not an automatic consequence of restore. |

Heavy data processing must not be assigned to the controller merely because it
is part of CI. GitLab artifacts can carry small diagnostic reports, not licensed
payloads or the authoritative backup. Storage credentials and site bindings
belong in protected downstream configuration, not this public package.

Reports should distinguish missing workspace, integrity mismatch, unprotected
progress, verified checkpoint availability, restore held, scheduler ambiguity,
and required user action. Include relevant evidence locations, last witnessed
and protected boundaries, and concrete recovery or explicit-relaunch guidance.
Never label local bundle integrity as scientific success or resumability.

## 9. Decisions required before implementation

1. Approve a persistence policy addressing expiry: retained data scope, lifetime,
   tolerated loss, warning/pause behavior, capacity, and responsible owner.
2. Confirm an authorized storage destination and its actual lifecycle/access
   guarantees. Only then decide whether to implement the small catalog/witness
   in a user project area and define its residual independence limits.
3. Agree checkpoint consistency/completeness and restore identity/reconciliation
   contracts; specify how runtime availability and external side effects are handled.
4. Establish a separate implementation cycle and acceptance gates: disappearance
   detection, coordinated-rollback observation, interrupted checkpoint/restore,
   duplicate-execution prevention, and live cross-host/storage verification.
5. Add operational protection first; add reviewed archival and derived-run
   continuation as separate capabilities. Do not present manual archival as a
   substitute for protection of active work.

Until those decisions and service integrations are completed, the current
foundation remains locally integrity-checked storage within its documented
filesystem assumptions, not guaranteed retention of expiring HPC workspaces.

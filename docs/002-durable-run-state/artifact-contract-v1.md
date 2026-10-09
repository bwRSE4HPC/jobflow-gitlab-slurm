# Artifact contract v1

Status: **approved on 2026-10-02; byte verification/private staging implemented
and user-verified; envelope models implemented; run creation installed and tests
reported passing**.
This fixes the original-flow byte/hash boundary for Stage 2. It does not
introduce worker execution or Slurm submission; run creation is implemented
separately in `storage.py`.

## Original-flow payload and envelope

```text
<run-root>/
  run.json              versioned manifest with request/site snapshots
  flow/original.json    versioned backend record identifying the original flow
  flow/payload.json     exact supplied serialized Flow artifact bytes
```

`workflow.serialized_flow_sha256` identifies the exact bytes of
`flow/payload.json`. Staging preserves those bytes; it must not decode,
normalize, import, or reserialize the Flow. Whitespace and line endings are
part of the byte identity. Hash verification alone does not establish that
the content is valid jobflow JSON or safe to execute.

`flow/original.json` is a backend record with schema version, kind, run ID,
creation timestamp, and the payload reference/digest. Its concrete model is
implemented in `records.py`. Its digest is not the workflow payload digest;
including a generated run ID in that envelope does not change the request's
previously computed payload identity.

The general rule that backend JSON records have versioned envelopes remains
in force. The unchanged `payload.json` is an opaque input artifact, not a
backend state record. It can therefore contain bare serialized jobflow JSON;
the associated backend record provides its versioned identity and provenance.

## Other identities and trust boundaries

- Consumer-code and worker-runtime digests also identify exact artifact bytes,
  not moving Git branches, image tags, or reconstructed representations.
- The selected site snapshot uses the approved `site-json-v1` encoding,
  specified in [record contract v1](record-contract-v1.md); its model and
  encoding are implemented and user-verified.
- Staging helpers must refuse to overwrite an existing destination and reject
  unsupported source file types. They operate in a trusted private staging
  directory, not an authoritative published run directory.
- Hash verification demonstrates the bytes read or staged at that time. It
  does not prevent later mutation of an external artifact; runtime pinning and
  publication checks must address that separately.
- No workflow callables are imported or executed by byte-verification helpers.
  Deserialization and execution require their own trusted-code boundary.

## First guided implementation slice

`artifacts.py` implements streaming SHA-256 checks and unchanged byte staging.
Its 26 offline tests cover byte preservation, digest errors, unsupported source
types, refusing overwrites, and cleanup of its own staging file after failure.
The user reported the 112-test suite and packaging checks pass at the artifact
checkpoint. The subsequent record-model slice expands this to 166 tests.
The helper itself does not implement envelope models (provided separately
by `records.py`), atomic run publication, locks, or cross-host durability.
Initial run publication and locking are implemented separately in `storage.py`,
with tests reported passing. Opaque result-bundle publication and explicit
recovery are implemented in their separately approved cycle 002 slices.
Worker/result-store integration remains Stage 3 work; cross-host guarantees
remain a live deployment gate. See the [cycle overview](../002-durable-run-state.md)
for the current implementation and verification boundary.

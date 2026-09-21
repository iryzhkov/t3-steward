# Consultation context retention and cold-call spike

This C0 spike is test-only. It does not add a production table, migration, command, or
transport path.

## Source-backed retention seam

`CoordinatorArtifactStore.Publish` already stages, hashes, fsyncs and renames content
into `objects/<prefix>/<sha256>` before publishing producer metadata. On metadata
failure it calls `ArtifactStoragePathReferenced` before deleting a newly created object.
`CoordinatorArtifactStore.Prune` deletes producer metadata first, then calls the same
reference query before deleting each object. Today SQLite's reference query examines only
`coordinator_artifacts`, while `PruneArtifacts` pins an entire producer run through
`coordinator_retention_pins`.

`TestConsultationContextReferenceRetainsBlobAfterProducerMetadataPrune` wraps the real
SQLite catalog with a test-only independent reference holder. It publishes through the
real content-addressed store, adds a project-context reference, expires the producer
artifact metadata, and proves the exact bytes remain while the producer artifact no
longer loads. `TestConsultationContextReferenceProtectsPublicationFailureCleanup`
injects metadata failure and proves the existing cleanup seam preserves a blob referenced
by independent context metadata.

The smallest production primitives before C1 are:

1. Immutable project-context version metadata: project ID, context ID/version, state,
   content role, instruction role, source artifact/provenance, byte size, SHA-256,
   storage path, created/retired timestamps, and actor.
2. Independent blob references keyed by storage path and owning context version. The
   authoritative “referenced?” query must cover producer artifacts and context versions
   in one database view/transaction.
3. Exact-version pins owned by submitted workflows and live consultation requests.
   Retirement rejects new pins but does not remove existing ones.
4. One atomic publication transaction that validates same-project provenance, verified
   successful producer state, source commit, roles, checksum, uniqueness, limits, and
   then inserts version plus blob reference.
5. Orphan reconciliation for crashes after rename and before metadata commit, and after
   metadata pruning and before unlink. It must use the same union reference query.

Whole-run retention pins are not a suitable context owner: they retain unrelated producer
metadata and prevent the required independent lifetime. The current two-phase
metadata-then-file deletion is fail-safe for a crash in the absence of concurrent adoption:
it may leak an unreferenced blob but does not by itself create dangling retained metadata.
That crash property does not provide concurrency safety.

SQLite's single connection serializes database transactions, but it does not close the
object lifecycle race: `CoordinatorArtifactStore.Prune` commits metadata deletion, queries
references, and then calls `os.Remove` outside a transaction. The deterministic
`TestConsultationPruneCanDeleteConcurrentlyAdoptedBlob` pauses after the query observes no
reference, publishes a new metadata owner for the existing hash, and proves prune then
unlinks the newly adopted blob. Publication-failure cleanup has the same read-then-unlink
shape.

The smallest robust fix is a transaction-backed blob lifecycle row keyed by storage path.
Prune atomically changes an unreferenced live object to `deleting`; publication may attach
a reference only while it is live and must retry or recreate after a deleting generation
finishes. Prune unlinks only its claimed generation, then removes or tombstones the row.
Startup reconciliation completes deleting claims and collects live zero-reference
orphans. A shared per-storage-path lock spanning publish's existence/metadata commit and
prune's reference-check/unlink is a smaller single-process alternative, but it must be
shared by every `CoordinatorArtifactStore` value for the canonical root and does not cover
multiple processes. Removing the last pin, retiring a version, producer pruning,
publication failure, and orphan collection require concurrency tests.

Backup coverage is structurally feasible because `backupsnapshot.Manager.Create` copies
the stopped coordinator database and artifact root under the same lock. C1 still needs a
restore test proving context metadata, pins, and shared content survive together, plus an
archive test proving producer records can expire independently. This spike does not claim
those tests or crash durability beyond the existing store behavior.

Proposed conservative policy follows the candidate ADR: 2 MiB per retained context
bundle, storage quotas that refuse new publication instead of evicting pinned versions,
and exact-version submission/request pins. Project aggregate byte and version caps must
be configured explicitly; no numeric aggregate default has yet been accepted.

## Cold-call UX fixture

A campaign can declare one common advisor without discovering workers, provider
credentials, or fleet state:

```yaml
advisor:
  model: codex/gpt-5.6-sol
```

Every task in that submitted campaign uses this explicit campaign-level model override
when it makes the ordinary cold call:

```sh
t3-steward task ask -- "Review this concurrency boundary and identify one concrete race."
```

The question is self-contained and the default command needs no alias. A campaign may
also declare named specialist bindings under `advisors`; choosing one is an optional
advanced form, not required for the common path.

The expected cold-call response is a durable request ID and deadline. The default form
parks the current task through the existing wait path. An explicit asynchronous variant
may return immediately, and a later task-level await may attach the current live turn.
Effective route, pinned definition/context versions, deadline and limits must appear in
campaign plan output before submission.

This surface pins the campaign's explicit advisor model in the submitted snapshot. If the
campaign instead declares `advisor: project-default`, submission resolves and pins that
project default. It never asks the caller to select a worker, inspect credentials, choose a quota pool, name
the default advisor, or discover whether a model session already exists. A missing common
advisor, ambiguous optional specialist, unsupported strict-context adapter, unavailable
route, or exceeded limit fails with a project-level explanation. This is a static UX
fixture only: no live inference, worker discovery, credential lookup, CLI implementation,
or usability study was run.

# Internal durable child-review graph contract

Plan: jocasta:41fbf8dd98bc0f2b67f6b593adb62055@1. This implements the internal enabling unit after accepted admission provenance; it does not complete M16.

## Trusted preparation and policy

The caller reuses the already issued FrozenAuthority and allocated CheckpointAuthority. PrepareReviewChild reads pre-staged coordinator-retained files through CoordinatorArtifactStore.Open, checks all returned metadata against the snapshot, rereads bounded descriptor bytes, and closes descriptors. review.PrepareChild copies exact bytes into an opaque preparation, verifies SHA256/size/ownership and canonical pinnedinput.Manifest, and requires the named criteria input with the exact frozen CriteriaDigest. It compares each prompt with the existing versioned reviewer template plus exact base/head/criteria information. Criteria bytes remain a pinned input, unchanged and visible to each reviewer.

This is a trusted internal coordinator preparation boundary, not authenticated public submission. The low-level pure constructor is suitable only for verified coordinator-retained bytes. An opener or artifact catalog supplied by an executor cannot establish authority. Storage cannot authenticate a caller's claimed custody: its SQL checks validate metadata and the copied verified content proof. There is no production caller or external request path.

SQLite reloads and compares the stored authority and checkpoint inside the transaction; the builder derives policy only from those stored values. There are no overrides for family, tier, role, required count, risk, base/head, manifest digest, or identities. Existing risky critical, two-family and round-limit requirements are unchanged. Fixture family diversity does not invoke providers or waive production policy. The internal default remains MaxTurns=12, empty route Options/QuotaPoolID and zero ResourceDemand. Before production wiring, the caller must deliberately carry approved effort and quota selection, choose the turn budget and capacity constraints, and explicitly pin the session's provider/model/effort restrictions (including Codex-only/medium). These defaults do not establish standalone parity or waive session route pins.

## Identity, graph and deadline

Child run ID is allocated RoundID. Each task ID is exactly MemberTaskID(member.ID). Workflow, initial attempt and input/prompt artifact IDs are deterministic versioned-prefix hashes of reserved checkpoint/member/name values. No UUID fallback or duplicate reviewer generation exists.

Independent and swarm tasks stay isolated. The judge sees only swarm verdict.json dependencies; required members use required task class, optional swarm lenses use surplus. Every task declares only review.md and verdict.json. The environment is a task-isolated Git checkout at the checkpoint's exact head. Base and criteria metadata are frozen in prompts. These declarations do not prove physical Git objects, checkout state or final head.

The builder uses BindRunSink; SQLite uses existing edge binding and initial graph-history conventions. Initial graph tasks are validated in ID order, matching existing SQL history. Bound child definitions are immutable within this unit; replay refuses amended/corrupt definitions instead of trying to reconcile them.

Allocated pending round owns the identity. Creation updates its zero deadline once, with revision fencing, rather than inserting another round. Deadline must be finite, after allocation and after the creation clock; identical round/task deadlines are retained. Replay accepts the original deadline even after it expires. Changed preparation deadline conflicts; it cannot extend time.

## SQLite atomicity and replay

MaterializeReviewChild first checks an existing marker in a coherent read-only transaction. A valid bound replay returns the original immutable ReviewMaterialization receipt, containing original issued authority, complete checkpoint and original graph records. It validates required workflow/run/task/initial attempt/artifact/history presence, immutable content, indexed identities, sink shape and round policy/deadline. It preserves mutable run/sink progress, attempt revisions/control/output state, later attempts and collected round results/reply. Every attempt, including later retries, is enumerated by indexed OR JSON child-run/member-task/initial-ID claims. SQL id/run/task/number/revision must agree with JSON; counters must be positive, membership must be declared, initial identities must be exact, and supervision activation identities must be absent. Duplicate task/number evidence, foreign claims and malformed/ambiguous records fail closed. Later retry revisions/progress may advance without matching original mutable values. Extra definitions/input rows still refuse.

If no marker exists, a writer transaction acquires the existing parent no-op writer fence, reloads authority/checkpoint/round and rechecks for a concurrent winner. Creation checks the parent's exact live current run/task/attempt/assignment/thread/epoch/route and latest-attempt identity. Current same-attempt revision progression is accepted; original IssuedRevision and policy are never refrozen. Ended, stale or superseded creation refuses.

The same transaction inserts child graph rows strictly (no conflict adoption), binds edges and history, freezes deadline, validates the complete graph and inserts the immutable marker. Any failure rolls all changes back, including deadline. Generic SaveCoordinatorRecords is unchanged and is never called by materialization or replay. Its existing runtime mutation conventions are used only by test fixtures to represent real progress.

An allocation with no graph and no child-owned SQL execution/artifact records is a supported preparation crash boundary. Under the no-marker writer fence, creation refuses attempts of every number, retained artifacts/outputs, assignments/waits, workflow/task/run definitions and graph history claiming reserved ownership through indexed OR JSON identities. A Number=2 orphan is not allocation-only recovery; refusal leaves its rows and the round/deadline unchanged. Strict deterministic ID collisions remain errors. Unbound rounds carrying a deadline, results, terminal state or conflicting graph rows refuse. Marker replay never repairs or creates missing runnable rows. A valid bound graph may be replayed read-only after parent termination, including through OpenReadOnly. A missing or corrupt bound graph refuses even if the parent has ended; there is no recovery that resurrects children.

## Retained files and SQL

Staging must finish and establish immutable retention before preparation and before SQL makes child artifacts visible. A read-only staged metadata catalog allows CoordinatorArtifactStore verification without inserting artifact metadata into SQL early. The caller must keep verified paths and bytes immutable and available through commit, replay and collection. No database graph is intentionally published before its retained inputs/prompts exist.

Filesystem custody and SQL commit are separate boundaries. SQLite cannot atomically pin filesystem descriptors or protect against an external process deleting/replacing files after preparation. The preparation proof is conditional on trusted immutable custody; it is not a filesystem transaction or authenticated path assertion. Replay validates SQL metadata and the supplied verified snapshot, not ongoing physical custody. Future callers should reprepare from retained descriptors and retain their staging ownership through commit. Paths outside coordinator custody are not acceptable.

This API performs no filesystem staging or cleanup. A losing concurrent call must not delete shared paths; the winning graph may reference them. Abandoned unique stages may later be reclaimed only after proving they are unreferenced. There is no promise that existing generic ingestion compensation implements this discipline for a future child caller.

## Migration and integration limits

V34 appends coordinator_review_materializations after V33 with unique checkpoint/workflow/run keys and update/delete refusal triggers. Migration is replay-safe and preserves V32 authority/checkpoint/round and V33 display schemas. The V33-specific test explicitly migrates through V33 before checking that historical boundary. Existing SQLite configuration does not enable universal FK enforcement; complete record validation and strict inserts, not REFERENCES declarations alone, protect this API.

There is no down migration or old-binary rollback claim. An old binary does not understand the new binding semantics; a future rollback must quiesce children and retain durable state under a reviewed recovery procedure, not drop markers or refreeze policy.

Public Manifest/Task/Workflow/Attempt/wire/CLI/config schemas and standalone review validation remain unchanged. Accepted title logic is unchanged. Parent wait/early completion, child cancellation/cascade, capacity reservation and final completion enforcement remain the next integration. No runtime hooks, public checkpoint command or completion authority are enabled. Independent declared Sol/Astra medium reviews must assess this candidate; this work is not self-accepted, published, deployed or source-CI verified.

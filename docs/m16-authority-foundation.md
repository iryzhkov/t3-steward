# M16 unit A internal authority contract

Status: inert internal foundation, pending independent review. This is not M16 acceptance or a usable nested review feature.

Baseline: 6b0a6798736d3c9277e7688ef3016c690bf7a9ee. Final local commit/tree, checks and bundle identity are in the collected handoff.md and verification.log receipts.

## Frozen origin and typed model

internal/review/authority.go introduces RequirementsSpec, MemberRequirement, immutable Requirements (private fields; copying constructor and Snapshot), ParentBinding, FrozenAuthority and CheckpointAuthority. NewRequirements validates and canonicalizes before hashing: fixed struct JSON, members sorted by unique safe ID, lowercase SHA-256 criteria/policy digests, exact full lowercase 40/64-character commit IDs, bounded identifiers/routes/roles, maximum 32 members. No map iteration enters the hash. A zero Requirements cannot confer authority.

Routine defaults to two rounds; risky defaults to three; a caller can freeze a stricter positive limit, never exceed these ceilings. RequiredReviewers equals the number of exact required members. The existing ValidateSelection checks role/tier/swarm/judge metadata. M16 requires at least two required deciding reviewers and two provider families, with no availability/route/diversity waiver. Risky additionally requires an independent critical-tier member. Member roles, routes, families, tiers and required flags are frozen exactly; no replacement routes are inferred.

Only a future trusted coordinator admission path may construct the expected snapshot. The criteria and policy digests must come from coordinator-owned requirements, and base/head/input provenance must come from trusted repository/pinned-input inspection. Executor artifacts, local JSON files, worker arguments and manifests are not that origin. The foundation deliberately does not establish these facts from filesystem evidence or expose an executor-facing API.

Repository is the immutable workflow project identifier, not a user supplied checkout path. Admission must resolve that project to the actual repository and pin the full base commit before freeze. The storage transaction verifies the project against the persisted parent workflow. Physical repository identity/base existence and criteria inclusion in the pinned input manifest remain obligations of the later admission integration.

## Storage APIs and identity

V32 appends two separate internal tables; old standalone round records and APIs remain unchanged. Frozen authority is unique per run/task, including across attempts. UPDATE and DELETE triggers protect authority and checkpoint rows. Results remain in the existing revision-fenced review-round table.

- FreezeReviewAuthority(ctx, expected FrozenAuthority): canonical trusted coordinator snapshot; stores once or returns an equal replay. A rehashed weaker/different policy, different parent attempt/thread/assignment/epoch/repository/base/executor route is refused.
- AllocateReviewCheckpoint(ctx, expected FrozenAuthority, checkpoint Checkpoint): requires the persisted equal frozen record; never treats supplied round metadata as policy. Checks current live latest attempt, exact run/task/thread/assignment binding, claimed assignment epoch and executor route, immutable task/workflow/project consistency. First freeze requires the exact current attempt revision; later calls allow the frozen issued revision to be behind the current revision, never ahead.
- CheckReviewAuthorityEvidence(ctx, expected FrozenAuthority, checkpoint Checkpoint, trustedHead string): loads the frozen record, immutable allocation and durable round in one read transaction, compares expected requirements/criteria/policy/identity, then invokes ValidateBoundEvidence. Takes no artifact/result/round payload from the caller.

Write transactions obtain SQLite's writer before reading allocation counts, and fence the current attempt with the existing updateAttemptTx CAS. Concurrent independent connections therefore cannot allocate the same number twice or exceed the stored limit. Allocation creates the pending round and checkpoint together; any failure rolls both back. It leaves parent progress/control/revision unchanged.

Checkpoint ID is unique within frozen authority. Identical canonical checkpoint replay returns the same record/round ID/number. Changed head or input for that ID refuses. Different checkpoint IDs allocate subsequent numbered rounds up to the frozen limit. There is no deallocation/reuse/reset/cancellation API. Authority and checkpoint IDs hash fixed identity arrays; member task IDs are independently bounded hashes, distinct from the parent executor task. Reusing a provider/model route does not reuse a task or session identity.

A bound round's ID and WorkflowRunID reserve the same deterministic child run identity; no child run or task is submitted here. Future integration must use those reserved run/member identities and establish fresh isolated child attempt/thread identities. All members preserve frozen roles/catalog metadata. Pending allocation is never labeled accepted.

## Durable evidence qualification

Only required members decide, consistent with existing standalone combined-verdict rules. Each must have a durable succeeded result, nonempty review.md, no failure, exact member/task/route/role/family/tier/required metadata and retained raw verdict JSON. The validator replays existing Round.ApplyResult/ValidateVerdict and CombinedVerdict instead of trusting cached Combined or a typed Verdict. Strict raw schema, input digest, route, size limits and blocking findings are preserved.

Pending, missing, failed, timed-out, cancelled, invalid, malformed, oversized or forged cached typed evidence cannot qualify. Unbound standalone rounds cannot qualify as bound checkpoints. A different final trusted head refuses, even if a completed review accepted the earlier head. Criteria/requirements mismatches are checked against the stored freeze, including fully rehashed alternate expectations.

A qualifying internal verdict is accept or accept-with-changes; it is evidence qualification, not task acceptance. CheckReviewAuthorityEvidence does not establish trusted HEAD, check live parent completion, park/submit/cancel tasks, release slots, advance a sink, or publish replies. The pure validator is usable only with a storage-loaded durable round by a trusted coordinator caller; it cannot itself attest durability.

## Inert boundary and compatibility

No public manifest, Task/Workflow/Attempt wire field, protocol schema, CLI flag/command, provider service, collector, runtime hook, admission path or completion gate is changed. New APIs have no production callers. V32 migration registration/schema-version advancement are the only existing code changes. Creating the empty tables at ordinary migration is storage preparation only.

Standalone CreateReviewRound/GetReviewRound/RecordReviewResult/PublishReviewReply, old strict clients, existing route selection and result revisions remain valid. Tests retain collector malformed/timeout/replay, composition/isolation/constraints, pending-accept rejection and task-wait identity/park regressions.

## Limits, risks and next units

Admission must freeze trusted policy and repository/input/criteria provenance before any public field is accepted. The coordinator must independently probe final head before completion validation. Atomic child admission/submission/parking, trusted collector child attempt identity, early completion, cancellation and slot release, round escalation, continuation failure ladder and ledger/notification integration are separate later units. Graph-added/supervision parent tasks are not admitted by this foundation; parent must exist as a declared immutable coordinator task and claimed live assignment. Assignment rebinding/new attempts cannot replace the freeze; later retry semantics need an explicit reviewed contract.

The coordinator database is the trusted boundary; arbitrary SQL writes to round evidence are outside the executor threat model. Immutable triggers protect frozen policy/checkpoints against ordinary mutation, and raw result validation protects against cached verdict forgery. No specialized authority export/import, backup validation, or admin pruning integration has been added; later enabling work must audit those integrations before public authority is enabled.

Validation here is local. Independent Sol/Astra medium reviews and eventual exact-source remote CI/full gates are still required before integration/publication. No source pushes, PRs, releases, host changes, or nested tasks are part of this unit.

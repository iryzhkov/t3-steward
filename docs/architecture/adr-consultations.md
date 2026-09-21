# Consultations and immutable project context

Status: C0 candidate; implementation admission requires the feasibility and regression receipts below.
Reviewed contract: Jocasta 288638cf804a0571f75daa2df06e0cfd@5. Execution authorized separately by Jocasta 8551e795ab59445b807b46c9abd3b391@1.
Source baseline: f9f81b18195aa05c7cb186c754bf09af8252b76a.

## Ownership and compatibility

The coordinator owns request identity, terminal response, scheduling, deadlines and caller resumption. Workers own inference effects. Advice has no gate, hold or scheduler authority. Reuse assignments, worker journals and task waits; do not create a second scheduler. No declaration means no consultation binding, context pin, answering attempt or extra session. Additive JSON fields are omitted when absent so historical package and task digests remain stable.

A consultation belongs to its original run/task/attempt and captured task definition, assignment and live turn. Recipient aliases resolve once to pinned bindings at submission. Project default changes apply only to future submissions. Every independent advisor question creates fresh answering work, outside the declared task graph, with no previous Q&A. An overseer binding instead selects an explicit bounded set of request IDs for its activation. Inbox watermarks do not answer requests.

## Transactions

| Operation | Required atomic authority and effect |
| --- | --- |
| Ask | Authenticate live caller, validate immutable allowed binding/counts/custody, compare canonical semantic replay payload, insert request and dispatch intent; default ask also checks wait exclusivity and parks in the same transaction. |
| Async ask | Same acceptance, without subscription or promised automatic wake. |
| Await | Return a terminal answer immediately or attach exclusive subscription to the current live turn; check and subscribe atomically. |
| Respond | Check bound respondent epoch, content custody, nonterminal request and coordinator time strictly before deadline; commit one response and wait settlement/ durable settlement intent together. Exact accepted replay succeeds after deadline; changed replay does not. |
| Cancel/timeout | Revoke response authority and settle subscription. Detach one shared overseer member without cancelling remaining obligations. Execution resources remain held until stop evidence. |
| Early resume/retry/caller completion | Detach or revoke subscriptions transactionally with authority change. Never wake retired attempts or inject into a busy turn. |
| Wake | Reuse normal admission and capacity reacquisition plus stable delivery identity. Preserve original attempt/thread and retained workspace/locks. |

Response, containment and delivery are independent state. Selected unanswered requests fail explicitly at execution termination; unselected requests stay queued. No semantic retry. Unknown effects require recovery evidence rather than fresh dispatch identities.

## Caller capability contract

Environment/task.env identifiers are discovery, not authentication. Consultation operations require a purpose-separated execution capability in addition to authoritative attempt/assignment/thread/live-turn checks. It grants only ask, own-request inspect, await and cancel for immutable allowed aliases; an answer capability grants only one bound request/epoch response. No supervisor or operator credential is introduced into an advisor package.

Do not derive authority from existing dispatch tokens: `internal/backlog/coordinator.go` computes them deterministically from assignment IDs with `stableCoordinatorID`, so they are effect identities rather than secret entropy. Use a cryptographically random execution capability or a purpose-separated MAC under an actual coordinator secret, bound to canonical assignment/attempt/thread/epoch identity. Repeated package delivery must recover the same capability without exposing a general signing key. Coordinator validation compares in constant time and checks the current authoritative binding in the transaction. Retry, assignment replacement, terminality and expiry revoke applicability independently of cryptographic validity. Issuance/custody/replay still needs a source-backed spike before acceptance. Full-access same-UID processes are not OS-isolated; application scope prevents accidental authority confusion, not hostile reads of another process's private files.

## Candidate schema and limits

Common YAML is `advisor: {model: INSTANCE/MODEL}`; project inheritance is `advisor: project-default`. Named bindings live under `advisors`, and multiple bindings require an explicit default. Each binding pins role, resolved route/options, definition version, optional exact context version, instruction artifact and limits. The authoring/plan surface must show all effective bindings before submission. Task policy restricts aliases and context-publication outputs; it never grants operator credentials.

Conservative defaults: question and answer 16 KiB each; 16 attachments and 1 MiB aggregate attachments; 2 MiB retained context bundle subject to a separate input-fit check; one outstanding request per task, 32 per run, 64 total per run, four concurrent answering executions per run; 15-minute deadline and 24-hour maximum; four respondent turns. Project-wide policy additionally caps concurrency and aggregate admitted usage with per-run fair shares. Route quota always applies.

The strict context target is 32k input tokens and 4k output tokens, limited further by backend capacity and reserved margin. Assembly and every runtime continuation must enforce the bound including envelopes, tools and accumulated transcript. A fresh session, prompt instruction, byte cap or provider output limit alone cannot establish this capability. Unsupported adapters must refuse strict packages. Exact accounting method and runtime qualification remain C0 evidence obligations.

## Core record partition

C1 uses a separate stable dispatch-intent identity committed with request acceptance;
request phase alone is not evidence that an external execution started. A separate
subscription row links the consultation to the existing task-wait record and delivery
identity. Ordinary wait algebra remains unchanged, with exclusive consultation parking
checked in both registration directions.

C1 owns durable purpose-scoped capability digest registration, validation and revocation;
C2 owns worker-side private capability emission and recovery. Transport mutations must
not be exposed with only caller-supplied identity or policy. Recipient bindings are
complete pinned values derived from trusted submitted workflow policy, never mutable
aliases resolved from task requests. Every mutation commits a stable native audit event;
exact replay returns its existing receipt without duplicating events.

Question and answer authority is a bounded content-addressed artifact under checked
coordinator custody, as required by the reviewed plan. The response metadata names its
digest and provenance. CLI returns and wake delivery materialize the bounded compact
answer from that verified artifact, so users do not need a second discovery/read call.
An inline delivery copy is a derived presentation, not a second authoritative answer.
Custody validation and retention references must cover the transaction and subsequent
recovery; a test adapter cannot substitute for production custody at the vertical slice.

Activation membership implementation belongs with the actual overseer vertical slice;
its immutable selected IDs and independent per-request outcomes are fixed by the
transition-seams contract. Core schema is not admitted until C0 evidence passes.

## Immutable context custody

Existing `CoordinatorArtifactStore.Publish` stages, hashes and retains blobs before metadata publication. `PruneArtifacts` currently protects whole runs using `coordinator_retention_pins`; this is unsuitable as the sole lifetime owner for project contexts. `ArtifactStoragePathReferenced` currently sees only `coordinator_artifacts`.

Publication must create independent project-owned immutable blob metadata, retaining the existing content-addressed bytes while allowing producer artifact/run records to expire. Both pruning and publication-failure cleanup must consult those independent references. Publication validates same-project provenance, source commits, content/instruction roles, checksums and successful verified producer result. Operator import is a separate actor-scoped operation. Failed or unverified producers cannot publish usable context.

Submitted workflows and live requests pin exact versions; retirement blocks new references while preserving existing pins. Storage limits refuse new publication rather than evicting pinned versions. Metadata remains in the coordinator database and bytes under its artifact root: `backupsnapshot.Manager.Create` already snapshots both under the stopped-database lock. Tests must establish restore and archival behavior, including shared blobs and retirement races.

## C0 evidence required before implementation

- Overseer audit/reproducers and bounded repairs: lifecycle independent of new admission, final report contract and authoritative capacity accounting.
- Live caller authentication and every wait/wake authority path; no mixed parking or inherited admin authority.
- Adapter-controlled materialization, tool results and pre-continuation context accounting.
- Before-feature deterministic regression fixtures and fixed throughput/latency budgets.
- Cold-call UX probe with no model/worker/credential discovery.

No C0 gate is claimed passed by this candidate document. Mutable execution status and evidence pointers live in the Jocasta execution ledger, not this ADR.

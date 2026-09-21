# Consultation caller authority feasibility seam

Status: C0 executable spike only. It adds no production API or schema.

## Result

Application-scoped random bearer capabilities are feasible with the existing worker
exchange and coordinator transaction seams. Task IDs, assignment IDs, `task.env`, thread
IDs, lease IDs, and dispatch tokens are discovery/fencing data. None is authentication.
In particular, `internal/backlog/coordinator.go` derives `DispatchToken` with
`stableCoordinatorID("dispatch", assignmentID)`; it must never sign or authorize a
consultation.

A full-access execution runs as the worker's Unix user. A separate process with the same
UID can read that process's accessible files and memory under the host's normal policy.
A mode-0600 execution-local token file therefore limits accidental/cross-workspace
exposure; it does not provide hostile same-UID isolation. Steward must describe this as
application authorization, not an OS security boundary.

## Source-backed seams

The authenticated worker protocol already HMAC-signs canonical envelopes with a
credential selected by principal/key ID. Server validation binds sender, recipient,
coordinator epoch, worker epoch, allowed message type, deadline, payload digest and
signature (`internal/workerproto/protocol.go`). This authenticates worker/coordinator
exchange; it does not authenticate an agent merely because the agent knows task
identifiers.

The worker's `ProtocolReplayStore` persists request digest and session sequence before
execution, resumes the exact pending request, returns a cached result for exact replay,
and refuses changed-content replay. Its database is bound to coordinator ID, worker ID,
coordinator epoch and worker epoch (`internal/workerruntime/protocol_replay.go`).

Coordinator worker commands provide the matching transaction pattern:
`CommitWorkerCommands` checks coordinator epoch, current worker snapshot/epoch/sequence,
and assignment epoch/state in one SQLite transaction; exact replay is idempotent and
changed replay is refused (`internal/store/sqlite/worker_commands.go`).

## Minimal contract

1. The worker generates 32 random bytes with `crypto/rand` for each assignment authority
   generation. It durably journals the raw token in worker-owned storage before creating
   the external execution or exposing the token.
2. The worker sends only SHA-256(token), capability ID, explicit allowed purposes,
   assignment ID/epoch, worker ID/epoch, and thread ID over the existing authenticated
   exchange. The raw token never enters coordinator storage or protocol logs.
3. Coordinator registration commits the digest only if the authenticated worker and its
   current snapshot own the live assignment at the exact epochs. Exact replay returns the
   existing record. Any changed digest or scope under the same ID is refused.
4. The worker writes the raw token to a mode-0600 file inside the execution-local control
   directory, using create, fsync, and atomic rename. It is not added to `task.env`,
   prompts, execution packages, command lines, environment variables, or logs.
5. Each task request carries capability ID, raw token, one purpose, assignment epoch and
   request idempotency key. In the same coordinator transaction that accepts the request,
   Steward constant-time compares the digest and verifies purpose, assignment/thread
   ownership, assignment and worker epochs, live turn, and nonterminal attempt.
6. Purpose is closed and operation-specific: initially `consultation.ask`,
   `consultation.await`, `consultation.cancel-own`, and `consultation.inspect-own`.
   An ask token cannot approve gates, mutate supervision, or act on another caller.
7. Retry/restart rereads the same journaled token. Rotation creates a new capability ID
   and generation; it never changes a digest under an existing ID. Assignment replacement,
   worker epoch change, thread replacement, terminal attempt/run, cancellation, or
   explicit revocation makes validation fail transactionally. Historical digests may be
   retained for audit but authorize nothing.
8. Transport support should be additive: old workers precisely refuse the new message
   type. Envelope authentication and replay validation occur before capability
   registration or use.

Using a coordinator-secret MAC instead would require proven secret custody and stable
replay access at every task-facing ingress. The current worker exchange secret authenticates
worker envelopes and should not be copied into executions. Random per-assignment bearer
tokens keep the stronger secret out of task custody.

## Executable evidence and limits

`TestConsultationCapabilityAuthoritySpike` creates a test-only table beside the real
migrated coordinator SQLite store. It writes a random 32-byte token to a mode-0600 worker journal before the simulated
effect, stores only its digest, and authorizes within
a transaction against real assignment and attempt rows. The test proves:

- exact registration and use replay succeed;
- forged capability ID/token, purpose mismatch, and assignment epoch mismatch fail;
- digest rotation under one capability ID fails;
- a terminal attempt revokes authority.

The spike does not prove crash-durable filename publication: it fsyncs the token file
before rename but does not fsync the parent directory. Its registration helper also accepts
an arbitrary nonempty purpose rather than enforcing the required closed purpose enum, and
the test record has no expiry field or expiry refusal. These remain mandatory C1 tests and
implementation work alongside protocol wiring, control-directory containment,
migration/backup behavior, multi-process races, and cleanup. The spike establishes only
that digest proof plus live assignment/attempt fencing fits the existing transaction seam
without trusting predictable identifiers.

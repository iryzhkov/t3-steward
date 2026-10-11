# Unit D21-4 handoff — STOP CONDITION

Base: 8837123841415cea741cd5780c75bcf2afa5186f (HEAD and .t3/base-commit verified).
Resulting head: pending report-only implementation commit; obtain exact SHA with git rev-parse HEAD. This handoff is included in that commit.
Changed paths: continuation.md, handoff.md only. No source or test changes.

The authoritative brief requires stopping if existing epoch-change handling cannot give in-flight attempts defined states without new recovery semantics. That condition applies. This is a split report, not a completed implementation or acceptance claim.

Evidence at the pinned base:
- internal/store/sqlite/fleet.go:145-150 matches the brief: same-epoch non-monotonic snapshots or non-increasing observation times return ErrStaleWorkerSnapshot. New-epoch snapshots permit a new counter but do not contain/reconcile attempts.
- internal/workerruntime/protocol_replay.go:289-295 matches the brief: a gap from the durable session sequence returns ErrorReordered. A new session allows sequence one but leaves attempt journal commands intact.
- internal/workerruntime/journal.go:213-224 refuses a changed worker epoch against an existing journal; only the coordinator epoch is adopted, without changing attempts.
- internal/workerruntime/service.go:291-305 reconciles local attempts before processing the authenticated request. internal/workerruntime/runtime.go:805-816 dispatches preparing/prepared records with retained dispatch commands. Restoring a journal to such a phase can therefore execute retained commands before a snapshot reply lets the coordinator apply its terminal-attempt stop rules. Merely creating a fresh protocol session does not meet the no-duplicate-execution requirement.
- internal/backlog/worker_reconcile.go:79-81 only selects claimed/unknown assignments, excluding offered assignments; :260-261 can release absent nonterminal assignments across worker epochs, while :236-241 preserves terminal old-epoch custody without positive observation. These rules do not provide a complete rewind reconciliation for every coordinator assignment and restored journal attempt.

Design decision: preserve existing execution, authentication, enrollment and monotonicity fences. Do not bypass journal identity checks, reset counters or discard custody to make a handshake appear successful. Safe recovery needs an explicit pre-execution recovery barrier and an authoritative attempt disposition exchange, which are new recovery semantics outside this unit's stop boundary.

Proposed split for plan revision:
1. Detection and handshake: typed rewind reasons on both sides, authenticated fresh-session negotiation, and a durable barrier preventing local execution while rewind recovery is unresolved.
2. Attempt reconciliation: bind coordinator dispositions to that barrier; cover offered, claimed/unknown, coordinator-terminal and journal-only attempts; contain old execution before retry; preserve retained-work evidence; release the barrier only after durable acknowledgement. Include copy/advance/restore regression coverage and worker operations documentation.
Neither phase is implemented here. Do not ship a session-only reset as recovery.

Verification (CPU limits: GOMAXPROCS=4 GOFLAGS=-p=4 MAKEFLAGS=-j4 CARGO_BUILD_JOBS=4):
- git rev-parse HEAD: exit 0, matched base before editing.
- make test-affected BASE=8837123841415cea741cd5780c75bcf2afa5186f: exit 0; no Go package changed, so no affected tests ran.
- make test: exit 0 (normal tests, full race tests and go vet).
- make lint: exit 0 (pinned staticcheck and gofmt gate).

Limitations/open risks: all four requested outcomes remain unimplemented, including the rollback regression. Existing stuck rewind behavior remains. No push, tag, release, deployment or nested submission was performed.

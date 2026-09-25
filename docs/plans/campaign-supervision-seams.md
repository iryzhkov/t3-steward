# Campaign supervision: design seam review

Status: verification gate 1 of `docs/plans/campaign-supervision.md`. Design review only.
No implementation, no campaign submission, no deployment is authorized by this document.

Date: 2026-09-16.
Branch: `feature/campaign-supervision`, based on `origin/main` at `d57d01a`.
Worktree: `/home/igor/Work/wt-overseer`.
Live coordinator observed: `normandy-coordinator`, release `0.11.0-rc.52`, commit
`35f89a6ca5a6a078b3e1656c83f997041a9030bf`, coordinator epoch 178, health `degraded`
(one stale worker, `worker:normandy:stale`).

Every file reference below is a path and line in this worktree at the revision named above.
Line numbers move; the symbol names do not, so both are given where the symbol is what matters.

---

## 1. Seam map

### 1.1 Domain types

| File and line | What must change | Why |
| --- | --- | --- |
| `internal/domain/orchestrator.go:19-42` (`ProgressState`) | Possibly add no new progress state; supervision holds are an admission predicate, not an attempt state. If a held task must be visible in `status`, add a *derived* label rather than a stored state. | `ProgressBlocked` already means "dependencies unmet". A new stored progress value would have to be handled by every `Terminal()` caller, the sink projection and the planner; a derived label costs nothing in the state machine. |
| `internal/domain/orchestrator.go:54-86` (`ControlState`, `HoldsProviderSlot`) | No change. | A held task is never assigned, so it has no control state. Adding one would repeat the H4 mistake of inventing a control state for something that holds no slot. |
| `internal/domain/orchestrator.go:106-134` (`Workflow`, `WorkflowRun`) | `WorkflowRun` gains a pointer to the durable supervision record (or its ID), alongside the existing `Sink *SinkTask` field. | The run is already the container for coordinator-owned, non-task state (the sink lives inside the run record, per `adr-s0-sink.md`). Supervision is the same shape of thing. |
| `internal/domain/orchestrator.go:167-207` (`Task`) | Task definitions are unchanged. Gate membership is graph-level, not task-level. | Task definitions are frozen at offer time (`adr-s0-amendment.md`); putting gate membership on the task would make every gate change a task-definition amendment. |
| `internal/domain/sink.go:41-69` (`RunExecutionsQuiescent`), `:143-202` (`ProjectRunSink`) | `ProjectRunSink` gains a supervision settlement barrier check before it writes a terminal sink. | This is the single place a run becomes terminal. The plan's "unresolved review incidents or final gates keep supervised runs nonterminal" has exactly one insertion point, and it is line 151. |
| New file `internal/domain/supervision.go` | `SupervisionRecord`, `Activation`, `Gate`, `GateDecision`, `Hold`, `ReviewIncident`, their validators and their transition functions. | Follows the existing pattern: `domain/task_wait.go`, `domain/node.go`, `domain/sink.go` each own one durable concept, its validation and its pure transition rules. |
| New file `internal/domain/supervision_readiness.go` | One exported predicate, `SupervisionAdmits(run, gates, holds, task) (bool, []Blocker)`, pure, no I/O. | Section 2 requires that candidate selection, offer and claim share one predicate. A pure function in `domain` is the only package all three already import. |

### 1.2 Store, schema and migrations

| File and line | What must change | Why |
| --- | --- | --- |
| `internal/store/sqlite/coordinator.go:13` (`currentSchemaVersion = 17`) | Becomes 18. | Every new durable record needs a version bump; `Migrate` refuses to open a database newer than this constant (`store.go:298-300`). |
| `internal/store/sqlite/store.go:301-321` | Add `{18, coordinatorMigrationV18}` to the versioned list. | Existing mechanism: one DDL string per version, applied in its own transaction by `applyVersionedMigration` (`store.go:334-350`). |
| New file `internal/store/sqlite/supervision.go` | `coordinatorMigrationV18` plus the load/save/transition helpers. Model it on `node_wait.go:15-35` and `task_wait.go:18-32`: one table per record, JSON `record` column, indexed by the identity actually queried, plus SQLite triggers for immutability and retention. | This is exactly how schema 13 and 16 were added, and it keeps `CoordinatorRecords` loading uniform via `loadJSON` (`node_wait.go:37-51`). |
| `internal/store/sqlite/fleet.go:206-364` (`CommitAssignmentPlan`) | Insert the supervision predicate next to `requireExternalSuccessTx` at `:270`. | Offer enforcement point. See section 2. |
| `internal/store/sqlite/fleet.go:392-487` (`ClaimAssignment`) | Insert the same predicate next to `requireExternalSuccessTx` at `:451`. | Claim/start enforcement point. See section 2. |
| `internal/store/sqlite/workflow_projection.go:112-241` (`CommitWorkflowProjection`) | Extend the read set compared inside the write transaction so a concurrent gate or incident change invalidates a settlement projection. | The sink projection already revalidates its complete run-local read set inside its write transaction (`adr-s0-sink.md`, S1 implementation notes); a supervision barrier that is not in that read set is a race. |
| `internal/store/sqlite/graph_revision.go`, `graph_candidate.go:13-54` | The amendment transaction must recompute gate protection and branch-hold closures and invalidate affected gate acceptances. | Plan requirement; see 1.7. |
| `internal/backupsnapshot/` | New tables must be in the coherent snapshot unit. | `adr-s0-sink.md` and the plan both require backup/restore to carry every new durable record and idempotency receipt. |

### 1.3 Readiness and candidate selection

| File and line | What must change | Why |
| --- | --- | --- |
| `internal/backlog/planner.go:363-458` (`planTask`) | Add supervision blockers to `decision.Blockers` before the `len(decision.Blockers) != 0` short circuit at `:443`. | This is where a task stops being a proposal. A blocker here means no offer is ever built for it. |
| `internal/backlog/planner.go:14-21`, `internal/backlog/quota_admission.go:13-25`, `internal/backlog/provider_routing.go:12-19` | Add `PlanningBlockerSupervisionGate` and `PlanningBlockerSupervisionHold` constants. | Blocker codes are the vocabulary `explain` and `campaign check` render; an unnamed blocker is an unexplainable one. |
| `internal/backlog/planner.go:28-40` (`PlanInput`) | Carry the run's supervision state into planning input, the way `ResourceOwners` and `WorkflowCheckoutOwners` already are. | `BuildPlan` is deterministic and pure (`planner.go:149-151`); it must be handed state, never read it. |
| `internal/backlog/planner.go:72-81` (`PlanningConstraint`, `PlanningConstraintSession`) | **Alternative shape**: implement supervision as a `PlanningConstraint` rather than an inline blocker. | Quota admission already is one (`quota_admission.go:75-115`). Recommended default in section 10. |
| `internal/backlogadmin/service.go:641-725` (`view/explanation`), `:727-870` (blocker composers) | Render supervision blockers with gate ID, hold ID, owner and reason. | `explain` is the operator's only window into why a task is not moving; a silent hold is the failure mode the plan forbids. |
| `internal/backlogadmin/viability.go`, `viability_service.go` | `campaign check` should report supervision as an `accepted_waiting` reason, never `impossible`. | `adr-h3-live-campaign-readiness.md` splits permanent from temporary by "does waiting fix it". A gate is temporary by construction. |

### 1.4 Offer creation and assignment claim/start authorization

| File and line | What must change | Why |
| --- | --- | --- |
| `internal/backlog/coordinator.go:54-148` (`FleetCoordinator.PlanAndCommit`) | No new logic. It already builds a pure plan and hands items to the store. | The fence belongs in the store transaction, not in the planner report. |
| `internal/store/sqlite/fleet.go:266-277` | `requireExternalSuccessTx` (`:270`), attempt revision fence (`:266`) and assignability check (`:274`) are joined by the supervision predicate. A refusal calls `skip(...)` so one held task does not block the rest of the plan. | `skip` at `:244-246` is the existing per-item failure mode, and it is the right one: a hold is not an error. |
| `internal/store/sqlite/fleet.go:443-453` | Same predicate, but a refusal returns `ErrAssignmentClaim`, not a skip. | A claim is a single request; the worker must be told no. |
| `internal/backlog/worker_exchange.go:163-315` (`FleetCoordinator.ReconcileWorker`) | No new predicate here. Offers are built from `offeredAssignmentsForWorker` (`:427-437`), and an assignment that was never offered cannot be delivered. | Adding a fourth check on an untransacted read would create a check that can disagree with the store. |
| `internal/store/sqlite/admin_apply.go:18-218` (`ApplyAdminCommand`) | `AdminCommandStart` must refuse a held or ungated task. | The plan is explicit: "Approval/release ... must never use backlog start (which bypasses admission controls)". `backlog start` carries a user-authorized quota waiver (`system-model.md:18-20`); it must not also become a gate waiver. |

### 1.5 T3 session dispatch

| File and line | What must change | Why |
| --- | --- | --- |
| `internal/control/t3/control.go:412-431` (`NewThreadInput`), `:437-512` (`CreateAndStartThread`) | Nothing. | The overseer is dispatched as ordinary assigned work; it needs no new T3 verb. `deterministicID` (`:584-593`) already gives a reproducible thread identity for a retried dispatch. |
| `internal/workerruntime/local_driver.go:579-585` | Nothing, if an activation is delivered as an execution package like any other task. | Reusing the worker path is the plan's explicit requirement ("do not launch raw model processes or directly SSH into agent hosts"). |
| `internal/workerproto/package.go:19` (`ExecutionPackageVersion = 1`) | An activation package needs its own kind marker, or a new package capability. See section 5. | An older worker must refuse an activation package rather than run it as a task. |
| `internal/backlog/execution_package.go`, `internal/backlog/prompt.go` | Build the bounded activation snapshot prompt. | The plan's activation snapshot (tasks, states, graph/gate revisions, event IDs, artifact digests, available actions) is a prompt-envelope problem, and `prompt.go` already owns bounded prompt envelopes. |
| `internal/backlog/turn_completion.go` | An activation's turn completion is not a task result: it must not publish outputs or release dependents. | `turn_completion.go` currently rejects unfinished/background-active results for tasks; an activation needs its own completion rule or it will be verified like a task and fail. |

### 1.6 Node waits and task waits

| File and line | What must change | Why |
| --- | --- | --- |
| `internal/domain/node.go:107-128` (`NodeWaitRequest`, `NodeWait`) | No change. | Node waits observe outcomes; they are the right mechanism for `campaign submit --notify-thread` and for waking an operator, and they already have durable delivery identity. |
| `internal/domain/task_wait.go:146-252` (`TaskWait`, `Parking`) | No change, and this is load-bearing. An overseer activation must **not** register a task-bound wait on itself while idle. | `adr-h4`: a parked attempt is not quiescent, holds its workspace and locks, and blocks the sink. An idle overseer parked on a task wait would hold exactly the resources the plan says it must not hold. |
| `internal/store/sqlite/task_wait.go:48-64` (`saveAttemptFencedTx`) | Reuse the pattern, not the table. | The compare-and-set-on-attempt-revision idiom is the correct fence shape for activation transitions; the table is the wrong home for them. |
| New: supervision outbox | Model on `coordinator_node_waits` delivery intent plus `SettleNodeWaits` in the boundary cycle (`cmd/t3-steward/backlog_v2_runtime.go:332-337`). | One durable wake intent, at-least-once delivery, observed-delivery evidence. The node-wait ADR already paid for this design. |

### 1.7 Sink settlement

| File and line | What must change | Why |
| --- | --- | --- |
| `internal/domain/sink.go:146-153` | Between `BindRunSink` and the quiescence check, return early when the run has unresolved review incidents or an unsatisfied final gate. | Single choke point; everything else that settles a run goes through here. |
| `internal/backlog/projection.go:27-121` (`ProjectWorkflowRuns`) | Pass supervision state in. | The projection loads run-local records already; supervision is one more. |
| `internal/backlog/projection.go:130-196` (`finalizeBlockedSinkPredecessors`) | A task blocked only by a gate must **not** be skipped as impossible. | This function exists to close graphs that can never progress. A gate is not "can never progress"; misclassifying it would silently skip the exact tasks supervision protects. |
| `internal/store/sqlite/workflow_projection.go:112-241` | Add the supervision rows to the compared read set. | Otherwise a gate accepted concurrently with settlement loses the race invisibly. |

### 1.8 Graph amendment

| File and line | What must change | Why |
| --- | --- | --- |
| `internal/backlogadmin/amendment.go:23-113` (`Service.AmendGraph`) | After validation and before `CommitGraphAmendment` (`:112`), recompute gate protection and branch-hold closure for the proposed task set; refuse when invariants cannot be preserved. | The plan requires atomic recomputation. This function already loads the full record set (`:41-44`) and fences on graph revision (`:55-57`). |
| `internal/store/sqlite/graph_revision.go` (`CommitGraphAmendment`) | Inside the same transaction: invalidate affected gate acceptances, revoke or revalidate outstanding offers, increment a review revision. | "Revoke or revalidate outstanding offers in the same transaction as any changed hold/gate scope" is a transaction-boundary requirement; it cannot live in the service. |
| `internal/backlogadmin/graph_clone.go`, `graph_rerun.go` | Decide whether a clone or rerun inherits supervision. Recommended default in section 10. | `adr-s0-amendment.md:98-103`: a clone gives everything new identity and copies no success. A copied gate acceptance would be copied success. |
| `internal/domain/amendment.go` (`AmendTasks`, `ValidateGraphAmendment`) | Reject an amendment that would orphan a gate's `after`/`before` names. | Same class of check as the existing cycle and reachability rejections. |

### 1.9 Admin capability and authorization

| File and line | What must change | Why |
| --- | --- | --- |
| `internal/backlogadmin/types.go:14-41` (`QueryKind` constants), `:43-59` (`QueryKinds`) | Add `QuerySupervision` as a read kind. | `authorizeRemoteAdmin` grants every declared query kind automatically (`cmd/t3-steward/backlog_admin.go:113-119`), so a read view needs nothing else. |
| `internal/backlogadmin/types.go:125-136` (`Action`) | Add the fields a run-and-epoch-scoped capability must be checked against: activation epoch, gate ID, hold ID, incident ID. | `Action` is the only thing the `Authorizer` sees (`service.go:24-26`). A scope it cannot see is a scope it cannot enforce. |
| `internal/backlogadmin/local_transport.go:23-34`, `:36-52` (`Operations`) | Add `supervision-decision` as an operation word. | Operations are a closed set validated by `ValidOperation` (`:54-62`) and pinned per key in `authorized_keys`; a new authority must be a new word, not a new payload on an old one. |
| `cmd/t3-steward/backlog_admin.go:44-62`, `:68-78`, `:106-124` | Add the new operation to `remoteAdminOperations()` only if remote supervision is wanted; add a *new* narrow role for the overseer itself. | Today there are exactly two roles, `local-admin` (everything) and `remote-admin` (reads plus an allowlist). Neither is scoped to a run. See section 4. |
| `internal/backlogadmin/remote_frame.go:52-57` (`RemoteAdminRole`, `LocalAdminRole`) | A third role constant, e.g. `supervisor`. | The role string is what the authorizer switches on (`backlog_admin.go:91-99`). |
| `internal/backlogadmin/remote_server.go:178-188` (`mutatingOperation`) | Classify the new operation as mutating so durable replay protection applies. | A supervision decision is an effect; replaying it must return the first answer, not act twice. |

### 1.10 Campaign CLI and manifest parsing

| File and line | What must change | Why |
| --- | --- | --- |
| `internal/backlog/manifest.go:33-46` (`Manifest`) | Add `Supervision *ManifestSupervision` and `Gates map[string]ManifestGate`. | Additive v2 extension, as the plan requires. |
| `internal/backlog/manifest.go:107-129` (`ParseManifest`) | Nothing. `decoder.KnownFields(true)` at `:112` already makes an older binary refuse a manifest carrying `supervision` or `gates`. | This is the compatibility mechanism, free of charge. See section 5. |
| `internal/backlog/manifest.go:274-340` (`validateManifest`), `:545-578` (`validateAcyclic`), `:667-692` (`validateManifestFiles`) | Gate name, scope, reachability and rubric-file validation; the gate-augmented graph must still be acyclic; `rubric_file` and `prompt_file` must pass the same safe-path rules. | Validation must stay in one validator (`campaign-manager.md:190-192`). |
| `internal/campaign/plan.go:64-91` (`Plan`), `:299-447` (`Project`), `:17` (`PlanSchemaVersion`) | Project gates, the overseer route and expected activation triggers into the static plan; bump the schema version. | "Plan output shows separate worker tasks, review boundaries, inherited routes, overseer route and expected activation triggers" is a `campaign plan` requirement. |
| `internal/campaign/render.go` | Render the above in text, JSON and DOT. | DOT must show gates as distinguishable nodes or edge annotations, not as invented tasks. |
| `internal/campaign/loader.go:119-169` (`Load`), `:226-292` (`inventory`) | `rubric_file` and the overseer `prompt_file` join the packed inventory with role `RolePrompt` or `RoleInput` (`loader.go:66-70`). | Otherwise the files are referenced but not shipped. |
| `internal/campaign/help.go` | New help topic for supervision; `campaign --help` gains the verb. | "Help is part of the contract" (`adr-h3`). |
| `cmd/t3-steward/campaign.go:260-288` | Add `case "supervision":` to the verb switch. | The switch is the whole CLI surface; note the default branch at `:286` already points recovery and amendment at `backlog`. |
| New file `cmd/t3-steward/campaign_supervision.go` | `show`, `decide`, `hold`, `release`, `escalate`, `resolve`. | Keeps `campaign.go` from growing a second responsibility, matching `campaign_check.go` and `campaign_rerun.go`. |
| `cmd/t3-steward/campaign_check.go:154-202` | `check` must report a supervision route that no eligible worker can serve. | An overseer route that cannot be placed makes the whole campaign permanently held; that is an `impossible` verdict at submission, not a surprise at runtime. |

---

## 2. Readiness today, and where the shared predicate goes

### 2.1 Where readiness is computed now

Readiness is computed in three places and they do not share code.

**Candidate selection** is `internal/backlog/planner.go:363-458`. `planTask` accumulates blockers from
four sources: progress and dependency state (`progressBlockers`, `planner.go:509-547`), resource-lock
and workflow-checkout ownership (`:377-396`), per-candidate provider routing
(`providerRouter.Candidates`, `:400`, implemented in `provider_routing.go:142-232`), and the pluggable
`PlanningConstraint` sessions (`:410-412`), of which quota admission is one
(`quota_admission.go:107-227`). If any blocker survives, the function returns at `:443-445` with no
proposal. This is pure: `BuildPlan` mutates nothing (`planner.go:149-151`).

**Offer creation** is `internal/store/sqlite/fleet.go:206-364`, `Store.CommitAssignmentPlan`. Inside one
SQLite transaction it re-checks, per item: the coordinator epoch (`:217`), that the attempt is not
already attached elsewhere (`:247-251`), the expected attempt revision (`:266-269`), external
dependency success (`requireExternalSuccessTx`, `:270-273`), that the attempt is non-terminal and
unassigned (`:274-277`), the worker snapshot epoch and sequence (`:300-311`), enrollment (`:313-320`)
and the graph binding (`bindAssignmentGraphTx`, `:321-324`). It then writes the assignment and bumps
the attempt revision under a compare-and-set (`updateAttemptTx(..., item.ExpectedAttemptRevision)`,
`:352`).

**Assignment claim and start authorization** is `internal/store/sqlite/fleet.go:392-487`,
`Store.ClaimAssignment`. In one transaction: coordinator epoch (`:401`), worker snapshot freshness
(`:404-414`), full assignment identity including lease token (`:419-422`), idempotent replay of an
already-claimed assignment (`:423-438`), attempt claimability (`:447-450`), external dependency
success again (`:451-453`), then a conditional `UPDATE ... WHERE assignment_state = 'offered'`
(`:461-471`) and an attempt-revision compare-and-set (`:472-479`).

The only predicate the three already share is `requireExternalSuccessTx`
(`internal/store/sqlite/node_edges.go:150-193`), and it is shared between the two store paths only,
not with the planner. That asymmetry is the template to follow and also the warning: the planner's
copy of dependency reasoning (`progressBlockers`) exists separately so it can *explain*, while the
store's copy exists so it can *refuse*.

### 2.2 Where the gate and hold check must be inserted

One pure predicate in `internal/domain`, called from three places:

1. `internal/backlog/planner.go:375`, appended to `decision.Blockers` alongside `progressBlockers`.
   Returns *blockers with reasons*, so `explain` can render them.
2. `internal/store/sqlite/fleet.go:270`, immediately after `requireExternalSuccessTx`, refusing via
   `skip(...)`.
3. `internal/store/sqlite/fleet.go:451`, immediately after `requireExternalSuccessTx`, refusing with
   `ErrAssignmentClaim`.

The store-side callers need a thin `supervisionStateTx(ctx, tx, runID)` loader in
`internal/store/sqlite/supervision.go` that reads the gate, hold and decision rows *inside the caller's
transaction*, exactly as `nodeDependenciesTx` does for node edges. The predicate itself takes plain
values and returns a verdict; it performs no I/O, so the same function answers for the planner from
the snapshot in `PlanInput`.

A fourth caller is required and easy to forget: `internal/store/sqlite/admin_apply.go:18-218`,
`ApplyAdminCommand`, for `AdminCommandStart`. Manual start bypasses quota by design; it must not
bypass a gate.

### 2.3 The transaction and revision fence

Offer and claim are serialized by a single SQLite database. Both are `s.db.BeginTx` transactions
(`fleet.go:212`, `fleet.go:396`) against one file, and both commit a compare-and-set on
`coordinator_attempts.revision` through `updateAttemptTx` with the revision read in the same
transaction. An offer moves the attempt from revision N to N+1 while attaching the assignment; a claim
moves it from N+1 to N+2 while flipping `assignment_state` from `offered` to `claimed` under
`WHERE assignment_state = ?` (`fleet.go:461-465`), with `RowsAffected() != 1` treated as a concurrent
change (`:469-471`). A stale offer therefore cannot slip through: by the time a worker presents a
claim, the assignment row must still be `offered` and the attempt must still be at the revision the
claim transaction read.

**A gate decision can join that fence, and it must.** It is the same database, so a decision that
writes the gate row and bumps a supervision revision in one transaction is linearized against every
offer and claim transaction by SQLite itself. The requirement is only that the decision transaction
also performs the compensating writes the plan demands — revoking or revalidating outstanding offers
whose readiness it invalidates — rather than leaving them to a later tick. Concretely, a decision that
*narrows* readiness (a hold, a rejection, an acceptance invalidation) must, in its own transaction,
find assignments in state `offered` for the affected task closure and release them, which is the same
row transition `CommitAssignmentPlan` already knows how to re-arm from (`fleet.go:278-298` handles a
previously released assignment by incrementing the assignment epoch and deriving new lease, dispatch
and thread tokens).

The boundary that must be tested explicitly is claim-versus-hold. An assignment already `claimed` when
the hold commits is past the start boundary; per the plan, v1 reports it and does not roll it back.
The receipt must name the assignment epoch and the attempt revision at which the hold committed, so
the permitted start boundary is observable rather than argued about.

One real limit: the coordinator is one process holding one database
(`internal/store/sqlite/lifecycle.go:16-74`, `AcquireCoordinator`), so this fence is a single-writer
fence. It does not extend to the worker. Between `CommitAssignmentPlan` and `ClaimAssignment` the
offer travels over the worker protocol (`internal/backlog/worker_exchange.go:231-257`) and the worker
may already be preparing a workspace. Holding is not stopping.

---

## 3. State machines

Guards marked *tx* must be evaluated inside the committing transaction, not before it.

### 3.1 Gate

States: `pending-evidence`, `ready-for-review`, `accepted`, `held`, `escalated`, `cancelled`.
Initial state is `pending-evidence`; a gate is closed before any producer runs.

| State | Event | Guard | Next |
| --- | --- | --- | --- |
| pending-evidence | producer task reaches terminal success | *tx* every `after` task succeeded, outputs verified and in coordinator custody | ready-for-review |
| pending-evidence | producer task fails, is cancelled or skipped | — | pending-evidence (gate cannot be approved; the protected task becomes unreachable by ordinary DAG rules) |
| pending-evidence | run cancelled | — | cancelled |
| ready-for-review | overseer or operator accepts | *tx* evidence snapshot matches current graph revision, upstream attempt IDs, result revisions, artifact digests and commit identities; actor holds scope; idempotency key unused or replayed identically | accepted |
| ready-for-review | overseer or operator rejects with corrections | *tx* same evidence match; rejection records correction text | held |
| ready-for-review | review deadline expires | *tx* no decision recorded | escalated |
| ready-for-review | producer retried or replaced | *tx* — | pending-evidence |
| ready-for-review | activation budget exhausted | — | escalated |
| accepted | producer retried, replaced or amended in a way that changes its prerequisites | *tx* no successor of this gate has been offered or started | pending-evidence (acceptance invalidated, receipt retained) |
| accepted | producer retried while a successor is already offered or started | — | refused; operator must use explicit recovery or a new run |
| accepted | graph amendment that changes no gate prerequisite or protected edge | *tx* atomic revalidation succeeds and writes an audit receipt | accepted (carried forward) |
| accepted | any other graph amendment touching the gate scope | — | pending-evidence (conservative invalidation, the default) |
| held | operator-authorized reconsideration with a reason | *tx* fresh evidence snapshot taken | ready-for-review |
| held | new eligible producer evidence | *tx* producers re-succeeded under normal validation | pending-evidence |
| held | overseer re-decides on the same rejected evidence | — | refused (no self-wake loop) |
| escalated | operator-authorized reconsideration | *tx* fresh snapshot | ready-for-review |
| escalated | operator concludes failure | *tx* — | accepted is **not** reachable; the run settles through incident resolution |
| any | run cancelled | — | cancelled |
| accepted, cancelled | anything after terminal sink settlement | — | refused; mutating capabilities are revoked |

### 3.2 Hold

A hold has an owner identity (`overseer:<run>:<epoch>` or `operator:<principal>`), a scope
(`run` or `branch:<task>`), a recorded graph revision and a resolved task set.

| State | Event | Guard | Next |
| --- | --- | --- | --- |
| (none) | overseer or operator places hold | *tx* actor scope covers the run; branch root exists at the recorded graph revision; downstream closure computed in the same transaction | active |
| active | duplicate hold request, same idempotency key and payload | — | active (replayed receipt) |
| active | duplicate key, different payload | — | refused |
| active | owner releases | *tx* releasing actor is the owner, or an operator | released |
| active | overseer attempts to release an operator-owned hold | — | refused |
| active | graph amendment adds descendants to the held branch | *tx* closure recomputed atomically | active (new descendants are held; already authorized work reported separately) |
| active | task inside the closure already claimed | — | active; the claimed attempt is reported, not interrupted |
| active | run cancelled or settled | — | released (authority revoked) |
| released | anything | — | terminal; releasing one hold never releases another, never grants a gate, never changes task success and never bypasses quota |

### 3.3 Supervision activation

An activation carries an epoch, a coordinator-issued renewable lease, a maximum elapsed time, and a
deterministic dispatch identity. At most one activation per run is valid at a time.

| State | Event | Guard | Next |
| --- | --- | --- | --- |
| idle | trigger fires and inbox is non-empty | *tx* run has supervision; activation budget not exhausted; no valid active activation | pending-dispatch |
| idle | trigger fires, activation budget exhausted | — | escalated (one escalation, then wait for operator) |
| pending-dispatch | dispatch confirmed | *tx* worker claimed, lease issued | active |
| pending-dispatch | dispatch provably undelivered | *tx* no execution observed | pending-dispatch (retry with the **original** dispatch identity) |
| pending-dispatch | dispatch ambiguous | — | recovery-required (escalate if recovery cannot prove safety) |
| active | decision recorded | *tx* lease valid, epoch matches, expected revision matches | active (high-water mark advanced atomically with the outcome) |
| active | turn limit or elapsed maximum reached | — | spent |
| active | lease expires | — | revoked (decision authority gone immediately; unacknowledged inbox preserved) |
| active | operator takeover | *tx* operator authenticated | revoked (late decisions from this epoch are fenced out) |
| active | thread lost | *tx* runtime reconciled and proven stopped | idle at epoch+1, with a compact state snapshot; counts toward limits if it ever started |
| revoked | old runtime reconciliation acknowledged | *tx* effect-safe recovery complete | idle (reservations released only now) |
| revoked | old runtime ambiguous | — | recovery-required; no replacement is granted execution resources |
| spent | new events arrive | *tx* budget remains | idle (events stay pending) |
| spent | budget exhausted | — | escalated |
| any | run settles terminally | — | closed; all mutating capabilities revoked |
| any | operator-authorized continuation | *tx* explicit authorization | idle with a fresh budget and a new receipt; counters are never silently reset |

At most two automatically recovered activations per incident, within the run budget; further failures
escalate.

### 3.4 Review incident

| State | Event | Guard | Next |
| --- | --- | --- | --- |
| (none) | trigger produces a decision-requiring condition | *tx* normalized block reason differs from the last recorded reason, or a threshold was crossed | open |
| open | matching gate acceptance recorded | *tx* incident ID matches the gate's own incident | resolved |
| open | overseer escalates with a reason | *tx* actor scope covers the run | escalated |
| open | overseer resolves an observed terminal task failure as conclude-failure | *tx* task is terminally failed; decision names the incident ID and expected revision | resolved |
| open | review deadline expires | — | escalated |
| open | bulk close naming an incomplete set | — | refused; bulk close requires the exact set and expected revisions |
| escalated | operator resolves after documented remediation | *tx* operator authenticated; receipt binds evidence, actor and outcome | resolved |
| escalated | operator requests cancellation | — | resolved through the existing cancellation path |
| escalated | overseer attempts to resolve | — | refused; escalation stays unresolved until operator action |
| resolved | newer pending incident exists | — | that incident stays open; closing one never dismisses another |
| any | run cancelled | — | resolved-by-cancellation; run settles by existing cancellation rules |

### 3.5 Supervised run status precedence

Highest wins. This is the order the plan requires be pinned by domain tests.

| Rank | Status | Condition |
| --- | --- | --- |
| 1 | active | any attempt in the run has a live turn (`Attempt.TurnLive`, `internal/domain/orchestrator.go:232-252`) |
| 2 | waiting-external | any attempt is parked on a task-bound wait (`ProgressWaitingExternal` / `ControlWaitingExternal`) |
| 3 | runnable | any task is `ProgressReady` with no supervision blocker |
| 4 | escalated / needs-input | any open escalated incident, or any attempt in `ProgressNeedsInput` |
| 5 | held / needs-review | any gate in `ready-for-review` or `held`, or any active hold covering a ready task |
| 6 | blocked | tasks remain but nothing above applies (ordinary dependency or capacity blocking) |
| 7 | terminal | sink settled |

Ranks 4 and 5 are deliberately ordered so an escalation outranks a hold: an escalation needs a human,
a hold needs only a decision. `RunExecutionsQuiescent` (`internal/domain/sink.go:41-69`) remains the
gate on rank 7 and is unchanged; the supervision barrier is an additional condition, not a replacement.

---

## 4. Authority boundary

### 4.1 How the admin transport authenticates today

One interface, `backlogadmin.CoordinatorAdminTransport`
(`internal/backlogadmin/transport.go:12-40`), two carriers (`adr-h1-coordinator-admin-transport.md`).

The **local carrier** is an owner-only Unix socket. `ListenLocal`
(`internal/backlogadmin/local_transport.go:197-229`) creates it 0600; the server reads the kernel peer
UID, overwrites whatever principal the request claimed, and grants `LocalAdminRole`
(`remote_frame.go:57`). `localAdminAuthorizer.Authorize` (`cmd/t3-steward/backlog_admin.go:87-100`)
returns `nil` for that role — full authority, no scoping.

The **remote carrier** is SSH to the restricted forced command `t3-steward coordinator-exchange
<operation>`. The operation word is one of the closed set in `local_transport.go:23-34`, validated by
`ValidOperation` (`:54-62`). `RemoteServer.Serve` (`remote_server.go:70-157`) verifies the signed
frame — version `backlog.admin.remote/v1` (`remote_frame.go:37`), session, request ID, sequence,
sent-at, deadline, payload digest, HMAC-SHA256 over a domain tag the worker protocol does not sign —
resolves the client's credential, and relays the ordinary operation envelope to the local socket with
a `RemoteAdminAssertion` (`local_transport.go:124-141`). The local server accepts that assertion only
from a peer that already holds local-admin authority and uses it to *narrow* authority to
`RemoteAdminRole` under the remote client's own principal (`local_transport.go:294-300`).
`authorizeRemoteAdmin` (`backlog_admin.go:106-124`) then grants every declared query kind plus a
hand-maintained allowlist of command kinds (`:44-62`) and operations (`:68-78`), and refuses worker
enrollment by name. Mutating operations get durable replay protection
(`remote_server.go:178-188`, `remote_replay.go`).

Credential namespaces are disjoint: workers use `secretref:f02-protocol/<host>`, admin clients use
`secretref:f03-admin/<client>`. This worktree's own client is configured that way
(`~/.config/t3-steward/coordinator-client.json`: `"credential_ref":
"secretref:f03-admin/omarchy-pc"`, address `normandy-steward-admin`, SSH, 30s timeout).

### 4.2 Can a run-and-epoch-scoped capability with an action allowlist be issued on this transport?

Yes, mechanically. Three things are needed and none is a new transport:

1. A third role constant beside `LocalAdminRole` and `RemoteAdminRole` in
   `internal/backlogadmin/remote_frame.go:52-57`.
2. Scope fields on `backlogadmin.Action` (`types.go:125-136`) — run ID, activation epoch, gate,
   hold, incident — because the `Authorizer` interface (`service.go:24-26`) sees nothing else. The
   existing `Action` already carries `WorkflowRunID` and `TaskID`, which is the shape to extend.
3. A `supervision-decision` operation word in `local_transport.go:23-34` and `Operations()`
   (`:36-52`), so an `authorized_keys` line can pin a key to that operation and nothing else, and so
   `mutatingOperation` (`remote_server.go:178-188`) applies replay protection to it.

The binding of a credential to a *run and epoch* is the part that does not exist. Today a credential
reference maps to a client name and a fixed role; there is no per-run issuance, no expiry and no
revocation list. Building it means either issuing a short-lived credential per activation into the
admin secret store (which is a credential-brokerage feature the campaign-manager plan explicitly
excluded, `campaign-manager.md:376`), or keeping one long-lived supervisor credential and enforcing
the run/epoch scope **server-side** by comparing the request's claimed run and epoch against the
coordinator's own supervision record. The second is strictly better: the coordinator already knows
which activation is valid, and a credential that can only act on the run it was woken for is a
property of the authorizer, not of the secret.

### 4.3 Credential isolation on a worker host — deployment blocker

**This cannot be enforced with the current deployment, and it should be reported as a deployment
blocker rather than advertised as isolation.**

The admin credential is a file. `FileAdminCredentialResolver`
(`internal/backlogadmin/remote_credential_store.go:18-69`) reads
`~/.config/upkeeper/secrets/f03-admin/<client>` — an owner-only 0600 regular file, resolved under the
invoking user's home directory (`:37-46`), read through `privatefile.Read` (`:60`). Any process
running as that user can read it.

A campaign task is a T3 thread created on the worker host by
`internal/workerruntime/local_driver.go:579-585`, through `Control.CreateAndStartThread`
(`internal/control/t3/control.go:437`). The backlog runner creates threads with
`RuntimeMode: "full-access"` (`internal/backlog/runner.go:648-650`). A full-access agent session runs
shell commands as the worker's user. That user's home is where the admin credential lives. Therefore,
on the current deployment, **any campaign task on a host that also has an admin credential can read
that credential and impersonate the admin client**, including one belonging to a different run.

The containment path would fix this, and it is explicitly not the production path.
`providercontainment.Spec.TaskEnvironment` (`internal/providercontainment/spec.go:20-25`) passes
exactly the six names in `domain.TaskWaitEnvironmentNames()`
(`internal/domain/task_wait.go:29-35`) into a contained process and drops everything else;
`taskIdentityEnvironment` (`internal/providercontainment/run_linux.go:31-42`) is the enforcement.
The worker credential prefix `T3_STEWARD_CREDENTIAL_`
(`internal/workerruntime/credentials.go:22-33`, `:88-96`) is not in that set, so under containment a
task cannot see it. But `cmd/t3-steward/provider_containment.go:17-18` states plainly: "Operator
qualification entry point. The normal worker remains fail-closed until supervisor identity/recovery
and scoped T3 control are integrated." Containment is an operator qualification tool, not the path
ordinary tasks take.

What is genuinely enforceable today, and should be stated as the actual boundary:

- The credential is a **file readable by the worker's user**, not a secret scoped to a run. The
  isolation claim that can be made honestly is "an unrelated *host* cannot use it", because the
  coordinator overwrites the principal from the verified signature and the client name is pinned in
  the frame.
- The abuse that matters is bounded by the **server-side authorizer**, not by credential placement.
  A supervisor role that refuses every operation outside the run and activation epoch the coordinator
  currently considers valid turns credential theft into "a task can make decisions on its own run's
  gates while an activation is live" — still wrong, but no longer a cross-run compromise.
- The activation epoch fence makes a stolen credential useless between activations, which is most of
  the time.

Recommended framing for the lead: implement the server-side run-and-epoch scope now, document the
co-tenancy exposure as a known limitation with the mitigation above, and make the isolation claim
conditional on either (a) running supervision only on a host with no campaign tasks, or (b) landing
contained execution as the default. Do not write "credentials are isolated from worker-task
environments" in any user-facing text until one of those is true.

---

## 5. Protocol capability negotiation

### 5.1 What is negotiated today

There are four independent version surfaces, and only one of them is a negotiation.

**Worker protocol.** `workerproto.Version = 1` (`internal/workerproto/protocol.go:22`) is in every
`Envelope` (`:52-68`). `CapabilityNegotiation` (`:70-75`) carries `SupportedVersions`, a free-form
`Capabilities []string`, and message/artifact byte limits. The worker advertises its capability set
from `advertisedCapabilities` (`internal/workerruntime/runtime.go:131-138`), which merges configured
capabilities with the ones the build supports, and those land in the snapshot inventory
(`WorkerSnapshot.Inventory.Capabilities`). The coordinator gates behaviour on them by string: see
`internal/backlog/worker_exchange.go:113`, which only sets the causal acknowledgement fields when the
worker's inventory contains `workerproto.CapabilityTaskWaitCollectionFence`
(`protocol.go:139`, `"task-wait-collection-fence-v1"`). This is the working precedent, and the live
fleet shows it in use: all three workers currently advertise
`["git","huyang","preflight","task-wait-collection-fence-v1"]`.

**Admin transport.** `backlogadmin.Version = "backlog.admin/v1"` (`types.go:10`) versions the
request/response schema; `LocalTransportVersion = "backlog.admin.local/v1"`
(`local_transport.go:21`) and `RemoteTransportVersion = "backlog.admin.remote/v1"`
(`remote_frame.go:37`) version the two carriers. These are checked for equality, not negotiated; a
mismatch is `ErrUnsupportedVersion` (`service.go:19`) classified as `ClassProtocol` (exit 7,
`adr-h1`).

**Manifest.** `backlog.ManifestVersion = 2` (`internal/backlog/manifest.go:21`), validated in
`validateManifest`.

**T3 server.** `internal/compat/compat.go:13-16` pins a tested range (`0.0.38..0.0.38`) with
`ControlAllowed` (`:65-86`) refusing control actions outside it unless overridden. This is *not* the
CLI/coordinator/worker negotiation the plan means; it is the watchdog's compatibility with the T3
server.

### 5.2 How an older peer is made to reject a supervised manifest

**An older CLI and an older coordinator already reject it, for free.** `ParseManifest`
(`internal/backlog/manifest.go:107-129`) builds its YAML decoder with `decoder.KnownFields(true)` at
line 112. A binary whose `Manifest` struct (`:33-46`) has no `supervision` or `gates` field fails to
decode with a field-name error, and `campaign validate`, `campaign plan` and coordinator ingestion all
go through that one function. No version bump and no feature flag is needed for that half. The refusal
message should be checked for quality: the default yaml error names the unknown field, which is
adequate, but a `campaign validate` against an old binary should ideally say "this binary does not
support supervision" rather than "field supervision not found in type backlog.Manifest". That is a
message-quality item, not a safety item. (Since addressed: the refusal names the release that
refused the field, and from rc.97 the decoder text reads "field supervision not found in the
workflow" rather than naming the Go type.)

**An older worker is the real gap**, and it needs an explicit capability. A worker one release behind
would happily execute an activation execution package as if it were an ordinary task, produce a turn,
and let the coordinator try to verify it as a task result. The mechanism to prevent it already exists
and should be copied exactly:

1. Declare `CapabilityCampaignSupervision = "campaign-supervision-v1"` in
   `internal/workerproto/protocol.go`, beside `CapabilityTaskWaitCollectionFence` at `:139`.
2. Advertise it from `internal/workerruntime/runtime.go:131-138`.
3. Gate on it in planning: a worker whose `Inventory.Capabilities` lacks it is not a candidate for a
   supervision activation. The natural home is the placement capability check that already exists —
   `internal/backlog/placement.go` handles `ManifestPlacement.Requires` and produces
   `ExclusionHostNotAllowed`-style exclusions — so an activation simply requires the capability the
   way a task requires `git` or `huyang`.
4. Refuse at admission, not at dispatch: `campaign check` (`cmd/t3-steward/campaign_check.go:154-202`)
   should report `impossible` when no eligible worker advertises the capability, matching the
   `adr-h3` rule that an unsatisfiable requirement is permanent.

**The coordinator itself** must refuse a supervised manifest when its own admission path is older than
the schema. Since the coordinator is the one parsing the manifest, `KnownFields(true)` covers it. The
remaining case — a newer CLI talking to an older coordinator — is covered by the same mechanism,
because the coordinator re-parses the bundle at ingestion (`internal/backlog/ingest.go`), so the CLI
cannot smuggle fields past it.

One trap worth naming: `CapabilityNegotiation.Capabilities` is advisory metadata in the handshake,
while the enforced set is the one in the durable worker snapshot inventory. `worker_exchange.go:113`
reads the snapshot, not the handshake. Follow that; a handshake capability is not evidence.

---

## 6. Schema

### 6.1 workflow.yaml v2, additive

Two new top-level keys. Both optional; absent means exactly today's behaviour.

```yaml
supervision:
  route:                      # one route, validated by the existing route validator
    instance: REGISTERED_INSTANCE
    model: REGISTERED_MODEL
    quota_pool: REGISTERED_POOL
  prompt_file: prompts/overseer.md
  max_activations: 12
  max_turns_per_activation: 3
  activation_deadline: 2h     # wall clock per activation
  idle_escalation_after: 24h
  escalation:                 # reuses existing notify configuration only
    notify_thread: true

gates:
  implementation_review:
    after: [implement]        # observed producers, task names
    before: [qualify]         # protected tasks, task names; empty means final gate
    rubric_file: inputs/acceptance.md
    final: false              # true guards run settlement instead of a downstream task
```

Validation rules, all in `internal/backlog/manifest.go`:

1. `supervision.route` is exactly one route, validated by `validateRoutes`
   (`manifest.go:503-524`) and `validateRoutePlacement` (`:526-543`). One route, not a list — the same
   reasoning `adr-s0-amendment.md:89` gives for `task set`: alternatives make the effective route
   ambiguous.
2. `supervision.prompt_file` and every `gates.*.rubric_file` are relative bundle paths validated by
   `validateRelativePath` (`:633-651`) and `validateManifestFiles` (`:667-692`), and refused for
   symlinks and traversal by `safeBundleFile` (`:694-728`).
3. Gate names match `manifestNamePattern` (`manifest.go:31`) and must not collide with any task name
   or with the reserved sink name `domain.SinkTaskName` (`internal/domain/sink.go:10`).
4. Every name in `after` and `before` is a declared task. Unknown names are refused.
5. `after` is non-empty. A gate with nothing to observe is a permanent hold.
6. `before` may be empty only when `final: true`. This is the "optional final gate guarding run
   settlement" the plan asks for, modelled explicitly rather than as a dummy agent task.
7. No task may appear in both `after` and `before` of the same gate; no gate may create a cycle in the
   task-plus-gate graph. Reuse and extend `validateAcyclic` (`manifest.go:545-578`).
8. Each protected task must already reach its observed producers through ordinary `needs` edges. A
   gate constrains dispatch; it does not create a data dependency and cannot substitute for one.
9. Multiple gates may protect one task; all must be accepted.
10. `gates` present without `supervision` is refused. This is the plan's v1 rule against an
    accidentally permanently-held unsupervised gate.
11. `max_activations`, `max_turns_per_activation` are positive integers with a declared upper bound;
    `activation_deadline` and `idle_escalation_after` are duration strings parsed the way the rest of
    the configuration parses durations (`hardening-frozen-interfaces.md:105-106` records that steward
    durations are strings, not seconds).

### 6.2 New durable records

All coordinator-owned, all in schema 18, all following the `id TEXT PRIMARY KEY` plus JSON `record`
column shape of `coordinator_node_waits` (`internal/store/sqlite/node_wait.go:16`) and
`coordinator_task_waits` (`task_wait.go:19-25`).

| Table | Key | Holds | Notes |
| --- | --- | --- | --- |
| `coordinator_supervision` | run ID | route, limits, prompt artifact ID, current epoch, budget consumed, event cursor, supervision revision | One row per supervised run. `UNIQUE(run_id)`. |
| `coordinator_supervision_activations` | activation ID | run, epoch, dispatch identity, lease token and expiry, state, consumed high-water mark, outcome | Indexed by run. An `UPDATE` trigger refusing changes to a closed activation, matching `immutable_task_wait_event` (`task_wait.go:30-31`). |
| `coordinator_supervision_gates` | gate ID (`gate:<run>:<name>`) | state, graph revision, observed producers, protected tasks, evidence snapshot digest | Recomputed on every graph revision, like the sink. |
| `coordinator_supervision_decisions` | decision ID | gate, activation epoch, actor, reason, evidence snapshot (attempt IDs, result revisions, artifact digests, commit identities), outcome, request ID | **Append-only.** A `BEFORE UPDATE` trigger raising `ABORT`, so history survives reconsideration. |
| `coordinator_supervision_holds` | hold ID | run, scope, owner identity, graph revision, resolved task set, state, reason | Indexed by run and by owner. |
| `coordinator_supervision_incidents` | incident ID | run, source event, source task attempt, revision, required disposition, state | Indexed by run and state. |
| `coordinator_supervision_outbox` | delivery ID | activation, payload reference, delivery state, observed message ID | Same observe-before-retry contract as the node-wait outbox (`adr-s0-node-wait.md:99-107`): a committed sending state never authorizes a second send. |
| `coordinator_supervision_receipts` | request ID | operation, payload digest, first answer | Same-key-same-payload replays the answer; same-key-different-payload is refused. Mirrors `remote_replay.go` and the submission service. |

Retention: a supervision record pins its run's artifacts the way `coordinator_retention_pins` does for
node references (`node_wait.go:17-34`), with a `release_supervision` trigger on run deletion modelled
on `release_rerun_source` (`graph_rerun.go:13-15`).

### 6.3 Migration approach

Exactly the existing one. Add `coordinatorMigrationV18` as a DDL string in the new
`internal/store/sqlite/supervision.go`, register `{18, coordinatorMigrationV18}` in the versioned list
(`store.go:301-321`), bump `currentSchemaVersion` to 18 (`coordinator.go:13`).
`applyVersionedMigration` (`store.go:334-350`) runs the DDL and records the version in one
transaction. Every statement uses `CREATE TABLE IF NOT EXISTS` / `CREATE TRIGGER IF NOT EXISTS`, as
every prior migration does.

Backfill is trivial and must be explicit: existing runs get **no** supervision row. Absence of a row
is the unsupervised case, which is the current behaviour, so no data migration is needed. Do not
create empty rows.

Forward-only, like schema 12 (`adr-s0-sink.md:79-81`): an older binary refuses a migrated database
because `Migrate` rejects a version newer than its own constant (`store.go:298-300`). Rollback is a
coherent stopped backup restore with a stated loss window, never a binary downgrade against migrated
state.

---

## 7. Registered routes

Read from `~/.config/t3-steward/` on this host and from the live coordinator, read-only. No model
quota was consumed.

**Local configuration.** `~/.config/t3-steward/config.yaml` is the watchdog configuration: thresholds,
resume policy, polling, per-bucket overrides for `claudeAgent`/`seven_day*` and `codex`/`secondary`,
and `backlog: enabled: true`. It declares no provider routes.
`~/.config/t3-steward/coordinator-client.json` points this host at `normandy-steward-admin` over SSH
as `normandy-coordinator` with `secretref:f03-admin/omarchy-pc`.
`~/.config/t3-steward/worker-bootstrap.json` and `persistent-worker.yaml` configure this host as a
worker. There is no local catalog of routes; the coordinator owns it.

**`testdata/fleet` is a fixture, not the fleet.** `testdata/fleet/fleet-intent.json` and
`testdata/fleet/projection-coordinator.json` name provider instances `claudeAgent`, `codex`,
`opencode`, quota pools `build` and `default`, and models `claude-haiku-4-5`, `claude-opus-4-5`,
`claude-sonnet-4-5`, `gpt-5-codex`, `glm-4.6`. **None of those pool names or model names matches the
live coordinator.** Do not take route IDs from testdata.

**Live registered routes**, from `t3-steward backlog workers --json` against `normandy-coordinator`
at 2026-09-16T14:29Z:

| Worker | State | CPU class | Executor slots | Quota pool concurrency | Provider instances and models |
| --- | --- | --- | --- | --- | --- |
| `normandy` | stale, not enrolled, health ready | — | — | `claude-main`:4, `codex-main`:5, `opencode-free`:5 | `claudeAgent` → `claude-haiku-4-5`, `claude-sonnet-5` (pool `claude-main`, available); `codex` → `gpt-5.6-sol` (pool `codex-main`, available); `opencode` → `opencode/muse-spark-1.3-contributor-free` (pool `opencode-free`, available) |
| `homelab` | draining, not enrolled, health ready | medium | 4 | `claude-main`:4, `codex-main`:5 | `claudeAgent` (pool `claude-main`, **available: false**); `codex` (pool `codex-main`, **available: false**) — no models advertised in the current snapshot |
| `omarchy-pc` | draining, not enrolled, health ready | high | 8 | `claude-main`:4, `codex-main`:5, `opencode-free`:5 | `claudeAgent`, `codex`, `opencode`, all **available: false** — no models advertised in the current snapshot |

All three advertise capabilities `git`, `huyang`, `preflight`, `task-wait-collection-fence-v1`.
`t3-steward backlog status --json` reports 3 open quota pools, 2 ready workers, 1 offline, one
reconciliation issue `worker:normandy:stale`, and coordinator health `degraded`.

**What this means for an overseer versus workers.**

- The registered instances are exactly three: `claudeAgent`, `codex`, `opencode`. The registered pools
  are exactly three: `claude-main`, `codex-main`, `opencode-free`.
- The only models currently advertised anywhere on the fleet are `claude-haiku-4-5`,
  `claude-sonnet-5`, `gpt-5.6-sol` and `opencode/muse-spark-1.3-contributor-free`, and they are
  advertised **only by `normandy`**, which is stale and unenrolled.
- An independent overseer route is therefore achievable at the *pool* level — put the overseer on
  `codex-main` and workers on `claude-main`, or the reverse — but there is **no registered model that
  is obviously "stronger"** in the way the user's shorthand "Fable" and "Astra" implies. Neither name
  appears anywhere in the fleet. The plan is right to say discover IDs instead of treating labels as
  configuration; the discovery result is that those labels do not currently resolve.
- A historical run used `gpt-5.6-luna` (see section 8), which is not in the current advertised set.
  Model sets move; a supervised campaign must name a model that the coordinator advertises at
  submission time, and `campaign check` must verify it, rather than pinning a name from a document.
- `claude-main` concurrency is 4 and `codex-main` is 5, so an overseer on its own pool does not
  contend with worker tasks for pool concurrency. That is the strongest available argument for
  separate pools rather than separate models.
- **Qualification blocker to record now**: with `homelab` and `omarchy-pc` draining and unenrolled and
  `normandy` stale, no worker is currently in a state to be offered work at all. Gates 3 through 7 of
  the plan cannot run until the fleet is re-enrolled.

---

## 8. Trace of a previous campaign (read-only)

The coordinator was reachable. Nothing was started, retried or submitted; only `campaign list`,
`campaign graph`, `campaign show`, `backlog workers` and `backlog status` were run, all read-only.

`t3-steward campaign list --json` returns 790 runs. Recent history, by declared task count:

| Campaign | Run | Tasks | Outcome |
| --- | --- | --- | --- |
| `track-s-evidence-batch` | `run-f0e0d14a…`, `run-dea852f6…`, `run-b9982f5a…` | 11 | cancelled, cancelled, ready |
| `matrix-spill` | `run-03cdffee…` | 8 | succeeded |
| `matrix-saturate` | `run-f0ca5526…` | 6 | succeeded |
| `dogfood-parallel-dag` | `run-9e31f845…`, `run-f6066f4d…`, `run-b39767c0…`, `run-be5d8db3…` | 3 | failed, succeeded, failed, succeeded |
| `upkeeper-go-migration` | `run-1594606f…`, `run-f009add9…` | 3 | failed, failed |
| `matrix-fresh-parallel` | `run-1e3fb063…` | 2 | succeeded |
| `dogfood-single-lead`, `cm4-idempotency-probe`, `hardening-live-*`, `legacy-single-task` | various | 1 | mixed |

Tracing one multi-node run in detail, `run-be5d8db3d951474c528e642fdfad643c`
(`dogfood-parallel-dag`, succeeded):

- `campaign graph --json` shows four nodes — `surface`, `tests`, `join`, `__sink` — and five edges:
  `surface → join`, `tests → join`, and all three tasks → sink. A genuine fan-in DAG.
- `campaign show --json` shows three distinct T3 threads, one per task:
  `thread-42d1c397cfac4423f44c33c53014b4be` (`surface`),
  `thread-dd2fade92ed9db66000f07a7c39bcaa6` (`tests`),
  `thread-fdc4371850abbed9f495178f5e0e9921` (`join`). All three placed on `homelab`, all three on
  model `gpt-5.6-luna`. `__sink` has no thread, no worker and no route, exactly as
  `adr-s0-sink.md` specifies.

**Conclusion, evidence-bound: earlier "single session" campaigns were a single-task authoring choice,
not a dispatch failure.** The scheduler demonstrably creates one separate T3 session per declared
task, with correct dependency ordering and a coordinator-owned sink, at three, six, eight and eleven
nodes. Runs that used one session declared one task. `upkeeper-go-migration` declared three tasks and
failed for the reason recorded in `adr-h4-wait-aware-task-lifecycle.md` — a task parked on CI waits
while the worker read the ended turn as a finished task — which is a lifecycle defect that has since
been addressed, not a failure to schedule separate sessions.

This matters for the plan's Stage A: the unsupervised multi-session path does not need to be built,
only proven and documented. Verification gate 2 is a template and guidance change plus a regression
test, not an implementation.

---

## 9. Implementation lane breakdown

**Constraint from the lead**: another engineer is changing `internal/policy` (quota extension rearm
mid-window) on `fix/quota-extension-rearm`. **No lane below touches `internal/policy`.** Quota
admission is a different package — `internal/backlog/quota_admission.go`, which implements the
`PlanningConstraint` interface (`internal/backlog/planner.go:72-81`) — and lanes treat
`NewQuotaAdmissionPolicy` / `StartPlan` / `Evaluate` / `Reserve`
(`quota_admission.go:75-238`) as an existing predicate to call. No lane modifies quota admission
either. If a lane finds itself needing to change quota behaviour, that is a stop condition for the
lead, not a local decision.

### Stage A lanes

**Lane A1 — authoring guidance and templates.**
- Files: `docs/examples/campaign/three-node/**`, a new `docs/examples/campaign/` multi-task template,
  `internal/campaign/help.go`, `cmd/t3-steward/campaign.go:19-109` (usage strings only).
- Tests: `internal/campaign/examples_test.go`, `cmd/t3-steward/campaign_test.go` golden help.
- Agrees with: nothing. Fully disjoint.

**Lane A2 — unsupervised baseline proof.**
- Files: test files only — a new `internal/backlog/` or `cmd/t3-steward/` integration test asserting a
  three-node DAG produces three distinct dispatch identities and no supervision record.
- Tests: its own.
- Agrees with: the absence of a supervision record, which is Lane B1's type. Can be written against a
  nil check and tightened after B1 lands.

### Stage B lanes

**Lane B1 — domain types and the readiness predicate.** *Blocking; everything else waits on its
interfaces.*
- Files: `internal/domain/supervision.go` (new), `internal/domain/supervision_readiness.go` (new),
  `internal/domain/supervision_test.go` (new). Additive fields on
  `internal/domain/orchestrator.go:119-134` (`WorkflowRun`) only.
- Tests: state-machine table tests for section 3, predicate truth tables.
- Must agree on: the exact signature of `SupervisionAdmits`, the blocker codes it returns, and the
  shape of the state snapshot it consumes. Publish these in the first commit and freeze them.

**Lane B2 — manifest, loader and static plan.**
- Files: `internal/backlog/manifest.go`, `internal/backlog/manifest_test.go`,
  `internal/campaign/loader.go`, `internal/campaign/plan.go`, `internal/campaign/render.go`, their
  tests, `docs/examples/campaign/` supervised example.
- Tests: `manifest_test.go` validation negatives (rules 1-11 of section 6.1), `plan_test.go`
  projection and ordering, `render_test.go` text/JSON/DOT goldens.
- Must agree on: the domain gate and supervision types from B1 (it produces them), and the
  `PlanSchemaVersion` bump with Lane B6.
- Does **not** touch: `internal/store`, `internal/backlogadmin`, `cmd/`.

**Lane B3 — store schema and transactional fences.**
- Files: `internal/store/sqlite/supervision.go` (new), `internal/store/sqlite/supervision_test.go`
  (new), plus surgical edits at `internal/store/sqlite/store.go:301-321`,
  `internal/store/sqlite/coordinator.go:13`, `internal/store/sqlite/fleet.go:270` and `:451`,
  `internal/store/sqlite/admin_apply.go` (start refusal),
  `internal/store/sqlite/workflow_projection.go:112-241` (read set).
- Tests: migration forward/idempotency, offer-versus-hold race against a shared store, claim-versus-hold
  race, decision-versus-settlement race, replay receipts, restart reconstruction.
- Must agree on: B1's predicate signature; the `supervisionStateTx` loader shape it exports for
  reuse; the assignment-release transition it performs when narrowing readiness (shared with B4's
  understanding of offer re-arm at `fleet.go:278-298`).
- Does **not** touch: `internal/backlog/planner.go` (that is B4).

**Lane B4 — planner, projection and sink barrier.**
- Files: `internal/backlog/planner.go`, `internal/backlog/planner_test.go`,
  `internal/backlog/projection.go`, `internal/backlog/projection_test.go`,
  `internal/domain/sink.go:143-202` and `internal/domain/sink_test.go`,
  `internal/backlog/sink_projection_test.go`.
- Tests: held task produces no proposal and an explainable blocker; a gate-blocked task is not skipped
  by `finalizeBlockedSinkPredecessors`; unresolved incident keeps the sink nonterminal; resolved
  incident lets it settle; unrelated branch still proceeds.
- Must agree on: B1's predicate and blocker codes; `PlanInput` field name for the supervision snapshot,
  which Lane B7 populates.
- Note the `internal/domain/sink.go` overlap with B1: B1 adds fields, B4 adds the barrier. Sequence
  B1 first; they touch different functions in the file.

**Lane B5 — activation lifecycle, outbox and dispatch.**
- Files: `internal/backlog/supervision_activation.go` (new),
  `internal/backlog/supervision_outbox.go` (new), their tests,
  `internal/backlog/turn_completion.go` (activation completion rule),
  `internal/backlog/execution_package.go` and `internal/backlog/prompt.go` (activation snapshot
  envelope), `internal/workerproto/protocol.go` (capability constant),
  `internal/workerruntime/runtime.go:131-138` (advertise it).
- Tests: lease expiry revokes authority; undelivered dispatch retries with the original identity; a
  started-then-vanished activation counts toward the budget; at most one valid activation per run;
  events arriving during review stay pending; coalescing preserves all evidence references.
- Must agree on: B3's activation table and receipt semantics; B1's activation state machine; B6's
  decision operation for what an activation is allowed to call.
- Does **not** touch: `internal/control/t3` (no change needed).

**Lane B6 — admin capability, authorization and transport operation.**
- Files: `internal/backlogadmin/supervision.go` (new), `internal/backlogadmin/types.go`,
  `internal/backlogadmin/local_transport.go:23-52`, `internal/backlogadmin/remote_frame.go:52-57`,
  `internal/backlogadmin/remote_server.go:178-188`, `cmd/t3-steward/backlog_admin.go:44-124`, their
  tests, `docs/backlog-v2-operations.md`.
- Tests: a supervisor role is refused every operation outside its run and epoch; a stale epoch is
  refused; same-key-same-payload replays, same-key-different-payload is refused; worker credentials are
  still refused on the admin namespace; a supervisor credential cannot read another run's artifacts.
- Must agree on: the `Action` scope fields with B1 and B5; the operation word with B7's CLI.
- Does **not** touch: `internal/store`.

**Lane B7 — CLI and amendment integration.** *Integration lane; lands last.*
- Files: `cmd/t3-steward/campaign_supervision.go` (new), `cmd/t3-steward/campaign.go:260-288`,
  `cmd/t3-steward/campaign_check.go`, `internal/backlogadmin/amendment.go:23-113`,
  `internal/store/sqlite/graph_revision.go` (amendment-side invalidation),
  `internal/backlogadmin/graph_clone.go`, `graph_rerun.go`, their tests.
- Tests: amendment recomputes closures atomically or refuses; a newly added descendant cannot escape
  an existing branch hold; outstanding offers are revoked in the same transaction; CLI JSON is
  versioned and distinguishes stale evidence, unauthorized scope, unmet prerequisites and temporary
  unavailability.
- Must agree on: everything above. This lane is the seam where the others meet, and it should be owned
  by the lead.

### Stage C lanes

**Lane C1 — adversarial and race integration** (`cmd/t3-steward/*_test.go`, disposable coordinator and
worker fixtures). **Lane C2 — compatibility and migration** (backup/restore, old-peer refusal, schema
18 forward/rollback). **Lane C3 — live qualification and publication** (lead-owned; requires the fleet
to be re-enrolled first, see section 7).

### Dependency order

```
A1, A2        independent, start immediately
B1            blocks B2, B3, B4, B5
B2, B3        parallel after B1
B4, B5        parallel after B1; B4 also reads B3's loader, B5 also reads B3's tables
B6            parallel after B1 (needs only the Action shape)
B7            after B2..B6
C1, C2        after B7
C3            lead only, after C1 and C2, and after fleet re-enrollment
```

### Files no lane may touch

`internal/policy/**` (concurrent work on `fix/quota-extension-rearm`),
`internal/backlog/quota_admission.go`, `internal/backlog/quota_bridge.go`,
`internal/backlog/quota_recovery*.go`, `internal/backlog/throttle*.go`. Quota admission is called, not
changed.

---

## 10. Risks and open questions for the lead

**1. Credential isolation on worker hosts cannot be enforced today.**
A full-access campaign task on a host holding `~/.config/upkeeper/secrets/f03-admin/<client>` can read
it (`internal/backlogadmin/remote_credential_store.go:45-60`;
`internal/backlog/runner.go:648-650`). Containment, which would prevent it
(`internal/providercontainment/spec.go:20-25`), is explicitly not the production path
(`cmd/t3-steward/provider_containment.go:17-18`).
*Recommended default*: enforce run-and-epoch scope server-side in the authorizer, ship it, and record
the co-tenancy exposure as a documented deployment limitation. Do not claim isolation in user-facing
text. Revisit when contained execution becomes the default.

**2. Supervision as a `PlanningConstraint`, or as an inline blocker in `planTask`?**
Quota admission is a constraint (`internal/backlog/quota_admission.go:107-115`); dependency state is
inline (`planner.go:509-547`).
*Recommended default*: **inline**, beside `progressBlockers` at `planner.go:375`. A constraint is
evaluated per candidate worker and route (`planner.go:410-412`); a gate is a property of the task, not
of the candidate, so a constraint would evaluate the same answer N times and report it N times in
`decision.Candidates`. Inline keeps the explanation readable.

**3. Does a clone or rerun inherit supervision and its gate acceptances?**
*Recommended default*: a clone or rerun inherits the **supervision configuration** (route, limits,
gate definitions) and inherits **no acceptances, no holds and no incidents**. `adr-s0-amendment.md:98-103`
is explicit that a clone copies no success; an acceptance is a statement about evidence that the clone
does not have.

**4. Where does an activation run, and does it hold an executor slot?**
An activation dispatched as ordinary assigned work consumes an executor slot on a worker for its
duration. The plan requires idle overseers to hold nothing, which this satisfies, but an *active*
review does occupy a slot.
*Recommended default*: accept it. It is one slot, bounded by the activation deadline, and the
alternative — a coordinator-side model client — reintroduces exactly the "launch raw model processes"
the plan forbids. Record the slot cost in the activation receipt so it is visible.

**5. Is there a registered "stronger" model at all?**
No. The fleet advertises `claude-haiku-4-5`, `claude-sonnet-5`, `gpt-5.6-sol` and
`opencode/muse-spark-1.3-contributor-free`. Neither "Fable" nor "Astra" resolves to anything.
*Recommended default*: make the design require an *independently configured* route, not a stronger
one, and let the operator choose. Qualify with the overseer on `codex-main`/`gpt-5.6-sol` and workers
on `claude-main`, so pool separation is exercised even when model strength is not. Ask the user which
registered model they meant before the qualification gate.
*Decision (2026-09-16)*: the overseer route is `claudeAgent` / `claude-fable-5-1` / `claude-main`.
Workers in the supervised example and the qualification campaign run on `codex` / `gpt-5.6-sol` /
`codex-main`. Both models are advertised by the live fleet since the catalog moved to the `"*"`
policy on steward rc.56.

**6. Should a held task be a stored progress state or a derived label?**
*Recommended default*: derived. A stored state has to be understood by `Terminal()`,
`RunExecutionsQuiescent`, the planner, the sink projection and every status renderer; a label is a
rendering concern. This is also the cheaper thing to reverse.

**7. Conservative invalidation on amendment is expensive. How conservative?**
The plan says conservative invalidation is the default with carry-forward only after explicit atomic
revalidation.
*Recommended default*: v1 invalidates unconditionally on any amendment touching a gate's observed or
protected sets, and carries forward only when the amendment touches neither. Implement the
audit-receipt carry-forward path but keep it narrow. A false invalidation costs one review; a false
carry-forward is an unreviewed dispatch.

**8. The fleet cannot currently run a qualification campaign.**
Two workers are draining and unenrolled, one is stale, coordinator health is `degraded`.
*Recommended default*: treat fleet re-enrollment as a prerequisite of verification gates 3-7, record
it now, and register a steward wait rather than polling when the time comes.

**9. Bulk incident close and the "exact set" rule adds CLI complexity.**
*Recommended default*: v1 ships single-incident `resolve` only, and defers bulk close. The exact-set
rule is then vacuously satisfied and one fewer race exists.

**10. Who owns the escalation channel?**
The only existing mechanism is `campaign submit --notify-thread`, a node wait on the run sink
(`cmd/t3-steward/campaign_notify.go:26-91`, `internal/backlogadmin/node_wait.go:43-111`). It fires at
settlement, not at a review-ready transition.
*Recommended default*: reuse the node-wait outbox machinery for escalation delivery to the configured
notify thread; add no new messaging integration and no recipient discovery, per the plan. Deduplicate
on incident ID so a re-escalation of the same incident does not re-notify.

**11. `internal/compat` is not the negotiation surface the plan assumed.**
It pins the T3 *server* version (`internal/compat/compat.go:13-16`), not CLI/coordinator/worker
compatibility. The real mechanisms are `KnownFields(true)` for manifests
(`internal/backlog/manifest.go:112`) and worker inventory capability strings
(`internal/workerproto/protocol.go:139`, consumed at `internal/backlog/worker_exchange.go:113`).
*Recommended default*: use both, and correct the plan's wording so a later reader does not go looking
for a negotiation in `internal/compat` that is not there.

**12. Estimate.**
The plan's 3-5 focused days for the basic supervised path is optimistic given lanes B3, B5 and B6 each
carry their own race matrix. With the lane split above and parallel agents, 5-8 days to a supervised
path that passes its own tests is realistic; the 1-2 week total including qualification holds only if
the fleet is re-enrolled first.

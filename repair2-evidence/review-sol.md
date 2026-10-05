# Independent Repair1 runtime terminal-fences review

Verdict: **changes-requested**.
Reviewer: GPT-6.1-Sol (gpt-6.1-sol), Codex harness, medium effort, independent local runtime compatibility / receipt replay / wait-wake / cleanup custody review. No delegation or nested review.
Producer: run-251ebacaebb86d66d39c2c9060ef5ea4.
Repair plan: jocasta:eeca88f548e4fcf821d56804974f5ab6@1.
Original plan: jocasta:f4414f38f3c0461bf857c71fe24873d9@1.

## Exact source and inspected inputs

Initial HEAD / sole prerequisite: 6b0a6798736d3c9277e7688ef3016c690bf7a9ee.
Reviewed detached HEAD: 6ff61d9669a41c90d781ccc60b1cfa2faa442219.
Tree: 6d15e085988a0fe9e62563b866758e70770d1c30.
Sole parent: 754eb9c0e9b220885252a33958208ee16b7d6902.
Bundle SHA256: 3ece71b5a663aaa587d4aacd33fb278aab70bcb90c99e4dcc6ce0efc1824a006.
Independently matched checksum, ran git bundle verify/list-heads, fetched locally, detached and asserted exact HEAD/tree/parent. Sole export is HEAD. No material mismatch.

Read all seven text inputs fully: original plan (22 lines), repair plan (16), BOTH actual prior reviews (Sol 231 / Astra 71), handoff (31), runtime contract (43), verification.log (93366 bytes / 934 lines). Used bounded complete log windows and reread output-truncated overlaps; summaries were not substituted for execution evidence. Prior Astra acceptance did not determine this verdict; its uncollected overlay bytes remain unavailable. Sol raw overlays under repair1-evidence were independently compared with both complete code blocks in its actual review: exact matches.

Huyang workspace_open and semantic reads/windows covered the eight-file original runtime unit, repair delta, four production seams, retained/adapted tests and necessary accepted cancellation / quiescence / wait-release dependencies. Mounted input paths were read through their external canonical paths after the workspace symlink-boundary refusal; a relative-root opening error was corrected to the absolute root. Trust stayed unchanged/untrusted. Explicitly requested foreground shell tests ran locally.

## Original F1–F3 disposition: all resolved

- **F1 resolved.** backlog/worker_reconcile.go:155–179 retains the original progress-park normalization, adds control-only park preservation, and preserves the stale worker waiting observation branch after a real coordinator wake. Durable park/wake controls and retained original F1 tests pass.
- **F2 resolved for finished attempts.** The early park returns at worker_reconcile.go:261, :301 and :336 now apply only when neither terminal Progress nor CompletedAt marks the attempt finished. Finished evidence remains exact, control stops, Released clears the assignment reference (:280), and accepted Collect settles completed custody. All 35 durable producer cleanup leaves and retained F2 tests pass. Its target-first positional assertion is valid because only sibling attempt ID and assignment AttemptID were changed to zz-live; assignment 0-live remains first in the transition batch and TaskID was reverted. This is a fixture identity correction, not unchanged raw bytes.
- **F3 resolved by safe omission.** worker_reconcile.go:221–233 omits absent finished repair when assignment/session epochs differ, without changing durable validator :186–188. Command planning coordinator.go:307–310 withholds new-session effects for old custody. Producer 20 durable leaves check exact raw custody/attempt bytes through repeat/reopen, positive live sibling commit/transport, later matching observation recovery and actual Stop ack before release. My additional absent / wrong assignment-epoch observation / missing-attempt mixed leaves pass. Omission creates neither an epoch transfer nor Stop evidence. Original F3 expectations and four producer restart leaves were explicitly adapted; an unchanged original F3 AFTER pass is not claimed. The recovery fixture acknowledges its real pending sibling Prepare before isolating Stop, preserving pending replay.

## F4 — P2, blocking: unfinished park release projections still cannot commit durably

This is an existing planner/store incompatibility, not a new Repair1 regression and not a claim that F1–F3 remain unfixed. It is inside the explicitly required park cleanup / abandonment compatibility boundary.

**Evidence:** internal/backlog/worker_reconcile.go:256–270 sets AssignmentReleased but returns the unfinished parked attempt with its AssignmentID retained. The unchanged validator in internal/store/sqlite/worker_reconcile.go:194–195 rejects every Released projection with a nonempty attempt reference. coordinator.go:201–209 commits the entire mixed transition batch before transport. The resulting refusal prevents both durable release evidence for the park's abandonment owner and unrelated sibling progress/delivery. The retained pure compatibility test at task_wait_release_test.go:46–115 never commits its projections; :118–145 explains why settled evidence must reach abandonment.

**Concrete replication:** restore the complete backlog overlay below as internal/backlog/repair1_acceptance_overlay_test.go using guarded Huyang. Run:
```sh
GOMAXPROCS=2 GOFLAGS=-p=2 go test ./internal/backlog -run '^TestIndependentRuntimeTerminalFences(ParkCleanupBoundary|MissingOldEvidenceMixed)$' -count=1 -v
```
TestIndependentRuntimeTerminalFencesParkCleanupBoundary extends the repository's parkedReleaseRecords compatibility fixture through real SQLite and FleetCoordinator, with a valid observed-running sibling sorted first. Four leaves fail: explicit released observation, actual accepted Stop receipt, actual rejected Dispatch receipt persisted while live then parked, and changed-epoch absence. Error: `stale worker state transition: released assignment remains attached to attempt`. Each failure verifies full coordinator-record rollback/no transport, repeats the exact evidence and verifies rollback after reopen. Completed observation and actual accepted Collect companions pass while retaining the park/reference; all three missing-old-evidence mixed leaves pass.

The fixture has unfinished ProgressWaitingExternal / ControlWaitingExternal and a bound claimed assignment, exactly the existing park compatibility shape; it does not assert that normal child cancellation creates this shape. The rejected start is truly committed while live and acknowledged through the durable API after park, rather than injecting a synthetic command record. No invalid current snapshot or sibling token collision explains the refusal.

**Required resolution:** make real unfinished-park release evidence and the existing abandonment/directory ownership contract compose with the durable released-binding invariant, so the batch and valid siblings can progress. Do not globally relax the released-ref validator, clear the park's binding blindly, synthesize wake, or treat omitted old custody as cleanup acknowledgement. A bounded repair needs durable tests of these release/receipt boundaries, using the existing park/abandonment owner. No production fix was made during review.

The same unconditional validator exists at accepted input 50230f and the unfinished park return predates Repair1, confirmed by Git source/diff inspection. I did not execute an independent F4 BEFORE run and do not claim one. Pre-existence limits attribution; it does not make this required durable compatibility path work.

## Other runtime conclusions

Finished preservation uses terminal Progress OR CompletedAt at worker_reconcile.go:118/:152/:221 and coordinator.go:359. Durable worker-state current-revision guard at sqlite/worker_reconcile.go:91–100 preserves progress/completion/failure/checkpoint/final artifact and requires stopped control. Assignment, snapshot, epoch, token and CAS checks precede writes; transaction rollback retains earlier sibling/audit atomicity. Native audit is required for exact state replay at :70–76.

New Prepare/Dispatch commit and pending load recheck durable attempt/binding within transactions (sqlite/worker_commands.go:113, :226, :606–619). Eligibility filtering precedes stale receipt supersession and preserves valid otherwise eligible siblings. Exact command replay at :81–89 and ack replay at :252–266 remain historical facts. Pending loading is distinct from replay. Missing attempt rows on start eligibility still error; the passing missing-attempt planner test does not promise such pending delivery succeeds.

Matching completed observations prioritize Collect (coordinator.go:351–357); other finished custody plans Stop. Cancellation alone does not release claimed/unknown capacity or quiescence. Accepted internal cancellation revokes authority and writes cancelled/stopped while retaining claimed/unknown custody (sqlite/review_child_cancel.go:158–199); domain/sink.go:140–155 fences quiescence. All four real cancellation integrations exercise actual cancellation and FleetCoordinator with simulated transport before eventual durable Stop/Collect ack. Existing live prepare/dispatch/resume/verification, nonterminal unknown/restart and wait/wake suites pass. The F4 failure is the missed actual durable unfinished release seam.

## Independent checks and producer evidence

Full independent results retained in review-checks.log.

1. Exact requested focused command, with TestIndependentRuntimeTerminalFences included, on pinned source BEFORE overlays: exit 0. SQLite 8.669s; backlog 2.022s.
2. New backlog overlay command above: exit 1, backlog 0.215s. Four release-boundary failures; two completed/Collect park controls and three old-evidence/sibling leaves pass.
3. New SQLite overlay:
```sh
GOMAXPROCS=2 GOFLAGS=-p=2 go test ./internal/store/sqlite -run '^TestIndependentRuntimeTerminalFencesTerminalOnlyCASReplay$' -count=1 -v
```
Exit 0, SQLite 0.210s. Four terminal types without CompletedAt, seven forged current-revision mutations each, stopped completed-custody commit, no-op native replay, and refusal without writes after deleting the native audit in the disposable database. No initial independent fixture errors, assertion weakening or production edits.

Complete producer repair log retains actual unchanged Sol BEFORE: F1 running park plus Released / Collect / old-epoch mixed batch failures; SQLite nine leaves pass. AFTER initial failures are transparently retained: positional F2 assertion selected sibling after a successful commit; attempted TaskID correction was ineffective and reverted; legitimate unacknowledged sibling Prepare accompanied recovered Stop. Final ID correction/real acknowledgement resolves fixtures without validator or replay weakening. BEFORE raw bytes are exact; adapted F3 bytes are not falsely presented as unchanged.

Producer retained core coverage remains 10 old-Dispatch, 32 evidence cleanup, 10 command/sibling/reopen/receipt/ack, 40 forged-transition rollback/replay, four actual cancellation integrations, plus repair 35 cleanup / 3 park-wake / 20 old-epoch leaves. Its ONE final gate passed build/vet/full short/staticcheck/format/backlog race 193.605s and SQLite race 840.736s. All five Repair1 Go fingerprints independently match the pre/postgate values in the inspected log. Full bundle-object/strict fsck verification and no-postgate-edit chronology are producer local evidence, not CI or independent chronology attestation. No full gate or race repeated here.

## Limits and hygiene

Pending-load-to-wire cancellation can commit between coordinator.go:229–235's local transaction and :246's transport call, or after an effect has already been sent. The snapshot fence cannot recall sent effects or prove distributed atomic cancellation. Existing worker/session/token authority and actual Stop/Collect/recovery evidence remain necessary; no stronger protocol authorization is invented.

Automatic review-cancellation invocation, review-specific retry and public review/completion APIs remain later units. No broad operator retry ban, full M16, CI, deployment, two-provider diversity, scalability or live effect claim. Token cost unavailable.

Both isolated Go overlays removed by exact revision-guarded Huyang deletes. Final git diff --check, tracked git diff --exit-code and git diff --cached --exit-code pass. Exact HEAD/tree/sole parent unchanged; production/index untouched. Only supplied inputs and local review artifacts remain untracked. No delegation/escalation, push/PR/tag/release/UpKeeper, fleet/config/trust/services/enrollment/schedule or live worker/model changes.

## Complete portable isolated overlays

Backlog bytes also retained as reviewer-overlay.go.txt; SQLite bytes as reviewer-sqlite-overlay.go.txt. Restore to the named paths through guarded Huyang, execute the commands above, then remove with guarded Huyang. Repository test helpers are part of the pinned source.

### internal/backlog/repair1_acceptance_overlay_test.go
```go
package backlog

import (
 "context"
 "path/filepath"
 "reflect"
 "testing"
 "time"
 "github.com/iryzhkov/t3-steward/internal/domain"
 "github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// Extend the repository's real park/release compatibility fixture through SQLite
// and FleetCoordinator, with a valid earlier sibling in the same transition batch.
func TestIndependentRuntimeTerminalFencesParkCleanupBoundary(t *testing.T) {
 for _, evidence := range []string{"released", "stop-accepted", "dispatch-rejected", "old-epoch-absent", "completed", "collect-accepted"} {
  t.Run(evidence, func(t *testing.T) {
   ctx := context.Background()
   path := filepath.Join(t.TempDir(), "state.db")
   s, err := sqlite.OpenMigrated(path); if err != nil { t.Fatal(err) }; defer s.Close()
   records, snapshot := parkedReleaseRecords()
   now := parkedReleaseTime
   snapshot.Inventory = domain.WorkerInventory{ID:snapshot.WorkerID, Health:domain.WorkerHealthReady, AcceptBacklog:true}
   original := records.Attempts[0]
   a := records.Assignments[0]
   if evidence == "old-epoch-absent" { snapshot.WorkerEpoch = "new-session" }
   if evidence == "released" || evidence == "completed" {
    state := domain.AssignmentReleased; if evidence == "completed" { state = domain.AssignmentCompleted }
    snapshot.Assignments = []domain.WorkerAssignmentObservation{{AssignmentID:a.ID, AssignmentEpoch:a.Epoch, State:state, ObservedAt:now}}
   }
   sibling := original
   sibling.ID = "sibling"; sibling.TaskID = "sibling-task"; sibling.AssignmentID = "0-live"
   sibling.Progress = domain.ProgressActive; sibling.Control = domain.ControlPreparing
   la := a; la.ID = sibling.AssignmentID; la.AttemptID = sibling.ID
   la.WorkerEpoch = snapshot.WorkerEpoch; la.LeaseToken = "sibling-lease"; la.DispatchToken = "sibling-dispatch"
   snapshot.Assignments = append(snapshot.Assignments, domain.WorkerAssignmentObservation{AssignmentID:la.ID, AssignmentEpoch:la.Epoch, State:domain.AssignmentClaimed, Control:domain.ControlRunning, ObservedAt:now})
   records.Attempts = append(records.Attempts, sibling); records.Assignments = append(records.Assignments, la)
   if err = s.SaveCoordinatorRecords(ctx,records); err != nil { t.Fatal(err) }
   if err = s.SaveWorkerSnapshot(ctx,snapshot); err != nil { t.Fatal(err) }
   if evidence == "stop-accepted" || evidence == "dispatch-rejected" || evidence == "collect-accepted" {
    kind := domain.WorkerCommandStop; accepted := true
    if evidence == "collect-accepted" { kind = domain.WorkerCommandCollect }
    if evidence == "dispatch-rejected" {
     kind = domain.WorkerCommandDispatch; accepted = false
     // Persist the start while live; park afterward, then retain its actual rejected receipt.
     live := original; live.Progress = domain.ProgressActive; live.Control = domain.ControlPreparing
     if err = s.SaveCoordinatorRecords(ctx,sqlite.CoordinatorRecords{Attempts:[]domain.Attempt{live}}); err != nil { t.Fatal(err) }
    }
    cmd := domain.WorkerCommand{ID:"actual-boundary", Kind:kind, WorkerID:snapshot.WorkerID, WorkerEpoch:snapshot.WorkerEpoch, CoordinatorEpoch:1, AssignmentID:a.ID, AssignmentEpoch:a.Epoch, ExpectedWorkerSequence:snapshot.Sequence, CreatedAt:now}
    got, e := s.CommitWorkerCommands(ctx,[]domain.WorkerCommand{cmd}); if e != nil || len(got)!=1 { t.Fatalf("receipt %+v %v",got,e) }
    if evidence == "dispatch-rejected" { if err = s.SaveCoordinatorRecords(ctx,sqlite.CoordinatorRecords{Attempts:[]domain.Attempt{original}}); err != nil { t.Fatal(err) } }
    if _,err = s.AcknowledgeWorkerCommand(ctx,domain.WorkerAcknowledgement{CommandID:cmd.ID,WorkerID:cmd.WorkerID,WorkerEpoch:cmd.WorkerEpoch,CoordinatorEpoch:1,AssignmentID:a.ID,AssignmentEpoch:a.Epoch,WorkerSequence:snapshot.Sequence,Accepted:accepted,AcknowledgedAt:now}); err != nil { t.Fatal(err) }
   }
   before := repair1Records(t,s)
   commands,err := s.LoadWorkerCommandRecords(ctx); if err != nil { t.Fatal(err) }
   trs,err := PlanWorkerStateTransitions(before,snapshot,commands,now)
   if err != nil || len(trs)!=2 || trs[0].Assignment.ID!=la.ID { t.Fatalf("mixed plan %+v %v",trs,err) }
   transport := &terminalFenceTransport{}
   c := FleetCoordinator{Store:s, Now:func()time.Time{return now}}
   _,err = c.ReconcileWorkerCommands(ctx,snapshot,transport)
   if err != nil {
    if !reflect.DeepEqual(before,repair1Records(t,s)) || len(transport.commands)!=0 { t.Fatal("failed batch partially committed/delivered") }
    // Repeat and reopen prove this is persistent incompatibility, not one stale snapshot.
    _,again := c.ReconcileWorkerCommands(ctx,snapshot,transport); if again == nil { t.Fatal("refusal unexpectedly transient") }
    if e:=s.Close();e!=nil {t.Fatal(e)}
    s,e := sqlite.OpenMigrated(path); if e!=nil {t.Fatal(e)}; defer s.Close()
    if !reflect.DeepEqual(before,repair1Records(t,s)) {t.Fatal("rollback not durable")}
    t.Fatalf("real park cleanup rejects mixed batch, preventing abandonment evidence and sibling delivery: %v",err)
   }
   got := repair1Records(t,s); at := repair1Attempt(t,got,original.ID); as := repair1Assignment(t,got,a.ID)
   if at.Progress!=original.Progress || at.Control!=original.Control || at.AssignmentID!=a.ID || at.CompletedAt!=nil { t.Fatalf("park evidence lost %+v",at) }
   want := domain.AssignmentReleased
   if evidence=="completed" {want=domain.AssignmentClaimed}
   if evidence=="collect-accepted" {want=domain.AssignmentCompleted}
   if as.State!=want || repair1Attempt(t,got,sibling.ID).Control!=domain.ControlRunning || len(transport.commands)==0 {t.Fatalf("boundary/sibling missing %+v",got)}
  })
 }
}

func TestIndependentRuntimeTerminalFencesMissingOldEvidenceMixed(t *testing.T) {
 for _, evidence := range []string{"absent", "wrong-assignment-epoch", "missing-attempt"} {
  t.Run(evidence,func(t *testing.T){
   ctx:=context.Background(); path:=filepath.Join(t.TempDir(),"state.db")
   s,err:=sqlite.OpenMigrated(path); if err!=nil {t.Fatal(err)}; defer s.Close()
   now:=coordinatorTestTime.Add(time.Minute)
   snapshot,a,attempt:=repair1Fixture(now)
   attempt.Progress=domain.ProgressReady; attempt.Control=domain.ControlWaitingExternal
   completed:=now.Add(-time.Second); attempt.CompletedAt=&completed
   snapshot.WorkerEpoch="replacement"
   if evidence=="wrong-assignment-epoch" {snapshot.Assignments=[]domain.WorkerAssignmentObservation{{AssignmentID:a.ID,AssignmentEpoch:a.Epoch+1,State:domain.AssignmentCompleted,ObservedAt:now}}}
   live:=attempt;live.ID="sibling";live.TaskID="sibling-task";live.AssignmentID="0-live";live.Progress=domain.ProgressActive;live.Control=domain.ControlResuming;live.CompletedAt=nil
   la:=a;la.ID=live.AssignmentID;la.AttemptID=live.ID;la.WorkerEpoch=snapshot.WorkerEpoch;la.LeaseToken="sibling-lease";la.DispatchToken="sibling-dispatch"
   snapshot.Assignments=append(snapshot.Assignments,domain.WorkerAssignmentObservation{AssignmentID:la.ID,AssignmentEpoch:la.Epoch,State:domain.AssignmentClaimed,Control:domain.ControlRunning,ObservedAt:now})
   records:=sqlite.CoordinatorRecords{Attempts:[]domain.Attempt{live},Assignments:[]domain.Assignment{a,la}}
   if evidence!="missing-attempt" {records.Attempts=append(records.Attempts,attempt)}
   if err=s.SaveCoordinatorRecords(ctx,records);err!=nil {t.Fatal(err)}
   if err=s.SaveWorkerSnapshot(ctx,snapshot);err!=nil {t.Fatal(err)}
   transport:=&terminalFenceTransport{};c:=FleetCoordinator{Store:s,Now:func()time.Time{return now}}
   for tick:=0;tick<3;tick++ {
    if _,err=c.ReconcileWorkerCommands(ctx,snapshot,transport);err!=nil {t.Fatal(err)}
    got:=repair1Records(t,s)
    if !reflect.DeepEqual(repair1Assignment(t,got,a.ID),a) {t.Fatal("invented epoch/release")}
    if evidence!="missing-attempt" && !reflect.DeepEqual(repair1Attempt(t,got,attempt.ID),attempt) {t.Fatal("old evidence altered")}
    if repair1Attempt(t,got,live.ID).Control!=domain.ControlRunning {t.Fatal("valid sibling blocked")}
   }
   if len(transport.commands)!=3 {t.Fatalf("expected pending sibling replay, got %+v",transport.commands)}
   for _,cmd:=range transport.commands {if cmd.AssignmentID!=la.ID || cmd.Kind!=domain.WorkerCommandPrepare {t.Fatalf("unsafe cleanup/start %+v",cmd)}}
  })
 }
}

```

### internal/store/sqlite/repair1_acceptance_overlay_test.go
```go
package sqlite

import (
 "context"
 "errors"
 "testing"
 "time"
 "github.com/iryzhkov/t3-steward/internal/domain"
)

func TestIndependentRuntimeTerminalFencesTerminalOnlyCASReplay(t *testing.T) {
 for _,progress:=range []domain.ProgressState{domain.ProgressCancelled,domain.ProgressFailed,domain.ProgressSucceeded,domain.ProgressSkipped} {
  t.Run(string(progress),func(t *testing.T){
   ctx:=context.Background();s:=openFleetTestStore(t);claimFleetAssignment(t,s)
   r,err:=s.LoadCoordinatorRecords(ctx);if err!=nil {t.Fatal(err)}
   a:=r.Attempts[0];as:=r.Assignments[0]
   a.Progress=progress;a.Control=domain.ControlStopped;a.CompletedAt=nil;a.Failure="failure";a.CheckpointArtifactID="cp";a.FinalSummaryArtifactID="final";a.Revision++
   saveFleetAttempt(t,s,a)
   now:=fleetTestTime.Add(3*time.Second)
   next:=a;next.Revision++;next.UpdatedAt=now
   na:=as;na.State=domain.AssignmentCompleted;na.LeaseExpiresAt=time.Time{};na.UpdatedAt=now
   valid:=domain.WorkerStateTransition{CoordinatorEpoch:1,WorkerID:as.WorkerID,WorkerEpoch:as.WorkerEpoch,WorkerSequence:1,TransitionedAt:now,ExpectedAssignment:as,ExpectedAttemptRevision:a.Revision,Assignment:na,Attempt:next,Reason:"collect-accepted"}
   for _,mutation:=range []string{"ready","running","waiting","failure","checkpoint","summary","invent-completion"} {
    forged:=valid
    switch mutation {
    case "ready":forged.Attempt.Progress=domain.ProgressReady
    case "running":forged.Attempt.Control=domain.ControlRunning
    case "waiting":forged.Attempt.Control=domain.ControlWaitingExternal
    case "failure":forged.Attempt.Failure=""
    case "checkpoint":forged.Attempt.CheckpointArtifactID=""
    case "summary":forged.Attempt.FinalSummaryArtifactID=""
    case "invent-completion":forged.Attempt.CompletedAt=&now
    }
    before:=cancellationSnapshot(t,s)
    if _,err=s.CommitWorkerStateTransitions(ctx,[]domain.WorkerStateTransition{forged});!errors.Is(err,ErrStaleWorkerStateTransition)||before!=cancellationSnapshot(t,s) {t.Fatalf("%s: %v",mutation,err)}
   }
   if _,err=s.CommitWorkerStateTransitions(ctx,[]domain.WorkerStateTransition{valid});err!=nil {t.Fatal(err)}
   before:=cancellationSnapshot(t,s)
   if _,err=s.CommitWorkerStateTransitions(ctx,[]domain.WorkerStateTransition{valid});err!=nil||before!=cancellationSnapshot(t,s) {t.Fatalf("native replay %v",err)}
   if _,err=s.db.ExecContext(ctx,"DELETE FROM coordinator_audit_events WHERE id = ?",workerStateAuditID(valid));err!=nil {t.Fatal(err)}
   before=cancellationSnapshot(t,s)
   if _,err=s.CommitWorkerStateTransitions(ctx,[]domain.WorkerStateTransition{valid});err==nil||before!=cancellationSnapshot(t,s) {t.Fatalf("unaudited replay %v",err)}
  })
 }
}

```

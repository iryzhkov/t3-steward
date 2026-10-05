# Repair1 independent acceptance review

Verdict: **changes-requested**.

Reviewer: GPT-6-Astra (`gpt-6-astra`), Codex harness, medium effort. Independent local review, run `run-296901bec9067f901dcdf29d98842def`, task `task-bd63c57e165e85e7520a3ed06aaae582`. No delegation, nested review, provider change or effort escalation. Producer: `run-251ebacaebb86d66d39c2c9060ef5ea4`.
Exact repair plan: `jocasta:eeca88f548e4fcf821d56804974f5ab6@1`; original unit plan: `jocasta:f4414f38f3c0461bf857c71fe24873d9@1`, supplied pinned texts.

## Source and evidence identity

- Initial HEAD / sole bundle prerequisite: `6b0a6798736d3c9277e7688ef3016c690bf7a9ee`.
- Bundle SHA256 independently matched: `3ece71b5a663aaa587d4aacd33fb278aab70bcb90c99e4dcc6ce0efc1824a006`.
- `git bundle verify` passed; sole export `6ff61d9669a41c90d781ccc60b1cfa2faa442219 HEAD`.
- Locally fetched and detached source: `6ff61d9669a41c90d781ccc60b1cfa2faa442219`.
- Tree: `6d15e085988a0fe9e62563b866758e70770d1c30`.
- Sole parent: `754eb9c0e9b220885252a33958208ee16b7d6902`. All pins matched.
- Inspected original eight-file runtime unit against accepted `50230f9820f40de124ff7c2762096d5a1a1c43eb`, repair delta, all four production seams, their tests and necessary accepted cancellation/quiescence/wait dependencies. Only repair production delta is `internal/backlog/worker_reconcile.go`.
- Read all seven supplied text inputs fully: original plan, repair plan, both actual prior reviews (including full Sol portable code), handoff, runtime contract and verification.log (93366 bytes / 934 lines). Complete bounded log windows were inspected, including initial failures and correction runs, separately from summaries. Output-truncated source/input overlap was reread.
- Huyang workspace `ws_aed2e7382a457f8ff85f901253cb7838`; semantic declaration reads, source windows and guarded overlay creation/removal. External input symlink refusal resolved by opening canonical input paths in a documents workspace. Trust unchanged; explicitly requested shell tests ran directly.
- Retained raw Sol files match the actual review's two code blocks including their final newline. Astra's uncollected overlay is unavailable; no recovered-bytes claim.
- All five repair Go fingerprints independently match the producer's pre/postgate fingerprints.

## Blocking finding

### R1 — P2: live parked release still aborts the durable mixed batch

**Blocking for the explicit parked cleanup/abandonment compatibility acceptance boundary; not a claim that repair1 introduced this defect.** All three original Sol findings are resolved as described below. This additional gap is exposed by the specifically requested independent durable park-cleanup tests.

At `internal/backlog/worker_reconcile.go:261–270`, an **unfinished** parked attempt takes the early return with its assignment reference intact after `:257` has projected the assignment Released. This happens both for a matching released observation (`:186–187`) and an actual accepted Stop with no observation (`:211–212`). The unchanged durable validator at `internal/store/sqlite/worker_reconcile.go:194–195` rejects every Released projection with a nonempty attempt assignment reference.

Replication on the exact reviewed source: restore the complete backlog overlay below; run:

```sh
GOMAXPROCS=2 GOFLAGS=-p=2 go test ./internal/backlog -run '^TestIndependentRepair1ParkCleanupMixedBatch' -count=1 -v
```

The full executed overlay command was `-run '^TestIndependentRepair1'`. Four failing leaves are `control-only/{released,accepted-stop}` and `progress-and-control/{released,accepted-stop}`. Both shapes have nil CompletedAt and nonterminal progress. The latter is the ordinary waiting-external/waiting-external park, not only a mixed recovery shape. The fixture saves real attempts/assignments and a fresh matching snapshot, acknowledges Stop through the real durable API for that case, and invokes the real FleetCoordinator with simulated transport. Assignment `0-live` sorts first and has valid observed-running evidence.

Exact failure:

```text
park cleanup blocks valid sibling and transport: stale worker state transition: released assignment remains attached to attempt
```

The test verifies the complete loaded coordinator records remain unchanged and no commands reach transport on failure. Earlier sibling work rolls back; the coordinator returns from `internal/backlog/coordinator.go:207–210` before planning or pending delivery. Repeating the same evidence cannot repair the contradiction. Positive `observed-completed` and `accepted-collect` companions pass for both park shapes, preserving park and binding while allowing sibling progress.

**Why this matters here:** the repair plan requires nonfinished parked attempts to retain their existing abandonment/directory-writer binding semantics, and the task explicitly requests durable park-cleanup boundary testing. Existing `internal/backlog/task_wait_release_test.go:46` checks only the pure projection, never its durable commit. Its accepted-Stop and observed-release projections therefore pass while the actual coordinator fails. The abandonment owner at `internal/store/sqlite/task_wait.go:1084–1100` needs settled execution evidence; the rejected release never makes that evidence durable. This is the same planner/store compatibility class as the original F2/F3 findings, with live parked release rather than finished waiting-control cleanup.

**Attribution and requested correction:** Git comparison confirms this live-park early return and Released-reference validator already coexist in accepted `50230f` and prior candidate `754eb9c`; repair1 does not newly regress them. Nevertheless the requested broad parked-cleanup acceptance cannot be certified. Reconcile this boundary through the existing wait/abandonment owner or a safe planner strategy that preserves the binding and does not block valid siblings. Do not simply clear a live park's binding, synthesize completion/Stop acknowledgement, or weaken the Released-reference/terminal/epoch validator to make this test pass. The current requirement to retain a live parked Released binding and the unconditional durable prohibition need an explicit, narrow resolution. No production fix was made in this review.

## Independent resolution of original F1–F3

1. **F1 resolved.** `backlog/worker_reconcile.go:164–169` preserves original progress and control for control-only parks under claimed/running observation. Progress-only normalization at `:155–163` remains; stale worker waiting after a coordinator wake at `:170–176` preserves the wake. The no-observation old Dispatch guard at `:235` still respects either parking field. Actual durable producer tests exercise absent/running/stale-wait-after-wake. Retained Sol F1 assertions pass.
2. **F2 resolved for the reported finished shapes.** Finished means Terminal OR CompletedAt. All three cleanup helpers now restrict their park-first branch to unfinished attempts (`:261`, `:301`, `:336`). Finished release stops control and clears binding; completed observation retains custody for Collect; accepted Collect settles Completed while preserving progress/completion/failure/artifacts. Retained F2 assertions and 35 durable producer cleanup leaves pass. The sibling ID change `live -> zz-live` plus assignment AttemptID is a correct repair of a positional assertion that selected attempts sorted by ID; assignment `0-live` still sorts first in the transition batch. TaskID was restored. This is not unchanged fixture bytes, and is not assertion weakening.
3. **F3 resolved using safe omission.** `:221–226` omits no-observation finished repair when old custody's worker epoch differs, retaining the exact attempt/control/custody. Commands at `coordinator.go:307–310` cannot create replacement-session authority for that old assignment. Current-epoch repair remains at `:228–233`; real matching observation still owns recovery. Producer's 20 durable leaves verify raw records/reopen/sibling delivery/current matching recovery/actual Stop ack. Independent 12-case old-epoch matrix additionally passes terminal-only parked and CompletedAt-plus-progress/control parked shapes with no evidence, wrong-assignment-epoch completed observation, pending Stop, rejected Stop, and accepted historical Stop/Collect. Omission cases retain exact record bytes across repeat/reopen; accepted receipts settle without rewriting the old worker epoch; all cases deliver the valid sibling. An old receipt is not new epoch authorization.

F3's original pure restart and durable restart assertions, and four original producer restart leaves, were intentionally adapted to omission. Their old expectations are not claimed to pass unchanged. The producer's recovery fixture acknowledges a real pending sibling Prepare before expecting only Stop; pending replay semantics were preserved. Original BEFORE log actually reproduces running park plus released/collect/restart errors; both subsequent fixture-failure runs and their corrections are retained.

## Other runtime fences assessed

- **Durable current revision and rollback:** `sqlite/worker_reconcile.go:70–100` retains native-audit exact replay and exact assignment/revision checks, then refuses finished evidence loss or nonstopped control before fresh writes. Assignment, attempt and audit writes share the rollback transaction. Existing 40-case tests check full application-table snapshots/reopen, sibling rollback and valid replay. Independent overlay adds four terminal-only (nil CompletedAt) cases with seven mutations each, including waiting control, synthesized completion, evidence loss and old-epoch custody. All refuse without any table/audit change, then valid batch/replay succeeds. Deleting the native transition audit in the disposable DB makes exact replay fail without mutation.
- **New effects versus historical receipts:** `sqlite/worker_commands.go:81–90` retains exact persisted command replay. New start commit at `:113–119` and pending load at `:223–232` require durable eligibility from `:606–619`: current binding, nonterminal progress, nil completion, no progress/control park and no stopped control. Eligibility precedes supersession/insertion. Independent mixed batches place a live resuming sibling first, an ineligible start second and Collect third; only sibling/Collect commit and deliver across reopen, pending load is nonmutating, and old start receipt replay remains unchanged. CompletedAt+ready/resuming and terminal-only+waiting cases pass for Prepare and Dispatch.
- **Cleanup ownership:** matching completed observation remains first at `coordinator.go:351–358`, finished/stopped unfinished custody plans Stop at `:359–363`. Actual receipts remain facts; cancellation alone does not release custody or establish quiescence. Accepted cancellation code revokes child tokens and marks cancelled/stopped while keeping claimed/unknown custody; `domain/sink.go:140–155` keeps unresolved custody nonquiescent. All four actual internal-child cancellation/FleetCoordinator integration cases pass using simulated transport and eventual durable acknowledgements.
- Existing live dispatch/resume/verification, wait/wake and nonterminal unknown/restart controls passed in the requested command. No general operator-retry ban or new worker authorization rule was introduced. R1 is a durable gap masked by existing pure park-release controls, not a failure of the tested live dispatch paths.

## Checks and limits

Exact requested command plus retained independent tests, on exact source before new overlays:

```sh
GOMAXPROCS=2 GOFLAGS=-p=2 go test ./internal/store/sqlite ./internal/backlog -run 'TestRuntimeTerminalFences|TestReviewChildCancellationWorkerCustodyIntegration|TestIndependentRuntimeTerminalFences|TestPlanWorkerStateTransitions|TestWorkerCommands|TestCommitWorkerStateTransition|TestReleasedWorkerState|TestFleetCoordinator|TestPlanWorkerCommands|Test.*Wait|Test.*Wake' -count=1
```

Complete result, exit 0:
```text
ok  github.com/iryzhkov/t3-steward/internal/store/sqlite 8.859s
ok  github.com/iryzhkov/t3-steward/internal/backlog 2.045s
```

Independent backlog overlay: exit 1, 0.485s. Twelve epoch/receipt leaves and four parked completed/Collect leaves pass; four parked Released/Stop leaves fail with R1. Independent SQLite overlay: exit 0, 0.271s, four terminal-only rollback/audit leaves and four mixed delivery leaves pass. Both overlays compiled and ran on their first execution; no fixture corrections or weakened assertions. Full execution output retained in reviewer-focused.log, reviewer-adversarial-backlog.log and reviewer-adversarial-sqlite.log. Full portable source is embedded below so declared review.md retains replication even if auxiliary artifacts are not collected.

Producer's ONE check-review passed build/vet/full short/staticcheck/format and backlog/SQLite races (193.605s / 840.736s). The complete supplied repair log and gated fingerprints were inspected; full-bundle object verification is producer evidence (10158 reachable objects, zero missing). No full gate or race was repeated. No claim of CI or independently witnessed producer chronology; original unit BEFORE/core matrix execution is described by the full actual prior reviews, while this supplied verification.log contains the repair executions. Those original core tests were independently rerun here through the focused command.

The pending-load-to-wire race remains: after `coordinator.go:229–234` completes its local read transaction, cancellation can commit before transport at `:246`, or after an effect was sent. A local snapshot fence cannot recall sent effects or prove distributed atomic cancellation. Existing worker/session/token protocol and real cleanup evidence remain the boundary.

Automatic review-cancellation invocation, review-specific retry admission and public review/completion APIs remain following units. No full M16, CI, deployment, two-provider diversity, scalability or live-worker claim. Token cost unavailable. No push/PR/tags/releases/UpKeeper/fleet/config/trust/services/enrollment/schedules changes.

## Full portable independent overlays

Restore these exact complete files on the pinned source using guarded Huyang, run the commands below, and remove with guarded Huyang. They depend only on existing test helpers in that pinned source. No production patch.

```sh
GOMAXPROCS=2 GOFLAGS=-p=2 go test ./internal/backlog -run '^TestIndependentRepair1' -count=1 -v
GOMAXPROCS=2 GOFLAGS=-p=2 go test ./internal/store/sqlite -run '^TestIndependentRepair1' -count=1 -v
```

### internal/backlog/independent_repair1_acceptance_test.go

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

// Actual durable receipts under the old epoch, followed by a replacement worker
// and a valid sibling. No receipt is synthesized in the planner input.
func TestIndependentRepair1EpochReceiptBoundary(t *testing.T) {
 for _, shape := range []string{"terminal-park", "completed-park"} {
  for _, evidence := range []string{"none", "wrong-observation", "pending-stop", "rejected-stop", "accepted-stop", "accepted-collect"} {
   t.Run(shape+"/"+evidence,func(t *testing.T) {
    ctx:=context.Background()
    path:=filepath.Join(t.TempDir(),"state.db")
    s,err:=sqlite.OpenMigrated(path); if err!=nil {t.Fatal(err)}
    defer func(){s.Close()}()
    now:=coordinatorTestTime.Add(time.Minute)
    snapshot,a,attempt:=repair1Fixture(now)
    if shape=="terminal-park" {attempt.Progress=domain.ProgressSkipped} else {at:=now.Add(-time.Second);attempt.CompletedAt=&at;attempt.Progress=domain.ProgressWaitingExternal}
    a.State=domain.AssignmentUnknown
    if err=s.SaveCoordinatorRecords(ctx,sqlite.CoordinatorRecords{Attempts:[]domain.Attempt{attempt},Assignments:[]domain.Assignment{a}});err!=nil {t.Fatal(err)}
    if err=s.SaveWorkerSnapshot(ctx,snapshot);err!=nil {t.Fatal(err)}
    var old domain.WorkerCommand
    if evidence!="none" && evidence!="wrong-observation" {
     kind:=domain.WorkerCommandStop;if evidence=="accepted-collect" {kind=domain.WorkerCommandCollect}
     old=domain.WorkerCommand{ID:"old-cleanup",Kind:kind,WorkerID:snapshot.WorkerID,WorkerEpoch:snapshot.WorkerEpoch,CoordinatorEpoch:1,AssignmentID:a.ID,AssignmentEpoch:1,ExpectedWorkerSequence:2,CreatedAt:now}
     if _,err=s.CommitWorkerCommands(ctx,[]domain.WorkerCommand{old});err!=nil {t.Fatal(err)}
     if evidence!="pending-stop" {
      _,err=s.AcknowledgeWorkerCommand(ctx,domain.WorkerAcknowledgement{CommandID:old.ID,WorkerID:old.WorkerID,WorkerEpoch:old.WorkerEpoch,CoordinatorEpoch:1,AssignmentID:a.ID,AssignmentEpoch:1,WorkerSequence:2,Accepted:evidence!="rejected-stop",AcknowledgedAt:now})
      if err!=nil {t.Fatal(err)}
     }
    }
    snapshot.WorkerEpoch="replacement";snapshot.Sequence++;snapshot.ObservedAt=now.Add(time.Second)
    live:=attempt;live.ID="zz-live";live.TaskID="live-task";live.AssignmentID="0-live";live.Progress=domain.ProgressActive;live.Control=domain.ControlPreparing;live.CompletedAt=nil
    la:=a;la.ID=live.AssignmentID;la.AttemptID=live.ID;la.WorkerEpoch=snapshot.WorkerEpoch;la.State=domain.AssignmentClaimed;la.DispatchToken="live-token";la.LeaseToken="live-lease"
    snapshot.Assignments=[]domain.WorkerAssignmentObservation{{AssignmentID:la.ID,AssignmentEpoch:1,State:domain.AssignmentClaimed,Control:domain.ControlRunning,ObservedAt:snapshot.ObservedAt}}
    if evidence=="wrong-observation" {snapshot.Assignments=append(snapshot.Assignments,domain.WorkerAssignmentObservation{AssignmentID:a.ID,AssignmentEpoch:2,State:domain.AssignmentCompleted,ObservedAt:snapshot.ObservedAt})}
    if err=s.SaveCoordinatorRecords(ctx,sqlite.CoordinatorRecords{Attempts:[]domain.Attempt{live},Assignments:[]domain.Assignment{la}});err!=nil {t.Fatal(err)}
    if err=s.SaveWorkerSnapshot(ctx,snapshot);err!=nil {t.Fatal(err)}
    before:=repair1RawCustody(t,path)
    c:=FleetCoordinator{Store:s,Now:func()time.Time{return now.Add(time.Second)}}
    transport:=&terminalFenceTransport{}
    settled:=evidence=="accepted-stop" || evidence=="accepted-collect"
    for tick:=0;tick<3;tick++ {
     if _,err=c.ReconcileWorkerCommands(ctx,snapshot,transport);err!=nil {t.Fatal(err)}
     got:=repair1Records(t,s);at:=repair1Attempt(t,got,attempt.ID);as:=repair1Assignment(t,got,a.ID)
     if !settled {
      if repair1RawCustody(t,path)!=before {t.Fatal("absence/pending/rejected/wrong-epoch evidence changed old custody")}
     } else {
      want:=domain.AssignmentReleased;if evidence=="accepted-collect" {want=domain.AssignmentCompleted}
      if as.State!=want || as.WorkerEpoch!=a.WorkerEpoch || at.Control!=domain.ControlStopped || at.Progress!=attempt.Progress || !reflect.DeepEqual(at.CompletedAt,attempt.CompletedAt) || at.Failure!=attempt.Failure || at.CheckpointArtifactID!=attempt.CheckpointArtifactID || at.FinalSummaryArtifactID!=attempt.FinalSummaryArtifactID {t.Fatalf("historical cleanup lost evidence or fabricated transfer: %+v %+v",as,at)}
      if want==domain.AssignmentReleased && at.AssignmentID!="" {t.Fatal("release kept binding")}
     }
     if repair1Attempt(t,got,live.ID).Control!=domain.ControlRunning {t.Fatal("sibling write lost")}
     if tick==0 {if err=s.Close();err!=nil {t.Fatal(err)};s,err=sqlite.OpenMigrated(path);if err!=nil {t.Fatal(err)};c.Store=s}
    }
    if len(transport.commands)!=3 {t.Fatalf("sibling delivery missing: %+v",transport.commands)}
    for _,cmd:=range transport.commands {if cmd.AssignmentID!=la.ID || cmd.Kind!=domain.WorkerCommandPrepare {t.Fatalf("old ownership fabricated: %+v",cmd)}}
    if old.ID!="" {got,err:=s.CommitWorkerCommands(ctx,[]domain.WorkerCommand{old});if err!=nil || !reflect.DeepEqual(got,[]domain.WorkerCommand{old}) {t.Fatalf("receipt replay lost: %+v %v",got,err)}}
   })
  }
 }
}

// A live parked attempt retains its park and writer binding while cleanup
// evidence is reconciled. A sibling sorts before it to expose whole-batch failure.
func TestIndependentRepair1ParkCleanupMixedBatch(t *testing.T) {
 for _,shape:=range []string{"control-only","progress-and-control"} {
  for _,evidence:=range []string{"released","accepted-stop","observed-completed","accepted-collect"} {
   t.Run(shape+"/"+evidence,func(t *testing.T) {
    ctx:=context.Background();s,err:=sqlite.OpenMigrated(filepath.Join(t.TempDir(),"state.db"));if err!=nil {t.Fatal(err)};defer s.Close()
    now:=coordinatorTestTime.Add(time.Minute);snapshot,a,attempt:=repair1Fixture(now)
    if shape=="progress-and-control" {attempt.Progress=domain.ProgressWaitingExternal}
    live:=attempt;live.ID="zz-live";live.TaskID="live-task";live.AssignmentID="0-live";live.Progress=domain.ProgressActive;live.Control=domain.ControlPreparing
    la:=a;la.ID=live.AssignmentID;la.AttemptID=live.ID;la.DispatchToken="live-token";la.LeaseToken="live-lease"
    snapshot.Assignments=[]domain.WorkerAssignmentObservation{{AssignmentID:la.ID,AssignmentEpoch:1,State:domain.AssignmentClaimed,Control:domain.ControlRunning,ObservedAt:now}}
    if evidence=="released" || evidence=="observed-completed" {
     state:=domain.AssignmentReleased;if evidence=="observed-completed" {state=domain.AssignmentCompleted}
     snapshot.Assignments=append(snapshot.Assignments,domain.WorkerAssignmentObservation{AssignmentID:a.ID,AssignmentEpoch:1,State:state,ObservedAt:now})
    }
    if err=s.SaveCoordinatorRecords(ctx,sqlite.CoordinatorRecords{Attempts:[]domain.Attempt{attempt,live},Assignments:[]domain.Assignment{a,la}});err!=nil {t.Fatal(err)}
    if err=s.SaveWorkerSnapshot(ctx,snapshot);err!=nil {t.Fatal(err)}
    if evidence=="accepted-stop" || evidence=="accepted-collect" {
     kind:=domain.WorkerCommandStop;if evidence=="accepted-collect" {kind=domain.WorkerCommandCollect}
     repair1Ack(t,s,domain.WorkerCommand{ID:"park-cleanup",Kind:kind,WorkerID:snapshot.WorkerID,WorkerEpoch:snapshot.WorkerEpoch,CoordinatorEpoch:1,AssignmentID:a.ID,AssignmentEpoch:1,ExpectedWorkerSequence:2,CreatedAt:now},now)
    }
    before:=repair1Records(t,s);transport:=&terminalFenceTransport{}
    c:=FleetCoordinator{Store:s,Now:func()time.Time{return now}}
    if _,err=c.ReconcileWorkerCommands(ctx,snapshot,transport);err!=nil {
     after:=repair1Records(t,s)
     if !reflect.DeepEqual(before,after) || len(transport.commands)!=0 {t.Fatal("invalid batch partially committed")}
     t.Fatalf("park cleanup blocks valid sibling and transport: %v",err)
    }
    got:=repair1Records(t,s);at:=repair1Attempt(t,got,attempt.ID);as:=repair1Assignment(t,got,a.ID)
    if at.Progress!=attempt.Progress || at.Control!=attempt.Control || at.AssignmentID!=a.ID || at.CompletedAt!=nil {t.Fatalf("park/writer binding changed: %+v",at)}
    want:=domain.AssignmentCompleted;if evidence=="released" || evidence=="accepted-stop" {want=domain.AssignmentReleased};if evidence=="observed-completed" {want=domain.AssignmentClaimed}
    if as.State!=want {t.Fatalf("cleanup evidence missing: %+v",as)}
    if repair1Attempt(t,got,live.ID).Control!=domain.ControlRunning || len(transport.commands)==0 || transport.commands[0].AssignmentID!=la.ID {t.Fatal("valid sibling did not progress")}
   })
  }
 }
}
```

### internal/store/sqlite/independent_repair1_acceptance_test.go

```go
package sqlite

import (
 "context"
 "errors"
 "reflect"
 "testing"
 "time"

 "github.com/iryzhkov/t3-steward/internal/domain"
)

func TestIndependentRepair1TerminalOnlyRollbackAndAudit(t *testing.T) {
 for _,progress:=range []domain.ProgressState{domain.ProgressCancelled,domain.ProgressFailed,domain.ProgressSucceeded,domain.ProgressSkipped} {
  t.Run(string(progress),func(t *testing.T) {
   ctx:=context.Background();s:=openFleetTestStore(t);claimFleetAssignment(t,s)
   records,err:=s.LoadCoordinatorRecords(ctx);if err!=nil {t.Fatal(err)}
   a:=records.Attempts[0];as:=records.Assignments[0];a.Progress=progress;a.CompletedAt=nil;a.Control=domain.ControlWaitingExternal;a.Revision++;a.Failure="keep";a.CheckpointArtifactID="cp";a.FinalSummaryArtifactID="final";saveFleetAttempt(t,s,a)
   now:=fleetTestTime.Add(3*time.Second)
   next:=a;next.Control=domain.ControlStopped;next.Revision++;next.UpdatedAt=now
   na:=as;na.State=domain.AssignmentCompleted;na.LeaseExpiresAt=time.Time{};na.UpdatedAt=now
   valid:=domain.WorkerStateTransition{CoordinatorEpoch:1,WorkerID:as.WorkerID,WorkerEpoch:as.WorkerEpoch,WorkerSequence:1,TransitionedAt:now,ExpectedAssignment:as,ExpectedAttemptRevision:a.Revision,Assignment:na,Attempt:next,Reason:"collect-accepted"}
   live:=a;live.ID="live";live.TaskID="live-task";live.AssignmentID="live-assignment";live.Progress=domain.ProgressActive;live.Control=domain.ControlResuming
   la:=as;la.ID=live.AssignmentID;la.AttemptID=live.ID;la.DispatchToken="live-token";la.LeaseToken="live-lease"
   if err=s.SaveCoordinatorRecords(ctx,CoordinatorRecords{Attempts:[]domain.Attempt{live},Assignments:[]domain.Assignment{la}});err!=nil {t.Fatal(err)}
   first:=valid;first.ExpectedAssignment=la;first.Assignment=la;first.Attempt=live;first.Attempt.Control=domain.ControlRunning;first.Attempt.Revision++;first.Attempt.UpdatedAt=now
   for _,mutation:=range []string{"ready","park","completion","failure","checkpoint","summary","old-epoch"} {
    forged:=valid
    switch mutation {
    case "ready":forged.Attempt.Progress=domain.ProgressReady
    case "park":forged.Attempt.Control=domain.ControlWaitingExternal
    case "completion":forged.Attempt.CompletedAt=&now
    case "failure":forged.Attempt.Failure=""
    case "checkpoint":forged.Attempt.CheckpointArtifactID=""
    case "summary":forged.Attempt.FinalSummaryArtifactID=""
    case "old-epoch":forged.Assignment.State=domain.AssignmentUnknown;forged.Assignment.WorkerEpoch="old"
    }
    before:=cancellationSnapshot(t,s)
    if _,err=s.CommitWorkerStateTransitions(ctx,[]domain.WorkerStateTransition{first,forged});!errors.Is(err,ErrStaleWorkerStateTransition) {t.Fatalf("%s accepted forged projection: %v",mutation,err)}
    if cancellationSnapshot(t,s)!=before {t.Fatalf("%s committed partial writes/audit",mutation)}
   }
   s=repair2Reopen(t,s)
   if _,err=s.CommitWorkerStateTransitions(ctx,[]domain.WorkerStateTransition{first,valid});err!=nil {t.Fatal(err)}
   before:=cancellationSnapshot(t,s)
   if _,err=s.CommitWorkerStateTransitions(ctx,[]domain.WorkerStateTransition{first,valid});err!=nil || before!=cancellationSnapshot(t,s) {t.Fatalf("valid native replay: %v",err)}
   if _,err=s.db.ExecContext(ctx,"DELETE FROM coordinator_audit_events WHERE id = ?",workerStateAuditID(valid));err!=nil {t.Fatal(err)}
   before=cancellationSnapshot(t,s)
   if _,err=s.CommitWorkerStateTransitions(ctx,[]domain.WorkerStateTransition{valid});err==nil || before!=cancellationSnapshot(t,s) {t.Fatalf("unaudited replay accepted/mutated: %v",err)}
  })
 }
}

func TestIndependentRepair1MixedPendingDelivery(t *testing.T) {
 for _,kind:=range []domain.WorkerCommandKind{domain.WorkerCommandPrepare,domain.WorkerCommandDispatch} {
  for _,shape:=range []string{"completed-ready","terminal-park"} {
   t.Run(string(kind)+"/"+shape,func(t *testing.T) {
    ctx:=context.Background();s:=openFleetTestStore(t);claimFleetAssignment(t,s)
    old:=fleetWorkerCommand(kind,"old-start",1,fleetTestTime.Add(2*time.Second))
    if _,err:=s.CommitWorkerCommands(ctx,[]domain.WorkerCommand{old});err!=nil {t.Fatal(err)}
    records,err:=s.LoadCoordinatorRecords(ctx);if err!=nil {t.Fatal(err)}
    a:=records.Attempts[0];a.Revision++
    if shape=="completed-ready" {a.Progress=domain.ProgressReady;a.Control=domain.ControlResuming;at:=fleetTestTime.Add(time.Second);a.CompletedAt=&at} else {a.Progress=domain.ProgressSkipped;a.Control=domain.ControlWaitingExternal}
    saveFleetAttempt(t,s,a)
    live:=a;live.ID="live";live.TaskID="live-task";live.AssignmentID="live-assignment";live.Progress=domain.ProgressActive;live.Control=domain.ControlResuming;live.CompletedAt=nil
    la:=records.Assignments[0];la.ID=live.AssignmentID;la.AttemptID=live.ID;la.DispatchToken="live-token";la.LeaseToken="live-lease"
    if err=s.SaveCoordinatorRecords(ctx,CoordinatorRecords{Attempts:[]domain.Attempt{live},Assignments:[]domain.Assignment{la}});err!=nil {t.Fatal(err)}
    sibling:=old;sibling.ID="0-live";sibling.AssignmentID=la.ID
    fresh:=old;fresh.ID="fresh-start"
    cleanup:=old;cleanup.ID="cleanup";cleanup.Kind=domain.WorkerCommandCollect
    got,err:=s.CommitWorkerCommands(ctx,[]domain.WorkerCommand{sibling,fresh,cleanup})
    if err!=nil || !reflect.DeepEqual(got,[]domain.WorkerCommand{sibling,cleanup}) {t.Fatalf("mixed commit: %+v %v",got,err)}
    for reopen:=0;reopen<2;reopen++ {
     before:=cancellationSnapshot(t,s)
     pending,err:=s.LoadPendingWorkerCommands(ctx,old.WorkerID,old.WorkerEpoch,1,1,fleetTestTime.Add(3*time.Second))
     if err!=nil || !reflect.DeepEqual(pending,[]domain.WorkerCommand{sibling,cleanup}) || before!=cancellationSnapshot(t,s) {t.Fatalf("pending mutated or delivered ineligible start: %+v %v",pending,err)}
     got,err=s.CommitWorkerCommands(ctx,[]domain.WorkerCommand{old});if err!=nil || !reflect.DeepEqual(got,[]domain.WorkerCommand{old}) || before!=cancellationSnapshot(t,s) {t.Fatalf("receipt changed: %+v %v",got,err)}
     if reopen==0 {s=repair2Reopen(t,s)}
    }
   })
  }
 }
}
```

## Final hygiene

Both isolated Go overlays were removed through exact document-revision guarded Huyang deletes. Full portable bytes are embedded above and copied to reviewer-overlay/*.go.txt. Final git diff --check, git diff --exit-code and git diff --cached --exit-code passed (exit 0); exact HEAD/tree/sole parent remain pinned. Only supplied .t3 inputs and local review artifacts are untracked. No production or index changes remain.

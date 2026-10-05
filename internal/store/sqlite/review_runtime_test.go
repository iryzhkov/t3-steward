package sqlite

import (
	"context"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Fixture mutations are disposable, separate-connection parent interleavings.
// All exercised admission operations are the actual production SQLite methods.
func reviewRuntimePath(t *testing.T, s *Store) string {
	t.Helper()
	var seq int
	var name, path string
	if err := s.db.QueryRow("PRAGMA database_list").Scan(&seq, &name, &path); err != nil {
		t.Fatal(err)
	}
	return path
}
func reviewRuntimeReopen(t *testing.T, s *Store) *Store {
	t.Helper()
	path := reviewRuntimePath(t, s)
	clock := s.now
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	reopened.now = clock
	t.Cleanup(func() { reopened.Close() })
	return reopened
}
func reviewRuntimeInvalidate(t *testing.T, s *Store, f review.FrozenAuthority, receipt ReviewMaterialization, kind string) {
	t.Helper()
	if strings.HasPrefix(kind, "deadline") {
		edge := *receipt.Graph.Tasks[0].Deadline
		if kind == "deadline-before" {
			edge = edge.Add(-time.Nanosecond)
		}
		if kind == "deadline-after" {
			edge = edge.Add(time.Nanosecond)
		}
		s.now = func() time.Time { return edge }
		return
	}
	other, err := OpenMigrated(reviewRuntimePath(t, s))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	records := cancellationRecords(t, other)
	switch kind {
	case "healthy":
	case "advance":
		a := loadAttempt(t, other, f.Parent.AttemptID)
		a.Revision++
		cancellationSave(t, other, CoordinatorRecords{Attempts: []domain.Attempt{a}})
	case "ended":
		cancellationEndParent(t, other, f, domain.ProgressFailed)
	case "superseded":
		a := loadAttempt(t, other, f.Parent.AttemptID)
		a.ID = "parent-hidden-retry"
		a.Number++
		a.Revision = 1
		a.AssignmentID = ""
		a.ThreadID = ""
		a.Progress = domain.ProgressReady
		a.Control = domain.ControlUnassigned
		cancellationSave(t, other, CoordinatorRecords{Attempts: []domain.Attempt{a}})
	case "run-ended":
		for _, run := range records.WorkflowRuns {
			if run.ID == f.Parent.RunID {
				run.Progress = domain.ProgressSucceeded
				run.Revision++
				cancellationSave(t, other, CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}})
			}
		}
	case "released", "unknown", "epoch-lost":
		for _, assignment := range records.Assignments {
			if assignment.ID == f.Parent.AssignmentID {
				if kind == "released" {
					assignment.State = domain.AssignmentReleased
				}
				if kind == "unknown" {
					assignment.State = domain.AssignmentUnknown
				}
				if kind == "epoch-lost" {
					assignment.Epoch++
				}
				cancellationSave(t, other, CoordinatorRecords{Assignments: []domain.Assignment{assignment}})
			}
		}
	case "round-ended":
		if _, err = other.db.Exec("UPDATE coordinator_review_rounds SET record=json_set(record,'$.combinedVerdict','reject') WHERE id=?", receipt.Checkpoint.RoundID); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unknown fixture disposition %s", kind)
	}
}

func reviewRuntimeWorker(t *testing.T, s *Store) domain.WorkerSnapshot {
	t.Helper()
	now := time.Now().UTC()
	snapshot := domain.WorkerSnapshot{WorkerID: "child-worker", WorkerEpoch: "child-epoch", CoordinatorEpoch: 1, Sequence: 1, Connected: true, ObservedAt: now, ValidUntil: now.Add(3 * time.Hour), Inventory: domain.WorkerInventory{ID: "child-worker", AcceptBacklog: true, Health: domain.WorkerHealthReady, ObservedAt: now}}
	saveFleetSnapshot(t, s, snapshot)
	return snapshot
}
func reviewRuntimeCommand(assignment domain.Assignment, kind domain.WorkerCommandKind) domain.WorkerCommand {
	return domain.WorkerCommand{ID: "fresh-" + string(kind), Kind: kind, WorkerID: assignment.WorkerID, WorkerEpoch: assignment.WorkerEpoch, CoordinatorEpoch: 1, AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch, ExpectedWorkerSequence: 1, CreatedAt: time.Now().UTC()}
}

func TestReviewRuntimeCurrentAdmissionInterleavings(t *testing.T) {
	ctx := context.Background()
	kinds := []string{"healthy", "advance", "deadline-before", "ended", "superseded", "run-ended", "released", "unknown", "epoch-lost", "deadline-exact", "deadline-after", "round-ended"}
	for _, boundary := range []string{"offer", "claim", "prepare", "dispatch", "pending", "retry"} {
		for _, kind := range kinds {
			t.Run(boundary+"/"+kind, func(t *testing.T) {
				s, f, _, receipt := parentWaitFixture(t)
				reviewRuntimeWorker(t, s)
				// Pure plan / earlier successful tick is deliberately before the parent change.
				if err := s.ReconcileMaterializedReviewChildren(ctx); err != nil {
					t.Fatal(err)
				}
				initial := receipt.Graph.Attempts[0]
				plan := fleetPlanCommit(1, "child-offer", initial.ID, "child-epoch", 1)
				plan.CommittedAt = time.Now().UTC()
				plan.Items[0].Assignment.WorkerID = "child-worker"
				plan.Items[0].Assignment.Project = f.Parent.Repository
				plan.Items[0].Assignment.Route = receipt.Graph.Tasks[0].Routes[0]
				var assignment domain.Assignment
				var command domain.WorkerCommand
				if boundary == "claim" {
					assignment = cancellationAssign(t, s, initial, domain.AssignmentOffered, f.Requirements.Members[0].Route)
				}
				if boundary == "prepare" || boundary == "dispatch" || boundary == "pending" {
					assignment = cancellationAssign(t, s, initial, domain.AssignmentClaimed, f.Requirements.Members[0].Route)
					command = reviewRuntimeCommand(assignment, domain.WorkerCommandPrepare)
					if boundary == "dispatch" {
						command = reviewRuntimeCommand(assignment, domain.WorkerCommandDispatch)
					}
					if boundary == "pending" {
						got, err := s.CommitWorkerCommands(ctx, []domain.WorkerCommand{command})
						if err != nil || len(got) != 1 {
							t.Fatalf("healthy pending setup %v %+v", err, got)
						}
					}
				}
				var application domain.AdminCommandApplication
				if boundary == "retry" {
					now := time.Now().UTC()
					failed := initial
					failed.Progress = domain.ProgressFailed
					failed.Control = domain.ControlStopped
					failed.Revision++
					failed.CompletedAt = &now
					cancellationSave(t, s, CoordinatorRecords{Attempts: []domain.Attempt{failed}})
					admin := domain.AdminCommand{ID: "review-retry", Kind: domain.AdminCommandRetry, TargetType: domain.AdminTargetAttempt, TargetID: failed.ID, ExpectedRevision: failed.Revision, Reason: "retry failed reviewer", RequestedBy: "operator", State: domain.AdminCommandPending, CreatedAt: now}
					if _, err := s.SubmitAdminCommand(ctx, admin); err != nil {
						t.Fatal(err)
					}
					next := failed
					next.Revision++
					next.UpdatedAt = now
					retry := initial
					retry.ID = "hidden-coherent-retry"
					retry.Number = 2
					retry.Revision = 1
					retry.UpdatedAt = now
					application = domain.AdminCommandApplication{CommandID: admin.ID, ExpectedCommandState: domain.AdminCommandPending, ExpectedTargetRevision: failed.Revision, State: domain.AdminCommandApplied, Attempt: &next, NewAttempt: &retry, AppliedAt: now}
				}
				reviewRuntimeInvalidate(t, s, f, receipt, kind)
				healthy := kind == "healthy" || kind == "advance" || kind == "deadline-before"
				before := cancellationSnapshot(t, s)
				parentBefore := parentWaitSnapshot(t, s, f)
				switch boundary {
				case "offer":
					got, err := s.CommitAssignmentPlan(ctx, plan)
					if err != nil {
						t.Fatal(err)
					}
					if (len(got) == 1) != healthy {
						t.Fatalf("offer healthy=%v got=%+v", healthy, got)
					}
				case "claim":
					now := time.Now().UTC()
					got, err := s.ClaimAssignment(ctx, domain.AssignmentClaimRequest{CoordinatorEpoch: 1, WorkerID: assignment.WorkerID, WorkerEpoch: assignment.WorkerEpoch, AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch, LeaseToken: assignment.LeaseToken, ClaimedAt: now, LeaseExpiresAt: now.Add(time.Hour)})
					if (err == nil) != healthy {
						t.Fatalf("claim healthy=%v got=%+v err=%v", healthy, got, err)
					}
				case "prepare", "dispatch":
					got, err := s.CommitWorkerCommands(ctx, []domain.WorkerCommand{command})
					if err != nil {
						t.Fatal(err)
					}
					if (len(got) == 1) != healthy {
						t.Fatalf("start healthy=%v got=%+v", healthy, got)
					}
				case "pending":
					got, err := s.LoadPendingWorkerCommands(ctx, "child-worker", "child-epoch", 1, 1, time.Now().UTC())
					if err != nil {
						t.Fatal(err)
					}
					if (len(got) == 1) != healthy {
						t.Fatalf("pending healthy=%v got=%+v", healthy, got)
					}
					// Exact persisted receipt remains historical even after current refusal.
					if replay, err := s.CommitWorkerCommands(ctx, []domain.WorkerCommand{command}); err != nil || len(replay) != 1 {
						t.Fatalf("historical receipt %v %+v", err, replay)
					}
				case "retry":
					recordsBefore := cancellationRecords(t, s)
					got, err := s.ApplyAdminCommand(ctx, application)
					if err != nil {
						t.Fatal(err)
					}
					want := domain.AdminCommandRejected
					if healthy {
						want = domain.AdminCommandApplied
					}
					if got.Command.State != want || !healthy && !strings.Contains(got.Command.Failure, "review retry refused") {
						t.Fatalf("retry healthy=%v got=%+v", healthy, got)
					}
					if !healthy {
						recordsAfter := cancellationRecords(t, s)
						recordsAfter.AdminCommands = recordsBefore.AdminCommands // only the command and its audit outcome may change.
						if !reflect.DeepEqual(recordsBefore, recordsAfter) {
							t.Fatal("rejected retry changed target/newattempt/run")
						}
						rejection := application
						rejection.State = domain.AdminCommandRejected
						rejection.Failure = got.Command.Failure
						rejection.Attempt = nil
						rejection.NewAttempt = nil
						stable := cancellationSnapshot(t, s)
						if _, err = s.ApplyAdminCommand(ctx, rejection); err != nil || stable != cancellationSnapshot(t, s) {
							t.Fatalf("rejected receipt replay %v", err)
						}
					}
				}
				if parentBefore != parentWaitSnapshot(t, s, f) {
					t.Fatal("admission changed parent history")
				}
				if !healthy && boundary != "retry" && before != cancellationSnapshot(t, s) {
					t.Fatal("refusal changed full logical tables/native audit")
				}
				after := cancellationSnapshot(t, s)
				s = reviewRuntimeReopen(t, s)
				if after != cancellationSnapshot(t, s) {
					t.Fatal("admission/rejection changed across reopen")
				}
			})
		}
	}
}

// Automatic enumeration uses ORIGINAL issued revision, all hidden retries and
// complete immutable graph validation. Repeated/reopened passes are stable.
func TestReviewRuntimeAutomaticOriginalAuthorityAndHistory(t *testing.T) {
	for _, kind := range []string{"healthy", "advance", "ended", "superseded", "run-ended", "released", "unknown", "epoch-lost", "deadline-before", "deadline-exact", "deadline-after"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			s, f, _, receipt := parentWaitFixture(t)
			initial := receipt.Graph.Attempts[0]
			done := initial
			done.Progress = domain.ProgressSucceeded
			completed := time.Now().UTC()
			done.CompletedAt = &completed
			done.Control = domain.ControlStopped
			done.Revision++
			retry := initial
			retry.ID = "nonprefix-hidden-child-retry"
			retry.Number = 2
			retry.Revision = 1
			cancellationSave(t, s, CoordinatorRecords{Attempts: []domain.Attempt{done, retry}})
			offer := cancellationAssign(t, s, retry, domain.AssignmentOffered, f.Requirements.Members[0].Route)
			reviewRuntimeInvalidate(t, s, f, receipt, kind)
			parent := parentWaitSnapshot(t, s, f)
			if err := s.ReconcileMaterializedReviewChildren(ctx); err != nil {
				t.Fatal(err)
			}
			healthy := kind == "healthy" || kind == "advance" || kind == "deadline-before"
			got := loadAttempt(t, s, retry.ID)
			if (got.Progress != domain.ProgressCancelled) != healthy {
				t.Fatalf("hidden retry %+v", got)
			}
			if !reflect.DeepEqual(loadAttempt(t, s, done.ID), done) {
				t.Fatal("automatic pass rewrote partial completed history")
			}
			for _, custody := range cancellationRecords(t, s).Assignments {
				if custody.ID == offer.ID && !healthy && (custody.DispatchToken != "" || custody.State != domain.AssignmentReleased) {
					t.Fatalf("unclaimed offer not revoked/released: %+v", custody)
				}
			}
			if parent != parentWaitSnapshot(t, s, f) {
				t.Fatal("automatic pass changed original parent")
			}
			after := cancellationSnapshot(t, s)
			s = reviewRuntimeReopen(t, s)
			for tick := 0; tick < 2; tick++ {
				if err := s.ReconcileMaterializedReviewChildren(ctx); err != nil {
					t.Fatal(err)
				}
				if after != cancellationSnapshot(t, s) {
					t.Fatal("repeated/reopened pass changed history")
				}
			}
		})
	}
}

func TestReviewRuntimeMalformedOwnershipFailsClosed(t *testing.T) {
	for _, corruption := range []string{"index", "runtime", "duplicate", "hidden-retry", "assignment-index", "assignment-runtime", "duplicate-json"} {
		t.Run(corruption, func(t *testing.T) {
			ctx := context.Background()
			s, f, _, receipt := parentWaitFixture(t)
			initial := receipt.Graph.Attempts[0]
			assignment := cancellationAssign(t, s, initial, domain.AssignmentClaimed, f.Requirements.Members[0].Route)
			reviewRuntimeWorker(t, s)
			switch corruption {
			case "index":
				_, _ = s.db.Exec("UPDATE coordinator_attempts SET workflow_run_id='foreign' WHERE id=?", initial.ID)
			case "runtime":
				_, _ = s.db.Exec("UPDATE coordinator_attempts SET record=json_set(record,'$.workflowRunId','foreign') WHERE id=?", initial.ID)
			case "duplicate":
				copy := initial
				copy.ID = "duplicate-owned"
				copy.TaskID = "foreign-index-task"
				cancellationSave(t, s, CoordinatorRecords{Attempts: []domain.Attempt{copy}})
				if _, err := s.db.Exec("UPDATE coordinator_attempts SET record=json_set(record,'$.taskId',?) WHERE id=?", initial.TaskID, copy.ID); err != nil {
					t.Fatal(err)
				}
			case "hidden-retry":
				copy := initial
				copy.ID = "hidden-malformed"
				copy.Number = 2
				copy.SupervisionActivationID = "forged"
				cancellationSave(t, s, CoordinatorRecords{Attempts: []domain.Attempt{copy}})
			case "assignment-index":
				_, _ = s.db.Exec("UPDATE coordinator_assignments SET attempt_id='foreign' WHERE id=?", assignment.ID)
			case "assignment-runtime":
				_, _ = s.db.Exec("UPDATE coordinator_assignments SET record=json_set(record,'$.attemptId','foreign') WHERE id=?", assignment.ID)
			case "duplicate-json":
				_, _ = s.db.Exec("UPDATE coordinator_attempts SET record=substr(record,1,length(record)-1)||',\"taskId\":\"foreign\"}' WHERE id=?", initial.ID)
			}
			before := cancellationSnapshot(t, s)
			if err := s.ReconcileMaterializedReviewChildren(ctx); err == nil {
				t.Fatal("automatic pass ignored corrupt ownership")
			}
			command := reviewRuntimeCommand(assignment, domain.WorkerCommandPrepare)
			if _, err := s.CommitWorkerCommands(ctx, []domain.WorkerCommand{command}); err == nil {
				t.Fatal("start ignored corrupt ownership")
			}
			if before != cancellationSnapshot(t, s) {
				t.Fatal("corrupt ownership mutated tables/audit")
			}
			s = reviewRuntimeReopen(t, s)
			if before != cancellationSnapshot(t, s) {
				t.Fatal("corruption rollback lost on reopen")
			}
		})
	}
}

func TestReviewRuntimeOfferAndClaimReplayDoNotGrantAdmission(t *testing.T) {
	for _, boundary := range []string{"offer", "claim"} {
		t.Run(boundary, func(t *testing.T) {
			ctx := context.Background()
			s, f, _, receipt := parentWaitFixture(t)
			reviewRuntimeWorker(t, s)
			initial := receipt.Graph.Attempts[0]
			assignment := cancellationAssign(t, s, initial, domain.AssignmentOffered, f.Requirements.Members[0].Route)
			now := time.Now().UTC()
			request := domain.AssignmentClaimRequest{CoordinatorEpoch: 1, WorkerID: assignment.WorkerID, WorkerEpoch: assignment.WorkerEpoch, AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch, LeaseToken: assignment.LeaseToken, ClaimedAt: now, LeaseExpiresAt: now.Add(time.Hour)}
			if boundary == "claim" {
				if _, err := s.ClaimAssignment(ctx, request); err != nil {
					t.Fatal(err)
				}
			}
			reviewRuntimeInvalidate(t, s, f, receipt, "ended")
			before := cancellationSnapshot(t, s)
			if boundary == "claim" {
				if _, err := s.ClaimAssignment(ctx, request); err == nil {
					t.Fatal("exact claimed lease replay granted current permission")
				}
			} else {
				commit := domain.AssignmentPlanCommit{CoordinatorEpoch: 1, CommittedAt: now, Items: []domain.AssignmentPlanItem{{Assignment: assignment, WorkerEpoch: assignment.WorkerEpoch, WorkerSnapshotSequence: 1, ExpectedAttemptRevision: initial.Revision}}}
				assignment.Estimate = &domain.TaskAdmissionEstimate{RemainingCost: 1, ExpectedRuntime: time.Hour, CheckpointMargin: time.Minute}
				commit.Items[0].Assignment = assignment
				if got, err := s.CommitAssignmentPlan(ctx, commit); err != nil || len(got) != 0 {
					t.Fatalf("offered replay %v %+v", err, got)
				}
			}
			if before != cancellationSnapshot(t, s) {
				t.Fatal("replay refusal mutated logical tables/audit")
			}
			s = reviewRuntimeReopen(t, s)
			if before != cancellationSnapshot(t, s) {
				t.Fatal("replay refusal lost across reopen")
			}
		})
	}
}

// Returning a pending snapshot is not a DB/wire transaction. Later invalidation
// withholds the next load but cannot recall a previously returned command slice.
func TestReviewRuntimePendingSnapshotCannotRecall(t *testing.T) {
	ctx := context.Background()
	s, f, _, receipt := parentWaitFixture(t)
	reviewRuntimeWorker(t, s)
	assignment := cancellationAssign(t, s, receipt.Graph.Attempts[0], domain.AssignmentClaimed, f.Requirements.Members[0].Route)
	command := reviewRuntimeCommand(assignment, domain.WorkerCommandPrepare)
	if _, err := s.CommitWorkerCommands(ctx, []domain.WorkerCommand{command}); err != nil {
		t.Fatal(err)
	}
	loaded, err := s.LoadPendingWorkerCommands(ctx, "child-worker", "child-epoch", 1, 1, time.Now().UTC())
	if err != nil || len(loaded) != 1 {
		t.Fatalf("healthy load %v %+v", err, loaded)
	}
	reviewRuntimeInvalidate(t, s, f, receipt, "ended")
	if len(loaded) != 1 || loaded[0].ID != command.ID {
		t.Fatal("prior snapshot was somehow recalled")
	}
	next, err := s.LoadPendingWorkerCommands(ctx, "child-worker", "child-epoch", 1, 1, time.Now().UTC())
	if err != nil || len(next) != 0 {
		t.Fatalf("later invalidation not fenced %v %+v", err, next)
	}
}

func TestReviewRuntimeMixedSiblingOfferAndCommand(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(fmt.Sprintf("corrupt=%v", corrupt), func(t *testing.T) {
			ctx := context.Background()
			s, f, _, receipt := parentWaitFixture(t)
			reviewRuntimeWorker(t, s)
			initial := receipt.Graph.Attempts[0]
			target := fleetPlanCommit(1, "review-offer", initial.ID, "child-epoch", 1)
			target.CommittedAt = time.Now().UTC()
			target.Items[0].Assignment.WorkerID = "child-worker"
			target.Items[0].Assignment.Project = "repo"
			target.Items[0].Assignment.Route = receipt.Graph.Tasks[0].Routes[0]
			sibling := fleetAttempt("rt-ordinary-sibling")
			sibling.TaskID = "ordinary-task"
			sibling.WorkflowRunID = "ordinary-run"
			cancellationSave(t, s, CoordinatorRecords{Workflows: []domain.Workflow{{ID: "ordinary", Name: "ordinary", Project: "repo"}}, WorkflowRuns: []domain.WorkflowRun{{ID: sibling.WorkflowRunID, WorkflowID: "ordinary", Revision: 1, Progress: domain.ProgressActive}}, Tasks: []domain.Task{{ID: sibling.TaskID, WorkflowID: "ordinary", Name: "ordinary", Routes: []domain.ProviderRoute{target.Items[0].Assignment.Route}}}, Attempts: []domain.Attempt{sibling}})
			first := target.Items[0]
			first.Assignment.ID = "ordinary-offer"
			first.Assignment.AttemptID = sibling.ID
			first.Assignment.LeaseToken = "ordinary-lease"
			first.Assignment.DispatchToken = "ordinary-token"
			first.Assignment.ThreadID = "ordinary-thread"
			target.Items = []domain.AssignmentPlanItem{first, target.Items[0]}
			if corrupt {
				if _, err := s.db.Exec("UPDATE coordinator_attempts SET record=json_set(record,'$.workflowRunId','forged') WHERE id=?", initial.ID); err != nil {
					t.Fatal(err)
				}
			} else {
				reviewRuntimeInvalidate(t, s, f, receipt, "ended")
			}
			before := cancellationSnapshot(t, s)
			offered, err := s.CommitAssignmentPlan(ctx, target)
			if corrupt {
				if err == nil || before != cancellationSnapshot(t, s) {
					t.Fatalf("malformed ownership failed batch rollback: %v", err)
				}
				s = reviewRuntimeReopen(t, s)
				if before != cancellationSnapshot(t, s) {
					t.Fatal("batch rollback lost after reopen")
				}
				return
			}
			if err != nil || len(offered) != 1 || offered[0].AttemptID != sibling.ID {
				t.Fatalf("invalid review blocked healthy sibling: %+v %v", offered, err)
			}
			if loadAttempt(t, s, initial.ID).AssignmentID != "" {
				t.Fatal("invalid review offer escaped skip")
			}
			assigned := offered[0]
			now := time.Now().UTC()
			if _, err = s.ClaimAssignment(ctx, domain.AssignmentClaimRequest{CoordinatorEpoch: 1, WorkerID: assigned.WorkerID, WorkerEpoch: assigned.WorkerEpoch, AssignmentID: assigned.ID, AssignmentEpoch: assigned.Epoch, LeaseToken: assigned.LeaseToken, ClaimedAt: now, LeaseExpiresAt: now.Add(time.Hour)}); err != nil {
				t.Fatal(err)
			}
			targetAssignment := cancellationAssign(t, s, initial, domain.AssignmentClaimed, f.Requirements.Members[0].Route)
			ordinaryCommand := reviewRuntimeCommand(assigned, domain.WorkerCommandPrepare)
			ordinaryCommand.ID = "ordinary-start"
			childCommand := reviewRuntimeCommand(targetAssignment, domain.WorkerCommandPrepare)
			got, err := s.CommitWorkerCommands(ctx, []domain.WorkerCommand{ordinaryCommand, childCommand})
			if err != nil || len(got) != 1 || got[0].ID != ordinaryCommand.ID {
				t.Fatalf("command sibling %+v %v", got, err)
			}
			pending, err := s.LoadPendingWorkerCommands(ctx, "child-worker", "child-epoch", 1, 1, now)
			if err != nil || len(pending) != 1 || pending[0].ID != ordinaryCommand.ID {
				t.Fatalf("pending sibling %+v %v", pending, err)
			}
		})
	}
}

func TestReviewRuntimeMalformedProposedRetryDurablyRejected(t *testing.T) {
	for _, field := range []string{"run", "task", "number", "revision", "activation", "assignment", "completion", "started", "missing"} {
		t.Run(field, func(t *testing.T) {
			ctx := context.Background()
			s, _, _, receipt := parentWaitFixture(t)
			now := time.Now().UTC()
			initial := receipt.Graph.Attempts[0]
			failed := initial
			failed.Progress = domain.ProgressFailed
			failed.Control = domain.ControlStopped
			failed.Revision++
			failed.CompletedAt = &now
			cancellationSave(t, s, CoordinatorRecords{Attempts: []domain.Attempt{failed}})
			command := domain.AdminCommand{ID: "malformed-retry", Kind: domain.AdminCommandRetry, TargetType: domain.AdminTargetAttempt, TargetID: failed.ID, ExpectedRevision: failed.Revision, Reason: "retry", RequestedBy: "operator", State: domain.AdminCommandPending, CreatedAt: now}
			if _, err := s.SubmitAdminCommand(ctx, command); err != nil {
				t.Fatal(err)
			}
			next := failed
			next.Revision++
			retry := initial
			retry.ID = "malformed-new"
			retry.Number = 2
			retry.Revision = 1
			switch field {
			case "run":
				retry.WorkflowRunID = "foreign"
			case "task":
				retry.TaskID = "foreign"
			case "number":
				retry.Number = 99
			case "revision":
				retry.Revision = 9
			case "activation":
				retry.SupervisionActivationID = "foreign"
			case "assignment":
				retry.AssignmentID = "foreign"
			case "completion":
				retry.CompletedAt = &now
			case "started":
				retry.StartedAt = &now
			}
			application := domain.AdminCommandApplication{CommandID: command.ID, ExpectedCommandState: domain.AdminCommandPending, ExpectedTargetRevision: failed.Revision, State: domain.AdminCommandApplied, Attempt: &next, NewAttempt: &retry, AppliedAt: now}
			if field == "missing" {
				application.NewAttempt = nil
			}
			before := cancellationRecords(t, s)
			decision, err := s.ApplyAdminCommand(ctx, application)
			if err != nil || decision.Command.State != domain.AdminCommandRejected || !strings.Contains(decision.Command.Failure, "malformed review retry") {
				t.Fatalf("malformed proposal %+v %v", decision, err)
			}
			after := cancellationRecords(t, s)
			after.AdminCommands = before.AdminCommands
			if !reflect.DeepEqual(before, after) {
				t.Fatal("malformed retry wrote execution records")
			}
			stable := cancellationSnapshot(t, s)
			s = reviewRuntimeReopen(t, s)
			if stable != cancellationSnapshot(t, s) {
				t.Fatal("durable rejection lost after reopen")
			}
		})
	}
}

func TestReviewRuntimeOrdinaryReviewLikeRetry(t *testing.T) {
	for _, progress := range []domain.ProgressState{domain.ProgressFailed, domain.ProgressCancelled} {
		t.Run(string(progress), func(t *testing.T) {
			ctx := context.Background()
			s := openFleetTestStore(t)
			now := time.Now().UTC()
			attempt := fleetAttempt("rt-ordinary-review-like-id")
			attempt.Progress = progress
			attempt.Control = domain.ControlStopped
			attempt.CompletedAt = &now
			cancellationSave(t, s, CoordinatorRecords{Workflows: []domain.Workflow{{ID: "ordinary", Project: "repo"}}, WorkflowRuns: []domain.WorkflowRun{{ID: attempt.WorkflowRunID, WorkflowID: "ordinary", Revision: 1, Progress: domain.ProgressActive}}, Tasks: []domain.Task{{ID: attempt.TaskID, WorkflowID: "ordinary", Name: "ordinary"}}, Attempts: []domain.Attempt{attempt}})
			command := domain.AdminCommand{ID: "ordinary-retry", Kind: domain.AdminCommandRetry, TargetType: domain.AdminTargetAttempt, TargetID: attempt.ID, ExpectedRevision: attempt.Revision, Reason: "retry ordinary task", RequestedBy: "operator", State: domain.AdminCommandPending, CreatedAt: now}
			if _, err := s.SubmitAdminCommand(ctx, command); err != nil {
				t.Fatal(err)
			}
			next := attempt
			next.Revision++
			retry := fleetAttempt("ordinary-next")
			retry.Number = 2
			app := domain.AdminCommandApplication{CommandID: command.ID, ExpectedCommandState: domain.AdminCommandPending, ExpectedTargetRevision: attempt.Revision, State: domain.AdminCommandApplied, Attempt: &next, NewAttempt: &retry, AppliedAt: now}
			got, err := s.ApplyAdminCommand(ctx, app)
			if err != nil || got.Command.State != domain.AdminCommandApplied {
				t.Fatalf("ordinary retry %+v %v", got, err)
			}
		})
	}
}

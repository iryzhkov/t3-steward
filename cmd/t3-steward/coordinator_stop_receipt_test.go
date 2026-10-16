package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
)

type coordinatorStopFixture struct {
	cfg        config.Config
	pool       domain.QuotaPool
	store      *sqlite.Store
	path       string
	now        time.Time
	record     domain.ThrottleAttemptRecord
	assignment domain.Assignment
	attempt    domain.Attempt
	snapshot   domain.WorkerSnapshot
	states     []domain.BucketState
}

func newCoordinatorStopFixture(t *testing.T) *coordinatorStopFixture {
	t.Helper()
	ctx := context.Background()
	f := &coordinatorStopFixture{now: time.Date(2030, 1, 1, 1, 0, 0, 0, time.UTC), path: filepath.Join(t.TempDir(), "state.db")}
	f.cfg, f.pool, f.record, f.states = collectorGrantFixture(f.now)
	var err error
	f.store, err = sqlitetest.OpenMigrated(f.path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.store.Close() })
	f.store.SetClock(func() time.Time { return f.now })
	epoch, err := f.store.CoordinatorEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	f.assignment = domain.Assignment{ID: "assignment", AttemptID: "attempt", WorkerID: "normandy", WorkerEpoch: "worker-1", Route: f.record.Command.Route, State: domain.AssignmentClaimed, Epoch: 1, ThreadID: "thread", LeaseToken: "lease", LeaseExpiresAt: f.now.Add(time.Hour), CreatedAt: f.now, UpdatedAt: f.now}
	f.attempt = domain.Attempt{ID: "attempt", WorkflowRunID: "run", TaskID: "task", Number: 1, Progress: domain.ProgressActive, Control: domain.ControlDraining, Revision: 1, AssignmentID: f.assignment.ID, UpdatedAt: f.now}
	if err := f.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: "workflow", Version: 1, Name: "workflow", Class: domain.TaskClassRequired, TaskIDs: []string{"task"}, CreatedAt: f.now}},
		WorkflowRuns: []domain.WorkflowRun{{ID: "run", WorkflowID: "workflow", GraphRevision: 1, Progress: domain.ProgressActive, Revision: 1, CreatedAt: f.now, UpdatedAt: f.now}},
		Tasks:        []domain.Task{{ID: "task", WorkflowID: "workflow", Name: "task", Class: domain.TaskClassRequired}},
		Attempts:     []domain.Attempt{f.attempt}, Assignments: []domain.Assignment{f.assignment},
	}); err != nil {
		t.Fatal(err)
	}
	actualStop := *f.record.AcknowledgedAt
	f.record.Delivery, f.record.Control, f.record.Result, f.record.AcknowledgedAt = domain.ThrottleDeliveryPending, domain.ControlDraining, "", nil
	if err := f.store.CommitThrottleAttemptTransitions(ctx, []domain.ThrottleAttemptTransition{{Record: f.record}}); err != nil {
		t.Fatal(err)
	}
	acks, err := backlog.PlanThrottleAcknowledgements([]domain.ThrottleAttemptRecord{f.record}, []domain.ThrottleAcknowledgement{{CommandID: f.record.Command.ID, AttemptID: f.record.AttemptID, Accepted: true, Result: domain.ThrottleResultStopped, AcknowledgedAt: actualStop}}, f.now)
	if err != nil || len(acks) != 1 {
		t.Fatalf("actual stop ACK: %+v %v", acks, err)
	}
	if err := f.store.CommitThrottleAttemptTransitions(ctx, acks); err != nil {
		t.Fatal(err)
	}
	f.record = acks[0].Record
	f.attempt.Control = domain.ControlPausedUncheckpointed
	f.snapshot = domain.WorkerSnapshot{WorkerID: "normandy", WorkerEpoch: "worker-1", CoordinatorEpoch: epoch, Sequence: 10, Connected: true, ObservedAt: f.now, ValidUntil: f.now.Add(time.Minute),
		Inventory:   domain.WorkerInventory{ID: "normandy"},
		Assignments: []domain.WorkerAssignmentObservation{{AssignmentID: f.assignment.ID, AssignmentEpoch: f.assignment.Epoch, State: domain.AssignmentClaimed, Control: domain.ControlPaused, ThreadID: "thread", WorkspacePath: f.record.Command.WorkspacePath, ObservedAt: f.now}}}
	f.refresh(t, false)
	return f
}

func (f *coordinatorStopFixture) refresh(t *testing.T, freshBatch bool) {
	t.Helper()
	f.now = f.now.Add(time.Nanosecond) // Each authenticated snapshot is a new observation.
	if freshBatch {
		at := f.now.Add(-3 * time.Minute)
		reset := f.now.Add(time.Hour)
		for i := range f.states {
			f.states[i].ObservedAt, f.states[i].UpdatedAt, f.states[i].ResetsAt, f.states[i].Epoch = at, at, &reset, domain.EpochFor(&reset)
			p := *f.states[i].Provenance
			p.AcquiredAt, p.AcquisitionID = at, "retry-batch"
			f.states[i].Provenance = &p
		}
	}
	for _, state := range f.states {
		if err := f.store.SaveBucket(context.Background(), state); err != nil {
			t.Fatal(err)
		}
	}
	f.snapshot.Sequence++
	f.snapshot.ObservedAt, f.snapshot.ValidUntil = f.now, f.now.Add(time.Minute)
	if err := f.store.SaveWorkerSnapshot(context.Background(), f.snapshot); err != nil {
		t.Fatal(err)
	}
}

func (f *coordinatorStopFixture) plan(t *testing.T) ([]domain.ThrottleAttemptTransition, []domain.ThrottleCommand) {
	t.Helper()
	authorize := func(pool domain.QuotaPool, record domain.ThrottleAttemptRecord, now time.Time) *domain.QuotaRecoveryGrant {
		g, err := newCoordinatorQuotaRecoveryGrant(f.cfg, f.store)(context.Background(), pool, record, f.snapshot, now)
		if err != nil {
			t.Fatal(err)
		}
		return g
	}
	transitions, commands, err := backlog.PlanThrottleResumes([]domain.ThrottleAttemptRecord{f.record}, []domain.QuotaAdmissionRecord{{QuotaPoolID: f.pool.ID, Admission: domain.AdmissionOpen}}, []domain.QuotaPool{f.pool}, f.now, authorize)
	if err != nil {
		t.Fatal(err)
	}
	return transitions, commands
}

func TestCoordinatorExpiredResumeReissuesFromDurableStopAfterRestart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newCoordinatorStopFixture(t)
	originalStop := f.record.StopReceipt
	first, commands := f.plan(t)
	if len(first) != 1 || len(commands) != 1 {
		t.Fatalf("first resume: %+v", first)
	}
	if err := f.store.CommitThrottleAttemptTransitions(ctx, first); err != nil {
		t.Fatal(err)
	}
	f.record = first[0].Record
	f.now = commands[0].QuotaRecovery.ExpiresAt.Add(time.Second)
	rejected, err := backlog.PlanThrottleAcknowledgements([]domain.ThrottleAttemptRecord{f.record}, []domain.ThrottleAcknowledgement{{CommandID: f.record.Command.ID, AttemptID: f.record.AttemptID, Error: "source authority expired", AcknowledgedAt: f.now}}, f.now)
	if err != nil || len(rejected) != 1 {
		t.Fatalf("expiry rejection: %+v %v", rejected, err)
	}
	if err := f.store.CommitThrottleAttemptTransitions(ctx, rejected); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	f.store, err = sqlitetest.OpenMigrated(f.path)
	if err != nil {
		t.Fatal(err)
	}
	f.store.SetClock(func() time.Time { return f.now })
	persisted, err := f.store.LoadThrottleAttemptRecords(ctx)
	if err != nil || len(persisted) != 1 {
		t.Fatalf("restart: %+v %v", persisted, err)
	}
	f.record = persisted[0]
	receipt, ok := f.record.AcceptedQuotaStop()
	if !ok || receipt.Command.ID != originalStop.Command.ID || !receipt.AcknowledgedAt.Equal(originalStop.AcknowledgedAt) || receipt.AcknowledgedAt.Equal(*f.record.AcknowledgedAt) {
		t.Fatalf("stop authority lost or replaced by rejection: %+v", f.record)
	}
	f.refresh(t, false)
	if pending, _ := f.plan(t); len(pending) != 0 {
		t.Fatal("expired old batch issued a fresh grant")
	}
	f.refresh(t, true)
	retry, retryCommands := f.plan(t)
	if len(retry) != 1 || len(retryCommands) != 1 || retryCommands[0].ID == commands[0].ID || retryCommands[0].QuotaRecovery.StopCommandID != originalStop.Command.ID ||
		retryCommands[0].QuotaRecovery.Provenance.AcquisitionID == commands[0].QuotaRecovery.Provenance.AcquisitionID {
		t.Fatalf("fresh source did not reissue exact stop authority: %+v", retry)
	}
	if err := f.store.CommitThrottleAttemptTransitions(ctx, retry); err != nil {
		t.Fatalf("fresh retry atomic commit: %v", err)
	}
	if err := f.store.CommitThrottleAttemptTransitions(ctx, retry); err != nil {
		t.Fatalf("exact commit replay: %v", err)
	}
	effects := 0
	transport := stopReceiptTransport(func(commands []domain.ThrottleCommand) []domain.ThrottleAcknowledgement {
		if len(commands) != 1 || commands[0].ID != retryCommands[0].ID {
			t.Fatalf("delivery: %+v", commands)
		}
		effects++
		return []domain.ThrottleAcknowledgement{{CommandID: commands[0].ID, AttemptID: commands[0].AttemptID, Accepted: true, Result: domain.ThrottleResultResumed, AcknowledgedAt: f.now}}
	})
	if _, err := backlog.ReconcilePendingThrottleCommands(ctx, f.store, transport, f.now); err != nil {
		t.Fatal(err)
	}
	if _, err := backlog.ReconcilePendingThrottleCommands(ctx, f.store, transport, f.now); err != nil {
		t.Fatal(err)
	}
	final, err := f.store.LoadThrottleAttemptRecords(ctx)
	if err != nil || len(final) != 1 || final[0].Control != domain.ControlRunning || final[0].StopReceipt != nil || effects != 1 {
		t.Fatalf("one-effect/replay final=%+v effects=%d err=%v", final, effects, err)
	}
}

type stopReceiptTransport func([]domain.ThrottleCommand) []domain.ThrottleAcknowledgement

func (f stopReceiptTransport) DeliverThrottleCommands(_ context.Context, _ string, commands []domain.ThrottleCommand) ([]domain.ThrottleAcknowledgement, error) {
	return f(commands), nil
}

func TestCoordinatorRetainedStopRejectsChangedAuthorityBeforeCommit(t *testing.T) {
	for _, change := range []string{"forged receipt", "changed stop ACK", "missing lineage", "lease expired", "assignment epoch", "worker epoch", "cancelled", "source changed", "new stop"} {
		t.Run(change, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			f := newCoordinatorStopFixture(t)
			planned, _ := f.plan(t)
			if len(planned) != 1 {
				t.Fatal("fixture has no qualified grant")
			}
			switch change {
			case "forged receipt":
				planned[0].Record.StopReceipt.Command.ID = "forged"
			case "changed stop ACK":
				planned[0].Record.StopReceipt.AcknowledgedAt = f.now
			case "missing lineage":
				planned[0].Record.PriorCommandIDs = nil
			case "lease expired":
				f.assignment.LeaseExpiresAt = f.now
			case "assignment epoch":
				f.assignment.Epoch++
			case "worker epoch":
				f.assignment.WorkerEpoch = "replacement"
			case "cancelled":
				f.attempt.Progress = domain.ProgressCancelled
			case "source changed":
				f.states[0].UsedPercent = 100
				f.states[0].Healthy = false
				f.states[0].Phase = domain.PhaseStopped
				f.refresh(t, false)
			case "new stop":
				f.attempt.Control = domain.ControlRunning
				if err := f.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{f.attempt}}); err != nil {
					t.Fatal(err)
				}
				next := f.record
				next.DirectiveID, next.Command.DirectiveID, next.Command.ID = "new-stop", "new-stop", "new-stop-command"
				next.Command.CreatedAt = f.now
				next.Revision, next.Delivery, next.Result, next.AcknowledgedAt, next.StopReceipt = 1, domain.ThrottleDeliveryPending, "", nil, nil
				next.Control = domain.ControlDraining
				if err := f.store.CommitThrottleAttemptTransitions(ctx, []domain.ThrottleAttemptTransition{{Record: next}}); err != nil {
					t.Fatal(err)
				}
				if grant, err := newCoordinatorQuotaRecoveryGrant(f.cfg, f.store)(ctx, f.pool, f.record, f.snapshot, f.now); err != nil || grant != nil {
					t.Fatalf("new stop did not revoke issuance: %+v %v", grant, err)
				}
			}
			if change != "new stop" {
				if err := f.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{f.attempt}, Assignments: []domain.Assignment{f.assignment}}); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.store.CommitThrottleAttemptTransitions(ctx, planned); err == nil {
				t.Fatal("changed stop/source/custody committed recovery")
			}
		})
	}
}

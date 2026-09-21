package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestGovernedTaskWakeRequiresFreshCoordinatorAdmission(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	a := records.Assignments[0]
	a.Route = domain.ProviderRoute{ProviderInstanceID: "provider", Model: "model", QuotaPoolID: "pool"}
	a.ExecutorDemand = &domain.ResourceDemand{}
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{
		Assignments: []domain.Assignment{a},
		QuotaPools:  []domain.QuotaPool{{ID: "pool", Admission: domain.AdmissionOpen, MaxConcurrent: 1}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitQuotaAdmissionTransitions(ctx, []domain.QuotaAdmissionTransition{{
		ExpectedRevision: 0,
		Record: domain.QuotaAdmissionRecord{
			QuotaPoolID: "pool", Revision: 1, Admission: domain.AdmissionOpen,
			ObservedAt: now, AppliedAt: now, Reason: "fresh fixture admission",
		},
	}}); err != nil {
		t.Fatal(err)
	}
	w, err := store.RegisterTaskWait(ctx, taskWaitRegistration(attempt, "governed", domain.WakeEach), now)
	if err != nil {
		t.Fatal(err)
	}
	settled := now.Add(time.Second)
	if _, err := store.SettleTaskWait(ctx, w.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, settled); err != nil {
		t.Fatal(err)
	}

	// The worker runner has no current coordinator admission evidence and cannot
	// jump the shared age arbiter.
	if wakes, err := store.WakeTaskWaits(ctx, settled); err != nil || len(wakes) != 0 {
		t.Fatalf("worker wake=%+v err=%v, want governed wake held", wakes, err)
	}
	// Stale evidence also fails closed.
	if wakes, err := store.WakeTaskWaitsBefore(ctx, settled, TaskWakeCutoffs{AdmissionValidAfter: now.Add(time.Millisecond)}); err != nil || len(wakes) != 0 {
		t.Fatalf("stale wake=%+v err=%v, want held", wakes, err)
	}
	// Current admission still respects authoritative pool occupancy.
	owner := domain.Attempt{
		ID: "pool-owner", WorkflowRunID: attempt.WorkflowRunID, TaskID: attempt.TaskID,
		Number: 2, AssignmentID: "pool-owner-assignment", Progress: domain.ProgressActive,
		Control: domain.ControlRunning, Revision: 1, UpdatedAt: now,
	}
	ownerAssignment := domain.Assignment{
		ID: owner.AssignmentID, AttemptID: owner.ID, WorkerID: "other-worker",
		WorkerEpoch: "other-epoch", Epoch: 1, State: domain.AssignmentClaimed,
		Route:          domain.ProviderRoute{ProviderInstanceID: "provider", Model: "model", QuotaPoolID: "pool"},
		ExecutorDemand: &domain.ResourceDemand{}, CreatedAt: now, UpdatedAt: now,
	}
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{
		Attempts: []domain.Attempt{owner}, Assignments: []domain.Assignment{ownerAssignment},
	}); err != nil {
		t.Fatal(err)
	}
	fresh := TaskWakeCutoffs{AdmissionValidAfter: now.Add(-time.Second)}
	if wakes, err := store.WakeTaskWaitsBefore(ctx, settled, fresh); err != nil || len(wakes) != 0 {
		t.Fatalf("full-pool wake=%+v err=%v, want held", wakes, err)
	}
	owner.Control, owner.Progress = domain.ControlStopped, domain.ProgressSucceeded
	ownerAssignment.State = domain.AssignmentCompleted
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{
		Attempts: []domain.Attempt{owner}, Assignments: []domain.Assignment{ownerAssignment},
	}); err != nil {
		t.Fatal(err)
	}
	// The coordinator's current admission resumes exactly once after release.
	wakes, err := store.WakeTaskWaitsBefore(ctx, settled, fresh)
	if err != nil || len(wakes) != 1 {
		t.Fatalf("fresh wake=%+v err=%v", wakes, err)
	}
}

func TestNewerSettledWakeYieldsToOlderOrdinaryCutoff(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	w, err := store.RegisterTaskWait(ctx, taskWaitRegistration(attempt, "newer-wake", domain.WakeEach), now)
	if err != nil {
		t.Fatal(err)
	}
	settled := now.Add(2 * time.Second)
	if _, err := store.SettleTaskWait(ctx, w.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, settled); err != nil {
		t.Fatal(err)
	}
	cutoff := TaskWakeCutoff{ReadyAt: now.Add(time.Second), AttemptID: "ordinary"}
	wakes, err := store.WakeTaskWaitsBefore(ctx, settled, TaskWakeCutoffs{Worker: map[string]TaskWakeCutoff{"worker": cutoff}})
	if err != nil {
		t.Fatal(err)
	}
	if len(wakes) != 0 || loadAttempt(t, store, attempt.ID).Control != domain.ControlWaitingExternal {
		t.Fatalf("newer wake bypassed older ordinary contender: %+v", wakes)
	}
}

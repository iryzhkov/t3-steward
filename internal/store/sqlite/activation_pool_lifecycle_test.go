package sqlite

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"path/filepath"
	"testing"
	"time"
)

func TestActivationPoolUnknownHoldsAndParkedReleases(t *testing.T) {
	s := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
	run := seedSupervisedRun(t, s, nil)
	run.Supervision = &domain.SupervisionRecord{RunID: run.ID}
	seed, _ := s.LoadCoordinatorRecords(context.Background())
	for i := range seed.WorkflowRuns {
		if seed.WorkflowRuns[i].ID == run.ID {
			seed.WorkflowRuns[i] = run
		}
	}
	if err := s.SaveCoordinatorRecords(context.Background(), seed); err != nil {
		t.Fatal(err)
	}
	w := supervisionWorkerSnapshot()
	w.Sequence = 2
	w.ObservedAt = supervisionTestTime.Add(time.Second)
	w.Inventory.ObservedAt = w.ObservedAt
	w.Inventory.Allocatable.ExecutorSlots = 2
	w.Inventory.Capabilities = []string{workerproto.CapabilityCampaignSupervision}
	if err := s.SaveWorkerSnapshot(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	mk := func(id string) ActivationAssignmentCommit {
		a := domain.Attempt{ID: "a-" + id, WorkflowRunID: run.ID, SupervisionActivationID: "x-" + id, Number: len(id), Progress: domain.ProgressReady, Control: domain.ControlUnassigned}
		x := domain.Assignment{ID: "x-" + id, AttemptID: a.ID, WorkerID: w.WorkerID, WorkerEpoch: w.WorkerEpoch, Route: domain.ProviderRoute{QuotaPoolID: "shared"}, State: domain.AssignmentOffered, Epoch: 1, LeaseToken: "l", DispatchToken: "d-" + id, ThreadID: "t-" + id}
		return ActivationAssignmentCommit{CoordinatorEpoch: 1, Attempt: a, Assignment: x, WorkerEpoch: w.WorkerEpoch, WorkerSnapshotSequence: w.Sequence, CommittedAt: w.ObservedAt, QuotaMaxConcurrent: 1}
	}
	if _, err := s.CommitActivationAssignment(context.Background(), mk("first")); err != nil {
		t.Fatal(err)
	}
	r, _ := s.LoadCoordinatorRecords(context.Background())
	for i := range r.Assignments {
		if r.Assignments[i].ID == "x-first" {
			r.Assignments[i].State = domain.AssignmentUnknown
		}
	}
	for i := range r.Attempts {
		if r.Attempts[i].ID == "a-first" {
			r.Attempts[i].Control = domain.ControlWaitingExternal
			r.Attempts[i].Progress = domain.ProgressWaitingExternal
		}
	}
	for i := range r.WorkflowRuns {
		if r.WorkflowRuns[i].ID == run.ID {
			r.WorkflowRuns[i] = run
		}
	}
	if err := s.SaveCoordinatorRecords(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CommitActivationAssignment(context.Background(), mk("blocked")); err == nil {
		t.Fatal("unknown did not hold slot")
	}
	r, _ = s.LoadCoordinatorRecords(context.Background())
	for i := range r.Assignments {
		if r.Assignments[i].ID == "x-first" {
			r.Assignments[i].State = domain.AssignmentClaimed
		}
	}
	for i := range r.WorkflowRuns {
		if r.WorkflowRuns[i].ID == run.ID {
			r.WorkflowRuns[i] = run
		}
	}
	if err := s.SaveCoordinatorRecords(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CommitActivationAssignment(context.Background(), mk("parked")); err != nil {
		t.Fatalf("parked did not release: %v", err)
	}
}

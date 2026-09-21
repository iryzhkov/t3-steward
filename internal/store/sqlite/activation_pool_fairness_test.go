package sqlite

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestActivationPoolMaxConcurrentFencesDistinctWorkers(t *testing.T) {
	store := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
	run := seedSupervisedRun(t, store, nil)
	run.Supervision = &domain.SupervisionRecord{RunID: run.ID}
	seed, _ := store.LoadCoordinatorRecords(context.Background())
	for i := range seed.WorkflowRuns {
		if seed.WorkflowRuns[i].ID == run.ID {
			seed.WorkflowRuns[i] = run
		}
	}
	if err := store.SaveCoordinatorRecords(context.Background(), seed); err != nil {
		t.Fatal(err)
	}
	first := supervisionWorkerSnapshot()
	first.Sequence = 2
	first.ObservedAt = supervisionTestTime.Add(time.Second)
	first.Inventory.ObservedAt = first.ObservedAt
	first.Inventory.Allocatable.ExecutorSlots = 1
	first.Inventory.Capabilities = []string{workerproto.CapabilityCampaignSupervision}
	second := first
	second.WorkerID = "worker-second"
	second.Inventory.ID = second.WorkerID
	second.WorkerEpoch = "worker-epoch-second"
	if err := store.SaveWorkerSnapshot(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveWorkerSnapshot(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	makeCommit := func(id string, snapshot domain.WorkerSnapshot) ActivationAssignmentCommit {
		attempt := domain.Attempt{ID: "attempt-" + id, WorkflowRunID: "run-1", SupervisionActivationID: "activation-" + id, Number: 1, Progress: domain.ProgressReady, Control: domain.ControlUnassigned}
		assignment := domain.Assignment{ID: "assignment-" + id, AttemptID: attempt.ID, WorkerID: snapshot.WorkerID, WorkerEpoch: snapshot.WorkerEpoch, Route: domain.ProviderRoute{QuotaPoolID: "shared"}, State: domain.AssignmentOffered, Epoch: 1, LeaseToken: "lease-" + id, DispatchToken: "dispatch-" + id, ThreadID: "thread-" + id}
		return ActivationAssignmentCommit{CoordinatorEpoch: 1, Attempt: attempt, Assignment: assignment, WorkerEpoch: snapshot.WorkerEpoch, WorkerSnapshotSequence: snapshot.Sequence, CommittedAt: snapshot.ObservedAt, QuotaMaxConcurrent: 1}
	}
	commits := []ActivationAssignmentCommit{makeCommit("first", first), makeCommit("second", second)}
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	for i := range commits {
		go func(c ActivationAssignmentCommit) {
			defer wg.Done()
			<-start
			_, _ = store.CommitActivationAssignment(context.Background(), c)
		}(commits[i])
	}
	close(start)
	wg.Wait()
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, a := range records.Assignments {
		if a.Route.QuotaPoolID == "shared" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("pool maxConcurrent=1 committed %d activation offers, want exactly one", count)
	}
}

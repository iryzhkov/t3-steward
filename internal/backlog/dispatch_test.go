package backlog

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

type dispatchWorkerResult struct {
	state DispatchThreadState
	err   error
}

type dispatchWorkerFake struct {
	observations []dispatchWorkerResult
	createErrs   []error
	beforeCreate func(DispatchCommand)
	observed     []DispatchCommand
	created      []DispatchCommand
}

func (worker *dispatchWorkerFake) ObserveThread(_ context.Context, command DispatchCommand) (DispatchThreadState, error) {
	worker.observed = append(worker.observed, command)
	if len(worker.observations) == 0 {
		return "", errors.New("unexpected observation")
	}
	result := worker.observations[0]
	worker.observations = worker.observations[1:]
	return result.state, result.err
}

func (worker *dispatchWorkerFake) CreateThread(_ context.Context, command DispatchCommand) error {
	if worker.beforeCreate != nil {
		worker.beforeCreate(command)
	}
	worker.created = append(worker.created, command)
	if len(worker.createErrs) == 0 {
		return nil
	}
	err := worker.createErrs[0]
	worker.createErrs = worker.createErrs[1:]
	return err
}

func TestReconcileAssignmentDispatchRecoversLostCreateResponse(t *testing.T) {
	store := openDispatchTestStore(t, filepath.Join(t.TempDir(), "state.db"))
	defer store.Close()
	input := dispatchTestAssignment()
	worker := &dispatchWorkerFake{
		observations: []dispatchWorkerResult{
			{state: DispatchThreadMissing},
			{state: DispatchThreadActive},
		},
		createErrs: []error{errors.New("response connection reset")},
		beforeCreate: func(command DispatchCommand) {
			persisted, err := store.LoadAssignmentDispatch(context.Background(), input.ID)
			if err != nil {
				t.Fatal(err)
			}
			if persisted.DispatchState != domain.DispatchCreating ||
				persisted.ThreadID != command.ThreadID || persisted.DispatchToken != command.DispatchToken {
				t.Fatalf("dispatch was not persisted before create: %#v", persisted)
			}
		},
	}
	now := input.CreatedAt.Add(time.Minute)

	got, err := ReconcileAssignmentDispatch(context.Background(), store, worker, input, now)
	if err != nil {
		t.Fatalf("reconcile lost response: %v", err)
	}
	if got.DispatchState != domain.DispatchConfirmed || got.DispatchConfirmedAt == nil ||
		got.State != domain.AssignmentClaimed {
		t.Fatalf("lost response result = %#v", got)
	}
	if len(worker.created) != 1 || len(worker.observed) != 2 {
		t.Fatalf("worker calls: observed=%d created=%d", len(worker.observed), len(worker.created))
	}
	wantCommand := dispatchCommand(input)
	for _, command := range append(worker.observed, worker.created...) {
		if !reflect.DeepEqual(command, wantCommand) {
			t.Fatalf("dispatch identity changed: got %#v want %#v", command, wantCommand)
		}
	}
	persisted, err := store.LoadAssignmentDispatch(context.Background(), input.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(persisted, got) {
		t.Fatalf("persisted assignment = %#v, want %#v", persisted, got)
	}
}

func TestReconcileAssignmentDispatchRetriesSameIdentityAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	store := openDispatchTestStore(t, path)
	input := dispatchTestAssignment()
	firstWorker := &dispatchWorkerFake{
		observations: []dispatchWorkerResult{
			{state: DispatchThreadMissing},
			{state: DispatchThreadCreated},
		},
		createErrs: []error{errors.New("lost create response")},
	}
	first, err := ReconcileAssignmentDispatch(
		context.Background(), store, firstWorker, input, input.CreatedAt.Add(time.Minute),
	)
	if !errors.Is(err, ErrDispatchUncertain) {
		t.Fatalf("first reconciliation error = %v, want uncertain", err)
	}
	if first.DispatchState != domain.DispatchUnknown || first.State != domain.AssignmentUnknown {
		t.Fatalf("first reconciliation = %#v", first)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store = openDispatchTestStore(t, path)
	defer store.Close()
	secondWorker := &dispatchWorkerFake{
		observations: []dispatchWorkerResult{{state: DispatchThreadCreated}},
	}
	second, err := ReconcileAssignmentDispatch(
		context.Background(), store, secondWorker, input, input.CreatedAt.Add(2*time.Minute),
	)
	if err != nil {
		t.Fatalf("restart reconciliation: %v", err)
	}
	if second.DispatchState != domain.DispatchConfirmed || second.State != domain.AssignmentClaimed {
		t.Fatalf("restart reconciliation = %#v", second)
	}
	if len(firstWorker.created) != 1 || len(secondWorker.created) != 1 {
		t.Fatalf("create calls across restart = %d + %d", len(firstWorker.created), len(secondWorker.created))
	}
	if !reflect.DeepEqual(firstWorker.created[0], secondWorker.created[0]) {
		t.Fatalf("restart changed dispatch identity: first=%#v second=%#v",
			firstWorker.created[0], secondWorker.created[0])
	}
}

func TestReconcileAssignmentDispatchRetainsAmbiguousWorkerOwnership(t *testing.T) {
	store := openDispatchTestStore(t, filepath.Join(t.TempDir(), "state.db"))
	defer store.Close()
	input := dispatchTestAssignment()
	unavailable := &dispatchWorkerFake{
		observations: []dispatchWorkerResult{{err: errors.New("worker offline")}},
	}
	unknown, err := ReconcileAssignmentDispatch(
		context.Background(), store, unavailable, input, input.CreatedAt.Add(time.Minute),
	)
	if !errors.Is(err, ErrDispatchUncertain) {
		t.Fatalf("offline reconciliation error = %v, want uncertain", err)
	}
	if unknown.DispatchState != domain.DispatchUnknown || unknown.State != domain.AssignmentUnknown {
		t.Fatalf("offline reconciliation = %#v", unknown)
	}
	if len(unavailable.created) != 0 {
		t.Fatalf("created %d threads while worker was unavailable", len(unavailable.created))
	}

	reconnected := &dispatchWorkerFake{
		observations: []dispatchWorkerResult{{state: DispatchThreadStopped}},
	}
	stopped, err := ReconcileAssignmentDispatch(
		context.Background(), store, reconnected, input, input.CreatedAt.Add(2*time.Minute),
	)
	if err != nil {
		t.Fatalf("stopped reconciliation: %v", err)
	}
	if stopped.DispatchState != domain.DispatchStopped || stopped.State != domain.AssignmentReleased {
		t.Fatalf("stopped reconciliation = %#v", stopped)
	}
	if len(reconnected.created) != 0 {
		t.Fatal("created a replacement thread instead of retaining the assignment")
	}
}

func openDispatchTestStore(t *testing.T, path string) *sqlite.Store {
	t.Helper()
	store, err := sqlite.OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func dispatchTestAssignment() domain.Assignment {
	now := time.Date(2026, time.September, 10, 18, 0, 0, 0, time.UTC)
	return domain.Assignment{
		ID: "assignment-1", AttemptID: "attempt-1", WorkerID: "normandy",
		Route: domain.ProviderRoute{
			WorkerID: "normandy", ProviderInstanceID: "codex",
			Model: "gpt-5.6-sol", QuotaPoolID: "openai-primary",
		},
		State: domain.AssignmentClaimed, Epoch: 7, LeaseToken: "lease-1",
		LeaseExpiresAt: now.Add(time.Hour),
		DispatchToken:  "dispatch-1", ThreadID: "thread-1",
		CreatedAt: now, UpdatedAt: now,
	}
}

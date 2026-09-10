package backlog

import (
	"errors"
	"sync"
	"testing"
)

func TestEnvironmentCoordinatorSerializesConcurrentWorkflowTasks(t *testing.T) {
	coordinator := NewEnvironmentCoordinator()
	start := make(chan struct{})
	results := make(chan error, 2)
	var ready sync.WaitGroup
	ready.Add(2)

	for _, attemptID := range []string{"attempt-a", "attempt-b"} {
		go func() {
			ready.Done()
			<-start
			_, err := coordinator.Reserve(environmentRequest("run-1", attemptID, "worker-a", EnvironmentScopeWorkflow))
			results <- err
		}()
	}
	ready.Wait()
	close(start)

	var successes, checkoutConflicts int
	for range 2 {
		err := <-results
		if err == nil {
			successes++
			continue
		}
		var conflict *EnvironmentConflict
		if errors.As(err, &conflict) && conflict.Kind == EnvironmentConflictCheckout {
			checkoutConflicts++
			continue
		}
		t.Fatalf("unexpected reservation error: %v", err)
	}
	if successes != 1 || checkoutConflicts != 1 {
		t.Fatalf("results = %d successes, %d checkout conflicts; want one of each", successes, checkoutConflicts)
	}
}

func TestEnvironmentCoordinatorAcquiresResourceLocksCanonicallyAndAtomically(t *testing.T) {
	coordinator := NewEnvironmentCoordinator()
	owner := environmentRequest("run-owner", "attempt-owner", "worker-a", EnvironmentScopeTask)
	owner.ResourceLocks = []string{"z-database", "a-deployment"}
	reservation, err := coordinator.Reserve(owner)
	if err != nil {
		t.Fatalf("reserve owner: %v", err)
	}
	if got, want := reservation.ResourceLocks, []string{"a-deployment", "z-database"}; !equalStrings(got, want) {
		t.Fatalf("canonical locks = %v, want %v", got, want)
	}

	contender := environmentRequest("run-contender", "attempt-contender", "worker-b", EnvironmentScopeTask)
	contender.ResourceLocks = []string{"z-database", "a-deployment"}
	_, err = coordinator.Reserve(contender)
	var conflict *EnvironmentConflict
	if !errors.As(err, &conflict) || conflict.Kind != EnvironmentConflictResource ||
		conflict.Resource != "a-deployment" || conflict.OwnerAttemptID != "attempt-owner" {
		t.Fatalf("contention error = %#v (%v), want first canonical lock owner", conflict, err)
	}

	if err := coordinator.Release(owner.AttemptID, EnvironmentReleaseTerminal); err != nil {
		t.Fatalf("release canonical-order owner: %v", err)
	}
	zOwner := environmentRequest("run-z", "attempt-z", "worker-a", EnvironmentScopeTask)
	zOwner.ResourceLocks = []string{"z-database"}
	if _, err := coordinator.Reserve(zOwner); err != nil {
		t.Fatalf("reserve z owner: %v", err)
	}
	atomicContender := environmentRequest("run-atomic", "attempt-atomic", "worker-b", EnvironmentScopeTask)
	atomicContender.ResourceLocks = []string{"a-deployment", "z-database"}
	if _, err := coordinator.Reserve(atomicContender); err == nil {
		t.Fatal("atomic contender unexpectedly acquired held z lock")
	}
	probe := environmentRequest("run-probe", "attempt-probe", "worker-c", EnvironmentScopeTask)
	probe.ResourceLocks = []string{"a-deployment"}
	if _, err := coordinator.Reserve(probe); err != nil {
		t.Fatalf("failed contender leaked its free lock: %v", err)
	}
}

func TestEnvironmentCoordinatorPinsWorkflowWorkerUntilCleanup(t *testing.T) {
	coordinator := NewEnvironmentCoordinator()
	first := environmentRequest("run-1", "attempt-1", "worker-a", EnvironmentScopeWorkflow)
	if _, err := coordinator.Reserve(first); err != nil {
		t.Fatalf("reserve first attempt: %v", err)
	}
	if err := coordinator.Release(first.AttemptID, EnvironmentReleaseTerminal); err != nil {
		t.Fatalf("release first attempt: %v", err)
	}

	mismatch := environmentRequest("run-1", "attempt-2", "worker-b", EnvironmentScopeWorkflow)
	_, err := coordinator.Reserve(mismatch)
	var conflict *EnvironmentConflict
	if !errors.As(err, &conflict) || conflict.Kind != EnvironmentConflictWorker ||
		conflict.OwnerWorkerID != "worker-a" {
		t.Fatalf("worker mismatch = %#v (%v), want worker-a owner", conflict, err)
	}

	if err := coordinator.CleanupWorkflow("run-1"); err != nil {
		t.Fatalf("cleanup terminal workflow: %v", err)
	}
	if _, err := coordinator.Reserve(mismatch); err != nil {
		t.Fatalf("reserve after cleanup on alternate worker: %v", err)
	}
}

func TestEnvironmentCoordinatorReleasePolicies(t *testing.T) {
	tests := []struct {
		name        string
		policy      EnvironmentReleasePolicy
		wantBlocked bool
		wantStored  bool
	}{
		{name: "terminal", policy: EnvironmentReleaseTerminal},
		{name: "cancellation", policy: EnvironmentReleaseCanceled},
		{name: "pause retains resources", policy: EnvironmentPauseRetainResources, wantBlocked: true, wantStored: true},
		{name: "pause releases resources", policy: EnvironmentPauseReleaseResources, wantStored: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			coordinator := NewEnvironmentCoordinator()
			first := environmentRequest("run-1", "attempt-1", "worker-a", EnvironmentScopeWorkflow)
			first.ResourceLocks = []string{"shared"}
			if _, err := coordinator.Reserve(first); err != nil {
				t.Fatalf("reserve first attempt: %v", err)
			}
			if err := coordinator.Release(first.AttemptID, test.policy); err != nil {
				t.Fatalf("release first attempt: %v", err)
			}

			_, stored := coordinator.Reservation(first.AttemptID)
			if stored != test.wantStored {
				t.Fatalf("reservation retained = %v, want %v", stored, test.wantStored)
			}
			next := environmentRequest("run-1", "attempt-2", "worker-a", EnvironmentScopeWorkflow)
			next.ResourceLocks = []string{"shared"}
			_, err := coordinator.Reserve(next)
			if test.wantBlocked {
				var conflict *EnvironmentConflict
				if !errors.As(err, &conflict) || conflict.OwnerAttemptID != first.AttemptID {
					t.Fatalf("next reservation error = %#v (%v), want first owner", conflict, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("next reservation after explicit release: %v", err)
			}
		})
	}
}

func TestEnvironmentCoordinatorPausedAttemptCanResumeAndRetryCanReacquire(t *testing.T) {
	coordinator := NewEnvironmentCoordinator()
	first := environmentRequest("run-1", "attempt-1", "worker-a", EnvironmentScopeWorkflow)
	first.ResourceLocks = []string{"shared"}
	if _, err := coordinator.Reserve(first); err != nil {
		t.Fatalf("reserve first attempt: %v", err)
	}
	if err := coordinator.Release(first.AttemptID, EnvironmentPauseReleaseResources); err != nil {
		t.Fatalf("pause first attempt: %v", err)
	}
	resumed, err := coordinator.Reserve(first)
	if err != nil {
		t.Fatalf("resume same attempt: %v", err)
	}
	if resumed.State != EnvironmentReservationActive || !resumed.HoldsResources {
		t.Fatalf("resumed reservation = %#v, want active resource owner", resumed)
	}
	if err := coordinator.Release(first.AttemptID, EnvironmentReleaseTerminal); err != nil {
		t.Fatalf("complete first attempt: %v", err)
	}

	retry := first
	retry.AttemptID = "attempt-2"
	if _, err := coordinator.Reserve(retry); err != nil {
		t.Fatalf("reserve retry: %v", err)
	}
}

func TestEnvironmentCoordinatorCleanupRequiresEveryAttemptTerminal(t *testing.T) {
	coordinator := NewEnvironmentCoordinator()
	request := environmentRequest("run-1", "attempt-1", "worker-a", EnvironmentScopeWorkflow)
	if _, err := coordinator.Reserve(request); err != nil {
		t.Fatalf("reserve attempt: %v", err)
	}
	if err := coordinator.Release(request.AttemptID, EnvironmentPauseReleaseResources); err != nil {
		t.Fatalf("pause attempt: %v", err)
	}
	if err := coordinator.CleanupWorkflow(request.WorkflowRunID); err == nil {
		t.Fatal("cleanup succeeded while paused attempt remained nonterminal")
	}
	if err := coordinator.Release(request.AttemptID, EnvironmentReleaseCanceled); err != nil {
		t.Fatalf("cancel attempt: %v", err)
	}
	if err := coordinator.CleanupWorkflow(request.WorkflowRunID); err != nil {
		t.Fatalf("cleanup after cancellation: %v", err)
	}
}

func TestEnvironmentCoordinatorReservationIsIdempotentButImmutable(t *testing.T) {
	coordinator := &EnvironmentCoordinator{}
	request := environmentRequest("run-1", "attempt-1", "worker-a", EnvironmentScopeTask)
	request.ResourceLocks = []string{"b", "a"}
	first, err := coordinator.Reserve(request)
	if err != nil {
		t.Fatalf("reserve attempt: %v", err)
	}
	second, err := coordinator.Reserve(request)
	if err != nil {
		t.Fatalf("repeat reservation: %v", err)
	}
	if !equalStrings(first.ResourceLocks, second.ResourceLocks) {
		t.Fatalf("repeated reservation changed locks: %v != %v", first.ResourceLocks, second.ResourceLocks)
	}

	changed := request
	changed.WorkerID = "worker-b"
	if _, err := coordinator.Reserve(changed); err == nil {
		t.Fatal("changed reservation for the same attempt succeeded")
	}
}

func environmentRequest(runID, attemptID, workerID, scope string) EnvironmentReservationRequest {
	return EnvironmentReservationRequest{
		WorkflowRunID: runID,
		TaskID:        "task-" + attemptID,
		AttemptID:     attemptID,
		WorkerID:      workerID,
		Scope:         scope,
	}
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func seedActivationDispatchFailure(t *testing.T, store *Store) domain.ActivationDispatchFailure {
	t.Helper()
	publication := seedActivationEvidenceOwner(t, store)
	assignment := domain.Assignment{
		ID: "activation-assignment", AttemptID: publication.Artifact.AttemptID,
		WorkerID: "worker-1", WorkerEpoch: "worker-epoch-1", State: domain.AssignmentOffered, Epoch: 1,
		LeaseToken: "lease", DispatchToken: "dispatch", ThreadID: "thread",
		CreatedAt: supervisionTestTime, UpdatedAt: supervisionTestTime,
	}
	if err := store.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{Assignments: []domain.Assignment{assignment}}); err != nil {
		t.Fatal(err)
	}
	return domain.ActivationDispatchFailure{
		ID: "activation-dispatch-failure-1", Code: "activation_prompt_too_large",
		SafeMessage: "activation prompt exceeds the final model context limit",
		NextAction:  "repair-activation-input", AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch,
		AttemptID: publication.Artifact.AttemptID, ActivationID: publication.ActivationID,
		ActivationEpoch: publication.ActivationEpoch, RunID: publication.RunID,
		GraphRevision: publication.GraphRevision, EvidenceRef: "activation://activation-evidence@1/graph/1",
		FirstSeenAt: supervisionTestTime, LastSeenAt: supervisionTestTime,
	}
}

func TestActivationDispatchFailureRejectsWrongAttemptBinding(t *testing.T) {
	store := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
	failure := seedActivationDispatchFailure(t, store)
	failure.AttemptID = "unrelated-attempt"
	if _, _, err := store.RecordActivationDispatchFailure(context.Background(), 1, failure); !errors.Is(err, ErrActivationDispatchFailureConflict) {
		t.Fatalf("wrong attempt error = %v, want %v", err, ErrActivationDispatchFailureConflict)
	}
}

func TestActivationDispatchFailureFreezesAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	store := openSupervisionStore(t, path)
	failure := seedActivationDispatchFailure(t, store)
	got, created, err := store.RecordActivationDispatchFailure(context.Background(), 1, failure)
	if err != nil || !created || got.ID != failure.ID {
		t.Fatalf("first freeze = %+v created=%v err=%v", got, created, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openSupervisionStore(t, path)
	replayed, created, err := store.RecordActivationDispatchFailure(context.Background(), 1, failure)
	if err != nil || created || replayed.ID != failure.ID {
		t.Fatalf("restart replay = %+v created=%v err=%v", replayed, created, err)
	}
}

func TestActivationDispatchFailureCurrentProjectionSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	store := openSupervisionStore(t, path)
	failure := seedActivationDispatchFailure(t, store)
	if _, _, err := store.RecordActivationDispatchFailure(context.Background(), 1, failure); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openSupervisionStore(t, path)
	got, err := store.ListCurrentActivationDispatchFailures(context.Background(), failure.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].SafeMessage != failure.SafeMessage || got[0].NextAction != failure.NextAction {
		t.Fatalf("current failures after restart = %+v", got)
	}
}

func TestActivationDispatchFailureConcurrentPublicationFreezesOneIdentity(t *testing.T) {
	store := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
	first := seedActivationDispatchFailure(t, store)
	second := first
	second.ID = "activation-dispatch-failure-2"
	second.Code = "activation_evidence_mismatch"

	type result struct {
		created bool
		err     error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for _, failure := range []domain.ActivationDispatchFailure{first, second} {
		wg.Add(1)
		go func(failure domain.ActivationDispatchFailure) {
			defer wg.Done()
			<-start
			_, created, err := store.RecordActivationDispatchFailure(context.Background(), 1, failure)
			results <- result{created, err}
		}(failure)
	}
	close(start)
	wg.Wait()
	close(results)
	var created, conflicts int
	for got := range results {
		if got.created && got.err == nil {
			created++
		} else if errors.Is(got.err, ErrActivationDispatchFailureConflict) {
			conflicts++
		} else {
			t.Fatalf("unexpected result: %+v", got)
		}
	}
	if created != 1 || conflicts != 1 {
		t.Fatalf("created=%d conflicts=%d, want one each", created, conflicts)
	}
}

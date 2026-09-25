package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func failedOfferSupersessionFixture(t *testing.T) (*Store, FailedActivationOfferSupersession, domain.AssignmentClaimRequest) {
	t.Helper()
	ctx := context.Background()
	store := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
	failure := seedActivationDispatchFailure(t, store)

	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records.Assignments) != 1 {
		t.Fatalf("seeded activation assignments = %+v", records.Assignments)
	}
	assignment := records.Assignments[0]
	assignment.WorkerID = "normandy"
	assignment.WorkerEpoch = "worker-epoch-1"
	assignment.Route = domain.ProviderRoute{WorkerID: "normandy", ProviderInstanceID: "claude", Model: "claude"}
	assignment.LeaseToken = "lease-token-1"
	assignment.LeaseExpiresAt = supervisionTestTime.Add(time.Hour)
	var attempt domain.Attempt
	for _, candidate := range records.Attempts {
		if candidate.ID == failure.AttemptID {
			attempt = candidate
			break
		}
	}
	if attempt.ID == "" {
		t.Fatalf("activation attempt %q is absent: %+v", failure.AttemptID, records.Attempts)
	}
	attempt.AssignmentID = assignment.ID
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{
		Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment},
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RecordActivationDispatchFailure(ctx, 1, failure); err != nil {
		t.Fatal(err)
	}
	eventID := "operator-reassessment"
	eventRaw, err := json.Marshal(map[string]any{
		"id": eventID, "runId": failure.RunID, "kind": "operator-reassessment",
		"reason": "repair package", "occurredAt": supervisionTestTime.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AppendSupervisionInbox(ctx, failure.RunID, []SupervisionInboxRow{{
		ID: eventID, RunID: failure.RunID, Record: eventRaw,
	}}); err != nil {
		t.Fatal(err)
	}
	state, err := store.LoadSupervisionActivationRows(ctx, failure.RunID)
	if err != nil {
		t.Fatal(err)
	}
	return store, FailedActivationOfferSupersession{
			CoordinatorEpoch: 1, RunID: failure.RunID,
			ActivationID: failure.ActivationID, ActivationEpoch: failure.ActivationEpoch,
			ExpectedRecordRevision: state.Record.Revision,
			AssignmentID:           failure.AssignmentID, AssignmentEpoch: failure.AssignmentEpoch,
			ReassessmentEventID: eventID, SupersededAt: supervisionTestTime.Add(2 * time.Minute),
		}, domain.AssignmentClaimRequest{
			CoordinatorEpoch: 1, WorkerID: assignment.WorkerID, WorkerEpoch: assignment.WorkerEpoch,
			AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch, LeaseToken: assignment.LeaseToken,
			ClaimedAt: supervisionTestTime.Add(2 * time.Minute), LeaseExpiresAt: supervisionTestTime.Add(time.Hour),
		}
}

func TestFailedActivationOfferClaimReassessmentRaceHasOneWinner(t *testing.T) {
	store, supersede, claim := failedOfferSupersessionFixture(t)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	var supersedeErr, claimErr error
	go func() {
		defer wg.Done()
		<-start
		supersedeErr = store.SupersedeFailedActivationOffer(context.Background(), supersede)
	}()
	go func() {
		defer wg.Done()
		<-start
		_, claimErr = store.ClaimAssignment(context.Background(), claim)
	}()
	close(start)
	wg.Wait()

	switch {
	case supersedeErr == nil:
		if !errors.Is(claimErr, ErrAssignmentClaim) {
			t.Fatalf("release won but claim error = %v", claimErr)
		}
	case claimErr == nil:
		if !errors.Is(supersedeErr, ErrActivationDispatch) {
			t.Fatalf("claim won but supersession error = %v", supersedeErr)
		}
	default:
		t.Fatalf("race had no winner: supersession=%v claim=%v", supersedeErr, claimErr)
	}
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records.Assignments) != 1 {
		t.Fatalf("assignments = %+v", records.Assignments)
	}
	state := records.Assignments[0].State
	if state != domain.AssignmentReleased && state != domain.AssignmentClaimed {
		t.Fatalf("race left unsafe assignment state %q", state)
	}
	attempt := activationAttemptOfAssignment(t, records, records.Assignments[0])
	switch state {
	case domain.AssignmentReleased:
		// The superseded offer never runs, so its attempt ends with the release
		// rather than staying ready beside a released assignment.
		if attempt.Progress != domain.ProgressCancelled || attempt.Control != domain.ControlStopped || attempt.CompletedAt == nil {
			t.Fatalf("superseded offer left its attempt %s/%s", attempt.Progress, attempt.Control)
		}
	case domain.AssignmentClaimed:
		if attempt.Progress != domain.ProgressActive || attempt.Control != domain.ControlPreparing {
			t.Fatalf("claimed offer's attempt is %s/%s", attempt.Progress, attempt.Control)
		}
	}
}

func activationAttemptOfAssignment(t *testing.T, records CoordinatorRecords, assignment domain.Assignment) domain.Attempt {
	t.Helper()
	for _, attempt := range records.Attempts {
		if attempt.ID == assignment.AttemptID {
			return attempt
		}
	}
	t.Fatalf("assignment %q has no attempt", assignment.ID)
	return domain.Attempt{}
}

// A supersession committed before its attempt was ended with it left the
// attempt ready beside a released assignment. Replaying the supersession ends
// it exactly once.
func TestFailedActivationOfferSupersessionEndsItsAttemptOnceAcrossReplays(t *testing.T) {
	ctx := context.Background()
	store, request, _ := failedOfferSupersessionFixture(t)
	if err := store.SupersedeFailedActivationOffer(ctx, request); err != nil {
		t.Fatal(err)
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	attempt := activationAttemptOfAssignment(t, records, records.Assignments[0])
	if records.Assignments[0].State != domain.AssignmentReleased || attempt.Progress != domain.ProgressCancelled ||
		attempt.Control != domain.ControlStopped || attempt.CompletedAt == nil {
		t.Fatalf("supersession left %s with attempt %s/%s", records.Assignments[0].State, attempt.Progress, attempt.Control)
	}
	ended := attempt.Revision

	// Put the attempt back the way the earlier binary left it.
	stale := attempt
	stale.Progress, stale.Control, stale.CompletedAt, stale.Revision = domain.ProgressReady, domain.ControlUnassigned, nil, ended+1
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{stale}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `DELETE FROM coordinator_audit_events WHERE id = ?`,
		"activation-attempt-cancelled:"+attempt.ID); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := store.SupersedeFailedActivationOffer(ctx, request); err != nil {
			t.Fatalf("replayed supersession: %v", err)
		}
	}
	records, err = store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	repaired := activationAttemptOfAssignment(t, records, records.Assignments[0])
	if repaired.Progress != domain.ProgressCancelled || repaired.Control != domain.ControlStopped || repaired.Revision != ended+2 {
		t.Fatalf("replay left attempt %s/%s at revision %d, want cancelled at %d",
			repaired.Progress, repaired.Control, repaired.Revision, ended+2)
	}
	var events int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM coordinator_audit_events WHERE id = ?`,
		"activation-attempt-cancelled:"+attempt.ID).Scan(&events); err != nil || events != 1 {
		t.Fatalf("attempt cancellation audit events = %d, %v; want 1", events, err)
	}
}

func TestFailedActivationOfferSupersessionRefusesUnsafeOrUnqualifiedWork(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Store, *FailedActivationOfferSupersession)
	}{
		{name: "claimed", mutate: func(store *Store, request *FailedActivationOfferSupersession) {
			records, _ := store.LoadCoordinatorRecords(context.Background())
			records.Assignments[0].State = domain.AssignmentClaimed
			records.Attempts[0].Control = domain.ControlPreparing
			records.Attempts[0].Progress = domain.ProgressActive
			if err := store.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{
				Assignments: records.Assignments, Attempts: records.Attempts,
			}); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "unknown", mutate: func(store *Store, request *FailedActivationOfferSupersession) {
			records, _ := store.LoadCoordinatorRecords(context.Background())
			records.Assignments[0].State = domain.AssignmentUnknown
			if err := store.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{
				Assignments: records.Assignments,
			}); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "unsupported event", mutate: func(store *Store, request *FailedActivationOfferSupersession) {
			raw, _ := json.Marshal(map[string]any{
				"id": request.ReassessmentEventID, "runId": request.RunID,
				"kind": "review-timeout", "occurredAt": supervisionTestTime,
			})
			if _, err := store.db.ExecContext(context.Background(),
				"UPDATE coordinator_supervision_inbox SET record = ? WHERE id = ?", raw, request.ReassessmentEventID); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "stale activation epoch", mutate: func(_ *Store, request *FailedActivationOfferSupersession) {
			request.ActivationEpoch++
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, request, _ := failedOfferSupersessionFixture(t)
			test.mutate(store, &request)
			if err := store.SupersedeFailedActivationOffer(context.Background(), request); !errors.Is(err, ErrActivationDispatch) {
				t.Fatalf("supersession error = %v, want %v", err, ErrActivationDispatch)
			}
			records, err := store.LoadCoordinatorRecords(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if records.Assignments[0].State == domain.AssignmentReleased {
				t.Fatalf("unsafe assignment was released: %+v", records.Assignments[0])
			}
		})
	}
}

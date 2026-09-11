package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestUnknownRecoveryIsEvidenceRevisionAndReplayFenced(t *testing.T) {
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	records := coordinatorFixture()
	records.Assignments[0].State = domain.AssignmentUnknown
	records.Assignments[0].DispatchState = domain.DispatchUnknown
	records.Assignments[0].Epoch = 3
	records.Assignments[0].WorkerEpoch = "worker-epoch-1"
	records.Assignments[0].LeaseToken = "lease-token-secret"
	records.Assignments[0].DispatchToken = "dispatch-token-secret"
	records.Attempts[0].Control = domain.ControlRunning
	records.QuotaPools[0].Admission = domain.AdmissionClosed
	if err := store.SaveCoordinatorRecords(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 10, 22, 0, 0, 0, time.UTC)
	recovery := domain.UnknownAssignmentRecovery{
		ID: "recovery-1", AssignmentID: "assignment-1", CoordinatorEpoch: 1,
		ExpectedAssignmentEpoch: 3, ExpectedAttemptRevision: records.Attempts[0].Revision,
		Outcome: domain.UnknownRecoveryStopped, EvidenceID: "worker-observation-20260910",
		EvidenceSHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Actor:          "local:1000", Reason: "operator reviewed stopped worker and T3 evidence", RecoveredAt: now,
	}
	decision, err := store.RecoverUnknownAssignment(context.Background(), recovery)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Replay || decision.Assignment.State != domain.AssignmentReleased ||
		decision.Assignment.DispatchState != domain.DispatchStopped ||
		decision.Attempt.Progress != domain.ProgressReady || decision.Attempt.Control != domain.ControlUnassigned ||
		decision.Attempt.AssignmentID != "" || decision.Event.Actor != recovery.Actor || decision.Event.Reason != recovery.Reason {
		t.Fatalf("decision = %#v", decision)
	}
	replayRequest := recovery
	replayRequest.RecoveredAt = recovery.RecoveredAt.Add(time.Hour)
	replay, err := store.RecoverUnknownAssignment(context.Background(), replayRequest)
	if err != nil || !replay.Replay || !reflect.DeepEqual(replay.Assignment, decision.Assignment) ||
		replay.Event.ID != decision.Event.ID || !replay.Recovery.RecoveredAt.Equal(recovery.RecoveredAt) {
		t.Fatalf("replay = %#v, %v", replay, err)
	}
	auditJSON, err := json.Marshal(decision.Event)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"lease-token-secret", "dispatch-token-secret"} {
		if strings.Contains(string(auditJSON), secret) {
			t.Fatalf("recovery audit exposed secret %q: %s", secret, auditJSON)
		}
	}
	changed := recovery
	changed.Outcome = domain.UnknownRecoveryFailed
	if _, err := store.RecoverUnknownAssignment(context.Background(), changed); !errors.Is(err, ErrInvalidUnknownRecovery) {
		t.Fatalf("changed replay error = %v", err)
	}
	loaded, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.QuotaPools) != 1 || loaded.QuotaPools[0].Admission != domain.AdmissionClosed {
		t.Fatalf("recovery bypassed closed quota: %#v", loaded.QuotaPools)
	}
}

func TestUnknownRecoveryStoppedPreservesTerminalAttempt(t *testing.T) {
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	records := coordinatorFixture()
	records.Assignments[0].State = domain.AssignmentUnknown
	records.Assignments[0].DispatchState = domain.DispatchUnknown
	records.Assignments[0].Epoch = 2
	completed := time.Date(2026, 9, 10, 21, 0, 0, 0, time.UTC)
	records.Attempts[0].Progress = domain.ProgressCancelled
	records.Attempts[0].Control = domain.ControlRunning
	records.Attempts[0].ThreadID = "thread-terminal"
	records.Attempts[0].CompletedAt = &completed
	if err := store.SaveCoordinatorRecords(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	now := completed.Add(time.Hour)
	decision, err := store.RecoverUnknownAssignment(context.Background(), domain.UnknownAssignmentRecovery{
		ID: "recovery-terminal", AssignmentID: "assignment-1", CoordinatorEpoch: 1,
		ExpectedAssignmentEpoch: 2, ExpectedAttemptRevision: records.Attempts[0].Revision,
		Outcome: domain.UnknownRecoveryStopped, EvidenceID: "terminal-thread-evidence",
		EvidenceSHA256: strings.Repeat("c", 64), Actor: "local:1000",
		Reason: "reviewed terminal thread evidence", RecoveredAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Assignment.State != domain.AssignmentReleased ||
		decision.Assignment.DispatchState != domain.DispatchStopped ||
		decision.Attempt.Progress != domain.ProgressCancelled ||
		decision.Attempt.Control != domain.ControlStopped ||
		decision.Attempt.ThreadID != "thread-terminal" ||
		decision.Attempt.CompletedAt == nil || !decision.Attempt.CompletedAt.Equal(completed) {
		t.Fatalf("terminal recovery decision = %+v", decision)
	}
}

func TestUnknownRecoveryRefusesMissingEvidenceAndStaleState(t *testing.T) {
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	records := coordinatorFixture()
	records.Assignments[0].State = domain.AssignmentUnknown
	records.Assignments[0].Epoch = 2
	if err := store.SaveCoordinatorRecords(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	request := domain.UnknownAssignmentRecovery{
		ID: "recovery", AssignmentID: "assignment-1", CoordinatorEpoch: 1,
		ExpectedAssignmentEpoch: 2, ExpectedAttemptRevision: records.Attempts[0].Revision,
		Outcome: domain.UnknownRecoveryFailed, EvidenceID: "evidence",
		EvidenceSHA256: "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
		Actor:          "operator", Reason: "reviewed evidence", RecoveredAt: time.Now().UTC(),
	}
	missing := request
	missing.EvidenceSHA256 = ""
	if _, err := store.RecoverUnknownAssignment(context.Background(), missing); !errors.Is(err, ErrInvalidUnknownRecovery) {
		t.Fatalf("missing evidence error = %v", err)
	}
	stale := request
	stale.ExpectedAttemptRevision++
	if _, err := store.RecoverUnknownAssignment(context.Background(), stale); !errors.Is(err, ErrStaleUnknownRecovery) {
		t.Fatalf("stale revision error = %v", err)
	}
}

func TestUnknownRecoveryCanResolveAssignmentAsTerminalFailure(t *testing.T) {
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	records := coordinatorFixture()
	records.Assignments[0].State = domain.AssignmentUnknown
	records.Assignments[0].DispatchState = domain.DispatchUnknown
	records.Assignments[0].Epoch = 2
	records.QuotaPools[0].Admission = domain.AdmissionClosed
	if err := store.SaveCoordinatorRecords(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 10, 23, 0, 0, 0, time.UTC)
	decision, err := store.RecoverUnknownAssignment(context.Background(), domain.UnknownAssignmentRecovery{
		ID: "recovery-failed", AssignmentID: "assignment-1", CoordinatorEpoch: 1,
		ExpectedAssignmentEpoch: 2, ExpectedAttemptRevision: records.Attempts[0].Revision,
		Outcome: domain.UnknownRecoveryFailed, EvidenceID: "incident-failed",
		EvidenceSHA256: strings.Repeat("b", 64), Actor: "local:1000",
		Reason: "reviewed failure evidence", RecoveredAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Assignment.State != domain.AssignmentReleased ||
		decision.Attempt.Progress != domain.ProgressFailed || decision.Attempt.Control != domain.ControlStopped ||
		decision.Attempt.CompletedAt == nil || !decision.Attempt.CompletedAt.Equal(now) {
		t.Fatalf("failed recovery decision = %+v", decision)
	}
	loaded, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if loaded.QuotaPools[0].Admission != domain.AdmissionClosed {
		t.Fatalf("failed recovery changed closed admission: %+v", loaded.QuotaPools)
	}
}

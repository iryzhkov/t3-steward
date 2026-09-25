package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// S13: an overseer offer to homelab from 2026-09-22 03:04 was never claimed,
// its activation closed at 04:06 when the run settled, and the offer stayed
// "offered" with its attempt ready/unassigned. The retained assignment refused
// every later coordinator reload that changed homelab's catalog.
func TestReleaseDeadActivationOffersReleasesOnlyOffersOfEndedActivations(t *testing.T) {
	ctx := context.Background()
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	epoch, err := store.AcquireCoordinator(ctx, "coordinator")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 22, 3, 4, 42, 0, time.UTC)
	offer := func(id, activation string, activationEpoch int64) (domain.Attempt, domain.Assignment) {
		attempt := domain.Attempt{ID: "attempt-" + id, WorkflowRunID: "run-" + id, TaskID: "task-" + id, Number: 1,
			Progress: domain.ProgressReady, Control: domain.ControlUnassigned, Revision: 1, AssignmentID: "assignment-" + id,
			SupervisionActivationID: activation, SupervisionActivationEpoch: activationEpoch, UpdatedAt: now}
		assignment := domain.Assignment{ID: "assignment-" + id, AttemptID: attempt.ID, WorkerID: "homelab",
			WorkerEpoch: "worker-1", State: domain.AssignmentOffered, Epoch: 1, LeaseToken: "lease-" + id,
			DispatchToken: "dispatch-" + id, CreatedAt: now, UpdatedAt: now}
		return attempt, assignment
	}
	closedAttempt, closedOffer := offer("closed", "activation-closed", 3)
	pendingAttempt, pendingOffer := offer("pending", "activation-pending", 1)
	movedAttempt, movedOffer := offer("moved", "activation-moved", 1)
	taskAttempt, taskOffer := offer("task", "", 0)
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{
		Attempts:    []domain.Attempt{closedAttempt, pendingAttempt, movedAttempt, taskAttempt},
		Assignments: []domain.Assignment{closedOffer, pendingOffer, movedOffer, taskOffer},
	}); err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		id, run, state string
		epoch          int64
	}{
		{"activation-closed", "run-closed", string(domain.ActivationClosed), 3},
		{"activation-pending", "run-pending", string(domain.ActivationPendingDispatch), 1},
		{"activation-moved", "run-moved", string(domain.ActivationPendingDispatch), 2},
	} {
		if _, err := store.db.ExecContext(ctx, `INSERT INTO coordinator_supervision_activations(id, run_id, epoch, state, record)
			VALUES (?, ?, ?, ?, '{}')`, row.id, row.run, row.epoch, row.state); err != nil {
			t.Fatal(err)
		}
	}

	released, err := store.ReleaseDeadActivationOffers(ctx, epoch, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(released) != 2 || released[0] != "assignment-closed" && released[1] != "assignment-closed" {
		t.Fatalf("released = %v, want the closed activation's and the moved activation's offers", released)
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]domain.AssignmentState{
		"assignment-closed": domain.AssignmentReleased, "assignment-moved": domain.AssignmentReleased,
		"assignment-pending": domain.AssignmentOffered, "assignment-task": domain.AssignmentOffered,
	}
	for _, assignment := range records.Assignments {
		if assignment.State != want[assignment.ID] {
			t.Fatalf("%s is %s, want %s", assignment.ID, assignment.State, want[assignment.ID])
		}
	}
	if _, found, err := store.LoadAuditEvent(ctx, "activation-offer-released:assignment-closed"); err != nil || !found {
		t.Fatalf("release left no audit event: found=%v err=%v", found, err)
	}
	// A second boundary finds nothing more to do.
	if again, err := store.ReleaseDeadActivationOffers(ctx, epoch, now.Add(2*time.Hour)); err != nil || len(again) != 0 {
		t.Fatalf("second release = %v, %v", again, err)
	}
}

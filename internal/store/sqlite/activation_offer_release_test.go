package sqlite

import (
	"context"
	"path/filepath"
	"slices"
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
	type offer struct {
		name            string
		activationState domain.ActivationState // "" means no activation row
		activationEpoch int64
		runEpoch        int64 // the run's current activation epoch; 0 means no record
		assignmentState domain.AssignmentState
		activation      bool
		wantReleased    bool
	}
	cases := []offer{
		// The field case: the run settled and closed the activation.
		{name: "closed", activationState: domain.ActivationClosed, activationEpoch: 3, runEpoch: 3, assignmentState: domain.AssignmentOffered, activation: true, wantReleased: true},
		{name: "spent", activationState: domain.ActivationSpent, activationEpoch: 1, runEpoch: 1, assignmentState: domain.AssignmentOffered, activation: true, wantReleased: true},
		{name: "revoked", activationState: domain.ActivationRevoked, activationEpoch: 1, runEpoch: 1, assignmentState: domain.AssignmentOffered, activation: true, wantReleased: true},
		{name: "missing", activationState: "", activationEpoch: 1, runEpoch: 1, assignmentState: domain.AssignmentOffered, activation: true, wantReleased: true},
		// A continuation writes a new row at epoch 2 and leaves the old one
		// saying pending-dispatch at epoch 1; only the run's record says so.
		{name: "superseded", activationState: domain.ActivationPendingDispatch, activationEpoch: 1, runEpoch: 2, assignmentState: domain.AssignmentOffered, activation: true, wantReleased: true},
		{name: "pending", activationState: domain.ActivationPendingDispatch, activationEpoch: 1, runEpoch: 1, assignmentState: domain.AssignmentOffered, activation: true},
		{name: "active", activationState: domain.ActivationActive, activationEpoch: 1, runEpoch: 1, assignmentState: domain.AssignmentOffered, activation: true},
		{name: "escalated", activationState: domain.ActivationEscalated, activationEpoch: 1, runEpoch: 1, assignmentState: domain.AssignmentOffered, activation: true},
		{name: "claimed", activationState: domain.ActivationClosed, activationEpoch: 1, runEpoch: 1, assignmentState: domain.AssignmentClaimed, activation: true},
		{name: "task", assignmentState: domain.AssignmentOffered},
	}
	var records CoordinatorRecords
	for _, c := range cases {
		attempt := domain.Attempt{ID: "attempt-" + c.name, WorkflowRunID: "run-" + c.name, TaskID: "task-" + c.name, Number: 1,
			Progress: domain.ProgressReady, Control: domain.ControlUnassigned, Revision: 1, AssignmentID: "assignment-" + c.name, UpdatedAt: now}
		if c.activation {
			attempt.SupervisionActivationID, attempt.SupervisionActivationEpoch = "activation-"+c.name, c.activationEpoch
		}
		records.Attempts = append(records.Attempts, attempt)
		records.Assignments = append(records.Assignments, domain.Assignment{ID: "assignment-" + c.name, AttemptID: attempt.ID,
			WorkerID: "homelab", WorkerEpoch: "worker-1", State: c.assignmentState, Epoch: 1, LeaseToken: "lease-" + c.name,
			DispatchToken: "dispatch-" + c.name, CreatedAt: now, UpdatedAt: now})
	}
	if err := store.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		if c.activation && c.activationState != "" {
			if _, err := store.db.ExecContext(ctx, `INSERT INTO coordinator_supervision_activations(id, run_id, epoch, state, record)
				VALUES (?, ?, ?, ?, '{}')`, "activation-"+c.name, "run-"+c.name, c.activationEpoch, string(c.activationState)); err != nil {
				t.Fatal(err)
			}
		}
		if c.runEpoch > 0 {
			if _, err := store.db.ExecContext(ctx, `INSERT INTO coordinator_supervision(run_id, revision, activation_epoch, event_cursor, record)
				VALUES (?, 1, ?, 0, '{}')`, "run-"+c.name, c.runEpoch); err != nil {
				t.Fatal(err)
			}
		}
	}

	if _, err := store.ReleaseDeadActivationOffers(ctx, epoch+1, now); err == nil {
		t.Fatal("a sweep under a stale coordinator epoch was accepted")
	}
	released, err := store.ReleaseDeadActivationOffers(ctx, epoch, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	var want []string
	for _, c := range cases {
		if c.wantReleased {
			want = append(want, "assignment-"+c.name)
		}
	}
	slices.Sort(released)
	slices.Sort(want)
	if !slices.Equal(released, want) {
		t.Fatalf("released = %v, want %v", released, want)
	}
	loaded, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, assignment := range loaded.Assignments {
		var c offer
		for _, candidate := range cases {
			if "assignment-"+candidate.name == assignment.ID {
				c = candidate
			}
		}
		wantState := c.assignmentState
		if c.wantReleased {
			wantState = domain.AssignmentReleased
		}
		if assignment.State != wantState {
			t.Fatalf("%s is %s, want %s", assignment.ID, assignment.State, wantState)
		}
	}
	if _, found, err := store.LoadAuditEvent(ctx, "activation-offer-released:assignment-closed:epoch:1"); err != nil || !found {
		t.Fatalf("release left no audit event: found=%v err=%v", found, err)
	}
	// A second boundary finds nothing more to do.
	if again, err := store.ReleaseDeadActivationOffers(ctx, epoch, now.Add(2*time.Hour)); err != nil || len(again) != 0 {
		t.Fatalf("second release = %v, %v", again, err)
	}
}

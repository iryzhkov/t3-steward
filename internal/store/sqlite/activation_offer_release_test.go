package sqlite

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"
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
		// wantCancelled is whether the never-started attempt ends. Every
		// released offer's attempt does, and so does the attempt of an offer
		// an earlier binary released without ending it.
		wantCancelled bool
	}
	cases := []offer{
		// The field case: the run settled and closed the activation.
		{name: "closed", activationState: domain.ActivationClosed, activationEpoch: 3, runEpoch: 3, assignmentState: domain.AssignmentOffered, activation: true, wantReleased: true, wantCancelled: true},
		{name: "spent", activationState: domain.ActivationSpent, activationEpoch: 1, runEpoch: 1, assignmentState: domain.AssignmentOffered, activation: true, wantReleased: true, wantCancelled: true},
		{name: "revoked", activationState: domain.ActivationRevoked, activationEpoch: 1, runEpoch: 1, assignmentState: domain.AssignmentOffered, activation: true, wantReleased: true, wantCancelled: true},
		{name: "missing", activationState: "", activationEpoch: 1, runEpoch: 1, assignmentState: domain.AssignmentOffered, activation: true, wantReleased: true, wantCancelled: true},
		// A continuation writes a new row at epoch 2 and leaves the old one
		// saying pending-dispatch at epoch 1; only the run's record says so.
		{name: "superseded", activationState: domain.ActivationPendingDispatch, activationEpoch: 1, runEpoch: 2, assignmentState: domain.AssignmentOffered, activation: true, wantReleased: true, wantCancelled: true},
		// rc.96 released the offer and left the attempt ready/unassigned, which
		// planning reported on every boundary; the sweep repairs that row.
		{name: "released-closed", activationState: domain.ActivationClosed, activationEpoch: 3, runEpoch: 3, assignmentState: domain.AssignmentReleased, activation: true, wantCancelled: true},
		{name: "released-superseded", activationState: domain.ActivationPendingDispatch, activationEpoch: 1, runEpoch: 2, assignmentState: domain.AssignmentReleased, activation: true, wantCancelled: true},
		// A released offer of a live activation is the undelivered-dispatch
		// retry's evidence, and its attempt is left alone.
		{name: "released-pending", activationState: domain.ActivationPendingDispatch, activationEpoch: 1, runEpoch: 1, assignmentState: domain.AssignmentReleased, activation: true},
		{name: "released-task", assignmentState: domain.AssignmentReleased},
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
	ended := now.Add(time.Hour)
	for _, attempt := range loaded.Attempts {
		var c offer
		for _, candidate := range cases {
			if "attempt-"+candidate.name == attempt.ID {
				c = candidate
			}
		}
		if !c.wantCancelled {
			if attempt.Progress != domain.ProgressReady || attempt.Control != domain.ControlUnassigned ||
				attempt.Revision != 1 || attempt.CompletedAt != nil {
				t.Fatalf("%s changed: %s/%s revision %d", attempt.ID, attempt.Progress, attempt.Control, attempt.Revision)
			}
			continue
		}
		if attempt.Progress != domain.ProgressCancelled || attempt.Control != domain.ControlStopped ||
			attempt.Revision != 2 || !attempt.UpdatedAt.Equal(ended) ||
			attempt.CompletedAt == nil || !attempt.CompletedAt.Equal(ended) ||
			attempt.AssignmentID != "assignment-"+c.name || attempt.Failure != "" {
			t.Fatalf("%s did not end as a cancelled never-started attempt: %+v", attempt.ID, attempt)
		}
		var revision int64
		if err := store.db.QueryRowContext(ctx, `SELECT revision FROM coordinator_attempts WHERE id = ?`, attempt.ID).Scan(&revision); err != nil || revision != 2 {
			t.Fatalf("%s revision column = %d, %v; want 2", attempt.ID, revision, err)
		}
	}
	if _, found, err := store.LoadAuditEvent(ctx, "activation-offer-released:assignment-closed:epoch:1"); err != nil || !found {
		t.Fatalf("release left no audit event: found=%v err=%v", found, err)
	}
	for _, id := range []string{"attempt-closed", "attempt-released-closed"} {
		event, found, err := store.LoadAuditEvent(ctx, "activation-attempt-cancelled:"+id)
		if err != nil || !found || event.Kind != "attempt-cancelled" || event.TargetID != id {
			t.Fatalf("ending %s left no audit event: %+v found=%v err=%v", id, event, found, err)
		}
	}
	auditCount := func() int {
		var count int
		if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM coordinator_audit_events`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}
	before := auditCount()
	// A second boundary finds nothing more to do, and records nothing more.
	if again, err := store.ReleaseDeadActivationOffers(ctx, epoch, now.Add(2*time.Hour)); err != nil || len(again) != 0 {
		t.Fatalf("second release = %v, %v", again, err)
	}
	if after := auditCount(); after != before {
		t.Fatalf("a repeated pass wrote %d more audit events", after-before)
	}
	reloaded, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, attempt := range reloaded.Attempts {
		if attempt.Progress == domain.ProgressCancelled && (attempt.Revision != 2 || !attempt.UpdatedAt.Equal(ended)) {
			t.Fatalf("a repeated pass rewrote %s: %+v", attempt.ID, attempt)
		}
	}
}

// An offered overseer assignment whose attempt row is gone still blocks a
// catalog reload, so the sweep releases it; an offer nothing identifies as an
// overseer's is left for its owner. A released offer whose attempt cleared its
// assignment reference, as releaseNarrowedOffersTx does, still ends the
// attempt, and one whose attempt names another assignment does not.
func TestReleaseDeadActivationOffersHandlesOrphanedOffersAndClearedReferences(t *testing.T) {
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
	for _, orphan := range []domain.Assignment{
		{ID: "assignment-orphan-activation", AttemptID: "activation-attempt-0123456789abcdef0123456789abcdef"},
		{ID: "assignment-orphan-task", AttemptID: "attempt:task-gone:1"},
	} {
		orphan.WorkerID, orphan.WorkerEpoch, orphan.State, orphan.Epoch = "homelab", "worker-1", domain.AssignmentOffered, 2
		orphan.DispatchToken, orphan.LeaseToken, orphan.CreatedAt, orphan.UpdatedAt = "dispatch-"+orphan.ID, "lease-"+orphan.ID, now, now
		raw, err := json.Marshal(orphan)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.db.ExecContext(ctx, `INSERT INTO coordinator_assignments(
			id, attempt_id, dispatch_token, dispatch_revision, dispatch_state,
			worker_id, worker_epoch, assignment_epoch, assignment_state, lease_expires_at, record
		) VALUES (?, ?, ?, 0, '', ?, ?, ?, ?, '', ?)`, orphan.ID, orphan.AttemptID, orphan.DispatchToken,
			orphan.WorkerID, orphan.WorkerEpoch, orphan.Epoch, orphan.State, raw); err != nil {
			t.Fatal(err)
		}
	}
	var records CoordinatorRecords
	for name, reference := range map[string]string{"cleared": "", "elsewhere": "assignment-other"} {
		records.Attempts = append(records.Attempts, domain.Attempt{ID: "attempt-" + name, WorkflowRunID: "run-" + name,
			TaskID: "activation-" + name, Number: 1, Progress: domain.ProgressReady, Control: domain.ControlUnassigned,
			Revision: 1, AssignmentID: reference, UpdatedAt: now,
			SupervisionActivationID: "activation-" + name, SupervisionActivationEpoch: 1})
		records.Assignments = append(records.Assignments, domain.Assignment{ID: "assignment-" + name, AttemptID: "attempt-" + name,
			WorkerID: "homelab", WorkerEpoch: "worker-1", State: domain.AssignmentReleased, Epoch: 1,
			LeaseToken: "lease-" + name, DispatchToken: "dispatch-" + name, CreatedAt: now, UpdatedAt: now})
	}
	if err := store.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}

	var logged bytes.Buffer
	restore := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, nil)))
	t.Cleanup(func() { slog.SetDefault(restore) })

	for pass := range 2 {
		released, err := store.ReleaseDeadActivationOffers(ctx, epoch, now.Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"assignment-orphan-activation"}
		if pass > 0 {
			want = nil
		}
		if !slices.Equal(released, want) {
			t.Fatalf("pass %d released %v, want %v", pass, released, want)
		}
	}
	if !strings.Contains(logged.String(), "count=1") {
		t.Fatalf("the unidentifiable orphan was not reported:\n%s", logged.String())
	}
	event, found, err := store.LoadAuditEvent(ctx, "activation-offer-released:assignment-orphan-activation:epoch:2")
	if err != nil || !found || !strings.HasSuffix(event.Reason, "the activation attempt no longer exists") {
		t.Fatalf("orphan release audit = %+v found=%v err=%v", event, found, err)
	}
	states := map[string]domain.AssignmentState{}
	rows, err := store.db.QueryContext(ctx, `SELECT id, assignment_state FROM coordinator_assignments`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id string
		var state domain.AssignmentState
		if err := rows.Scan(&id, &state); err != nil {
			t.Fatal(err)
		}
		states[id] = state
	}
	rows.Close()
	if states["assignment-orphan-activation"] != domain.AssignmentReleased || states["assignment-orphan-task"] != domain.AssignmentOffered {
		t.Fatalf("orphan states = %v", states)
	}
	loaded, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, attempt := range loaded.Attempts {
		wantCancelled := attempt.ID == "attempt-cleared"
		if (attempt.Progress == domain.ProgressCancelled) != wantCancelled {
			t.Fatalf("%s is %s/%s, want cancelled=%v", attempt.ID, attempt.Progress, attempt.Control, wantCancelled)
		}
	}
}

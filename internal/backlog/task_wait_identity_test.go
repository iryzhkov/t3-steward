package backlog

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// A task registers a wait with the identity its own execution package gave it,
// against the coordinator that issued that package, after the coordinator has
// advanced the attempt exactly as it does in production.
//
// This is the gap every earlier test left open. The unit tests either built a
// package and never registered anything, or registered a wait with a revision
// the test had just read from the store. Neither could observe that the
// coordinator advances the attempt between building the package and the turn
// running, so `wait add --task current` could not park anything on a real
// fleet while every test passed.
func TestTaskRegistersAWaitWithTheIdentityItsPackageGaveIt(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 10, 22, 0, 0, 0, time.UTC)
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	records, assignment := packageBuilderFixture(now)
	if err := store.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveWorkerSnapshot(ctx, taskWaitIdentitySnapshot(assignment, now)); err != nil {
		t.Fatal(err)
	}

	// The package the worker is handed, built by the real builder from the
	// durable records.
	builder := packageBuilder(t, records)
	builder.Store = store
	offer, err := builder.BuildAssignmentOffer(ctx, assignment, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	identity := offer.Package.Package.Identity

	// The coordinator now advances the attempt the way it always does: the
	// worker claims the assignment, and then reports the thread running. The
	// package in the worker's hands is already one revision behind before the
	// turn starts, and two behind by the time the agent can run anything.
	claimed, err := store.ClaimAssignment(ctx, domain.AssignmentClaimRequest{
		CoordinatorEpoch: 1, WorkerID: assignment.WorkerID, WorkerEpoch: assignment.WorkerEpoch,
		AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch, LeaseToken: assignment.LeaseToken,
		ClaimedAt: now.Add(time.Minute), LeaseExpiresAt: now.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	running := taskWaitIdentityAttempt(t, store, identity.AttemptID)
	running.Control = domain.ControlRunning
	running.ThreadID = assignment.ThreadID
	running.Revision++
	running.UpdatedAt = now.Add(2 * time.Minute)
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Attempts: []domain.Attempt{running}, Assignments: []domain.Assignment{claimed},
	}); err != nil {
		t.Fatal(err)
	}
	if running.Revision <= identity.AttemptRevision {
		t.Fatalf("the fixture does not reproduce the production advance: package revision %d, attempt revision %d",
			identity.AttemptRevision, running.Revision)
	}

	// The task now names itself from exactly what it was given: the record the
	// worker writes into the workspace, read back the way the CLI reads it.
	content, err := domain.RenderTaskIdentityFile(identity.TaskEnvironment())
	if err != nil {
		t.Fatal(err)
	}
	values, err := domain.ParseTaskIdentityFile(content)
	if err != nil {
		t.Fatal(err)
	}
	issued, err := strconv.ParseInt(values[domain.TaskWaitEnvAttemptRevision], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	wait, err := store.RegisterTaskWait(ctx, domain.TaskWaitRegistration{
		RequestID:      "req-identity",
		WorkflowRunID:  values[domain.TaskWaitEnvWorkflowRunID],
		TaskID:         values[domain.TaskWaitEnvTaskID],
		AttemptID:      values[domain.TaskWaitEnvAttemptID],
		IssuedRevision: issued,
		ThreadID:       values[domain.TaskWaitEnvThreadID],
		Wake:           domain.WakeEach, MaxDuration: time.Hour,
		Name: "ci", Condition: "gh run view",
	}, now.Add(3*time.Minute))
	if err != nil {
		t.Fatalf("a task could not park itself with the identity it was given: %v", err)
	}
	if !wait.Live() {
		t.Fatalf("the registration did not park the attempt: %+v", wait)
	}
	parked := taskWaitIdentityAttempt(t, store, identity.AttemptID)
	if parked.Progress != domain.ProgressWaitingExternal || parked.Control != domain.ControlWaitingExternal {
		t.Fatalf("attempt is %q/%q after a registration that reported success", parked.Progress, parked.Control)
	}
	if parked.Revision != running.Revision+1 {
		t.Fatalf("the park was not fenced on the live revision: %d, want %d", parked.Revision, running.Revision+1)
	}
	// The identity is kept as evidence of what the task was told, which is the
	// only honest reading of a number that was stale when it was written.
	if wait.IssuedRevision != identity.AttemptRevision {
		t.Fatalf("the wait records issued revision %d, want the package's %d", wait.IssuedRevision, identity.AttemptRevision)
	}
}

func taskWaitIdentityAttempt(t *testing.T, store *sqlite.Store, id string) domain.Attempt {
	t.Helper()
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, attempt := range records.Attempts {
		if attempt.ID == id {
			return attempt
		}
	}
	t.Fatalf("attempt %q is missing", id)
	return domain.Attempt{}
}

func taskWaitIdentitySnapshot(assignment domain.Assignment, now time.Time) domain.WorkerSnapshot {
	return domain.WorkerSnapshot{
		WorkerID: assignment.WorkerID, WorkerEpoch: assignment.WorkerEpoch,
		CoordinatorEpoch: 1, Sequence: 1, Connected: true,
		Inventory: domain.WorkerInventory{
			ID: assignment.WorkerID, AcceptBacklog: true, Health: domain.WorkerHealthReady,
		},
		ObservedAt: now, ValidUntil: now.Add(time.Hour),
	}
}

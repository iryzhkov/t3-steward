package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestGovernedTaskWakeRequiresFreshCoordinatorAdmission(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	a := records.Assignments[0]
	a.WorkerEpoch = "worker-epoch-1"
	a.Project = "project"
	a.Route = domain.ProviderRoute{ProviderInstanceID: "provider", Model: "model", QuotaPoolID: "pool"}
	a.ExecutorDemand = &domain.ResourceDemand{}
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{
		Assignments: []domain.Assignment{a},
		Workflows:   []domain.Workflow{{ID: "w", Project: "project"}},
		QuotaPools:  []domain.QuotaPool{{ID: "pool", Admission: domain.AdmissionOpen, MaxConcurrent: 1}},
	}); err != nil {
		t.Fatal(err)
	}
	auth := taskWakeAuthorizationFixture(t, store, now, "pool")
	if err := store.CommitQuotaAdmissionTransitions(ctx, []domain.QuotaAdmissionTransition{{
		ExpectedRevision: 0,
		Record: domain.QuotaAdmissionRecord{
			QuotaPoolID: "pool", Revision: 1, Admission: domain.AdmissionOpen,
			ObservedAt: now, AppliedAt: now, Reason: "fresh fixture admission",
		},
	}}); err != nil {
		t.Fatal(err)
	}
	w, err := store.RegisterTaskWait(ctx, taskWaitRegistration(attempt, "governed", domain.WakeEach), now)
	if err != nil {
		t.Fatal(err)
	}
	settled := now.Add(time.Second)
	if _, err := store.SettleTaskWait(ctx, w.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, settled); err != nil {
		t.Fatal(err)
	}

	// The worker runner has no current coordinator admission evidence and cannot
	// jump the shared age arbiter.
	if wakes, err := store.WakeTaskWaits(ctx, settled); err != nil || len(wakes) != 0 {
		t.Fatalf("worker wake=%+v err=%v, want governed wake held", wakes, err)
	}
	// Stale evidence also fails closed.
	if wakes, err := store.WakeTaskWaitsBefore(ctx, settled, TaskWakeCutoffs{AdmissionValidAfter: now.Add(time.Millisecond)}); err != nil || len(wakes) != 0 {
		t.Fatalf("stale wake=%+v err=%v, want held", wakes, err)
	}
	// Current admission still respects authoritative pool occupancy.
	owner := domain.Attempt{
		ID: "pool-owner", WorkflowRunID: attempt.WorkflowRunID, TaskID: attempt.TaskID,
		Number: 2, AssignmentID: "pool-owner-assignment", Progress: domain.ProgressActive,
		Control: domain.ControlRunning, Revision: 1, UpdatedAt: now,
	}
	ownerAssignment := domain.Assignment{
		ID: owner.AssignmentID, AttemptID: owner.ID, WorkerID: "other-worker",
		WorkerEpoch: "other-epoch", Epoch: 1, State: domain.AssignmentClaimed,
		Route:          domain.ProviderRoute{ProviderInstanceID: "provider", Model: "model", QuotaPoolID: "pool"},
		ExecutorDemand: &domain.ResourceDemand{}, CreatedAt: now, UpdatedAt: now,
	}
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{
		Attempts: []domain.Attempt{owner}, Assignments: []domain.Assignment{ownerAssignment},
	}); err != nil {
		t.Fatal(err)
	}
	fresh := TaskWakeCutoffs{AdmissionValidAfter: now.Add(-time.Second), AuthorizedWorkers: auth}
	if wakes, err := store.WakeTaskWaitsBefore(ctx, settled, fresh); err != nil || len(wakes) != 0 {
		t.Fatalf("full-pool wake=%+v err=%v, want held", wakes, err)
	}
	owner.Control, owner.Progress = domain.ControlStopped, domain.ProgressSucceeded
	ownerAssignment.State = domain.AssignmentCompleted
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{
		Attempts: []domain.Attempt{owner}, Assignments: []domain.Assignment{ownerAssignment},
	}); err != nil {
		t.Fatal(err)
	}
	revoked := fresh
	revoked.AuthorizedWorkers = map[string]TaskWakeWorkerAuthorization{"worker": fresh.AuthorizedWorkers["worker"]}
	workerAuthorization := revoked.AuthorizedWorkers["worker"]
	workerAuthorization.Providers = nil
	revoked.AuthorizedWorkers["worker"] = workerAuthorization
	if wakes, err := store.WakeTaskWaitsBefore(ctx, settled, revoked); err != nil || len(wakes) != 0 {
		t.Fatalf("revoked-route wake=%+v err=%v, want held", wakes, err)
	}
	snapshots, err := store.LoadWorkerSnapshots(ctx)
	if err != nil || len(snapshots) != 1 {
		t.Fatalf("load snapshot for revocation: count=%d err=%v", len(snapshots), err)
	}
	currentSnapshot := snapshots[0]
	revokedSnapshot := currentSnapshot
	revokedSnapshot.Inventory.Projects = nil
	writeTaskWakeSnapshotFixture(t, store, revokedSnapshot)
	if wakes, err := store.WakeTaskWaitsBefore(ctx, settled, fresh); err != nil || len(wakes) != 0 {
		t.Fatalf("durable-project-revoked wake=%+v err=%v, want held", wakes, err)
	}
	writeTaskWakeSnapshotFixture(t, store, currentSnapshot)
	changedSnapshot := currentSnapshot
	changedSnapshot.Sequence++
	changedSnapshot.Inventory.CatalogRevision = "catalog-3"
	writeTaskWakeSnapshotFixture(t, store, changedSnapshot)
	if wakes, err := store.WakeTaskWaitsBefore(ctx, settled, fresh); err != nil || len(wakes) != 0 {
		t.Fatalf("changed-catalog wake=%+v err=%v, want held", wakes, err)
	}
	writeTaskWakeSnapshotFixture(t, store, currentSnapshot)
	// The coordinator's current admission resumes exactly once after release.
	wakes, err := store.WakeTaskWaitsBefore(ctx, settled, fresh)
	if err != nil || len(wakes) != 1 {
		t.Fatalf("fresh wake=%+v err=%v", wakes, err)
	}
}

func TestNewerSettledWakeYieldsToOlderOrdinaryCutoff(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	w, err := store.RegisterTaskWait(ctx, taskWaitRegistration(attempt, "newer-wake", domain.WakeEach), now)
	if err != nil {
		t.Fatal(err)
	}
	settled := now.Add(2 * time.Second)
	if _, err := store.SettleTaskWait(ctx, w.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, settled); err != nil {
		t.Fatal(err)
	}
	cutoff := TaskWakeCutoff{ReadyAt: now.Add(time.Second), AttemptID: "ordinary"}
	wakes, err := store.WakeTaskWaitsBefore(ctx, settled, TaskWakeCutoffs{Worker: map[string]TaskWakeCutoff{"worker": cutoff}})
	if err != nil {
		t.Fatal(err)
	}
	if len(wakes) != 0 || loadAttempt(t, store, attempt.ID).Control != domain.ControlWaitingExternal {
		t.Fatalf("newer wake bypassed older ordinary contender: %+v", wakes)
	}
}

func TestSettledWakeAgeSurvivesStoreRestart(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	w, err := store.RegisterTaskWait(ctx, taskWaitRegistration(attempt, "restart-age", domain.WakeEach), now)
	if err != nil {
		t.Fatal(err)
	}
	settled := now.Add(2 * time.Second)
	if _, err := store.SettleTaskWait(ctx, w.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, settled); err != nil {
		t.Fatal(err)
	}
	var sequence int
	var name, path string
	if err := store.db.QueryRowContext(ctx, "PRAGMA database_list").Scan(&sequence, &name, &path); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	olderOrdinary := TaskWakeCutoff{ReadyAt: now.Add(time.Second), AttemptID: "ordinary"}
	wakes, err := reopened.WakeTaskWaitsBefore(ctx, settled, TaskWakeCutoffs{Worker: map[string]TaskWakeCutoff{"worker": olderOrdinary}})
	if err != nil {
		t.Fatal(err)
	}
	if len(wakes) != 0 || loadAttempt(t, reopened, attempt.ID).Control != domain.ControlWaitingExternal {
		t.Fatalf("restart lost durable settlement age: wakes=%+v", wakes)
	}
}

func TestHasReadyTaskWaitsIgnoresPartialWakeAll(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	first, err := store.RegisterTaskWait(ctx, taskWaitRegistration(attempt, "all-first", domain.WakeAll), now)
	if err != nil {
		t.Fatal(err)
	}
	attempt = loadAttempt(t, store, attempt.ID)
	if _, err := store.RegisterTaskWait(ctx, taskWaitRegistration(attempt, "all-second", domain.WakeAll), now); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SettleTaskWait(ctx, first.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if ready, err := store.HasReadyTaskWaits(ctx); err != nil || ready {
		t.Fatalf("partial WakeAll ready=%v err=%v, want false", ready, err)
	}
}

func TestChecksDisabledGovernedWakeNeedsCoordinatorArbitrationButNoAdmissionRecord(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assignment := records.Assignments[0]
	assignment.WorkerEpoch = "worker-epoch-1"
	// Empty project models a pre-upgrade assignment; the immutable workflow
	// identity supplies its frozen project during reauthorization.
	assignment.Project = ""
	assignment.Route = domain.ProviderRoute{ProviderInstanceID: "provider", Model: "model", QuotaPoolID: "pool"}
	assignment.ExecutorDemand = &domain.ResourceDemand{}
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{
		Assignments: []domain.Assignment{assignment},
		Workflows:   []domain.Workflow{{ID: "w", Project: "project"}},
		QuotaPools:  []domain.QuotaPool{{ID: "pool", ChecksDisabled: true, MaxConcurrent: 1}},
	}); err != nil {
		t.Fatal(err)
	}
	auth := taskWakeAuthorizationFixture(t, store, now, "pool")
	w, err := store.RegisterTaskWait(ctx, taskWaitRegistration(attempt, "checks-disabled", domain.WakeEach), now)
	if err != nil {
		t.Fatal(err)
	}
	settled := now.Add(time.Second)
	if _, err := store.SettleTaskWait(ctx, w.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, settled); err != nil {
		t.Fatal(err)
	}
	if wakes, err := store.WakeTaskWaits(ctx, settled); err != nil || len(wakes) != 0 {
		t.Fatalf("worker wake=%+v err=%v, want governed wake held for coordinator arbitration", wakes, err)
	}
	wakes, err := store.WakeTaskWaitsBefore(ctx, settled, TaskWakeCutoffs{AdmissionValidAfter: now, AuthorizedWorkers: auth})
	if err != nil || len(wakes) != 1 {
		t.Fatalf("coordinator wake=%+v err=%v, want checks-disabled wake without admission record", wakes, err)
	}
}

func taskWakeAuthorizationFixture(t *testing.T, store *Store, now time.Time, poolID string) map[string]TaskWakeWorkerAuthorization {
	t.Helper()
	snapshots, err := store.LoadWorkerSnapshots(context.Background())
	if err != nil || len(snapshots) != 1 {
		t.Fatalf("load worker snapshot: count=%d err=%v", len(snapshots), err)
	}
	snapshot := snapshots[0]
	snapshot.Sequence++
	snapshot.ValidUntil = now.Add(time.Hour)
	snapshot.Inventory.CatalogRevision = "catalog-2"
	snapshot.Inventory.Providers = []domain.WorkerProviderInventory{{InstanceID: "provider", Models: []string{"model"}, QuotaPoolID: poolID, Available: true}}
	snapshot.Inventory.Projects = []domain.WorkerProjectInventory{{Name: "project", Available: true}}
	writeTaskWakeSnapshotFixture(t, store, snapshot)
	return map[string]TaskWakeWorkerAuthorization{"worker": {
		WorkerEpoch: snapshot.WorkerEpoch, SnapshotSequence: snapshot.Sequence,
		CatalogRevision: snapshot.Inventory.CatalogRevision, ValidUntil: snapshot.ValidUntil,
		Providers: snapshot.Inventory.Providers,
		Projects:  snapshot.Inventory.Projects,
	}}
}

func writeTaskWakeSnapshotFixture(t *testing.T, store *Store, snapshot domain.WorkerSnapshot) {
	t.Helper()
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(context.Background(), "UPDATE coordinator_worker_snapshots SET record=? WHERE worker_id=?", raw, snapshot.WorkerID); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkHasReadyTaskWaitsEmpty(b *testing.B) {
	store, err := OpenMigrated(b.TempDir() + "/state.db")
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if ready, err := store.HasReadyTaskWaits(ctx); err != nil || ready {
			b.Fatalf("ready=%v err=%v", ready, err)
		}
	}
}

func BenchmarkHasReadyTaskWaitsThousandHistorical(b *testing.B) {
	store, err := OpenMigrated(b.TempDir() + "/state.db")
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	now := time.Now().UTC()
	tx, err := store.db.Begin()
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 1000; index++ {
		wait := domain.TaskWait{ID: fmt.Sprintf("historical-%04d", index), RequestID: fmt.Sprintf("request-%04d", index), AttemptID: "old", ThreadID: "old", SettledAt: &now, WokenAt: &now}
		raw, err := json.Marshal(wait)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := tx.Exec(`INSERT INTO coordinator_task_waits(id,request_id,attempt_id,thread_id,record) VALUES(?,?,?,?,?)`, wait.ID, wait.RequestID, wait.AttemptID, wait.ThreadID, raw); err != nil {
			b.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if ready, err := store.HasReadyTaskWaits(ctx); err != nil || ready {
			b.Fatalf("ready=%v err=%v", ready, err)
		}
	}
}

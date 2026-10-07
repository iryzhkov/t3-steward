package sqlite

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestQuotaTelemetrySourceAuditTailAfterSequence(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	source, err := OpenQueryOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()

	if maximum, err := source.MaxAuditSequence(ctx); err != nil || maximum != 0 {
		t.Fatalf("empty audit maximum = %d, %v; want 0", maximum, err)
	}
	now := time.Date(2026, 10, 7, 10, 0, 0, 0, time.UTC)
	kinds := []string{"worker-snapshot-observed", "assignment-offered", "worker-snapshot-observed", "assignment-claimed", "turn-outcome-done"}
	var events []domain.AuditEvent
	for index, kind := range kinds {
		events = append(events, domain.AuditEvent{
			ID: fmt.Sprintf("event-%d", index), Kind: kind, WorkflowRunID: "run-1", TaskID: "task-1", AttemptID: "attempt-1",
			TargetType: domain.AdminTargetAssignment, TargetID: "assignment-1",
			Detail:    []byte(fmt.Sprintf(`{"assignmentEpoch":%d}`, index+1)),
			CreatedAt: now.Add(time.Duration(index) * time.Second),
		})
	}
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{AuditEvents: events}); err != nil {
		t.Fatal(err)
	}

	maximum, err := source.MaxAuditSequence(ctx)
	if err != nil || maximum != 5 {
		t.Fatalf("audit maximum = %d, %v; want 5", maximum, err)
	}
	first, err := source.AuditEventsAfter(ctx, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 || first[0].Sequence != 1 || first[1].Sequence != 2 || first[0].Kind != kinds[0] || first[1].Kind != kinds[1] {
		t.Fatalf("first page = %+v; want sequences 1 and 2 in order, every kind included", first)
	}
	if first[1].TargetID != "assignment-1" || first[1].AttemptID != "attempt-1" || string(first[1].Detail) != `{"assignmentEpoch":2}` ||
		!first[1].CreatedAt.Equal(now.Add(time.Second)) {
		t.Fatalf("audit row fields = %+v", first[1])
	}
	// The caller advances past ignored kinds by continuing after the last
	// sequence it received, whatever that row was.
	rest, err := source.AuditEventsAfter(ctx, first[1].Sequence, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 3 || rest[0].Sequence != 3 || rest[2].Sequence != 5 {
		t.Fatalf("rest = %+v; want sequences 3..5", rest)
	}
	if none, err := source.AuditEventsAfter(ctx, 5, 10); err != nil || len(none) != 0 {
		t.Fatalf("after maximum = %+v, %v; want none", none, err)
	}
	if _, err := source.AuditEventsAfter(ctx, 0, 0); err == nil {
		t.Fatal("a zero limit was accepted")
	}
}

func TestQuotaTelemetrySourceIsQueryOnly(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 10, 7, 10, 0, 0, 0, time.UTC)
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{AuditEvents: []domain.AuditEvent{{
		ID: "event-0", Kind: "assignment-offered", TargetType: domain.AdminTargetAssignment, TargetID: "a", CreatedAt: now,
	}}}); err != nil {
		t.Fatal(err)
	}
	source, err := OpenQueryOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()

	if _, err := source.db.ExecContext(ctx, `INSERT INTO kv(key, value) VALUES ('probe', 'x')`); err == nil {
		t.Fatal("a write through the query-only connection succeeded")
	}
	if _, err := source.db.ExecContext(ctx, `DELETE FROM coordinator_audit_events`); err == nil {
		t.Fatal("a delete through the query-only connection succeeded")
	}

	// The coordinator's writer holds an open write transaction; in WAL mode a
	// reader neither waits for it nor blocks it.
	writer, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Rollback()
	if _, err := writer.ExecContext(ctx, `INSERT INTO kv(key, value) VALUES ('held', 'x')`); err != nil {
		t.Fatal(err)
	}
	readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	started := time.Now()
	maximum, err := source.MaxAuditSequence(readCtx)
	if err != nil || maximum != 1 {
		t.Fatalf("read during an open write transaction = %d, %v; want 1", maximum, err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("read waited %s for the writer", elapsed)
	}
	if _, err := writer.ExecContext(ctx, `INSERT INTO kv(key, value) VALUES ('held-2', 'x')`); err != nil {
		t.Fatalf("writer blocked after the reader: %v", err)
	}
	if err := writer.Commit(); err != nil {
		t.Fatalf("writer commit after the reader: %v", err)
	}

	if _, err := OpenQueryOnly(filepath.Join(t.TempDir(), "missing.db")); err == nil {
		t.Fatal("a missing database was opened")
	}
	if _, err := OpenQueryOnly(":memory:"); err == nil {
		t.Fatal("an in-memory database was accepted")
	}
}

func TestQuotaTelemetrySourcePointLoads(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 10, 7, 10, 0, 0, 0, time.UTC)
	task := domain.Task{ID: "task-1", WorkflowID: "workflow-1", Name: "implement", Class: domain.TaskClassRequired,
		PromptArtifactID: "prompt-1", Verification: []string{"go test ./..."}}
	attempt := domain.Attempt{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-1", Number: 2,
		Progress: domain.ProgressActive, Control: domain.ControlRunning, Revision: 1, UpdatedAt: now}
	assignment := domain.Assignment{ID: "assignment-1", AttemptID: "attempt-1", WorkerID: "worker-1",
		Route: domain.ProviderRoute{ProviderInstanceID: "claudeAgent", Model: "claude-opus-5-5", Options: map[string]string{"effort": "medium"}, QuotaPoolID: "claude-main"},
		State: domain.AssignmentClaimed, Epoch: 1, LeaseToken: "lease", DispatchToken: "dispatch", CreatedAt: now, UpdatedAt: now}
	artifact := func(id, attemptID string, kind domain.ArtifactKind, name string) domain.Artifact {
		return domain.Artifact{ID: id, WorkflowRunID: "run-1", TaskID: "task-1", AttemptID: attemptID, Kind: kind, Name: name,
			MediaType: "application/json", Size: 2, SHA256: fmt.Sprintf("%064d", len(id)), StoragePath: "objects/00/x", Producer: "worker", CreatedAt: now}
	}
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{
		Workflows:   []domain.Workflow{{ID: "workflow-1"}},
		Tasks:       []domain.Task{task},
		Attempts:    []domain.Attempt{attempt},
		Assignments: []domain.Assignment{assignment},
		Artifacts: []domain.Artifact{
			artifact("verify-1", "attempt-1", domain.ArtifactVerification, "verification/001.json"),
			artifact("gate-1", "attempt-1", domain.ArtifactGate, "gate"),
			artifact("output-1", "attempt-1", domain.ArtifactOutput, "unit.bundle"),
			artifact("verify-other", "attempt-0", domain.ArtifactVerification, "verification/001.json"),
		},
	}); err != nil {
		t.Fatal(err)
	}
	source, err := OpenQueryOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()

	gotAssignment, found, err := source.LoadAssignment(ctx, "assignment-1")
	if err != nil || !found || gotAssignment.Route.QuotaPoolID != "claude-main" || gotAssignment.Route.Options["effort"] != "medium" {
		t.Fatalf("assignment = %+v found=%v err=%v", gotAssignment, found, err)
	}
	if _, found, err := source.LoadAssignment(ctx, "missing"); err != nil || found {
		t.Fatalf("missing assignment found=%v err=%v", found, err)
	}
	gotAttempt, found, err := source.LoadAttempt(ctx, "attempt-1")
	if err != nil || !found || gotAttempt.Number != 2 || gotAttempt.Progress != domain.ProgressActive {
		t.Fatalf("attempt = %+v found=%v err=%v", gotAttempt, found, err)
	}
	if _, found, err := source.LoadAttempt(ctx, "missing"); err != nil || found {
		t.Fatalf("missing attempt found=%v err=%v", found, err)
	}
	gotTask, found, err := source.LoadTask(ctx, "task-1")
	if err != nil || !found || gotTask.Name != "implement" || len(gotTask.Verification) != 1 {
		t.Fatalf("task = %+v found=%v err=%v", gotTask, found, err)
	}
	if _, found, err := source.LoadTask(ctx, "missing"); err != nil || found {
		t.Fatalf("missing task found=%v err=%v", found, err)
	}
	checks, err := source.ListCheckArtifacts(ctx, "task-1", "attempt-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(checks) != 2 || checks[0].ID != "gate-1" || checks[1].ID != "verify-1" {
		t.Fatalf("check artifacts = %+v; want gate-1 and verify-1 only, by id", checks)
	}
	loaded, err := source.LoadArtifacts(ctx, []string{"verify-1"})
	if err != nil || len(loaded) != 1 || loaded[0].Name != "verification/001.json" {
		t.Fatalf("artifact by id = %+v, %v", loaded, err)
	}
	if _, err := source.LoadArtifacts(ctx, []string{"missing"}); err == nil {
		t.Fatal("a missing artifact loaded")
	}

	if err := store.SaveBucket(ctx, domain.BucketState{Key: domain.BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: "five_hour"},
		Phase: domain.PhaseNormal, UsedPercent: 12, ObservedAt: now}); err != nil {
		t.Fatal(err)
	}
	buckets, err := source.ListBuckets(ctx)
	if err != nil || len(buckets) != 1 || buckets[0].UsedPercent != 12 {
		t.Fatalf("buckets = %+v, %v", buckets, err)
	}
	snapshots, err := source.LoadWorkerSnapshots(ctx)
	if err != nil || len(snapshots) != 0 {
		t.Fatalf("worker snapshots = %+v, %v; want none", snapshots, err)
	}
}

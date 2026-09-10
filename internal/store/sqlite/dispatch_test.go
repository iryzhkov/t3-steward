package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestAssignmentDispatchPreparationAndOptimisticTransitions(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	input := assignmentDispatchFixture()

	prepared, err := store.PrepareAssignmentDispatch(context.Background(), input)
	if err != nil {
		t.Fatalf("prepare assignment dispatch: %v", err)
	}
	if prepared.DispatchState != domain.DispatchPrepared || prepared.DispatchRevision != 1 {
		t.Fatalf("prepared assignment = %#v", prepared)
	}
	replayed, err := store.PrepareAssignmentDispatch(context.Background(), input)
	if err != nil {
		t.Fatalf("replay assignment preparation: %v", err)
	}
	if !reflect.DeepEqual(replayed, prepared) {
		t.Fatalf("preparation replay = %#v, want %#v", replayed, prepared)
	}
	conflicting := input
	conflicting.ThreadID = "another-thread"
	if _, err := store.PrepareAssignmentDispatch(context.Background(), conflicting); err == nil {
		t.Fatal("conflicting dispatch identity was accepted")
	}

	creating := prepared
	creating.DispatchState = domain.DispatchCreating
	creating.DispatchRevision++
	creating.UpdatedAt = creating.UpdatedAt.Add(time.Minute)
	transition := domain.AssignmentDispatchTransition{
		ExpectedRevision: prepared.DispatchRevision,
		Assignment:       creating,
	}
	if err := store.CommitAssignmentDispatch(context.Background(), transition); err != nil {
		t.Fatalf("commit creating transition: %v", err)
	}
	if err := store.CommitAssignmentDispatch(context.Background(), transition); err != nil {
		t.Fatalf("replay creating transition: %v", err)
	}
	stale := creating
	stale.State = domain.AssignmentUnknown
	stale.DispatchState = domain.DispatchUnknown
	stale.DispatchRevision = 2
	if err := store.CommitAssignmentDispatch(context.Background(), domain.AssignmentDispatchTransition{
		ExpectedRevision: 1,
		Assignment:       stale,
	}); !errors.Is(err, ErrStaleAssignmentDispatchRevision) {
		t.Fatalf("stale transition error = %v", err)
	}
}

func TestMigrationFromVersionSixRestoresDispatchProjection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`DROP INDEX coordinator_assignments_worker_state`,
		`ALTER TABLE coordinator_assignments DROP COLUMN assignment_state`,
		`ALTER TABLE coordinator_assignments DROP COLUMN assignment_epoch`,
		`ALTER TABLE coordinator_assignments DROP COLUMN worker_epoch`,
		`ALTER TABLE coordinator_assignments DROP COLUMN worker_id`,
		`DROP TABLE coordinator_worker_snapshots`,
		`DROP TABLE coordinator_runtime`,
		`DROP INDEX coordinator_assignments_dispatch`,
		`ALTER TABLE coordinator_assignments DROP COLUMN dispatch_state`,
		`ALTER TABLE coordinator_assignments DROP COLUMN dispatch_revision`,
		`DELETE FROM schema_version WHERE version >= 7`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("restore version 6 schema: %v", err)
		}
	}
	legacy := assignmentDispatchFixture()
	raw, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO coordinator_assignments(id, attempt_id, dispatch_token, record)
		 VALUES (?, ?, ?, ?)`,
		legacy.ID, legacy.AttemptID, legacy.DispatchToken, raw,
	); err != nil {
		t.Fatalf("insert version 6 assignment: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(path)
	if err != nil {
		t.Fatalf("migrate version 6 database: %v", err)
	}
	defer store.Close()
	got, err := store.LoadAssignmentDispatch(context.Background(), legacy.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.DispatchState != domain.DispatchConfirmed || got.DispatchRevision != 1 || got.DispatchConfirmedAt == nil {
		t.Fatalf("migrated dispatch projection = %#v", got)
	}
}

func assignmentDispatchFixture() domain.Assignment {
	now := time.Date(2026, time.September, 10, 18, 0, 0, 0, time.UTC)
	return domain.Assignment{
		ID: "assignment-1", AttemptID: "attempt-1", WorkerID: "normandy",
		Route: domain.ProviderRoute{
			WorkerID: "normandy", ProviderInstanceID: "codex",
			Model: "gpt-5.6-sol", QuotaPoolID: "openai-primary",
		},
		State: domain.AssignmentClaimed, Epoch: 4, LeaseToken: "lease-1",
		LeaseExpiresAt: now.Add(time.Hour),
		DispatchToken:  "dispatch-1",
		ThreadID:       "thread-1",
		CreatedAt:      now,
		UpdatedAt:      now,
	}
}

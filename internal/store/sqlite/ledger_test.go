package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestLedgerStateRoundTripAndUpsert(t *testing.T) {
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()

	if states, err := store.LoadLedgerStates(ctx); err != nil || len(states) != 0 {
		t.Fatalf("empty store = %v, %v", states, err)
	}
	next := time.Date(2026, 10, 6, 13, 0, 0, 0, time.UTC)
	state := domain.LedgerState{
		RunID: "run-1", Project: "steward", Path: "steward/handoffs/run-1.md",
		Revision: 3, Applied: []string{"open", "attempt:attempt-1"}, LastBoundary: "attempt:attempt-1",
		Behind: true, Failures: 2, NextAttemptAt: &next, LastError: "jocasta get: unavailable",
		UpdatedAt: next.Add(-time.Minute),
	}
	if err := store.SaveLedgerState(ctx, state); err != nil {
		t.Fatal(err)
	}
	state.Revision = 4
	state.Applied = append(state.Applied, "close")
	state.LastBoundary, state.Closed, state.Behind, state.Failures, state.NextAttemptAt, state.LastError = "close", true, false, 0, nil, ""
	if err := store.SaveLedgerState(ctx, state); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveLedgerState(ctx, domain.LedgerState{RunID: "run-0", Project: "steward", Path: "steward/handoffs/run-0.md"}); err != nil {
		t.Fatal(err)
	}
	states, err := store.LoadLedgerStates(ctx)
	if err != nil || len(states) != 2 || states[0].RunID != "run-0" || states[1].RunID != "run-1" {
		t.Fatalf("states = %#v, %v", states, err)
	}
	got := states[1]
	if got.Revision != 4 || !got.Closed || got.Behind || len(got.Applied) != 3 || got.LastBoundary != "close" || got.NextAttemptAt != nil {
		t.Fatalf("upserted state = %#v", got)
	}

	if err := store.SaveLedgerState(ctx, domain.LedgerState{}); err == nil {
		t.Fatal("a state without a run was accepted")
	}
}

func TestLedgerStateTableIsSchemaVersion37(t *testing.T) {
	if currentSchemaVersion < 37 {
		t.Fatalf("current schema version %d does not include the ledger migration", currentSchemaVersion)
	}
	found := false
	for _, migration := range versionedMigrations {
		if migration.version == 37 {
			found = migration.ddl == coordinatorMigrationV37
		}
	}
	if !found {
		t.Fatal("migration 37 is not the ledger migration")
	}
}

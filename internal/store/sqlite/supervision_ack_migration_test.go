package sqlite

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
)

func TestMigrationV22ProjectsLegacyConsumedReviewerEvents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	old, err := open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := old.migrateThrough(21); err != nil {
		t.Fatal(err)
	}
	seedSupervisedRun(t, old, nil)
	if _, err := old.AppendSupervisionInbox(context.Background(), "run-1", []SupervisionInboxRow{{
		ID: "legacy-review", Record: json.RawMessage(`{"id":"legacy-review"}`),
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := old.db.ExecContext(context.Background(),
		"UPDATE coordinator_supervision_inbox SET consumed = 1 WHERE id = ?", "legacy-review"); err != nil {
		t.Fatal(err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}

	store, err := OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	rows, err := store.ListSupervisionInbox(context.Background(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	assertRoleAck(t, rows, "legacy-review", "", true)
}

package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// Retirement leaves historical watchdog custody useful to archiving and quota
// history. It must not require a Markdown directory or discard old records.
func TestRetainedLegacyRecordsStillProtectArchiveAndForecast(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := openMigratedFixture(path)
	if err != nil {
		t.Fatal(err)
	}
	raw := `{"id":"old","status":"needs-input","threadId":"historical-thread"}`
	if _, err := store.db.ExecContext(ctx, "INSERT INTO backlog_tasks(id,state,status,updated_at) VALUES(?,?,?,?)", "old", raw, "needs-input", time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, "INSERT INTO dispatched_threads(thread_id,task_id,project,dispatched_at) VALUES(?,?,?,?)", "historical-thread", "old", "project", time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = openMigratedFixture(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	states, err := store.SessionArchiveStates(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := states["historical-thread"]; !got.Background || got.Busy == "" {
		t.Fatalf("archive lost retained custody: %+v", got)
	}
	dispatched, err := store.DispatchedThreads(ctx)
	if err != nil || dispatched["historical-thread"] != "old" {
		t.Fatalf("forecast lost background identity: %v %v", dispatched, err)
	}
	saved, err := store.LoadTaskStates(ctx)
	if err != nil || string(saved["old"]) != raw {
		t.Fatalf("historical bytes changed: %s %v", saved["old"], err)
	}
}

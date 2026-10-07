package sqlite

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// runCollectionFixture is a coordinator with three runs and the node waits
// that make threads own them: a succeeded run and an active run owned by
// thread-a, and a failed run owned by both thread-a and thread-b.
func runCollectionFixture(t *testing.T) (*Store, time.Time) {
	t.Helper()
	ctx := context.Background()
	store := openSchemaFixture(t)
	now := time.Date(2026, 10, 7, 15, 0, 0, 0, time.UTC)
	finished := now.Add(-time.Hour)
	runs := []domain.WorkflowRun{
		{ID: "run-done", WorkflowID: "w", Progress: domain.ProgressSucceeded, Revision: 4, CreatedAt: now.Add(-2 * time.Hour), UpdatedAt: finished, CompletedAt: &finished},
		{ID: "run-failed", WorkflowID: "w", Progress: domain.ProgressFailed, Revision: 5, CreatedAt: now.Add(-2 * time.Hour), UpdatedAt: finished, CompletedAt: &finished},
		{ID: "run-active", WorkflowID: "w", Progress: domain.ProgressActive, Revision: 2, CreatedAt: now.Add(-2 * time.Hour), UpdatedAt: now},
		{ID: "run-unowned", WorkflowID: "w", Progress: domain.ProgressSucceeded, Revision: 2, CreatedAt: now.Add(-2 * time.Hour), UpdatedAt: finished, CompletedAt: &finished},
	}
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{WorkflowRuns: runs}); err != nil {
		t.Fatal(err)
	}
	for _, w := range []struct{ id, thread, run string }{
		{"nw-campaign-done", "thread-a", "run-done"},
		{"nw-campaign-failed-a", "thread-a", "run-failed"},
		{"nw-campaign-failed-b", "thread-b", "run-failed"},
		{"nw-campaign-active", "thread-a", "run-active"},
	} {
		saveTestNodeWait(t, store, domain.NodeWait{
			Request:   domain.NodeWaitRequest{ID: w.id, ThreadID: w.thread, Target: domain.NodeRef{RunID: w.run, TaskID: domain.SinkTaskName}, Timeout: time.Hour},
			CreatedAt: now.Add(-2 * time.Hour), Delivery: "pending",
		})
	}
	return store, now
}

func saveTestNodeWait(t *testing.T, store *Store, w domain.NodeWait) {
	t.Helper()
	ctx := context.Background()
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := saveNodeWaitTx(ctx, tx, w); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// kvSnapshot is every row of the kv table, so a refusal can be shown to have
// written nothing at all rather than nothing under one key.
func kvSnapshot(t *testing.T, store *Store) map[string]string {
	t.Helper()
	rows, err := store.db.QueryContext(context.Background(), `SELECT key, value FROM kv`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			t.Fatal(err)
		}
		out[key] = value
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestRunCollectionRecordsFinishedOwnedRun(t *testing.T) {
	ctx := context.Background()
	store, now := runCollectionFixture(t)
	first, changed, err := store.CollectRun(ctx, "thread-a", "run-done", "operator", now)
	if err != nil {
		t.Fatal(err)
	}
	finished := now.Add(-time.Hour)
	want := domain.RunCollection{RunID: "run-done", ThreadID: "thread-a", Progress: domain.ProgressSucceeded, CompletedAt: &finished, CollectedAt: now, Actor: "operator"}
	if !changed || !reflect.DeepEqual(first, want) {
		t.Fatalf("first collection = %+v changed=%v, want %+v changed=true", first, changed, want)
	}

	// The same run as it finished, recorded again: idempotent, and the first
	// collectedAt stands.
	again, changed, err := store.CollectRun(ctx, "thread-a", "run-done", "someone-else", now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if changed || !reflect.DeepEqual(again, want) {
		t.Fatalf("replay = %+v changed=%v, want the first record unchanged", again, changed)
	}

	// The run finishes again (a rerun amended it): a new collection replaces
	// the old one.
	refinished := now.Add(30 * time.Minute)
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var run domain.WorkflowRun
	for _, candidate := range records.WorkflowRuns {
		if candidate.ID == "run-done" {
			run = candidate
		}
	}
	run.Progress, run.CompletedAt, run.Revision = domain.ProgressFailed, &refinished, run.Revision+1
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}}); err != nil {
		t.Fatal(err)
	}
	replaced, changed, err := store.CollectRun(ctx, "thread-a", "run-done", "operator", now.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if !changed || replaced.Progress != domain.ProgressFailed || replaced.CompletedAt == nil || !replaced.CompletedAt.Equal(refinished) || !replaced.CollectedAt.Equal(now.Add(2*time.Hour)) {
		t.Fatalf("recollection after a new finish = %+v changed=%v", replaced, changed)
	}
	listed, err := store.ListRunCollections(ctx, "thread-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || !reflect.DeepEqual(listed[0], replaced) {
		t.Fatalf("listed = %+v, want only %+v", listed, replaced)
	}
}

func TestRunCollectionRefusals(t *testing.T) {
	ctx := context.Background()
	store, now := runCollectionFixture(t)
	before := kvSnapshot(t, store)
	for _, tc := range []struct {
		name, thread, run, want string
		refused                 bool
	}{
		{"unknown run", "thread-a", "run-missing", "unknown run run-missing", true},
		{"active run", "thread-a", "run-active", "run run-active is active and cannot be collected", true},
		{"no node wait for the thread", "thread-b", "run-done", "run run-done has no notification for thread thread-b; nothing was collected", true},
		{"unowned run", "thread-a", "run-unowned", "has no notification for thread thread-a", true},
		{"run ID with a slash", "thread-a", "run-done/sink", `contains "/"`, false},
		{"thread ID with a slash", "thread/a", "run-done", `contains "/"`, false},
		{"empty run ID", "thread-a", "", "the run ID is empty", false},
		{"empty thread ID", "", "run-done", "the thread ID is empty", false},
		{"oversized run ID", "thread-a", strings.Repeat("r", domain.MaxRunCollectionIDBytes+1), "longer than 256 bytes", false},
	} {
		_, changed, err := store.CollectRun(ctx, tc.thread, tc.run, "operator", now)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: err = %v, want %q", tc.name, err, tc.want)
		}
		if changed {
			t.Fatalf("%s: a refusal reported a change", tc.name)
		}
		if got := errors.Is(err, domain.ErrRunCollectionRefused); got != tc.refused {
			t.Fatalf("%s: errors.Is(ErrRunCollectionRefused) = %v, want %v", tc.name, got, tc.refused)
		}
		if after := kvSnapshot(t, store); !reflect.DeepEqual(after, before) {
			t.Fatalf("%s: kv changed:\nbefore %v\nafter  %v", tc.name, before, after)
		}
	}
	if _, err := store.ListRunCollections(ctx, "thread/a"); err == nil {
		t.Fatal("a thread ID with a slash was listed")
	}
}

func TestRunCollectionListIsPerThread(t *testing.T) {
	ctx := context.Background()
	store, now := runCollectionFixture(t)
	for _, c := range []struct{ thread, run string }{
		{"thread-a", "run-done"},
		{"thread-a", "run-failed"},
		{"thread-b", "run-failed"},
	} {
		if _, _, err := store.CollectRun(ctx, c.thread, c.run, "operator", now); err != nil {
			t.Fatalf("%s %s: %v", c.thread, c.run, err)
		}
	}
	// A thread whose ID is a prefix of another's must see only its own.
	saveTestNodeWait(t, store, domain.NodeWait{
		Request:   domain.NodeWaitRequest{ID: "nw-campaign-prefix", ThreadID: "thread", Target: domain.NodeRef{RunID: "run-done", TaskID: domain.SinkTaskName}, Timeout: time.Hour},
		CreatedAt: now, Delivery: "pending",
	})
	list := func(thread string) []string {
		t.Helper()
		collections, err := store.ListRunCollections(ctx, thread)
		if err != nil {
			t.Fatal(err)
		}
		runs := []string{}
		for _, c := range collections {
			if c.ThreadID != thread {
				t.Fatalf("thread %s listed a collection of %s", thread, c.ThreadID)
			}
			runs = append(runs, c.RunID)
		}
		return runs
	}
	if got := list("thread-a"); !reflect.DeepEqual(got, []string{"run-done", "run-failed"}) {
		t.Fatalf("thread-a = %v", got)
	}
	if got := list("thread-b"); !reflect.DeepEqual(got, []string{"run-failed"}) {
		t.Fatalf("thread-b = %v", got)
	}
	if got := list("thread"); len(got) != 0 {
		t.Fatalf("a prefix thread listed %v", got)
	}
	if got := list("thread-c"); len(got) != 0 {
		t.Fatalf("a thread with no collections listed %v", got)
	}
}

// Self-review regression: CollectRun reads before it writes, so when another
// connection (another process on the coordinator host) commits in between,
// SQLite refuses the upgrade to a write with "database is locked" and no busy
// timeout applies. The collection is retried rather than reported as a
// failure the caller can do nothing about.
func TestRunCollectionSurvivesAConcurrentWriter(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	first, err := openMigratedFixture(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	now := time.Date(2026, 10, 7, 15, 0, 0, 0, time.UTC)
	finished := now.Add(-time.Hour)
	if err := first.SaveCoordinatorRecords(ctx, CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{
		{ID: "run-x", WorkflowID: "w", Progress: domain.ProgressSucceeded, Revision: 1, CreatedAt: finished, UpdatedAt: finished, CompletedAt: &finished},
	}}); err != nil {
		t.Fatal(err)
	}
	saveTestNodeWait(t, first, domain.NodeWait{
		Request:   domain.NodeWaitRequest{ID: "nw-1", ThreadID: "thread", Target: domain.NodeRef{RunID: "run-x", TaskID: domain.SinkTaskName}, Timeout: time.Hour},
		CreatedAt: finished, Delivery: "pending",
	})
	var wg sync.WaitGroup
	errs := make(chan error, 400)
	worker := func(store *Store) {
		defer wg.Done()
		for i := 0; i < 60; i++ {
			// The run finishes again each time, so every collection writes.
			done := now.Add(time.Duration(i) * time.Second).Format(time.RFC3339Nano)
			if _, err := store.db.ExecContext(ctx, `UPDATE coordinator_workflow_runs SET record = json_set(record, '$.completedAt', ?) WHERE id = 'run-x'`, done); err != nil {
				errs <- fmt.Errorf("update: %w", err)
				return
			}
			if _, _, err := store.CollectRun(ctx, "thread", "run-x", "operator", now); err != nil {
				errs <- err
			}
		}
	}
	wg.Add(2)
	go worker(first)
	go worker(second)
	wg.Wait()
	close(errs)
	counts := map[string]int{}
	for err := range errs {
		counts[err.Error()]++
	}
	if len(counts) != 0 {
		t.Fatalf("collections failed under a concurrent writer: %v", counts)
	}
}

// Self-review regression: the list was a substr comparison whose length was
// counted in bytes, so a thread ID with a multi-byte character was recorded
// and then never listed again.
func TestRunCollectionListFindsNonASCIIThread(t *testing.T) {
	ctx := context.Background()
	store, now := runCollectionFixture(t)
	for i, thread := range []string{"thrëad-a", "线程-1", "thread%_x"} {
		saveTestNodeWait(t, store, domain.NodeWait{
			Request:   domain.NodeWaitRequest{ID: fmt.Sprintf("nw-unicode-%d", i), ThreadID: thread, Target: domain.NodeRef{RunID: "run-done", TaskID: domain.SinkTaskName}, Timeout: time.Hour},
			CreatedAt: now, Delivery: "pending",
		})
		if _, _, err := store.CollectRun(ctx, thread, "run-done", "operator", now); err != nil {
			t.Fatalf("%q: %v", thread, err)
		}
		listed, err := store.ListRunCollections(ctx, thread)
		if err != nil || len(listed) != 1 || listed[0].ThreadID != thread || listed[0].RunID != "run-done" {
			t.Fatalf("%q: listed %+v, %v; want the one collection just recorded", thread, listed, err)
		}
	}
}

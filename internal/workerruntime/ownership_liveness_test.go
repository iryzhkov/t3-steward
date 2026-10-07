package workerruntime

import (
	"context"
	"testing"
	"time"
)

func TestOwnershipLivenessBounds(t *testing.T) {
	now := runtimeTestNow
	for _, tc := range []struct {
		name                      string
		lease, updated, heartbeat time.Time
		stale                     bool
	}{
		{"missing heartbeat", now.Add(-time.Second), now, time.Time{}, true},
		{"fresh heartbeat", now.Add(-time.Second), now.Add(-time.Hour), now, false},
		{"future heartbeat", now.Add(-time.Second), now, now.Add(time.Minute), true},
		{"valid lease", now.Add(time.Minute), now.Add(-time.Hour), time.Time{}, false},
		{"no lease keeps age bound", time.Time{}, now.Add(-OwnershipMaxAge - time.Second), now, true},
		{"no lease stays fresh", time.Time{}, now.Add(-time.Minute), time.Time{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			record := AttemptRecord{UpdatedAt: tc.updated}
			record.Assignment.LeaseExpiresAt = tc.lease
			if got := ownershipStale(record, now, tc.heartbeat); (got != "") != tc.stale {
				t.Fatalf("staleness = %q, want stale %v", got, tc.stale)
			}
		})
	}
}

func TestJournalThreadOwnershipKeepsExpiredLeaseWhileWorkerLives(t *testing.T) {
	host, projection := catalogHostFixture(t)
	bootstrapWorkerFile(t, host)
	publishCatalog(t, host, "first", CatalogRequest{Projection: projection})
	now := runtimeTestNow
	runtime := host.service.Exchange.Runtime
	runtime.config.Now = func() time.Time { return now }
	// An idle reconcile pass must publish worker liveness even when no attempt
	// changes. The old lease-only check returned this live worker's thread.
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := runtime.journal.update(func(state *journalState) error {
		record := AttemptRecord{Phase: PhaseRunning, ThreadID: "thread-live", UpdatedAt: now.Add(-time.Hour)}
		record.Assignment.ID = "live"
		record.Assignment.AttemptID = "attempt-live"
		record.Assignment.LeaseExpiresAt = now.Add(-time.Second)
		record.Package.Package.Identity.AssignmentID = "live"
		state.Attempts["live"] = record
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	ownership := &JournalThreadOwnership{Home: host.Home, Now: func() time.Time { return now }}
	for _, elapsed := range []time.Duration{0, 10 * time.Minute} {
		now = runtimeTestNow.Add(elapsed)
		owners, err := ownership.Threads(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if owners.Live["thread-live"] != "attempt-live" {
			t.Fatalf("live worker lost ownership at %s: %v", elapsed, owners)
		}
	}
	now = runtimeTestNow.Add(10*time.Minute + time.Nanosecond)
	owners, err := ownership.Threads(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(owners.Live) != 0 {
		t.Fatalf("dead worker retained ownership past grace: %v", owners)
	}
	// Renewal through reconciliation, independent of record mutation, restores
	// ownership after the watchdog has observed stale liveness.
	if err := runtime.journal.update(func(state *journalState) error { delete(state.Attempts, "live"); return nil }); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := runtime.journal.update(func(state *journalState) error {
		record := AttemptRecord{Phase: PhaseRunning, ThreadID: "thread-live"}
		record.Assignment.ID, record.Assignment.AttemptID = "live", "attempt-live"
		record.Assignment.LeaseExpiresAt = now.Add(-time.Second)
		record.Package.Package.Identity.AssignmentID = "live"
		state.Attempts["live"] = record
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	owners, err = ownership.Threads(context.Background())
	if err != nil || owners.Live["thread-live"] != "attempt-live" {
		t.Fatalf("renewed liveness: %v, %v", owners, err)
	}
}

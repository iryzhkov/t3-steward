package sqlite

import (
	"context"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"strings"
	"testing"
	"time"
)

func TestCoordinatorAuditRetentionBoundAndReplayEvidence(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, err := openMigratedFixture(t.TempDir() + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	cutoff := time.Date(2026, 10, 1, 0, 0, 0, 100, time.UTC)
	protected := []string{
		"admin-command-submitted", "admin-command-rejected", "admin-command-applied", "admin-command-failed",
		"assignment-released", "assignment-lease-renewed", "worker-state-claimed", "worker-state-completed", "worker-state-released",
		"assignment-dispatch-accepted", "assignment-dispatch-rejected",
		"worker-command-committed", "worker-command-accepted", "worker-command-rejected",
		"artifact-published", "native-question", "native-answer", "checkpoint-import-rejected",
		"unknown-assignment-recovered", "schedule-definition-updated", "operator-reassessment",
	}
	var events []domain.AuditEvent
	for i, kind := range protected {
		events = append(events, domain.AuditEvent{ID: fmt.Sprintf("protected-%d", i), Kind: kind, CreatedAt: cutoff.Add(-365 * 24 * time.Hour)})
	}
	// More than one batch, spanning the no-fraction/fraction timestamp boundary.
	for i := range CoordinatorAuditPruneBatch + 7 {
		events = append(events, domain.AuditEvent{ID: fmt.Sprintf("expired-%d", i), Kind: "worker-snapshot-observed", CreatedAt: cutoff.Add(-100 * time.Nanosecond)})
	}
	events = append(events,
		domain.AuditEvent{ID: "fraction-expired", Kind: "worker-snapshot-disconnected", CreatedAt: cutoff.Add(-time.Nanosecond)},
		domain.AuditEvent{ID: "cutoff", Kind: "worker-snapshot-observed", CreatedAt: cutoff},
		domain.AuditEvent{ID: "new", Kind: "workflow-run-created", CreatedAt: cutoff.Add(time.Nanosecond)})
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{AuditEvents: events}); err != nil {
		t.Fatal(err)
	}
	for i, want := range []int{CoordinatorAuditPruneBatch, 8, 0} {
		n, err := store.PruneCoordinatorAuditEvents(ctx, cutoff)
		if err != nil || n != want {
			t.Fatalf("batch %d = %d, %v; want %d", i, n, err, want)
		}
	}
	for i := range protected {
		id := fmt.Sprintf("protected-%d", i)
		event, found, err := store.LoadAuditEvent(ctx, id)
		if err != nil || !found || event.Kind != protected[i] {
			t.Fatalf("protected event %s lost: %+v %v %v", id, event, found, err)
		}
	}
	for _, id := range []string{"cutoff", "new"} {
		if _, found, err := store.LoadAuditEvent(ctx, id); err != nil || !found {
			t.Fatalf("%s lost: %v %v", id, found, err)
		}
	}
	all, err := store.LoadAuditEvents(ctx, "")
	if err != nil || len(all) != len(protected)+2 {
		t.Fatalf("remaining = %d, %v", len(all), err)
	}
	// The query must search the partial index, without sorting or walking the
	// protected history. Its ordering and predicates match the production query.
	rows, err := store.db.QueryContext(ctx, "EXPLAIN QUERY PLAN SELECT sequence FROM coordinator_audit_events WHERE "+prunableCoordinatorAuditKinds+" AND "+coordinatorAuditTime+" < ? ORDER BY "+coordinatorAuditTime+" LIMIT ?", cutoff.Format("2006-01-02T15:04:05.000000000"), CoordinatorAuditPruneBatch)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan += detail + "\n"
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan, "SEARCH coordinator_audit_events USING INDEX coordinator_audit_events_retention") || strings.Contains(plan, "TEMP B-TREE") {
		t.Fatalf("unbounded prune query plan: %s", plan)
	}
}

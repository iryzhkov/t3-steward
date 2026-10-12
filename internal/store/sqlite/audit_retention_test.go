package sqlite

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestAuditRetentionBoundAgeProtectedKindsAndIndex(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := openMigratedFixture(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	cutoff := time.Date(2026, 10, 1, 12, 0, 0, 100000000, time.UTC)
	protectedKinds := []string{
		"admin-command-submitted", "admin-command-rejected", "admin-command-applied", "admin-command-failed",
		"assignment-released", "assignment-dispatch-prepared", "assignment-dispatch-sent",
		"assignment-dispatch-acknowledged", "assignment-lease-renewed",
		"worker-state-claimed", "worker-state-unknown", "worker-state-released",
		"artifact-published", "checkpoint-import-rejected", "unknown-assignment-recovered",
		"native-question", "native-answer", "schedule-definition-created", "schedule-definition-updated",
	}
	var events []domain.AuditEvent
	appendEvent := func(id, kind string, at time.Time) {
		events = append(events, domain.AuditEvent{ID: id, Kind: kind, TargetType: "worker",
			TargetID: "worker-1", CreatedAt: at})
	}
	for i, kind := range protectedKinds {
		appendEvent(fmt.Sprintf("protected-%d", i), kind, cutoff.Add(-365*24*time.Hour))
	}
	// More than a full batch, with a non-observation kind also eligible.
	for i := range AuditPruneBatchSize + 3 {
		appendEvent(fmt.Sprintf("expired-%d", i), "worker-snapshot-observed", cutoff.Add(-time.Hour))
	}
	appendEvent("other-expired", "workflow-run-created", cutoff.Add(-time.Minute))
	appendEvent("whole-second", "worker-snapshot-observed", cutoff.Truncate(time.Second))
	appendEvent("fraction-before", "worker-snapshot-observed", cutoff.Add(-time.Nanosecond))
	appendEvent("at-cutoff", "worker-snapshot-observed", cutoff)
	appendEvent("fraction-after", "worker-snapshot-observed", cutoff.Add(time.Nanosecond))
	appendEvent("recent", "worker-snapshot-observed", cutoff.Add(time.Hour))
	if err := s.SaveCoordinatorRecords(ctx, CoordinatorRecords{AuditEvents: events}); err != nil {
		t.Fatal(err)
	}

	rows, err := s.db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+auditPruneCandidates,
		strings.TrimSuffix(cutoff.Format(time.RFC3339Nano), "Z"), AuditPruneBatchSize)
	if err != nil {
		t.Fatal(err)
	}
	var plan strings.Builder
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	if !strings.Contains(plan.String(), "SEARCH coordinator_audit_events USING INDEX coordinator_audit_events_retention") ||
		strings.Contains(plan.String(), "TEMP B-TREE") {
		t.Fatalf("prune must use bounded index range in timestamp order: %s", plan.String())
	}

	if n, err := s.PruneAuditEvents(ctx, cutoff); err != nil || n != AuditPruneBatchSize {
		t.Fatalf("first batch = %d, %v", n, err)
	}
	// One batch leaves eligible events for the next boundary.
	var remaining int
	if err := s.db.QueryRow("SELECT count(*) FROM coordinator_audit_events").Scan(&remaining); err != nil ||
		remaining != len(events)-AuditPruneBatchSize {
		t.Fatalf("remaining = %d, %v", remaining, err)
	}
	if n, err := s.PruneAuditEvents(ctx, cutoff.In(time.FixedZone("offset", 3*3600))); err != nil || n != 6 {
		t.Fatalf("second batch = %d, %v; want 6", n, err)
	}
	if n, err := s.PruneAuditEvents(ctx, cutoff); err != nil || n != 0 {
		t.Fatalf("drained batch = %d, %v", n, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = openMigratedFixture(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := range protectedKinds {
		id := fmt.Sprintf("protected-%d", i)
		if _, found, err := s.LoadAuditEvent(ctx, id); err != nil || !found {
			t.Fatalf("replay evidence %s lost after reopen: %v", id, err)
		}
	}
	for _, id := range []string{"at-cutoff", "fraction-after", "recent"} {
		if _, found, err := s.LoadAuditEvent(ctx, id); err != nil || !found {
			t.Fatalf("unexpired event %s lost: %v", id, err)
		}
	}
	for _, id := range []string{"whole-second", "fraction-before", "other-expired"} {
		if _, found, err := s.LoadAuditEvent(ctx, id); err != nil || found {
			t.Fatalf("expired event %s retained: %v", id, err)
		}
	}
}

func TestAuditRetentionUpgradeFrom42AddsIndexWithoutDeletingHistory(t *testing.T) {
	s := migrationDowngradeFixture(t, 42, 43)
	event := domain.AuditEvent{ID: "old-observation", Kind: "worker-snapshot-observed",
		TargetType: "worker", TargetID: "worker-1", CreatedAt: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)}
	if err := s.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{AuditEvents: []domain.AuditEvent{event}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(); err != nil {
		t.Fatal(err)
	}
	if _, found, err := s.LoadAuditEvent(context.Background(), event.ID); err != nil || !found {
		t.Fatalf("migration deleted history: %v", err)
	}
	if n, err := s.PruneAuditEvents(context.Background(), time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)); err != nil || n != 1 {
		t.Fatalf("upgraded prune = %d, %v", n, err)
	}
}

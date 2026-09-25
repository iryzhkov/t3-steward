package sqlite

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// M5: every coordinator boundary loads the records from dozens of call sites,
// and each load decoded the whole audit history, 217,834 events and 109 MB of
// JSON on the fleet coordinator. A boundary configured for 10 s took 40-50 s,
// and a collection, which waits for two worker exchanges, waited one or two
// minutes. The records a boundary reads no longer carry the history; the
// history is read by run or by id, and a save of loaded records deletes none of
// it.
func TestCoordinatorRecordsLoadLeavesAuditHistoryInTheTable(t *testing.T) {
	ctx := context.Background()
	store, err := OpenMigrated(t.TempDir() + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 9, 25, 7, 0, 0, 0, time.UTC)
	var events []domain.AuditEvent
	for index := range 5 {
		events = append(events, domain.AuditEvent{
			ID: fmt.Sprintf("event-%d", index), Kind: "workflow-run-created", WorkflowRunID: "run-1",
			TargetType: "workflow-run", TargetID: "run-1", CreatedAt: now,
		})
	}
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{AuditEvents: events}); err != nil {
		t.Fatal(err)
	}

	loaded, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.AuditEvents) != 0 {
		t.Fatalf("a boundary load decoded %d audit events; the history must stay in the table", len(loaded.AuditEvents))
	}
	// Saving what a boundary loaded, as every read-modify-write path does,
	// must not lose the history it did not load.
	if err := store.SaveCoordinatorRecords(ctx, loaded); err != nil {
		t.Fatal(err)
	}
	byRun, err := store.LoadAuditEvents(ctx, "run-1")
	if err != nil || len(byRun) != 5 {
		t.Fatalf("run audit events = %d, err %v; want 5", len(byRun), err)
	}
	one, found, err := store.LoadAuditEvent(ctx, "event-3")
	if err != nil || !found || one.ID != "event-3" || one.Sequence < 1 {
		t.Fatalf("audit event by id = %+v found=%v err=%v", one, found, err)
	}
	if _, found, err := store.LoadAuditEvent(ctx, "missing"); err != nil || found {
		t.Fatalf("missing audit event found=%v err=%v", found, err)
	}
	all, err := store.LoadCoordinatorRecordsWithAudit(ctx)
	if err != nil || len(all.AuditEvents) != 5 {
		t.Fatalf("records with audit = %d events, err %v", len(all.AuditEvents), err)
	}
}

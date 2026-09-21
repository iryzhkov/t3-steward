package backlog

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestMaterializationLeavesSQLiteInboxUnreadAcrossRestart(t *testing.T) {
	for _, count := range []int{100, 1000} {
		t.Run(fmt.Sprintf("%d", count), func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "state.db")
			store, err := sqlite.OpenMigrated(path)
			if err != nil {
				t.Fatal(err)
			}
			rows := make([]sqlite.SupervisionInboxRow, 0, count)
			for index := 0; index < count; index++ {
				event := SupervisionEvent{
					ID: fmt.Sprintf("consultation:%04d", index), RunID: "run-1",
					Kind: TriggerOperatorReassessment, Reason: "bounded consultation summary",
					OccurredAt: time.Unix(int64(index+1), 0).UTC(),
				}
				record, err := json.Marshal(event)
				if err != nil {
					t.Fatal(err)
				}
				rows = append(rows, sqlite.SupervisionInboxRow{ID: event.ID, Record: record})
			}
			written, err := store.AppendSupervisionInbox(ctx, "run-1", rows)
			if err != nil || written != count {
				t.Fatalf("append wrote %d events (err %v), want %d", written, err, count)
			}
			events := loadStoredSupervisionEvents(t, store, "run-1")
			inbox := CoalesceSupervisionEvents("run-1", 0, events)
			materialized, err := materializeActivationInbox(inbox, 2048)
			if err != nil {
				t.Fatal(err)
			}
			if len(materialized.Selected) != maxMaterializedConsultations ||
				len(materialized.Omitted) != count-maxMaterializedConsultations {
				t.Fatalf("selected=%d omitted=%d", len(materialized.Selected), len(materialized.Omitted))
			}
			if len(materialized.JSON) > 2048 {
				t.Fatalf("serialized bytes=%d, cap=2048", len(materialized.JSON))
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}

			reopened, err := sqlite.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reopened.Close() })
			reloaded := loadStoredSupervisionEvents(t, reopened, "run-1")
			remaining := CoalesceSupervisionEvents("run-1", 0, reloaded)
			if len(remaining.Events) != count {
				t.Fatalf("events after restart=%d, want all %d unconsumed events", len(remaining.Events), count)
			}
			for _, row := range mustListInbox(t, reopened, "run-1") {
				if row.Consumed {
					t.Fatalf("event %q was consumed by materialization", row.ID)
				}
			}
		})
	}
}

func TestMaterializationOversizedEntryDoesNotStarveSmallerEntries(t *testing.T) {
	events := []SupervisionEvent{
		{ID: "consultation:oversized", RunID: "run-1", Kind: TriggerOperatorReassessment, Sequence: 1, Reason: strings.Repeat("x", 4096), OccurredAt: time.Unix(1, 0).UTC()},
		{ID: "consultation:small-1", RunID: "run-1", Kind: TriggerOperatorReassessment, Sequence: 2, Reason: "small", OccurredAt: time.Unix(2, 0).UTC()},
		{ID: "consultation:small-2", RunID: "run-1", Kind: TriggerOperatorReassessment, Sequence: 3, Reason: "small", OccurredAt: time.Unix(3, 0).UTC()},
	}
	materialized, err := materializeActivationInbox(CoalesceSupervisionEvents("run-1", 0, events), 1024)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(materialized.Selected, ","); got != "consultation:small-1,consultation:small-2" {
		t.Fatalf("selected=%q, want both smaller entries", got)
	}
	if len(materialized.Oversized) != 1 || materialized.Oversized[0].ID != "consultation:oversized" {
		t.Fatalf("oversized=%+v, want explicit oversized outcome", materialized.Oversized)
	}

	// This reproduces why the current scalar cursor cannot acknowledge selected
	// sequence 2: advancing it to 2 also hides omitted sequence 1.
	afterScalarAck := CoalesceSupervisionEvents("run-1", 2, events)
	if got := strings.Join(afterScalarAck.EventIDs(), ","); got != "consultation:small-2" {
		t.Fatalf("events after scalar cursor 2=%q, want only sequence 3", got)
	}
}

func loadStoredSupervisionEvents(t *testing.T, store *sqlite.Store, runID string) []SupervisionEvent {
	t.Helper()
	rows := mustListInbox(t, store, runID)
	events := make([]SupervisionEvent, 0, len(rows))
	for _, row := range rows {
		var event SupervisionEvent
		if err := json.Unmarshal(row.Record, &event); err != nil {
			t.Fatal(err)
		}
		event.Sequence = row.Sequence
		events = append(events, event)
	}
	return events
}

func mustListInbox(t *testing.T, store *sqlite.Store, runID string) []sqlite.SupervisionInboxRow {
	t.Helper()
	rows, err := store.ListSupervisionInbox(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

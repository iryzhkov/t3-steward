package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Field defect 2026-10-08: three attempts whose turns had ended deferred
// their collection for hours, read as active running, and triage reported
// nothing. A collection a worker has deferred for longer than ten minutes is
// now an item that needs an operator, with the reason the worker last gave.
func TestTriageListsACollectionDeferredForLong(t *testing.T) {
	now := time.Date(2026, 10, 8, 6, 0, 0, 0, time.UTC)
	observation := func(id, phase string, note string) domain.WorkerAssignmentObservation {
		return domain.WorkerAssignmentObservation{AssignmentID: id, AssignmentEpoch: 1, State: domain.AssignmentClaimed,
			Control: domain.ControlRunning, Journal: &domain.WorkerJournalExcerpt{Phase: phase, TurnEnd: note}}
	}
	reason := "thread archive could not be decoded (pass 1 of 3): result import thread archive is invalid: json: cannot unmarshal object into Go struct field .thread.activities.payload.detail of type string"
	workers := []backlogadmin.Worker{{Snapshot: domain.WorkerSnapshot{WorkerID: "agent-a", Assignments: []domain.WorkerAssignmentObservation{
		observation("assignment-stuck", "collecting", domain.CollectionDeferredNote(now.Add(-3*time.Hour), reason)),
		// Deferred only briefly: a collection waiting for T3's final message.
		observation("assignment-fresh", "collecting", domain.CollectionDeferredNote(now.Add(-2*time.Minute), "T3 turn is not yet terminal")),
		// A running attempt's turn-end note is about its commands, not a deferral.
		observation("assignment-running", "running", "waiting for 1 command"),
		// A note left from collecting is not reported once the attempt moved on.
		observation("assignment-done", "completed", domain.CollectionDeferredNote(now.Add(-3*time.Hour), reason)),
	}}}}
	report := triageReport{Items: []triageItem{}}
	triageDeferredCollections(&report, workers, now)
	if len(report.Items) != 1 {
		t.Fatalf("items %+v, want the one long deferral", report.Items)
	}
	item := report.Items[0]
	if item.Kind != "collection-deferred" || item.Severity != "action" || item.Subject != "assignment-stuck" ||
		item.Since == nil || !item.Since.Equal(now.Add(-3*time.Hour)) {
		t.Fatalf("item %+v", item)
	}
	if !strings.Contains(item.Summary, "agent-a") || !strings.Contains(item.Summary, "payload.detail of type string") || !strings.Contains(item.Summary, "3h") {
		t.Fatalf("summary %q does not name the worker, the duration and the reason", item.Summary)
	}
	if len(item.Commands) == 0 || item.Commands[0].Host != "agent-a" {
		t.Fatalf("commands %+v", item.Commands)
	}
	var out bytes.Buffer
	if err := renderTriage(&out, report); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "collection-deferred") {
		t.Fatalf("rendered triage does not list the item:\n%s", out.String())
	}
}

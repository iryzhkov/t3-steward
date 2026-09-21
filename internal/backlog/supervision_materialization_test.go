package backlog

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestMaterializeSupervisionEventsBoundsSyntheticConsultationInputs(t *testing.T) {
	for _, count := range []int{100, 1000} {
		t.Run(fmt.Sprintf("%d", count), func(t *testing.T) {
			events := make([]SupervisionEvent, 0, count)
			for index := 0; index < count; index++ {
				events = append(events, SupervisionEvent{
					ID:    fmt.Sprintf("consultation:%04d", index),
					RunID: "run-1", Kind: TriggerOperatorReassessment,
					Sequence: int64(index + 1), Reason: "bounded consultation summary",
					OccurredAt: time.Unix(int64(index+1), 0).UTC(),
				})
			}
			inbox := CoalesceSupervisionEvents("run-1", 0, events)
			materialized, err := MaterializeActivationInbox(inbox, 4, 2048)
			if err != nil {
				t.Fatal(err)
			}
			if len(materialized.Selected) != 4 || len(materialized.Omitted) != count-4 {
				t.Fatalf("selected=%d omitted=%d, want 4 and %d", len(materialized.Selected), len(materialized.Omitted), count-4)
			}
			if len(materialized.JSON) > 2048 {
				t.Fatalf("serialized bytes=%d, cap=2048", len(materialized.JSON))
			}
			for index, id := range materialized.Selected {
				want := fmt.Sprintf("consultation:%04d", index)
				if id != want {
					t.Fatalf("selected[%d]=%q, want %q", index, id, want)
				}
			}

			// Materialization is a projection, not acknowledgement. Re-reading the
			// durable input at the same cursor still exposes every omitted event.
			remaining := CoalesceSupervisionEvents("run-1", 0, events)
			if len(remaining.Events) != count {
				t.Fatalf("events after materialization=%d, want all %d durable events", len(remaining.Events), count)
			}
			if remaining.Events[4].ID != materialized.Omitted[0] {
				t.Fatalf("first omitted event %q is not durably readable as %q", materialized.Omitted[0], remaining.Events[4].ID)
			}
		})
	}
}

func TestMaterializeSupervisionEventsOversizedEntryDoesNotStarveSmallerEntries(t *testing.T) {
	events := []SupervisionEvent{
		{ID: "consultation:oversized", RunID: "run-1", Kind: TriggerOperatorReassessment, Sequence: 1, Reason: strings.Repeat("x", 4096), OccurredAt: time.Unix(1, 0).UTC()},
		{ID: "consultation:small-1", RunID: "run-1", Kind: TriggerOperatorReassessment, Sequence: 2, Reason: "small", OccurredAt: time.Unix(2, 0).UTC()},
		{ID: "consultation:small-2", RunID: "run-1", Kind: TriggerOperatorReassessment, Sequence: 3, Reason: "small", OccurredAt: time.Unix(3, 0).UTC()},
	}
	inbox := CoalesceSupervisionEvents("run-1", 0, events)
	materialized, err := MaterializeActivationInbox(inbox, 4, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(materialized.Selected, ","); got != "consultation:small-1,consultation:small-2" {
		t.Fatalf("selected=%q, want both smaller entries", got)
	}
	if len(materialized.Oversized) != 1 || materialized.Oversized[0].ID != "consultation:oversized" {
		t.Fatalf("oversized=%+v, want explicit oversized outcome", materialized.Oversized)
	}
	if len(materialized.JSON) > 1024 {
		t.Fatalf("serialized bytes=%d, cap=1024", len(materialized.JSON))
	}
	remaining := CoalesceSupervisionEvents("run-1", 0, events)
	if len(remaining.Events) != 3 || remaining.Events[0].ID != "consultation:oversized" {
		t.Fatalf("materialization consumed or reordered durable input: %+v", remaining.EventIDs())
	}
}

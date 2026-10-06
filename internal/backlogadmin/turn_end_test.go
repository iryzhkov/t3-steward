package backlogadmin

import (
	"slices"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// While a worker holds an attempt whose turn ended with commands still
// running, the explanation and the task detail say what it is waiting for.
func TestExplanationShowsTurnEndBackgroundCommands(t *testing.T) {
	now := adminTestNow
	records := parkedRecords(now)
	records.Attempts[0].Progress, records.Attempts[0].Control = domain.ProgressActive, domain.ControlRunning
	note := "waiting for 2 background commands: sh -c make check-review, sleep 300 (nudge 1 of 2)"
	workers := []domain.WorkerSnapshot{{
		WorkerID: "homelab", WorkerEpoch: "worker-1", Sequence: 4, Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Minute),
		Inventory: domain.WorkerInventory{ID: "homelab"},
		Assignments: []domain.WorkerAssignmentObservation{{
			AssignmentID: "assignment-1", AssignmentEpoch: 1, State: domain.AssignmentClaimed, Control: domain.ControlRunning,
			ThreadID: "thread-1", ObservedAt: now,
			Journal: &domain.WorkerJournalExcerpt{Phase: "stopped", ThreadState: "stopped", TurnEnd: note},
		}},
	}}
	v := newView(records, workers, nil, RuntimeInfo{}, now)
	explanation, ok := v.explanation("run-1", "nest-model")
	if !ok {
		t.Fatal("no explanation")
	}
	if !slices.Contains(explanation.Details, "turn end: "+note) {
		t.Fatalf("details = %q", explanation.Details)
	}
	detail, ok := v.taskDetail("run-1", "nest-model")
	if !ok || detail.Evidence == nil || detail.Evidence.TurnEnd != note {
		t.Fatalf("evidence = %#v", detail.Evidence)
	}

	// Nothing is added for an attempt whose worker reports no turn-end state.
	workers[0].Assignments[0].Journal.TurnEnd = ""
	v = newView(records, workers, nil, RuntimeInfo{}, now)
	explanation, _ = v.explanation("run-1", "nest-model")
	for _, line := range explanation.Details {
		if len(line) >= 9 && line[:9] == "turn end:" {
			t.Fatalf("details = %q", explanation.Details)
		}
	}
}

// A host that cannot look for background commands collects the turn as
// before, so the attempt reaches a terminal state with the warning still in
// the worker's evidence. Explain keeps showing it once the result is
// imported, whether the attempt succeeded or failed. A stale waiting note is
// not shown for a terminal attempt, which no longer waits for anything.
func TestExplanationShowsTheUnsupportedTurnEndWarningForTerminalAttempts(t *testing.T) {
	now := adminTestNow
	warning := "process inspection is unavailable on darwin; the turn end was collected without looking for background commands"
	waiting := "waiting for 1 background command: sleep 300 (nudge 1 of 2)"
	for _, tc := range []struct {
		progress domain.ProgressState
		note     string
		shown    bool
	}{
		{domain.ProgressSucceeded, warning, true},
		{domain.ProgressFailed, warning, true},
		{domain.ProgressSucceeded, waiting, false},
	} {
		records := parkedRecords(now)
		records.Attempts[0].Progress, records.Attempts[0].Control = tc.progress, domain.ControlRunning
		workers := []domain.WorkerSnapshot{{
			WorkerID: "homelab", WorkerEpoch: "worker-1", Sequence: 4, Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Minute),
			Inventory: domain.WorkerInventory{ID: "homelab"},
			Assignments: []domain.WorkerAssignmentObservation{{
				AssignmentID: "assignment-1", AssignmentEpoch: 1, State: domain.AssignmentClaimed, Control: domain.ControlRunning,
				ThreadID: "thread-1", ObservedAt: now,
				Journal: &domain.WorkerJournalExcerpt{Phase: "completed", ThreadState: "stopped", TurnEnd: tc.note},
			}},
		}}
		v := newView(records, workers, nil, RuntimeInfo{}, now)
		explanation, ok := v.explanation("run-1", "nest-model")
		if !ok {
			t.Fatal("no explanation")
		}
		if explanation.Summary != "task is terminal" {
			t.Fatalf("%s: summary = %q", tc.progress, explanation.Summary)
		}
		if got := slices.Contains(explanation.Details, "turn end: "+tc.note); got != tc.shown {
			t.Fatalf("%s %q: shown = %v, want %v; details = %q", tc.progress, tc.note, got, tc.shown, explanation.Details)
		}
	}
}

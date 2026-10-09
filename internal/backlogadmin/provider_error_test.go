package backlogadmin

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// While a worker resumes an attempt whose turn a provider error ended, the
// explanation and the task detail carry the error as an infrastructure
// failure event of the attempt.
func TestExplanationShowsTheProviderErrorBeingResumed(t *testing.T) {
	now := adminTestNow
	records := parkedRecords(now)
	records.Attempts[0].Progress, records.Attempts[0].Control = domain.ProgressActive, domain.ControlRunning
	resumeAt := now.Add(5 * time.Minute)
	reported := &domain.WorkerProviderError{
		Kind: domain.ProviderErrorCapacity, Detail: "Selected model is at capacity", TurnID: "turn-1", ObservedAt: now,
		State: domain.ProviderResumeScheduled, Resumes: 2, Budget: 3, Errors: 2, ResumeAt: &resumeAt,
	}
	workers := []domain.WorkerSnapshot{{
		WorkerID: "homelab", WorkerEpoch: "worker-1", Sequence: 4, Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Minute),
		Inventory: domain.WorkerInventory{ID: "homelab"},
		Assignments: []domain.WorkerAssignmentObservation{{
			AssignmentID: "assignment-1", AssignmentEpoch: 1, State: domain.AssignmentClaimed, Control: domain.ControlRunning,
			ThreadID: "thread-1", ObservedAt: now,
			Journal: &domain.WorkerJournalExcerpt{Phase: "stopped", ThreadState: "stopped", ProviderError: reported},
		}},
	}}
	v := newView(records, workers, nil, RuntimeInfo{}, now)
	explanation, ok := v.explanation("run-1", "nest-model")
	if !ok {
		t.Fatal("no explanation")
	}
	want := "provider error: capacity (infrastructure): Selected model is at capacity; resume 2 of 3 of the same session at " + resumeAt.UTC().Format(time.RFC3339)
	if !slices.Contains(explanation.Details, want) {
		t.Fatalf("details = %q, want %q", explanation.Details, want)
	}
	detail, ok := v.taskDetail("run-1", "nest-model")
	if !ok || detail.Evidence == nil || detail.Evidence.ProviderError == nil || detail.Evidence.ProviderError.Resumes != 2 {
		t.Fatalf("evidence = %#v", detail.Evidence)
	}

	// A worker that reports none adds nothing.
	workers[0].Assignments[0].Journal.ProviderError = nil
	v = newView(records, workers, nil, RuntimeInfo{}, now)
	explanation, _ = v.explanation("run-1", "nest-model")
	for _, line := range explanation.Details {
		if strings.HasPrefix(line, "provider error:") {
			t.Fatalf("details = %q", explanation.Details)
		}
	}
}

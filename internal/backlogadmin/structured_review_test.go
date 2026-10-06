package backlogadmin

import (
	"bytes"
	"encoding/json"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"testing"
)

func TestStructuredReviewLegacyNodeTransport(t *testing.T) {
	verdict := &domain.ReviewVerdict{Verdict: "changes-requested", BlockingFindings: 2, FindingTitles: []string{"lost evidence"}}
	obs := domain.NodeObservation{Target: domain.NodeRef{RunID: "r"}, Progress: domain.ProgressSucceeded, Reason: "succeeded\nReview verdict: " + verdict.Prose(), Fields: map[string]string{"review": "changes-requested", "blocking": "2"}, ReviewVerdict: verdict}
	response := NodeWaitResponse{Waits: []domain.NodeWait{{Observation: &obs}}}
	raw, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	var wire struct {
		Waits []struct{ Observation json.RawMessage }
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	// Pre-H1 worker observation, decoded with the admin carrier's strict rule.
	var legacy struct {
		Target          domain.NodeRef
		RunRevision     int64
		AttemptID       string
		AttemptRevision int64
		Progress        domain.ProgressState
		ExitCode        int
		Reason          string
		Outcome         domain.TaskWaitOutcome
		Fields          map[string]string
	}
	dec := json.NewDecoder(bytes.NewReader(wire.Waits[0].Observation))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&legacy); err != nil {
		t.Fatalf("older worker refused wake: %v", err)
	}
	if legacy.Reason != obs.Reason || legacy.Fields["blocking"] != "2" {
		t.Fatalf("lost bounded evidence: %+v", legacy)
	}
	if response.Waits[0].Observation.ReviewVerdict != verdict {
		t.Fatal("wire encoding mutated coordinator evidence")
	}
}

func TestStructuredReviewExplanation(t *testing.T) {
	verdict := &domain.ReviewVerdict{Verdict: "changes-requested", BlockingFindings: 2, FindingTitles: []string{"lost evidence"}}
	records := sqlite.CoordinatorRecords{Workflows: []domain.Workflow{{ID: "w"}}, WorkflowRuns: []domain.WorkflowRun{{ID: "r", WorkflowID: "w", Sink: &domain.SinkTask{ID: domain.SinkTaskID("r"), Progress: domain.ProgressSucceeded}}}, Tasks: []domain.Task{{ID: "t", Name: "review", WorkflowID: "w"}}, Attempts: []domain.Attempt{{ID: "a", WorkflowRunID: "r", TaskID: "t", Number: 1, Progress: domain.ProgressSucceeded, ReviewVerdict: verdict}}}
	v := newView(records, nil, nil, RuntimeInfo{}, adminTestNow)
	for _, task := range []string{"t", domain.SinkTaskName} {
		got, ok := v.explanation("r", task)
		if !ok || got.ReviewVerdict == nil || got.ReviewVerdict.Verdict != "changes-requested" || got.ReviewVerdict.BlockingFindings != 2 {
			t.Fatalf("%s: %+v %v", task, got, ok)
		}
	}
}

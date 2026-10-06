package wait

import (
	"encoding/json"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"strings"
	"testing"
)

func TestStructuredReviewLegacyWake(t *testing.T) {
	ref := domain.NodeRef{RunID: "r", TaskID: "t"}
	verdict := &domain.ReviewVerdict{Verdict: "changes-requested", BlockingFindings: 2, FindingTitles: []string{"lost evidence"}}
	obs, err := domain.ResolveNodeState(ref, domain.NodeStateTerminal, []domain.WorkflowRun{{ID: "r"}}, []domain.Task{{ID: "t"}}, []domain.Attempt{{ID: "a", TaskID: "t", WorkflowRunID: "r", Number: 1, Progress: domain.ProgressSucceeded, ReviewVerdict: verdict}}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(obs)
	if err != nil {
		t.Fatal(err)
	}
	// The pre-H1 observation has no ReviewVerdict field.
	var legacy struct {
		Reason string
		Fields map[string]string
	}
	if err := json.Unmarshal(raw, &legacy); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(legacy.Reason, "lost evidence") || !strings.Contains(legacy.Reason, verdict.Summary()) {
		t.Fatalf("legacy wake lost review: %+v", legacy)
	}
	w := domain.NodeWait{Request: domain.NodeWaitRequest{Target: ref}, Observation: &obs}
	if strings.Count(nodeWakeProse(w), "lost evidence") != 1 {
		t.Fatal("duplicated review prose")
	}
}

func TestStructuredReviewWake(t *testing.T) {
	obs := domain.NodeObservation{Target: domain.NodeRef{RunID: "r", TaskID: "t"}, Progress: domain.ProgressSucceeded, ExitCode: 0, ReviewVerdict: &domain.ReviewVerdict{Verdict: "changes-requested", BlockingFindings: 2, FindingTitles: []string{"lost evidence"}}}
	obs.Fields = map[string]string{"review": "changes-requested", "blocking": "2"}
	w := domain.NodeWait{Request: domain.NodeWaitRequest{ID: "w", Target: obs.Target}, Observation: &obs}
	prose := nodeWakeProse(w)
	if !strings.Contains(prose, "review=changes-requested blocking=2") || !strings.Contains(prose, "lost evidence") {
		t.Fatalf("wake: %s", prose)
	}
	fields := domain.NodeTrailerFields(obs)
	if fields["review"] != "changes-requested" || fields["blocking"] != "2" {
		t.Fatal(fields)
	}
	for _, v := range fields {
		if strings.Contains(v, "lost evidence") {
			t.Fatal("title in trailer")
		}
	}
}

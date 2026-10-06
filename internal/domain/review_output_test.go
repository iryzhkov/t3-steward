package domain

import (
	"reflect"
	"testing"
)

func TestStructuredReviewRunAggregation(t *testing.T) {
	verdict := &ReviewVerdict{Verdict: "changes-requested", BlockingFindings: 2, FindingTitles: []string{"z"}}
	attempts := []Attempt{{ID: "a", WorkflowRunID: "r", TaskID: "t", Number: 1, Progress: ProgressSucceeded, ReviewVerdict: verdict},
		{ID: "b", WorkflowRunID: "r", TaskID: "t2", Number: 1, Progress: ProgressSucceeded, ReviewVerdict: &ReviewVerdict{Verdict: "accept", BlockingFindings: 1, FindingTitles: []string{"a"}}},
		{ID: "other", WorkflowRunID: "other", TaskID: "t", Number: 1, Progress: ProgressSucceeded, ReviewVerdict: verdict}}
	run := WorkflowRun{ID: "r", WorkflowID: "w", Sink: &SinkTask{ID: SinkTaskID("r"), Progress: ProgressSucceeded}}
	obs, err := ResolveNodeState(NodeRef{RunID: "r", TaskID: SinkTaskName}, NodeStateTerminal, []WorkflowRun{run}, nil, attempts, nil, nil)
	if err != nil || obs.Fields["review"] != "changes-requested" || obs.Fields["blocking"] != "3" || !reflect.DeepEqual(obs.ReviewVerdict.FindingTitles, []string{"a", "z"}) {
		t.Fatalf("%+v %v", obs, err)
	}
	attempts = append(attempts, Attempt{ID: "retry", WorkflowRunID: "r", TaskID: "t", Number: 2, Progress: ProgressActive})
	got := AggregateReviewVerdicts("r", "", attempts)
	if got.Verdict != "accept" || got.BlockingFindings != 1 {
		t.Fatal(got)
	}
	maxInt := int(^uint(0) >> 1)
	attempts[0].ReviewVerdict = &ReviewVerdict{Verdict: "accept", BlockingFindings: maxInt}
	got = AggregateReviewVerdicts("r", "", attempts[:2])
	if got.BlockingFindings != maxInt {
		t.Fatal("overflow", got)
	}
}

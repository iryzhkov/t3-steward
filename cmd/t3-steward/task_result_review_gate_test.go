package main

import (
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// task result shows the completion gate's decision and the heads it compared,
// for a pass as much as for a failure, in both forms.
func TestTaskResultShowsTheReviewGateDecision(t *testing.T) {
	reviewed, physical := strings.Repeat("a", 40), strings.Repeat("b", 40)
	round := &domain.ReviewRoundHead{RoundID: "rc-1", Number: 1, CheckpointID: "cp-1", HeadCommit: reviewed, Verdict: "accept", Accepted: true}

	failed := domain.EvaluateReviewCompletionGate(round, &domain.WorkspaceHead{Schema: domain.WorkspaceHeadSchema, Head: physical}, nil)
	f := newTaskResultFixture(t)
	f.detail.Tasks[0].Attempt.Progress = domain.ProgressFailed
	f.detail.Tasks[0].Attempt.Failure = failed.Failure()
	f.detail.Tasks[0].Attempt.ReviewGate = &failed
	f.detail.Summary.Run.Progress = domain.ProgressFailed
	_ = f.run("run-1")
	for _, want := range []string{"review gate:", "head-changed-after-review", reviewed, physical} {
		if !strings.Contains(f.stdout.String(), want) {
			t.Fatalf("text form does not show %q:\n%s", want, f.stdout.String())
		}
	}

	passed := domain.EvaluateReviewCompletionGate(round, &domain.WorkspaceHead{Schema: domain.WorkspaceHeadSchema, Head: reviewed}, nil)
	f = newTaskResultFixture(t)
	f.detail.Tasks[0].Attempt.ReviewGate = &passed
	if err := f.run("run-1", "--json"); err != nil {
		t.Fatal(err)
	}
	document := f.document(t)
	if gate := document.Tasks[0].ReviewGate; gate == nil || !gate.Passed || gate.ReviewedHead != reviewed || gate.PhysicalHead != reviewed {
		t.Fatalf("--json review gate = %+v", gate)
	}

	f = newTaskResultFixture(t)
	if err := f.run("run-1"); err != nil || strings.Contains(f.stdout.String(), "review gate") {
		t.Fatalf("a task without a gate decision printed one: %v\n%s", err, f.stdout.String())
	}
}

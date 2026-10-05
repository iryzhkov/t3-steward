package wait

import (
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"strings"
	"testing"
)

func TestIndependentMessageFix1ResultBoundary(t *testing.T) {
	for _, run := range []string{"r", strings.Repeat("a", 128), strings.Repeat("a", 129), "run.a_b-9", "-bad", "run;true", "run\nnext", "é", "run`x", "run$(x)"} {
		w := domain.NodeWait{Observation: &domain.NodeObservation{Target: domain.NodeRef{RunID: run}, Progress: domain.ProgressFailed}}
		got := nodeWakeResult(w)
		want := run == "r" || len(run) == 128 || run == "run.a_b-9"
		if (got != "") != want {
			t.Fatalf("run=%q got=%q", run, got)
		}
		if want && !strings.Contains(got, "t3-steward task result "+run+"`") {
			t.Fatal("wrong command")
		}
		fmt.Printf("run=%q command=%v\n", run, got != "")
	}
	for _, outcome := range []domain.TaskWaitOutcome{domain.TaskWaitMet, domain.TaskWaitFailed, domain.TaskWaitCancelled, domain.TaskWaitGaveUp, domain.TaskWaitTimedOut} {
		w := domain.NodeWait{Request: domain.NodeWaitRequest{ID: "quota", Name: "quota reset", Quota: &domain.QuotaWaitCondition{Pool: "pool"}}, Observation: &domain.NodeObservation{Outcome: outcome, Reason: "elapsed reset", Progress: domain.ProgressFailed, Target: domain.NodeRef{RunID: "run-valid"}}}
		text := nodeWakeProse(w)
		if strings.Contains(text, "task result") || !strings.Contains(text, "reset deadline alone does not confirm recovery") || !strings.Contains(text, "Cancellation and pause") {
			t.Fatal(text)
		}
		fmt.Printf("quota outcome=%s no-command fresh-eligibility-required=true\n", outcome)
	}
}

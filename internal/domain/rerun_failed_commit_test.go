package domain

import "testing"

func TestPlanRerunFailedCommitIsExplicitAndAttemptBound(t *testing.T) {
	run := WorkflowRun{ID: "r", WorkflowID: "w", Progress: ProgressFailed}
	tasks := []Task{{ID: "producer", WorkflowID: "w", Name: "producer"}, {ID: "review", WorkflowID: "w", Name: "review", Needs: []string{"producer"}}}
	attempts := []Attempt{{ID: "failed", WorkflowRunID: "r", TaskID: "producer", Number: 1, Progress: ProgressFailed}, {ID: "review-attempt", WorkflowRunID: "r", TaskID: "review", Number: 1, Progress: ProgressSkipped}}
	if _, err := PlanRerun(run, tasks, attempts, "review"); err == nil {
		t.Fatal("ordinary rerun reused failure")
	}
	scope, err := PlanRerun(run, tasks, attempts, "review", RerunCommitOptions{FailedAttempts: map[string]string{"producer": "failed"}})
	if err != nil || len(scope.Reuse) != 1 {
		t.Fatalf("explicit commit reuse: %+v %v", scope, err)
	}
	attempts = append(attempts, Attempt{ID: "newer", WorkflowRunID: "r", TaskID: "producer", Number: 2, Progress: ProgressFailed})
	if _, err := PlanRerun(run, tasks, attempts, "review", RerunCommitOptions{FailedAttempts: map[string]string{"producer": "failed"}}); err == nil {
		t.Fatal("stale failure record authorized newer attempt")
	}
}

package backlogadmin

import (
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// A dependency blocker names the dependency by its manifest name and says where
// it is. It used to say "dependency has not succeeded" and carry only the task
// id in DependsOn, which the text form does not print, so the reader was not
// told which dependency it was.
func TestExplainNamesTheDependencyThatHasNotSucceeded(t *testing.T) {
	reader := explainReader{records: sqlite.CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: "wf-1", Project: "t3-steward"}},
		WorkflowRuns: []domain.WorkflowRun{{ID: "run-1", WorkflowID: "wf-1", Progress: domain.ProgressReady}},
		Tasks: []domain.Task{
			{ID: "task-draft", Name: "draft", WorkflowID: "wf-1"},
			{ID: "task-review", Name: "review", WorkflowID: "wf-1", Needs: []string{"task-draft"}},
		},
	}}
	explanation := explainTask(t, explainService(t, reader, false), "task-review")
	for _, blocker := range explanation.Blockers {
		if blocker.Code != "dependency" {
			continue
		}
		if blocker.Detail != "dependency draft has not succeeded (queued)" || blocker.DependsOn != "task-draft" {
			t.Fatalf("dependency blocker = %+v", blocker)
		}
		return
	}
	t.Fatalf("no dependency blocker in %+v", explanation.Blockers)
}

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

// A cross-run blocker names its source node in the detail too, not only in
// DependsOn, which the text form does not print.
func TestExplainNamesTheCrossRunSourceThatHasNotSucceeded(t *testing.T) {
	source := domain.NodeRef{RunID: "run-0", TaskID: "task-build"}
	reader := explainReader{records: sqlite.CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: "wf-1", Project: "t3-steward"}},
		WorkflowRuns: []domain.WorkflowRun{{ID: "run-1", WorkflowID: "wf-1", Progress: domain.ProgressReady}},
		Tasks: []domain.Task{
			{ID: "task-review", Name: "review", WorkflowID: "wf-1", ExternalNeeds: []domain.NodeRef{source}},
		},
	}}
	explanation := explainTask(t, explainService(t, reader, false), "task-review")
	for _, blocker := range explanation.Blockers {
		if blocker.Code == "cross-run-dependency" {
			if blocker.Detail != "source "+source.String()+" has not succeeded" {
				t.Fatalf("cross-run blocker = %+v", blocker)
			}
			return
		}
	}
	t.Fatalf("no cross-run blocker in %+v", explanation.Blockers)
}

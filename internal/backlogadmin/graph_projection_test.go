package backlogadmin

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestAddedAndClonedTasksProjectWithRunDefinitions(t *testing.T) {
	ctx := context.Background()
	s, store := graphFixture(t)
	p := Principal{ID: "operator"}
	add := graphRequest("project-add", "task-add", "", 1)
	add.Task = &domain.Task{Name: "c", Verification: []string{"git status --porcelain"}, Class: domain.TaskClassSurplus, MaxTurns: 1, Needs: []string{"a"}, Routes: []domain.ProviderRoute{{ProviderInstanceID: "codex", Model: "new"}}}
	add.Prompt = "review"
	if _, err := s.AmendGraph(ctx, p, add); err != nil {
		t.Fatal(err)
	}
	clone, err := s.AmendGraph(ctx, p, graphRequest("project-clone", "clone", "", 2))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = backlog.ProjectWorkflowRuns(ctx, store, time.Now()); err != nil {
		t.Fatal(err)
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, attempt := range records.Attempts {
		if attempt.WorkflowRunID != "run" && attempt.WorkflowRunID != clone.Run.ID {
			continue
		}
		task, ok := domain.TaskForAttempt(attempt, records.WorkflowRuns, records.Tasks)
		if !ok {
			t.Fatal("projected attempt lost task definition")
		}
		want := domain.ProgressReady
		if task.Name == "c" {
			want = domain.ProgressBlocked
		}
		if attempt.Progress != want {
			t.Fatalf("%s/%s progress=%s want=%s", attempt.WorkflowRunID, task.Name, attempt.Progress, want)
		}
		checked++
	}
	if checked != 6 {
		t.Fatalf("projected attempts=%d", checked)
	}
	// A completed sink is immutable even when the target task has not run.
	for _, run := range records.WorkflowRuns {
		if run.ID != "run" {
			continue
		}
		run.Sink.Progress = domain.ProgressSucceeded
		if err = store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}}); err != nil {
			t.Fatal(err)
		}
	}
	timeout := time.Minute
	set := graphRequest("closed-sink", "task-set", "b", 2)
	set.Timeout = &timeout
	if _, err = s.AmendGraph(ctx, p, set); err == nil {
		t.Fatal("completed sink amended")
	}
}

package sqlite

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func graphStoreFixture(t *testing.T) (*Store, domain.WorkflowRun, domain.Task) {
	t.Helper()
	store := openFleetTestStore(t)
	ctx := context.Background()
	task := domain.Task{ID: "task-1", WorkflowID: "workflow", Name: "task", Class: domain.TaskClassSurplus, MaxTurns: 1, PromptArtifactID: "prompt", Routes: []domain.ProviderRoute{{ProviderInstanceID: "codex", Model: "gpt-5.6-sol"}}}
	run, err := domain.BindRunSink(domain.WorkflowRun{ID: "run-1", WorkflowID: "workflow", GraphRevision: 1, Revision: 1, Progress: domain.ProgressQueued, CreatedAt: fleetTestTime, UpdatedAt: fleetTestTime}, []domain.Task{task})
	if err != nil {
		t.Fatal(err)
	}
	if err = store.SaveCoordinatorRecords(ctx, CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}, Tasks: []domain.Task{task}, Attempts: []domain.Attempt{fleetAttempt("attempt-1")}, Artifacts: []domain.Artifact{{ID: "prompt", WorkflowRunID: run.ID, TaskID: task.ID, Kind: domain.ArtifactInput, Name: "prompt.md", MediaType: "text/markdown", Size: 1, SHA256: "hash", StoragePath: "prompt"}}}); err != nil {
		t.Fatal(err)
	}
	saveFleetSnapshot(t, store, fleetSnapshot(1, "worker-epoch-1", 1, true, fleetTestTime.Add(time.Hour)))
	return store, run, task
}
func graphStoreCommit(t *testing.T, run domain.WorkflowRun, task domain.Task, id string) GraphCommit {
	t.Helper()
	timeout := time.Minute
	request := domain.GraphAmendment{ID: id, RunID: run.ID, ExpectedRevision: 1, Operation: "task-set", TaskID: task.ID, Timeout: &timeout, Reason: "bound execution"}
	tasks, err := domain.AmendTasks(request, run, []domain.Task{task}, "", "")
	if err != nil {
		t.Fatal(err)
	}
	return GraphCommit{Request: request, Actor: "operator", Before: run, Tasks: tasks, Now: fleetTestTime}
}
func TestGraphAssignmentRaceFencesDefinition(t *testing.T) {
	for i := 0; i < 6; i++ {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			ctx := context.Background()
			store, run, task := graphStoreFixture(t)
			commit := graphStoreCommit(t, run, task, "amend")
			plan := fleetPlanCommit(1, "assignment-1", "attempt-1", "worker-epoch-1", 1)
			var assignments []domain.Assignment
			var amendment domain.GraphAmendmentResult
			var planErr, amendErr error
			start := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(2)
			go func() { defer wg.Done(); <-start; assignments, planErr = store.CommitAssignmentPlan(ctx, plan) }()
			go func() { defer wg.Done(); <-start; amendment, amendErr = store.CommitGraphAmendment(ctx, commit) }()
			close(start)
			wg.Wait()
			if len(assignments) > 0 && amendErr == nil {
				t.Fatal("obsolete plan and amendment both committed")
			}
			if planErr != nil && amendErr != nil {
				t.Fatalf("neither operation progressed: %v / %v", planErr, amendErr)
			}
			if amendErr == nil {
				if amendment.Graph.Revision != 2 {
					t.Fatal("amendment did not advance revision")
				}
				replay, err := store.CommitAssignmentPlan(ctx, plan)
				if err != nil || len(replay) != 0 {
					t.Fatalf("stale plan offered: %v %v", replay, err)
				}
			} else if len(assignments) > 0 {
				if assignments[0].TaskDigest != domain.TaskDigest(task) || assignments[0].GraphRevision != 1 {
					t.Fatal("assignment did not pin original definition")
				}
				if _, err := store.CommitGraphAmendment(ctx, commit); err == nil {
					t.Fatal("assigned definition edited")
				}
			}
		})
	}
}
func TestGraphHistoryImmutableAndFailedTransactionRollsBack(t *testing.T) {
	ctx := context.Background()
	store, run, task := graphStoreFixture(t)
	original, err := store.LoadGraphRevisions(ctx, run.ID)
	if err != nil || len(original) != 1 {
		t.Fatal(original, err)
	}
	commit := graphStoreCommit(t, run, task, "failure")
	// Fail the audit write after the revision, attempt and run changes.
	if _, err = store.db.Exec("CREATE TRIGGER reject_graph_audit BEFORE INSERT ON coordinator_audit_events WHEN NEW.id='graph-amended:failure' BEGIN SELECT RAISE(ABORT,'injected audit failure'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err = store.CommitGraphAmendment(ctx, commit); err == nil {
		t.Fatal("injected failure ignored")
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if records.WorkflowRuns[0].GraphRevision != 1 || records.Attempts[0].Revision != 1 {
		t.Fatal("partial graph transaction visible")
	}
	history, err := store.LoadGraphRevisions(ctx, run.ID)
	if err != nil || len(history) != 1 || history[0].Digest != original[0].Digest {
		t.Fatal("rollback changed history")
	}
	if _, err = store.db.Exec("DROP TRIGGER reject_graph_audit"); err != nil {
		t.Fatal(err)
	}
	if _, err = store.CommitGraphAmendment(ctx, commit); err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.Exec("UPDATE coordinator_graph_revisions SET record='{}'"); err == nil {
		t.Fatal("immutable history rewritten")
	}
	history, err = store.LoadGraphRevisions(ctx, run.ID)
	if err != nil || len(history) != 2 || history[0].Digest != original[0].Digest {
		t.Fatal("original snapshot changed")
	}
}

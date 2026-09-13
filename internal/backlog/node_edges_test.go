package backlog

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestCrossRunDependencyWaitsThenReleasesOrSkips(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "cancelled"}[cancelled], func(t *testing.T) {
			ctx := context.Background()
			now := time.Now().UTC()
			store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			sourceTask := domain.Task{ID: "source", Name: "source", WorkflowID: "source-w"}
			source, err := domain.BindRunSink(domain.WorkflowRun{ID: "source-r", WorkflowID: "source-w", Progress: domain.ProgressActive, Revision: 1, CreatedAt: now, UpdatedAt: now}, []domain.Task{sourceTask})
			if err != nil {
				t.Fatal(err)
			}
			attempt := domain.Attempt{ID: "a", TaskID: "source", WorkflowRunID: "source-r", Number: 1, Progress: domain.ProgressActive, Control: domain.ControlRunning, UpdatedAt: now}
			if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{source}, Tasks: []domain.Task{sourceTask}, Attempts: []domain.Attempt{attempt}}); err != nil {
				t.Fatal(err)
			}
			bundle := validBundle(t)
			rewriteBundleManifest(t, bundle, "version: 2\nname: dependent\nclass: required\nenvironment: {project: t3-steward}\ntasks:\n  inspect:\n    prompt_file: prompts/inspect.md\n    needs: source-r/__sink\n")
			ingester := BundleIngester{Store: store, StorageRoot: filepath.Join(t.TempDir(), "bundles")}
			t.Cleanup(func() { _ = removeIngestedTree(ingester.StorageRoot) })
			ingested, err := ingester.Ingest(ctx, bundle)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ProjectWorkflowRuns(ctx, store, now); err != nil {
				t.Fatal(err)
			}
			records, err := store.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, a := range records.Attempts {
				if a.WorkflowRunID == ingested.RunID && a.Progress != domain.ProgressBlocked {
					t.Fatal("released pending source", a)
				}
			}
			attempt.Control = domain.ControlStopped
			attempt.Progress = domain.ProgressSucceeded
			if cancelled {
				attempt.Progress = domain.ProgressCancelled
			}
			if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{attempt}}); err != nil {
				t.Fatal(err)
			}
			// Two projection boundaries cover either deterministic run ordering.
			for range 2 {
				if _, err := ProjectWorkflowRuns(ctx, store, now.Add(time.Minute)); err != nil {
					t.Fatal(err)
				}
			}
			records, err = store.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			want := domain.ProgressReady
			if cancelled {
				want = domain.ProgressSkipped
			}
			for _, a := range records.Attempts {
				if a.WorkflowRunID == ingested.RunID && a.Progress != want {
					t.Fatalf("dependent=%s want=%s", a.Progress, want)
				}
			}
		})
	}
}

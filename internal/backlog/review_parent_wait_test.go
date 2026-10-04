package backlog

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"io"
	"testing"
	"time"
)

func TestReviewParentWaitRetainedDAGCollector(t *testing.T) {
	for _, park := range []bool{false, true} {
		t.Run(map[bool]string{false: "execution before collection", true: "park then execution"}[park], func(t *testing.T) {
			ctx := context.Background()
			f := newRetainedChild(t, false)
			p, err := f.prepare(ctx)
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := f.parent.store.MaterializeReviewChild(ctx, f.frozen, f.checkpoint, p)
			if err != nil {
				t.Fatal(err)
			}
			if park {
				got, err := f.parent.store.WaitReviewParent(ctx, f.frozen, f.checkpoint)
				if err != nil || got.Status != "parked" {
					t.Fatalf("park: %+v %v", got, err)
				}
			}
			dag, err := NewDAGExecution(DAGState{Run: receipt.Graph.Run, Tasks: receipt.Graph.Tasks, Attempts: receipt.Graph.Attempts})
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			for _, a := range receipt.Graph.Attempts {
				if err = dag.StartAttempt(a.ID, now); err != nil {
					t.Fatal(err)
				}
				if err = dag.CompleteAttempt(a.ID, CompletionResult{Failure: "execution failed"}, now); err != nil {
					t.Fatal(err)
				}
			}
			state := dag.Snapshot()
			state.Run, err = domain.ProjectRunSink(state.Run, state.Tasks, state.Attempts, nil, now)
			if err != nil {
				t.Fatal(err)
			}
			if state.Run.Sink == nil || !state.Run.Sink.Progress.Terminal() {
				t.Fatal("DAG did not end child sink")
			}
			if err = f.parent.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{state.Run}, Attempts: state.Attempts}); err != nil {
				t.Fatal(err)
			}
			// Reopen before either settlement or collection.
			other, err := sqlite.OpenMigrated(f.parent.store.dbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer other.Close()
			if park {
				if err = other.SettleNodeWaits(ctx, now); err != nil {
					t.Fatal(err)
				}
			}
			got, err := other.WaitReviewParent(ctx, f.frozen, f.checkpoint)
			want := "finished"
			if park {
				want = "settled"
			}
			if err != nil || got.Status != want || !got.CollectionPending || got.RoundState != "pending" {
				t.Fatalf("execution status: %+v %v", got, err)
			}
			persisted := f.retained
			persisted.Catalog = other
			collector := ReviewCollector{Store: other, Results: t.TempDir(), Open: func(ctx context.Context, id string) (domain.Artifact, io.ReadCloser, error) {
				return persisted.Open(ctx, id)
			}}
			if err = collector.Tick(ctx, now); err != nil {
				t.Fatal(err)
			}
			got, err = other.WaitReviewParent(ctx, f.frozen, f.checkpoint)
			if err != nil || got.Status != want || got.CollectionPending || got.RoundState == "pending" {
				t.Fatalf("collection status: %+v %v", got, err)
			}
		})
	}
}

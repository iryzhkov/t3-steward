package backlog

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
)

func TestFixLoopSQLiteProjectionAndRestart(t *testing.T) {
	for _, acceptRound := range []int{1, 0} {
		t.Run(fmt.Sprintf("accept-%d", acceptRound), func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "state.db")
			store, err := sqlitetest.OpenMigrated(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = store.Close() }()
			state := loopDAG(t).Snapshot()
			state.Run, err = domain.BindRunSink(state.Run, state.Tasks)
			if err != nil {
				t.Fatal(err)
			}
			for i := range state.Attempts {
				state.Attempts[i].Revision = 1
			}
			records := sqlite.CoordinatorRecords{
				Workflows:    []domain.Workflow{{ID: "workflow", Version: 2, Name: "workflow", Class: domain.TaskClassRequired, CreatedAt: dagTestTime}},
				WorkflowRuns: []domain.WorkflowRun{state.Run}, Tasks: state.Tasks, Attempts: state.Attempts,
			}
			if err := store.SaveCoordinatorRecords(ctx, records); err != nil {
				t.Fatal(err)
			}
			rounds := 4
			if acceptRound > 0 {
				rounds = acceptRound
			}
			for round := 1; round <= rounds; round++ {
				loaded, err := store.LoadCoordinatorRecords(ctx)
				if err != nil {
					t.Fatal(err)
				}
				// Feed persisted worker completion evidence into the production projection,
				// without calling DAG completion/refresh helpers to skip later branches.
				for i := range loaded.Attempts {
					attempt := &loaded.Attempts[i]
					if attempt.ID != fmt.Sprintf("implement%d-1", round) && attempt.ID != fmt.Sprintf("review%d-1", round) {
						continue
					}
					attempt.Progress = domain.ProgressSucceeded
					attempt.Control = domain.ControlStopped
					attempt.Revision++
					completed := dagTestTime.Add(time.Duration(round) * time.Minute)
					attempt.CompletedAt = &completed
					attempt.UpdatedAt = completed
					if attempt.ID == fmt.Sprintf("review%d-1", round) {
						verdict := "changes-requested"
						if round == acceptRound {
							verdict = "accept"
						}
						attempt.ReviewVerdict = &domain.ReviewVerdict{Verdict: verdict}
					}
				}
				if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: loaded.Attempts}); err != nil {
					t.Fatal(err)
				}
				if _, err := ProjectWorkflowRuns(ctx, store, dagTestTime.Add(time.Duration(round)*time.Minute)); err != nil {
					t.Fatal(err)
				}
				if round < rounds {
					projected, err := store.LoadCoordinatorRecords(ctx)
					if err != nil {
						t.Fatal(err)
					}
					for _, attempt := range projected.Attempts {
						if attempt.ID == fmt.Sprintf("implement%d-1", round+1) && attempt.Progress != domain.ProgressReady {
							t.Fatalf("next round not admitted by projection: %+v", attempt)
						}
					}
				}
			}
			final, err := store.LoadCoordinatorRecordsWithAudit(ctx)
			if err != nil {
				t.Fatal(err)
			}
			sink := final.WorkflowRuns[0].Sink
			want := domain.ProgressSucceeded
			if acceptRound == 0 {
				want = domain.ProgressFailed
			}
			if sink == nil || sink.Progress != want || final.WorkflowRuns[0].Progress != want {
				t.Fatalf("run/sink not settled: %+v", final.WorkflowRuns[0])
			}
			summaries := sink.Result.FixLoops
			if len(summaries) != 1 || summaries[0].Rounds != rounds || summaries[0].Exhausted != (acceptRound == 0) {
				t.Fatalf("persisted loop summary: %+v", summaries)
			}
			if acceptRound == 0 && summaries[0].Escalation != domain.FixLoopExhausted {
				t.Fatalf("missing typed exhaustion: %+v", summaries[0])
			}
			if acceptRound == 1 {
				for _, attempt := range final.Attempts {
					if attempt.ID == "implement1-1" || attempt.ID == "review1-1" {
						continue
					}
					if attempt.Progress != domain.ProgressSkipped || attempt.Control != domain.ControlStopped || attempt.CompletedAt == nil {
						t.Fatalf("unexecuted branch skip not persisted: %+v", attempt)
					}
				}
				if len(sink.Result.SkippedTaskIDs) != 0 || len(sink.Result.FailedTaskIDs) != 0 {
					t.Fatalf("intentional branches counted as unsuccessful tasks: %+v", sink.Result)
				}
			}
			if len(final.Tasks) != 8 || len(final.Attempts) != 8 || len(final.Assignments) != 0 {
				t.Fatal("projection changed declared bound")
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = sqlitetest.OpenMigrated(path)
			if err != nil {
				t.Fatal(err)
			}
			report, err := ProjectWorkflowRuns(ctx, store, dagTestTime.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			replay, err := store.LoadCoordinatorRecordsWithAudit(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(report.Runs) != 0 || !reflect.DeepEqual(final, replay) {
				t.Fatal("restart changed persisted loop projection")
			}
		})
	}
}

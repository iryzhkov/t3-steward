package backlogadmin

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

type waitExplainReader struct {
	explainReader
	waits []domain.TaskWait
}

func (r waitExplainReader) ListTaskWaits(context.Context) ([]domain.TaskWait, error) {
	return r.waits, nil
}

// explain names the typed reason a parked task's settled wait has not resumed
// it yet, and the role re-resolution its assignment records; run documents
// carry both.
func TestExplainNamesWakeDeferralAndRouteReresolution(t *testing.T) {
	settled := explainTestNow.Add(-time.Minute)
	attempt := domain.Attempt{ID: "attempt-verify", WorkflowRunID: "run-1", TaskID: "task-verify", Number: 1, Revision: 3,
		AssignmentID: "assignment-verify", Progress: domain.ProgressWaitingExternal, Control: domain.ControlWaitingExternal}
	moved := &domain.RouteReresolution{Role: "execute", FromRoute: "claudeAgent/opus", FromPool: "claude-main",
		ToRoute: "codex/gpt", ToPool: "codex-main", Effort: "medium", Reason: "claude-main is at its concurrency limit"}
	reader := waitExplainReader{
		explainReader: explainReader{records: sqlite.CoordinatorRecords{
			Workflows:    []domain.Workflow{{ID: "wf-1", Project: "t3-steward"}},
			WorkflowRuns: []domain.WorkflowRun{{ID: "run-1", WorkflowID: "wf-1", Progress: domain.ProgressActive}},
			Tasks:        []domain.Task{{ID: "task-verify", Name: "verify", WorkflowID: "wf-1"}},
			Attempts:     []domain.Attempt{attempt},
			Assignments: []domain.Assignment{{ID: "assignment-verify", AttemptID: attempt.ID, WorkerID: "omarchy-pc",
				State: domain.AssignmentClaimed, Placement: &domain.PlacementDecision{SelectedWorkerID: "omarchy-pc", RouteReresolution: moved}}},
		}},
		waits: []domain.TaskWait{{
			ID: "tw-1", WorkflowRunID: "run-1", TaskID: "task-verify", AttemptID: attempt.ID, SettledAt: &settled,
			Result: &domain.TaskWaitResult{Outcome: domain.TaskWaitMet},
			WakeDeferral: &domain.TaskWaitWakeDeferral{Code: domain.WakeDeferredExecutorCapacity,
				Detail: "worker \"omarchy-pc\" has 0 available executor-slots", WorkerID: "omarchy-pc", ObservedAt: settled},
		}},
	}
	service, err := New(reader, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return explainTestNow })
	admin := Principal{ID: "local:1000", Roles: []string{LocalAdminRole}}
	explained, err := service.Query(context.Background(), Query{Version: CurrentReadVersion, Kind: QueryExplanation,
		Principal: admin, WorkflowRunID: "run-1", TaskID: "task-verify"})
	if err != nil || explained.Explanation == nil {
		t.Fatalf("explain: %+v %v", explained, err)
	}
	explanation := *explained.Explanation
	found := false
	for _, blocker := range explanation.Blockers {
		if blocker.Code == domain.WakeDeferredExecutorCapacity && blocker.WorkerID == "omarchy-pc" &&
			strings.Contains(blocker.Detail, "wait tw-1 settled; its wake is deferred") {
			found = true
		}
	}
	if !found {
		t.Fatalf("blockers = %+v, want the typed wake deferral", explanation.Blockers)
	}
	if !strings.Contains(strings.Join(explanation.Details, "\n"),
		"role execute re-resolved at planning from claudeAgent/opus (pool claude-main) to codex/gpt (pool codex-main, effort medium)") {
		t.Fatalf("details = %v, want the re-resolution", explanation.Details)
	}
	if explanation.Placement == nil || explanation.Placement.RouteReresolution == nil {
		t.Fatal("the placement receipt lost its re-resolution")
	}

	response, err := service.Query(context.Background(), Query{Version: CurrentReadVersion, Kind: QueryWorkflow, WorkflowRunID: "run-1",
		Principal: admin})
	if err != nil {
		t.Fatal(err)
	}
	if waits := response.Workflow.TaskWaits; len(waits) != 1 || waits[0].WakeDeferral == nil || waits[0].WakeDeferral.Code != domain.WakeDeferredExecutorCapacity {
		t.Fatalf("run document waits = %+v, want the deferral carried", waits)
	}
}

// A capacity deadlock the last fresh planning pass reported is carried on its
// run's summary, which is where triage reads it; a stale pass reports none.
func TestWorkflowSummaryCarriesFreshCapacityDeadlocks(t *testing.T) {
	reader, _ := planningExplainFixture()
	plan := backlog.Plan{Decisions: []backlog.TaskPlanningDecision{{
		WorkflowRunID: "run", TaskID: "task", AttemptID: "attempt", Progress: domain.ProgressReady,
		Blockers: []backlog.PlanningBlocker{{Code: backlog.PlanningBlockerCapacityDeadlock, OwnerID: "parent-1",
			Detail: "every executor or pool slot this task can use is held by attempts waiting on this run"}},
	}}}
	for _, test := range []struct {
		name string
		age  time.Duration
		want int
	}{{"fresh", 10 * time.Second, 1}, {"stale", time.Hour, 0}} {
		t.Run(test.name, func(t *testing.T) {
			holder := NewPlanningSnapshotHolder(time.Minute)
			holder.Record(plan, reader.records.Attempts, adminTestNow.Add(-test.age))
			response, err := planningExplainService(t, reader, holder).Query(context.Background(), Query{Version: CurrentReadVersion, Kind: QueryWorkflows})
			if err != nil {
				t.Fatal(err)
			}
			if len(response.Workflows) != 1 || len(response.Workflows[0].CapacityDeadlocks) != test.want {
				t.Fatalf("summaries = %+v, want %d deadlocks", response.Workflows, test.want)
			}
			// A client of an older read version decodes strictly; the field
			// is left out of its answer.
			older, err := planningExplainService(t, reader, holder).Query(context.Background(), Query{Version: RC118ReadVersion, Kind: QueryWorkflows})
			if err != nil || len(older.Workflows) != 1 || older.Workflows[0].CapacityDeadlocks != nil {
				t.Fatalf("rc118 answer = %+v err=%v, want no capacity deadlocks key", older.Workflows, err)
			}
			if test.want == 1 {
				deadlock := response.Workflows[0].CapacityDeadlocks[0]
				if deadlock.TaskName != "work" || deadlock.AttemptID != "attempt" || deadlock.HolderID != "parent-1" {
					t.Fatalf("deadlock = %+v", deadlock)
				}
			}
		})
	}
}

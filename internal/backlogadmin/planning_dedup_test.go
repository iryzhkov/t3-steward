package backlogadmin

import (
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestPlanningExplainDuplicateTimestampOrderIsDeterministic(t *testing.T) {
	reader, _ := planningExplainFixture()
	early, late := adminTestNow.Add(time.Minute), adminTestNow.Add(2*time.Minute)
	var first Explanation
	for i, times := range [][]*time.Time{{&early, &late}, {&late, &early}, {nil, &late, &early}, {&early, nil, &late}} {
		decision := backlog.TaskPlanningDecision{WorkflowRunID: "run", TaskID: "task", AttemptID: "attempt", Progress: domain.ProgressReady}
		for _, at := range times {
			decision.Candidates = append(decision.Candidates, backlog.CandidateEvaluation{WorkerID: "worker", Blockers: []backlog.PlanningBlocker{{Code: backlog.PlanningBlockerPoolConcurrency, Detail: "pool is full", QuotaPoolID: "claude-main", EarliestAt: at}}})
		}
		holder := NewPlanningSnapshotHolder(time.Minute)
		holder.Record(backlog.Plan{Decisions: []backlog.TaskPlanningDecision{decision}}, reader.records.Attempts, adminTestNow)
		got := planningQuery(t, planningExplainService(t, reader, holder))
		if len(got.Blockers) != 1 || got.Blockers[0].EarliestAt == nil || !got.Blockers[0].EarliestAt.Equal(early) {
			t.Fatalf("order %d: %+v", i, got.Blockers)
		}
		if i == 0 {
			first = got
		} else if !reflect.DeepEqual(first, got) {
			t.Fatalf("order %d changed explanation: first=%+v got=%+v", i, first, got)
		}
	}
}

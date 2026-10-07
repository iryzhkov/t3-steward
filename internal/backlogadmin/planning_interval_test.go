package backlogadmin

import (
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestPlanningExplainLargeIntervalDoesNotOverflowStaleness(t *testing.T) {
	for _, interval := range []time.Duration{4000000000000000000, time.Duration(1<<63 - 1)} {
		reader, _ := planningExplainFixture()
		holder := NewPlanningSnapshotHolder(interval)
		holder.Record(backlog.Plan{Decisions: []backlog.TaskPlanningDecision{{
			WorkflowRunID: "run", TaskID: "task", AttemptID: "attempt",
			Progress: domain.ProgressReady, Proposed: true,
		}}}, reader.records.Attempts, adminTestNow.Add(-time.Second))
		got := planningQuery(t, planningExplainService(t, reader, holder))
		if !got.Eligible || got.Summary != "task is eligible to start" {
			t.Fatalf("fresh proposed decision with interval %s: %#v", interval, got)
		}
	}
}

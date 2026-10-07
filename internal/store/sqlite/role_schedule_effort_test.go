package sqlite

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"path/filepath"
	"testing"
)

func TestMalformedRoleEffortSuppressed(t *testing.T) {
	for _, effort := range []string{"max", "bogus", "-1", "0.5"} {
		t.Run(effort, func(t *testing.T) {
			s := openScheduleTriggerStore(t, filepath.Join(t.TempDir(), "state.db"), domain.ScheduleFailureNextCycle, nil)
			defer s.Close()
			if err := s.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{Tasks: []domain.Task{{ID: "role-task", WorkflowID: "workflow-1", Name: "execute", Role: "execute"}}}); err != nil {
				t.Fatal(err)
			}
			q := scheduleTriggerRequest("invalid", "no-run", scheduleTriggerTestTime)
			q.Source = domain.ScheduleTriggerManual
			q.RouteSelections = map[string]domain.RoleSelection{"role-task": {Role: "execute", Route: "codex/gpt", Effort: effort}}
			r, err := s.CommitScheduleTrigger(context.Background(), q)
			if err != nil {
				t.Fatal(err)
			}
			if r.Trigger.Reason != "role-unresolved" || r.WorkflowRun != nil {
				t.Fatalf("malformed effort %q accepted: state=%s reason=%s run=%v", effort, r.Trigger.State, r.Trigger.Reason, r.WorkflowRun != nil)
			}
		})
	}
}

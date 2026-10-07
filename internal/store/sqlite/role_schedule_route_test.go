package sqlite

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"path/filepath"
	"strings"
	"testing"
)

func TestMalformedRoleRouteSuppressed(t *testing.T) {
	for _, route := range []string{"codex/bad model", "codex/" + strings.Repeat("m", 257)} {
		t.Run(route[:8], func(t *testing.T) {
			s := openScheduleTriggerStore(t, filepath.Join(t.TempDir(), "state.db"), domain.ScheduleFailureNextCycle, nil)
			defer s.Close()
			if err := s.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{Tasks: []domain.Task{{ID: "role-task", WorkflowID: "workflow-1", Name: "execute", Role: "execute"}}}); err != nil {
				t.Fatal(err)
			}
			q := scheduleTriggerRequest("invalid", "no-run", scheduleTriggerTestTime)
			q.Source = domain.ScheduleTriggerManual
			q.RouteSelections = map[string]domain.RoleSelection{"role-task": {Role: "execute", Route: route, Effort: "low"}}
			r, err := s.CommitScheduleTrigger(context.Background(), q)
			if err != nil {
				t.Fatal(err)
			}
			if r.Trigger.Reason != "role-unresolved" || r.WorkflowRun != nil {
				t.Fatalf("malformed route accepted: state=%s reason=%s run=%v", r.Trigger.State, r.Trigger.Reason, r.WorkflowRun != nil)
			}
		})
	}
}

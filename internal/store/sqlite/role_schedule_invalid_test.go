package sqlite

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"path/filepath"
	"testing"
)

func TestScheduledRoleInvalidSelectionCannotCreateRun(t *testing.T) {
	for _, selection := range []domain.RoleSelection{
		{Role: "read", Route: "codex/gpt", Effort: "low"},
		{Role: "execute", Route: "codex", Effort: "low"},
		{Role: "execute", Route: "/gpt", Effort: "low"},
		{Role: "execute", Route: "codex/", Effort: "low"},
		{Role: "execute", Route: "codex/gpt"},
	} {
		t.Run(selection.Role+"/"+selection.Route+"/"+selection.Effort, func(t *testing.T) {
			store := openScheduleTriggerStore(t, filepath.Join(t.TempDir(), "state.db"), domain.ScheduleFailureNextCycle, nil)
			defer store.Close()
			if err := store.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{Tasks: []domain.Task{{ID: "role-task", WorkflowID: "workflow-1", Role: "execute"}}}); err != nil {
				t.Fatal(err)
			}
			request := scheduleTriggerRequest("invalid", "no-run", scheduleTriggerTestTime)
			request.Source = domain.ScheduleTriggerManual
			request.RouteSelections = map[string]domain.RoleSelection{"role-task": selection}
			result, err := store.CommitScheduleTrigger(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			if result.Trigger.Reason != "role-unresolved" || result.WorkflowRun != nil {
				t.Fatalf("invalid selection accepted: %#v", result)
			}
		})
	}
}

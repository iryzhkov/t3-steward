package backlogadmin

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"testing"
	"time"
)

func TestManualScheduleRunUsesRoleResolverBeforeAtomicCommit(t *testing.T) {
	store := openAdminTestStore(t)
	ctx := context.Background()
	now := adminTestNow
	records := sqlite.CoordinatorRecords{Workflows: []domain.Workflow{{ID: "workflow", Version: 2, Name: "workflow", TaskIDs: []string{"task"}}}, Tasks: []domain.Task{{ID: "task", WorkflowID: "workflow", Name: "inspect", Role: "execute", Class: domain.TaskClassRequired}}, Schedules: []domain.Schedule{{ID: "schedule", Version: 1, WorkflowID: "workflow", Enabled: true, Overlap: domain.ScheduleOverlapForbid, Misfire: domain.ScheduleMisfireSkip, AfterFailure: domain.ScheduleFailureNextCycle, Revision: 1, CreatedAt: now, UpdatedAt: now}}, ScheduleTemplates: []domain.ScheduleTemplate{{ScheduleID: "schedule", Version: 1, WorkflowID: "workflow", Expression: "0 3 * * *", Timezone: "UTC", Overlap: domain.ScheduleOverlapForbid, Misfire: domain.ScheduleMisfireSkip, AfterFailure: domain.ScheduleFailureNextCycle, CreatedAt: now}}}
	if err := store.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}
	service, err := New(store, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return now })
	calls := 0
	service.SetScheduleTriggerResolver(func(_ context.Context, r domain.ScheduleTriggerRequest) (domain.ScheduleTriggerRequest, error) {
		calls++
		r.RouteSelections = map[string]domain.RoleSelection{"task": {Role: "execute", Route: "codex/model", Effort: "low"}}
		return r, nil
	})
	request := Mutation{Version: Version, Principal: Principal{ID: "operator"}, ID: "manual-role", Kind: domain.AdminCommandScheduleRun, ScheduleID: "schedule", ExpectedRevision: 1, Reason: "run"}
	if _, err := service.Mutate(ctx, request); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ExecutePendingCommands(ctx); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || len(loaded.WorkflowRuns) != 1 || loaded.WorkflowRuns[0].RouteSelections["task"].Route != "codex/model" {
		t.Fatalf("manual seam calls=%d records=%#v", calls, loaded)
	}
	if _, err := service.ExecutePendingCommands(ctx); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal("replayed admin command re-resolved")
	}
}

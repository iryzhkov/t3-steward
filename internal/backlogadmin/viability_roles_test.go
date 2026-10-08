package backlogadmin

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"testing"
)

func TestRoleResolutionPrecedesReadinessPlacement(t *testing.T) {
	v := viabilityView(t, nil)
	settings := viabilityCatalog(t)
	task := viabilityTaskRequest()
	task.Role = "execute"
	task.Routes = nil
	calls := 0
	settings.ResolveRoles = func(_ context.Context, tasks []ViabilityTask, projects []Project, match RoleWorkerEligible) (map[string]domain.RoleSelection, map[string]ViabilityReason) {
		calls++
		if len(tasks) != 1 || tasks[0].Role != "execute" || len(projects) == 0 {
			t.Fatal("incomplete resolver input")
		}
		return map[string]domain.RoleSelection{task.Name: {Role: "execute", Route: "t3-primary/opus", Effort: "low", PolicyDigest: "digest"}}, nil
	}
	matrix := v.viability(context.Background(), settings, ViabilityRequest{Tasks: []ViabilityTask{task}})
	if calls != 1 || matrix.Outcome != ViabilityReady || matrix.Tasks[0].RoleSelection == nil || matrix.Tasks[0].SelectedWorker == "" {
		t.Fatalf("%+v", matrix)
	}
	settings.ResolveRoles = func(context.Context, []ViabilityTask, []Project, RoleWorkerEligible) (map[string]domain.RoleSelection, map[string]ViabilityReason) {
		return nil, map[string]ViabilityReason{task.Name: newViabilityReason("unknown-role", "choose a known role")}
	}
	matrix = v.viability(context.Background(), settings, ViabilityRequest{Tasks: []ViabilityTask{task}})
	if matrix.Outcome != ViabilityImpossible || matrix.Tasks[0].Reasons[0].Code != "unknown-role" || !matrix.Tasks[0].Reasons[0].Permanent {
		t.Fatalf("%+v", matrix)
	}
	settings.ResolveRoles = nil
	matrix = v.viability(context.Background(), settings, ViabilityRequest{Tasks: []ViabilityTask{task}})
	if matrix.Tasks[0].Reasons[0].Code != "role-unsupported" {
		t.Fatalf("%+v", matrix)
	}
}
func TestRolePlacementAdvertisedFallbackKeepsStaticDemand(t *testing.T) {
	v := viabilityView(t, nil)
	workers := v.viabilityWorkers()
	match := v.roleWorkerEligible(viabilityCatalog(t), workers)
	task := viabilityTaskRequest()
	task.Resources.MemoryMB = 1 << 50
	if match(task, "homelab", false) {
		t.Fatal("advertised fallback ignored static capacity")
	}
}

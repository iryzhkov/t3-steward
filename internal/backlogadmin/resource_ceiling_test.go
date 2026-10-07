package backlogadmin

import (
	"context"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// The build load ceiling is temporary pressure: a loaded worker makes a build
// wait, never makes it impossible.
func TestResourceViabilityReportsBuildLoadCeilingAsTemporary(t *testing.T) {
	settings := viabilityCatalog(t)
	settings.Projects[0].Type = "fresh"
	settings.ResourcePolicy = domain.DefaultResourcePlacementPolicy()
	v := viabilityView(t, nil)
	// The ceiling acts only on complete telemetry, so every field is reported.
	cpus, load, memory, swap, free, running := 4, 20.0, int64(32000), int64(0), int64(100000), 0
	v.workers[0].Inventory.Telemetry = &domain.WorkerTelemetry{ObservedAt: viabilityNow, CPUCount: &cpus, Load1: &load, Load5: &load, MemoryAvailableMB: &memory,
		SwapUsedMB: &swap, WorkspaceFreeMB: &free, TempFreeMB: &free, RunningAttempts: &running}
	task := viabilityTaskRequest()
	task.Type = "fresh"
	task.ResourcePreset = "build"
	matrix := v.viability(context.Background(), settings, ViabilityRequest{Tasks: []ViabilityTask{task}})
	if matrix.Outcome != ViabilityAcceptedWaiting {
		t.Fatalf("build under the load ceiling should wait, got %+v", matrix)
	}
	found := false
	for _, reason := range matrix.Tasks[0].Candidates[0].Reasons {
		if reason.Code == "resource-pressure" && !reason.Permanent && strings.Contains(reason.Detail, "exceeds build ceiling") {
			found = true
		}
	}
	if !found {
		t.Fatalf("load ceiling not reported as temporary pressure: %+v", matrix.Tasks[0].Candidates[0].Reasons)
	}
}

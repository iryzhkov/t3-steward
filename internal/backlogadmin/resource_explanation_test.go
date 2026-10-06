package backlogadmin

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestResourceViabilityReportsPressureAsTemporaryAndConfiguredPolicy(t *testing.T) {
	settings := viabilityCatalog(t)
	settings.Projects[0].Type = "fresh"
	settings.ResourcePolicy = domain.DefaultResourcePlacementPolicy()
	settings.ResourcePolicy.MemoryReserveMB = 1000
	v := viabilityView(t, nil)
	memory := int64(900)
	v.workers[0].Inventory.Telemetry = &domain.WorkerTelemetry{ObservedAt: viabilityNow, MemoryAvailableMB: &memory}
	task := viabilityTaskRequest()
	task.Type = "fresh"
	matrix := v.viability(context.Background(), settings, ViabilityRequest{Tasks: []ViabilityTask{task}})
	if matrix.Outcome != ViabilityAcceptedWaiting {
		t.Fatalf("pressure should wait, got %+v", matrix)
	}
	candidate := matrix.Tasks[0].Candidates[0]
	found := false
	for _, reason := range candidate.Reasons {
		if reason.Code == "resource-pressure" && !reason.Permanent {
			found = true
		}
	}
	if !found || candidate.ResourceEvaluation == nil || candidate.ResourceEvaluation.Telemetry == nil || *candidate.ResourceEvaluation.Telemetry.MemoryAvailableMB != memory {
		t.Fatalf("missing telemetry/temporary pressure: %+v", candidate)
	}
	settings.ResourcePolicy.MemoryReserveMB = 100
	matrix = v.viability(context.Background(), settings, ViabilityRequest{Tasks: []ViabilityTask{task}})
	if matrix.Outcome != ViabilityReady || matrix.Tasks[0].SelectedWorker != "homelab" {
		t.Fatalf("configured reserve not used: %+v", matrix)
	}
}

func TestResourceCheckSelectsFreshHeadroomBeforeUnknownWorker(t *testing.T) {
	settings := viabilityCatalog(t)
	settings.Projects[0].Type = "fresh"
	v := viabilityView(t, nil)
	v.requirements = nil
	v.enrollments = nil
	unknown := v.workers[0]
	unknown.WorkerID = "a-unknown"
	unknown.Inventory.ID = "a-unknown"
	known := v.workers[0]
	known.WorkerID = "b-known"
	known.Inventory.ID = "b-known"
	cpu := 8
	running := 0
	load := float64(1)
	memory := int64(8192)
	swap := int64(0)
	disk := int64(20000)
	known.Inventory.Telemetry = &domain.WorkerTelemetry{ObservedAt: viabilityNow, CPUCount: &cpu, Load1: &load, Load5: &load, MemoryAvailableMB: &memory, SwapUsedMB: &swap, WorkspaceFreeMB: &disk, TempFreeMB: &disk, RunningAttempts: &running}
	v.workers = []domain.WorkerSnapshot{unknown, known}
	task := viabilityTaskRequest()
	task.Type = "fresh"
	result := v.viability(context.Background(), settings, ViabilityRequest{Tasks: []ViabilityTask{task}}).Tasks[0]
	if result.SelectedWorker != "b-known" {
		t.Fatalf("selected %q: %+v", result.SelectedWorker, result)
	}
	ranks := map[string]int{}
	for _, candidate := range result.Candidates {
		if candidate.ResourceEvaluation == nil {
			t.Fatalf("missing per-worker telemetry: %+v", candidate)
		}
		ranks[candidate.Worker] = candidate.ResourceEvaluation.Rank
	}
	if ranks["b-known"] != 1 || ranks["a-unknown"] != 2 {
		t.Fatalf("ranking: %+v", ranks)
	}
}

func TestResourceExplanationPreservesAssignedDecisionAfterCompletion(t *testing.T) {
	now := time.Now().UTC()
	decision := &domain.PlacementDecision{SelectedWorkerID: "original", ResourceEvaluations: []domain.ResourceEvaluation{{WorkerID: "original", State: "known", Rank: 1}}}
	records := sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{{ID: "run", WorkflowID: "workflow"}},
		Tasks:        []domain.Task{{ID: "task", WorkflowID: "workflow"}},
		Attempts:     []domain.Attempt{{ID: "attempt", TaskID: "task", WorkflowRunID: "run", Progress: domain.ProgressSucceeded}},
		Assignments:  []domain.Assignment{{ID: "assignment", AttemptID: "attempt", Placement: decision}},
	}
	v := newView(records, nil, nil, RuntimeInfo{}, now)
	explanation, ok := v.explanation("run", "task")
	if !ok || explanation.Placement == nil || explanation.Placement.SelectedWorkerID != "original" || explanation.Placement.ResourceEvaluations[0].State != "known" {
		t.Fatalf("lost durable decision: %+v", explanation)
	}
}

func TestResourceExplanationReportsDecision(t *testing.T) {
	now := time.Now().UTC()
	inventory := domain.WorkerInventory{ID: "worker-a", AcceptBacklog: true, Health: domain.WorkerHealthReady}
	v := newView(sqlite.CoordinatorRecords{}, []domain.WorkerSnapshot{{WorkerID: "worker-a", Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Minute), Inventory: inventory}}, nil, RuntimeInfo{MaxWorkerSnapshotAge: time.Minute}, now)
	var explanation Explanation
	v.addWorkerBlocker(&explanation, domain.Task{})
	raw, err := json.Marshal(explanation)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"placement"`, `"resourceEvaluations"`, `"selectedWorkerId":"worker-a"`, `"state":"unknown"`} {
		if !strings.Contains(string(raw), want) {
			t.Errorf("explain lacks %s: %s", want, raw)
		}
	}
}

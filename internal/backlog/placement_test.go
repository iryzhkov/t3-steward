package backlog

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

var placementTestTime = time.Date(2026, 9, 10, 18, 0, 0, 0, time.UTC)

func TestMatchWorkersAlternateHostsDeterministically(t *testing.T) {
	task := placementTask()
	task.Placement.Hosts = []string{"normandy", "homelab"}
	inventory := []domain.WorkerInventory{
		placementWorker("normandy", "internet", "docker"),
		placementWorker("gaming-pc", "internet", "gpu"),
		placementWorker("homelab", "docker", "internet"),
	}

	result := mustMatchWorkers(t, task, inventory)
	if want := []string{"homelab", "normandy"}; !reflect.DeepEqual(result.EligibleWorkerIDs, want) {
		t.Fatalf("eligible workers = %#v, want %#v", result.EligibleWorkerIDs, want)
	}
	if got := evaluationCodes(t, result, "gaming-pc"); !reflect.DeepEqual(got, []string{ExclusionHostNotAllowed}) {
		t.Fatalf("gaming-pc exclusions = %#v", got)
	}
	if got := evaluationIDs(result); !reflect.DeepEqual(got, []string{"gaming-pc", "homelab", "normandy"}) {
		t.Fatalf("evaluation order = %#v", got)
	}
}

func TestMatchWorkersGPUCapability(t *testing.T) {
	task := placementTask()
	task.Placement.Capabilities = []string{"internet", "gpu"}
	inventory := []domain.WorkerInventory{
		placementWorker("normandy", "internet", "docker"),
		placementWorker("gaming-pc", "gpu", "internet"),
	}

	result := mustMatchWorkers(t, task, inventory)
	if want := []string{"gaming-pc"}; !reflect.DeepEqual(result.EligibleWorkerIDs, want) {
		t.Fatalf("eligible workers = %#v, want %#v", result.EligibleWorkerIDs, want)
	}
	if got := evaluationCodes(t, result, "normandy"); !reflect.DeepEqual(got, []string{ExclusionMissingCapability}) {
		t.Fatalf("normandy exclusions = %#v", got)
	}
	evaluation := evaluationFor(t, result, "normandy")
	if !strings.Contains(evaluation.Exclusions[0].Detail, `"gpu"`) {
		t.Fatalf("missing-capability detail = %q", evaluation.Exclusions[0].Detail)
	}
}

func TestMatchWorkersBacklogOptIn(t *testing.T) {
	laptop := placementWorker("laptop", "mobile")
	laptop.AcceptBacklog = false
	task := placementTask()
	task.Placement.Capabilities = []string{"mobile"}

	ordinary := mustMatchWorkers(t, task, []domain.WorkerInventory{laptop})
	if len(ordinary.EligibleWorkerIDs) != 0 {
		t.Fatalf("ordinary eligible workers = %#v", ordinary.EligibleWorkerIDs)
	}
	if got := evaluationCodes(t, ordinary, "laptop"); !reflect.DeepEqual(got, []string{ExclusionBacklogDisabled}) {
		t.Fatalf("ordinary exclusions = %#v", got)
	}

	task.Placement.Hosts = []string{"laptop"}
	optedIn := mustMatchWorkers(t, task, []domain.WorkerInventory{laptop})
	if want := []string{"laptop"}; !reflect.DeepEqual(optedIn.EligibleWorkerIDs, want) {
		t.Fatalf("opted-in eligible workers = %#v, want %#v", optedIn.EligibleWorkerIDs, want)
	}
}

func TestMatchWorkersOfflineStaleAndEveryExclusion(t *testing.T) {
	task := placementTask()
	task.Placement.Hosts = []string{"another-host"}
	task.Placement.Capabilities = []string{"docker", "gpu"}
	worker := placementWorker("worker", "docker")
	worker.AcceptBacklog = false
	worker.Health = domain.WorkerHealthOffline
	worker.ObservedAt = placementTestTime.Add(-11 * time.Minute)
	worker.Projects[0].Available = false

	result := mustMatchWorkers(t, task, []domain.WorkerInventory{worker})
	want := []string{
		ExclusionBacklogDisabled,
		ExclusionHostNotAllowed,
		ExclusionMissingCapability,
		ExclusionProjectUnavailable,
		ExclusionWorkerHealth,
		ExclusionWorkerStale,
	}
	if got := evaluationCodes(t, result, "worker"); !reflect.DeepEqual(got, want) {
		t.Fatalf("exclusions = %#v, want every reason %#v", got, want)
	}
}

func TestMatchWorkersProviderInventoryDoesNotAffectPlacement(t *testing.T) {
	task := placementTask()
	worker := placementWorker("normandy", "internet")
	worker.Providers = []domain.WorkerProviderInventory{{
		InstanceID: "codex", Models: []string{"gpt-5.6-sol"}, Available: false,
	}}

	result := mustMatchWorkers(t, task, []domain.WorkerInventory{worker})
	if want := []string{"normandy"}; !reflect.DeepEqual(result.EligibleWorkerIDs, want) {
		t.Fatalf("eligible workers = %#v, want provider-independent placement", result.EligibleWorkerIDs)
	}
}

func TestMatchWorkersRequiresProjectInventory(t *testing.T) {
	task := placementTask()
	worker := placementWorker("normandy", "internet")
	worker.Projects = nil

	result := mustMatchWorkers(t, task, []domain.WorkerInventory{worker})
	if got := evaluationCodes(t, result, "normandy"); !reflect.DeepEqual(got, []string{ExclusionProjectUnavailable}) {
		t.Fatalf("exclusions = %#v", got)
	}
}

func TestMatchWorkersRejectsInvalidInventory(t *testing.T) {
	tests := []struct {
		name      string
		inventory []domain.WorkerInventory
		want      string
	}{
		{
			name: "duplicate worker",
			inventory: []domain.WorkerInventory{
				placementWorker("normandy", "internet"),
				placementWorker("normandy", "internet"),
			},
			want: "duplicate worker ID",
		},
		{
			name: "duplicate capability",
			inventory: []domain.WorkerInventory{
				placementWorker("normandy", "internet", "internet"),
			},
			want: "repeats capability",
		},
		{
			name: "invalid health",
			inventory: []domain.WorkerInventory{
				func() domain.WorkerInventory {
					worker := placementWorker("normandy", "internet")
					worker.Health = "unknown"
					return worker
				}(),
			},
			want: "invalid health",
		},
		{
			name: "duplicate project",
			inventory: []domain.WorkerInventory{
				func() domain.WorkerInventory {
					worker := placementWorker("normandy", "internet")
					worker.Projects = append(worker.Projects, worker.Projects[0])
					return worker
				}(),
			},
			want: "repeats project",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := MatchWorkers(placementRequest(placementTask()), test.inventory)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestMatchWorkersValidatesRequest(t *testing.T) {
	request := placementRequest(placementTask())
	request.MaxSnapshotAge = 0
	if _, err := MatchWorkers(request, nil); err == nil || !strings.Contains(err.Error(), "snapshot age") {
		t.Fatalf("invalid age error = %v", err)
	}
	request.MaxSnapshotAge = 10 * time.Minute
	request.Now = time.Time{}
	if _, err := MatchWorkers(request, nil); err == nil || !strings.Contains(err.Error(), "current time") {
		t.Fatalf("missing time error = %v", err)
	}
}

func placementTask() domain.Task {
	return domain.Task{
		ID: "task", WorkflowID: "workflow", Name: "task",
		Placement: domain.Placement{Capabilities: []string{"internet"}},
	}
}

func placementWorker(id string, capabilities ...string) domain.WorkerInventory {
	return domain.WorkerInventory{
		ID: id, AcceptBacklog: true, Health: domain.WorkerHealthReady,
		Capabilities: append([]string(nil), capabilities...),
		Projects: []domain.WorkerProjectInventory{{
			Name: "project", Available: true, Revision: "abc123", UpdatedAt: placementTestTime,
		}},
		ObservedAt: placementTestTime,
	}
}

func placementRequest(task domain.Task) WorkerPlacementRequest {
	return WorkerPlacementRequest{
		Task: task, Project: "project", Now: placementTestTime, MaxSnapshotAge: 10 * time.Minute,
	}
}

func mustMatchWorkers(t *testing.T, task domain.Task, inventory []domain.WorkerInventory) WorkerPlacement {
	t.Helper()
	result, err := MatchWorkers(placementRequest(task), inventory)
	if err != nil {
		t.Fatalf("match workers: %v", err)
	}
	return result
}

func evaluationFor(t *testing.T, result WorkerPlacement, workerID string) WorkerEvaluation {
	t.Helper()
	for _, evaluation := range result.Evaluations {
		if evaluation.WorkerID == workerID {
			return evaluation
		}
	}
	t.Fatalf("worker %q has no evaluation", workerID)
	return WorkerEvaluation{}
}

func evaluationCodes(t *testing.T, result WorkerPlacement, workerID string) []string {
	t.Helper()
	evaluation := evaluationFor(t, result, workerID)
	codes := make([]string, 0, len(evaluation.Exclusions))
	for _, exclusion := range evaluation.Exclusions {
		codes = append(codes, exclusion.Code)
	}
	return codes
}

func evaluationIDs(result WorkerPlacement) []string {
	ids := make([]string, 0, len(result.Evaluations))
	for _, evaluation := range result.Evaluations {
		ids = append(ids, evaluation.WorkerID)
	}
	return ids
}

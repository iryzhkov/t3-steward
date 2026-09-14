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

func TestMatchWorkersCPUClassIsAHardFloor(t *testing.T) {
	task := placementTask()
	task.ResourceDemand = domain.ResourceDemand{
		MinCPUClass: domain.CPUClassMedium, PreferredCPUClass: domain.CPUClassHigh,
	}
	low := placementWorker("normandy", "internet")
	low.CPUClass = domain.CPUClassLow
	medium := placementWorker("homelab", "internet")
	medium.CPUClass = domain.CPUClassMedium
	unclassified := placementWorker("laptop", "internet")

	result := mustMatchWorkers(t, task, []domain.WorkerInventory{low, medium, unclassified})
	if want := []string{"homelab"}; !reflect.DeepEqual(result.EligibleWorkerIDs, want) {
		t.Fatalf("eligible workers = %#v, want %#v", result.EligibleWorkerIDs, want)
	}
	if got := evaluationCodes(t, result, "normandy"); !reflect.DeepEqual(got, []string{ExclusionCPUClassBelowMinimum}) {
		t.Fatalf("normandy exclusions = %#v", got)
	}
	if got := evaluationCodes(t, result, "laptop"); !reflect.DeepEqual(got, []string{ExclusionCPUClassUnknown}) {
		t.Fatalf("unclassified worker exclusions = %#v", got)
	}
}

func TestMatchWorkersCapacityIsAHardConstraintAndPressureIsNot(t *testing.T) {
	task := placementTask()
	task.ResourceDemand = domain.ResourceDemand{CPUUnits: 4, MemoryMB: 4096}

	busy := placementWorker("homelab", "internet")
	busy.Allocatable = domain.AllocatableCapacity{ExecutorSlots: 4, CPUUnits: 8, MemoryMB: 8192}
	busy.Reserved = domain.ReservedCapacity{ExecutorSlots: 4, CPUUnits: 8, MemoryMB: 8192}

	// A fully loaded worker with free allocatable capacity stays eligible:
	// pressure is an observation that scores a candidate, never a limit that
	// admits or refuses work.
	pressured := placementWorker("omarchy-pc", "internet")
	pressured.Allocatable = domain.AllocatableCapacity{ExecutorSlots: 8, CPUUnits: 16, MemoryMB: 16384}
	pressured.Pressure = domain.CPUPressure{Normalized: 1, Load: 42}

	unconfigured := placementWorker("laptop", "internet")

	result := mustMatchWorkers(t, task, []domain.WorkerInventory{busy, pressured, unconfigured})
	if want := []string{"omarchy-pc"}; !reflect.DeepEqual(result.EligibleWorkerIDs, want) {
		t.Fatalf("eligible workers = %#v, want %#v", result.EligibleWorkerIDs, want)
	}
	// Every dimension that refused the task is reported, not only the first:
	// homelab is short of slots, cpu units and memory at once.
	if got := evaluationCodes(t, result, "homelab"); len(got) != 3 {
		t.Fatalf("homelab exclusions = %#v, want one per exhausted dimension", got)
	}
	for _, workerID := range []string{"homelab", "laptop"} {
		for _, code := range evaluationCodes(t, result, workerID) {
			if code != ExclusionCapacityExhausted {
				t.Fatalf("%s exclusion = %q, want %q", workerID, code, ExclusionCapacityExhausted)
			}
		}
	}
	if got := evaluationCodes(t, result, "laptop"); len(got) != 1 {
		t.Fatalf("unconfigured worker exclusions = %#v, want one capacity refusal", got)
	}

	// A task that declares no demand is never excluded for capacity it did
	// not ask for.
	undemanding := mustMatchWorkers(t, placementTask(), []domain.WorkerInventory{busy})
	if want := []string{"homelab"}; !reflect.DeepEqual(undemanding.EligibleWorkerIDs, want) {
		t.Fatalf("eligible workers for an undemanding task = %#v, want %#v", undemanding.EligibleWorkerIDs, want)
	}
}

func TestMatchWorkersStaleOrSupersededCapacityCannotAdmitWork(t *testing.T) {
	task := placementTask()
	task.ResourceDemand = domain.ResourceDemand{CPUUnits: 1}

	stale := placementWorker("homelab", "internet")
	stale.Epoch = "worker-7"
	stale.Allocatable = domain.AllocatableCapacity{ExecutorSlots: 4, CPUUnits: 8}
	stale.ObservedAt = placementTestTime.Add(-11 * time.Minute)

	superseded := placementWorker("omarchy-pc", "internet")
	superseded.Epoch = "worker-3"
	superseded.Allocatable = domain.AllocatableCapacity{ExecutorSlots: 8, CPUUnits: 16}

	request := placementRequest(task)
	request.ExpectedWorkerEpochs = map[string]string{"homelab": "worker-7", "omarchy-pc": "worker-4"}
	result, err := MatchWorkers(request, []domain.WorkerInventory{stale, superseded})
	if err != nil {
		t.Fatalf("match workers: %v", err)
	}
	if len(result.EligibleWorkerIDs) != 0 {
		t.Fatalf("eligible workers = %#v, want none", result.EligibleWorkerIDs)
	}
	if got := evaluationCodes(t, result, "homelab"); !reflect.DeepEqual(got, []string{ExclusionWorkerStale}) {
		t.Fatalf("stale worker exclusions = %#v", got)
	}
	supersededEvaluation := evaluationFor(t, result, "omarchy-pc")
	if got := evaluationCodes(t, result, "omarchy-pc"); !reflect.DeepEqual(got, []string{ExclusionWorkerEpochSuperseded}) {
		t.Fatalf("superseded worker exclusions = %#v", got)
	}
	if !strings.Contains(supersededEvaluation.Exclusions[0].Detail, `"worker-4"`) {
		t.Fatalf("superseded detail = %q, want the expected epoch named", supersededEvaluation.Exclusions[0].Detail)
	}
}

func TestSelectWorkerPrefersTheLowestSuitableCPUClass(t *testing.T) {
	task := placementTask()
	task.ResourceDemand = domain.ResourceDemand{MinCPUClass: domain.CPUClassLow}

	selection, err := SelectWorker(placementRequest(task), placementFleet())
	if err != nil {
		t.Fatalf("select worker: %v", err)
	}
	if selection.Decision.SelectedWorkerID != "normandy" {
		t.Fatalf("selected worker = %q, want the lowest suitable class to keep high-class headroom free",
			selection.Decision.SelectedWorkerID)
	}
	if len(selection.Decision.Scores) != 3 {
		t.Fatalf("scores = %#v, want one per eligible worker", selection.Decision.Scores)
	}
	if low, high := selectionScore(t, selection, "normandy"), selectionScore(t, selection, "omarchy-pc"); low <= high {
		t.Fatalf("low-class score %v did not beat high-class score %v for light work", low, high)
	}
}

func TestSelectWorkerExplainsAPlacementFromDurableState(t *testing.T) {
	task := placementTask()
	task.ID = "build"
	task.ResourceDemand = domain.ResourceDemand{
		MinCPUClass: domain.CPUClassMedium, PreferredCPUClass: domain.CPUClassHigh, CPUUnits: 2,
	}

	selection, err := SelectWorker(placementRequest(task), placementFleet())
	if err != nil {
		t.Fatalf("select worker: %v", err)
	}
	decision := selection.Decision
	if decision.SelectedWorkerID != "omarchy-pc" {
		t.Fatalf("selected worker = %q, want the preferred class for build work", decision.SelectedWorkerID)
	}
	if want := []string{"homelab", "normandy", "omarchy-pc"}; !reflect.DeepEqual(decision.CandidateIDs, want) {
		t.Fatalf("candidates = %#v, want every evaluated worker %#v", decision.CandidateIDs, want)
	}
	if len(decision.Rejections) != 1 ||
		decision.Rejections[0].WorkerID != "normandy" ||
		decision.Rejections[0].Code != ExclusionCPUClassBelowMinimum {
		t.Fatalf("rejections = %#v, want the class floor that removed normandy", decision.Rejections)
	}
	if len(decision.Snapshots) != 3 || decision.Snapshots[0].WorkerID != "homelab" {
		t.Fatalf("snapshot references = %#v, want one per evaluated worker", decision.Snapshots)
	}
	score := scoreFor(decision.Scores, "omarchy-pc")
	if len(score.Components) != len(preferenceOrder) {
		t.Fatalf("score components = %#v, want every named component reported", score.Components)
	}
	for _, component := range score.Components {
		switch component.Name {
		case PreferenceWarmCache, PreferenceQueueDepth, PreferenceEnergyCost, PreferenceOperatorAffinity:
			if component.Weight != 0 {
				t.Fatalf("seam component %q carries weight %v", component.Name, component.Weight)
			}
		}
	}

	reservation := domain.ResourceReservation{
		ID: "reservation-1", WorkerID: "omarchy-pc", PoolName: "default",
		SlotFencingToken: "token", AssignmentID: "assignment-1", AttemptID: "attempt-1",
	}
	placed, err := selection.WithReservation(reservation)
	if err != nil {
		t.Fatalf("record reservation: %v", err)
	}
	if placed.ReservationID != "reservation-1" || placed.AssignmentID != "assignment-1" {
		t.Fatalf("placement decision after reservation = %#v", placed)
	}
	elsewhere := reservation
	elsewhere.WorkerID = "homelab"
	if _, err := selection.WithReservation(elsewhere); err == nil {
		t.Fatal("a reservation on an unselected worker was accepted")
	}
}

func TestSelectWorkerWithoutAnEligibleWorkerStillExplainsItself(t *testing.T) {
	task := placementTask()
	task.ResourceDemand = domain.ResourceDemand{MinCPUClass: domain.CPUClassHigh, CPUUnits: 64}
	worker := placementWorker("normandy", "internet")
	worker.CPUClass = domain.CPUClassLow

	selection, err := SelectWorker(placementRequest(task), []domain.WorkerInventory{worker})
	if err != nil {
		t.Fatalf("select worker: %v", err)
	}
	if selection.Decision.Placed() || selection.Decision.ReservationID != "" {
		t.Fatalf("decision = %#v, want no placement", selection.Decision)
	}
	if len(selection.Decision.Rejections) != 2 {
		t.Fatalf("rejections = %#v, want both the class floor and the capacity refusal", selection.Decision.Rejections)
	}
	if _, err := selection.WithReservation(domain.ResourceReservation{ID: "r", WorkerID: "normandy"}); err == nil {
		t.Fatal("an unplaced decision accepted a reservation")
	}
}

func TestSelectWorkerRejectsContradictoryDemand(t *testing.T) {
	task := placementTask()
	task.ResourceDemand = domain.ResourceDemand{
		MinCPUClass: domain.CPUClassHigh, PreferredCPUClass: domain.CPUClassLow,
	}
	if _, err := SelectWorker(placementRequest(task), placementFleet()); err == nil ||
		!strings.Contains(err.Error(), "below its minimum") {
		t.Fatalf("contradictory demand error = %v", err)
	}
}

func placementFleet() []domain.WorkerInventory {
	normandy := placementWorker("normandy", "internet")
	normandy.CPUClass = domain.CPUClassLow
	normandy.Allocatable = domain.AllocatableCapacity{ExecutorSlots: 2, CPUUnits: 4, MemoryMB: 4096}
	homelab := placementWorker("homelab", "internet")
	homelab.CPUClass = domain.CPUClassMedium
	homelab.Allocatable = domain.AllocatableCapacity{ExecutorSlots: 4, CPUUnits: 8, MemoryMB: 8192}
	omarchy := placementWorker("omarchy-pc", "internet")
	omarchy.CPUClass = domain.CPUClassHigh
	omarchy.Allocatable = domain.AllocatableCapacity{ExecutorSlots: 8, CPUUnits: 16, MemoryMB: 16384}
	return []domain.WorkerInventory{normandy, homelab, omarchy}
}

func selectionScore(t *testing.T, selection PlacementSelection, workerID string) float64 {
	t.Helper()
	for _, score := range selection.Decision.Scores {
		if score.WorkerID == workerID {
			return score.Total
		}
	}
	t.Fatalf("worker %q has no score", workerID)
	return 0
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

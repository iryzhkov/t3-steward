package backlog

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

const (
	ExclusionBacklogDisabled    = "backlog-disabled"
	ExclusionHostNotAllowed     = "host-not-allowed"
	ExclusionMissingCapability  = "missing-capability"
	ExclusionProjectUnavailable = "project-unavailable"
	ExclusionWorkerHealth       = "worker-health"
	ExclusionWorkerStale        = "worker-stale"
)

// WorkerPlacementRequest is the immutable input to worker capability matching.
type WorkerPlacementRequest struct {
	Task           domain.Task
	Project        string
	Now            time.Time
	MaxSnapshotAge time.Duration
}

// WorkerExclusion explains one independent reason a worker cannot run a task.
type WorkerExclusion struct {
	Code   string `json:"code"`
	Detail string `json:"detail"`
}

// WorkerEvaluation records the complete placement decision for one worker.
type WorkerEvaluation struct {
	WorkerID   string            `json:"workerId"`
	Eligible   bool              `json:"eligible"`
	Exclusions []WorkerExclusion `json:"exclusions,omitempty"`
}

// WorkerPlacement is a deterministic capability-matching result. Provider
// availability is deliberately absent and belongs to the later routing phase.
type WorkerPlacement struct {
	EligibleWorkerIDs []string           `json:"eligibleWorkerIds,omitempty"`
	Evaluations       []WorkerEvaluation `json:"evaluations"`
}

// MatchWorkers evaluates every worker independently and reports every exclusion.
func MatchWorkers(request WorkerPlacementRequest, inventory []domain.WorkerInventory) (WorkerPlacement, error) {
	if request.Now.IsZero() {
		return WorkerPlacement{}, fmt.Errorf("match workers: current time is required")
	}
	if request.MaxSnapshotAge <= 0 {
		return WorkerPlacement{}, fmt.Errorf("match workers: maximum snapshot age must be positive")
	}

	workers := append([]domain.WorkerInventory(nil), inventory...)
	sort.Slice(workers, func(i, j int) bool { return workers[i].ID < workers[j].ID })
	seenWorkers := make(map[string]struct{}, len(workers))
	result := WorkerPlacement{Evaluations: make([]WorkerEvaluation, 0, len(workers))}
	for _, worker := range workers {
		if worker.ID == "" {
			return WorkerPlacement{}, fmt.Errorf("match workers: worker ID is required")
		}
		if _, duplicate := seenWorkers[worker.ID]; duplicate {
			return WorkerPlacement{}, fmt.Errorf("match workers: duplicate worker ID %q", worker.ID)
		}
		seenWorkers[worker.ID] = struct{}{}
		if err := validateWorkerInventory(worker); err != nil {
			return WorkerPlacement{}, err
		}

		evaluation := evaluateWorker(request, worker)
		result.Evaluations = append(result.Evaluations, evaluation)
		if evaluation.Eligible {
			result.EligibleWorkerIDs = append(result.EligibleWorkerIDs, worker.ID)
		}
	}
	return result, nil
}

func evaluateWorker(request WorkerPlacementRequest, worker domain.WorkerInventory) WorkerEvaluation {
	var exclusions []WorkerExclusion
	explicitHost := stringSet(request.Task.Placement.Hosts)
	if len(explicitHost) != 0 {
		if _, allowed := explicitHost[worker.ID]; !allowed {
			exclusions = append(exclusions, WorkerExclusion{
				Code:   ExclusionHostNotAllowed,
				Detail: fmt.Sprintf("worker %q is not in the task host allowlist", worker.ID),
			})
		}
	}
	if !worker.AcceptBacklog {
		if _, optedIn := explicitHost[worker.ID]; !optedIn {
			exclusions = append(exclusions, WorkerExclusion{
				Code:   ExclusionBacklogDisabled,
				Detail: fmt.Sprintf("worker %q does not accept backlog work without explicit task opt-in", worker.ID),
			})
		}
	}
	if worker.Health != domain.WorkerHealthReady {
		exclusions = append(exclusions, WorkerExclusion{
			Code:   ExclusionWorkerHealth,
			Detail: fmt.Sprintf("worker %q health is %q", worker.ID, worker.Health),
		})
	}
	if worker.ObservedAt.IsZero() || request.Now.Sub(worker.ObservedAt) > request.MaxSnapshotAge {
		exclusions = append(exclusions, WorkerExclusion{
			Code:   ExclusionWorkerStale,
			Detail: fmt.Sprintf("worker %q inventory is older than %s", worker.ID, request.MaxSnapshotAge),
		})
	}

	capabilities := stringSet(worker.Capabilities)
	required := append([]string(nil), request.Task.Placement.Capabilities...)
	sort.Strings(required)
	for _, capability := range required {
		if _, available := capabilities[capability]; !available {
			exclusions = append(exclusions, WorkerExclusion{
				Code:   ExclusionMissingCapability,
				Detail: fmt.Sprintf("worker %q lacks capability %q", worker.ID, capability),
			})
		}
	}
	if request.Project != "" {
		projectAvailable := false
		for _, project := range worker.Projects {
			if project.Name == request.Project {
				projectAvailable = project.Available
				break
			}
		}
		if !projectAvailable {
			exclusions = append(exclusions, WorkerExclusion{
				Code:   ExclusionProjectUnavailable,
				Detail: fmt.Sprintf("worker %q cannot prepare project %q", worker.ID, request.Project),
			})
		}
	}

	sort.Slice(exclusions, func(i, j int) bool {
		if exclusions[i].Code == exclusions[j].Code {
			return exclusions[i].Detail < exclusions[j].Detail
		}
		return exclusions[i].Code < exclusions[j].Code
	})
	return WorkerEvaluation{
		WorkerID: worker.ID, Eligible: len(exclusions) == 0, Exclusions: exclusions,
	}
}

func validateWorkerInventory(worker domain.WorkerInventory) error {
	if worker.Health != domain.WorkerHealthReady &&
		worker.Health != domain.WorkerHealthDegraded &&
		worker.Health != domain.WorkerHealthOffline {
		return fmt.Errorf("match workers: worker %q has invalid health %q", worker.ID, worker.Health)
	}
	if duplicate := firstDuplicate(worker.Capabilities); duplicate != "" {
		return fmt.Errorf("match workers: worker %q repeats capability %q", worker.ID, duplicate)
	}
	projectNames := make([]string, 0, len(worker.Projects))
	for _, project := range worker.Projects {
		if strings.TrimSpace(project.Name) == "" {
			return fmt.Errorf("match workers: worker %q has a project with no name", worker.ID)
		}
		projectNames = append(projectNames, project.Name)
	}
	if duplicate := firstDuplicate(projectNames); duplicate != "" {
		return fmt.Errorf("match workers: worker %q repeats project %q", worker.ID, duplicate)
	}
	providerIDs := make([]string, 0, len(worker.Providers))
	for _, provider := range worker.Providers {
		if strings.TrimSpace(provider.InstanceID) == "" {
			return fmt.Errorf("match workers: worker %q has a provider with no instance ID", worker.ID)
		}
		providerIDs = append(providerIDs, provider.InstanceID)
		if duplicate := firstDuplicate(provider.Models); duplicate != "" {
			return fmt.Errorf("match workers: worker %q provider %q repeats model %q", worker.ID, provider.InstanceID, duplicate)
		}
	}
	if duplicate := firstDuplicate(providerIDs); duplicate != "" {
		return fmt.Errorf("match workers: worker %q repeats provider %q", worker.ID, duplicate)
	}
	return nil
}

func stringSet(values []string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}

func firstDuplicate(values []string) string {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if _, duplicate := seen[value]; duplicate {
			return value
		}
		seen[value] = struct{}{}
	}
	return ""
}

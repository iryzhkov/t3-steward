package backlog

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

const (
	ExclusionBacklogDisabled       = "backlog-disabled"
	ExclusionHostNotAllowed        = "host-not-allowed"
	ExclusionMissingCapability     = "missing-capability"
	ExclusionProjectUnavailable    = "project-unavailable"
	ExclusionWorkerHealth          = "worker-health"
	ExclusionWorkerStale           = "worker-stale"
	ExclusionWorkerEpochSuperseded = "worker-epoch-superseded"
	ExclusionCPUClassBelowMinimum  = "cpu-class-below-minimum"
	ExclusionCPUClassUnknown       = "cpu-class-unknown"
	ExclusionCapacityExhausted     = "capacity-exhausted"
)

// Preference component names. A component scores a surviving candidate; it can
// never admit a worker that hard-constraint filtering rejected.
//
// The first three are weighted. The remainder are declared seams: they are
// named and reported with zero weight so that an operator policy can weight
// them later without changing the shape of a placement explanation, and they
// do not influence the ranking today.
const (
	PreferenceCPUClassFit      = "cpu-class-fit"
	PreferenceCPUHeadroom      = "cpu-headroom"
	PreferenceDataLocality     = "data-locality"
	PreferenceWarmCache        = "warm-cache"
	PreferenceQueueDepth       = "queue-depth"
	PreferenceEnergyCost       = "energy-cost"
	PreferenceOperatorAffinity = "operator-affinity"
)

// DefaultPreferenceWeights is the shipped preference policy. Class fit
// dominates so that work which does not need high performance leaves
// high-class headroom free, and observed headroom breaks ties between workers
// of an equally suitable class.
func DefaultPreferenceWeights() map[string]float64 {
	return map[string]float64{
		PreferenceCPUClassFit:      1,
		PreferenceCPUHeadroom:      0.5,
		PreferenceDataLocality:     0.25,
		PreferenceWarmCache:        0,
		PreferenceQueueDepth:       0,
		PreferenceEnergyCost:       0,
		PreferenceOperatorAffinity: 0,
	}
}

// preferenceOrder fixes the order components are reported in, so a placement
// explanation is byte-stable for the same inputs.
var preferenceOrder = []string{
	PreferenceCPUClassFit,
	PreferenceCPUHeadroom,
	PreferenceDataLocality,
	PreferenceWarmCache,
	PreferenceQueueDepth,
	PreferenceEnergyCost,
	PreferenceOperatorAffinity,
}

// WorkerPlacementRequest is the immutable input to worker capability matching.
type WorkerPlacementRequest struct {
	Task           domain.Task
	Project        string
	Now            time.Time
	MaxSnapshotAge time.Duration
	// ExpectedWorkerEpochs, when it names a worker, is the epoch the
	// coordinator last accepted for it. A snapshot from any other epoch has
	// been superseded and cannot admit work.
	ExpectedWorkerEpochs map[string]string
	// PreferenceWeights overrides DefaultPreferenceWeights for scoring. It
	// never affects hard-constraint filtering.
	PreferenceWeights map[string]float64
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

	exclusions = append(exclusions, epochExclusions(request, worker)...)
	exclusions = append(exclusions, capacityExclusions(request.Task.ResourceDemand, worker)...)

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

// epochExclusions rejects a snapshot the coordinator has already superseded.
// Staleness by age is reported separately, because an old snapshot and a
// snapshot from a replaced worker epoch are different facts.
func epochExclusions(request WorkerPlacementRequest, worker domain.WorkerInventory) []WorkerExclusion {
	expected, declared := request.ExpectedWorkerEpochs[worker.ID]
	if !declared || expected == worker.Epoch {
		return nil
	}
	return []WorkerExclusion{{
		Code: ExclusionWorkerEpochSuperseded,
		Detail: fmt.Sprintf("worker %q reports epoch %q, superseded by %q",
			worker.ID, worker.Epoch, expected),
	}}
}

// capacityExclusions applies the CPU-class floor and the allocatable-capacity
// constraints. A task that declares no demand constrains nothing, so a worker
// is never excluded for capacity it was not asked to provide.
//
// Only the class floor and allocatable capacity are hard constraints here.
// Observed pressure is deliberately absent: it is an observation that scores a
// candidate, and admitting or refusing work on live load alone would make the
// resource model a load average.
func capacityExclusions(demand domain.ResourceDemand, worker domain.WorkerInventory) []WorkerExclusion {
	if demand.IsZero() {
		return nil
	}
	var exclusions []WorkerExclusion
	if demand.MinCPUClass != "" {
		switch {
		case !worker.CPUClass.Valid():
			exclusions = append(exclusions, WorkerExclusion{
				Code: ExclusionCPUClassUnknown,
				Detail: fmt.Sprintf("worker %q has no configured cpu class and the task requires at least %q",
					worker.ID, string(demand.MinCPUClass)),
			})
		case worker.CPUClass.Compare(demand.MinCPUClass) < 0:
			exclusions = append(exclusions, WorkerExclusion{
				Code: ExclusionCPUClassBelowMinimum,
				Detail: fmt.Sprintf("worker %q cpu class %q is below the required minimum %q",
					worker.ID, string(worker.CPUClass), string(demand.MinCPUClass)),
			})
		}
	}

	snapshot := worker.CapacitySnapshot()
	sized := demand.CPUUnits > 0 || demand.MemoryMB > 0 || demand.ScratchMB > 0
	if sized && snapshot.Allocatable.IsZero() {
		return append(exclusions, WorkerExclusion{
			Code:   ExclusionCapacityExhausted,
			Detail: fmt.Sprintf("worker %q declares no allocatable capacity for a sized task", worker.ID),
		})
	}
	if sized && snapshot.FreeSlots() < 1 {
		exclusions = append(exclusions, WorkerExclusion{
			Code: ExclusionCapacityExhausted,
			Detail: fmt.Sprintf("worker %q has no free executor slot of %d",
				worker.ID, snapshot.Allocatable.ExecutorSlots),
		})
	}
	if demand.CPUUnits > 0 && snapshot.FreeCPUUnits() < demand.CPUUnits {
		exclusions = append(exclusions, WorkerExclusion{
			Code: ExclusionCapacityExhausted,
			Detail: fmt.Sprintf("worker %q has %v free cpu units of %v allocatable, task requires %v",
				worker.ID, snapshot.FreeCPUUnits(), snapshot.Allocatable.CPUUnits, demand.CPUUnits),
		})
	}
	if demand.MemoryMB > 0 && snapshot.FreeMemoryMB() < demand.MemoryMB {
		exclusions = append(exclusions, WorkerExclusion{
			Code: ExclusionCapacityExhausted,
			Detail: fmt.Sprintf("worker %q has %d MB free memory of %d allocatable, task requires %d MB",
				worker.ID, snapshot.FreeMemoryMB(), snapshot.Allocatable.MemoryMB, demand.MemoryMB),
		})
	}
	if demand.ScratchMB > 0 && snapshot.FreeScratchMB() < demand.ScratchMB {
		exclusions = append(exclusions, WorkerExclusion{
			Code: ExclusionCapacityExhausted,
			Detail: fmt.Sprintf("worker %q has %d MB free scratch of %d allocatable, task requires %d MB",
				worker.ID, snapshot.FreeScratchMB(), snapshot.Allocatable.ScratchMB, demand.ScratchMB),
		})
	}
	return exclusions
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
	if err := worker.CPUClass.Validate(); err != nil {
		return fmt.Errorf("match workers: worker %q %w", worker.ID, err)
	}
	if err := worker.Allocatable.Validate(); err != nil {
		return fmt.Errorf("match workers: worker %q %w", worker.ID, err)
	}
	if err := worker.Pressure.Validate(); err != nil {
		return fmt.Errorf("match workers: worker %q %w", worker.ID, err)
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

// PlacementSelection is one complete placement: the independent per-worker
// evaluation, the preference ranking of the survivors, and the durable
// explanation a placement trace is read from.
type PlacementSelection struct {
	Placement WorkerPlacement          `json:"placement"`
	Decision  domain.PlacementDecision `json:"decision"`
}

// SelectWorker runs the three placement phases in order: hard-constraint
// filtering through MatchWorkers, then preference scoring of the survivors,
// then selection of the worker a reservation should be acquired on. It does
// not acquire the reservation: capacity is committed by the executor registry
// in the same transaction that commits the assignment, and the reservation ID
// is recorded on the returned decision with WithReservation.
//
// A selection with no eligible worker is not an error. It returns a decision
// that names every candidate and every rejection, so a queued task explains
// itself from the same durable evidence a placed task does.
func SelectWorker(request WorkerPlacementRequest, inventory []domain.WorkerInventory) (PlacementSelection, error) {
	if err := request.Task.ResourceDemand.Validate(); err != nil {
		return PlacementSelection{}, fmt.Errorf("select worker: %w", err)
	}
	placement, err := MatchWorkers(request, inventory)
	if err != nil {
		return PlacementSelection{}, err
	}

	workers := make(map[string]domain.WorkerInventory, len(inventory))
	for _, worker := range inventory {
		workers[worker.ID] = worker
	}
	decision := domain.PlacementDecision{
		TaskID: request.Task.ID, Demand: request.Task.ResourceDemand, DecidedAt: request.Now,
	}
	weights := request.PreferenceWeights
	if weights == nil {
		weights = DefaultPreferenceWeights()
	}
	for _, evaluation := range placement.Evaluations {
		worker := workers[evaluation.WorkerID]
		decision.CandidateIDs = append(decision.CandidateIDs, evaluation.WorkerID)
		snapshot := worker.CapacitySnapshot()
		decision.Snapshots = append(decision.Snapshots, domain.CapacitySnapshotRef{
			WorkerID:    snapshot.WorkerID,
			WorkerEpoch: snapshot.WorkerEpoch,
			Sequence:    snapshot.Sequence,
			ObservedAt:  snapshot.ObservedAt,
		})
		for _, exclusion := range evaluation.Exclusions {
			decision.Rejections = append(decision.Rejections, domain.PlacementRejection{
				WorkerID: evaluation.WorkerID, Code: exclusion.Code, Detail: exclusion.Detail,
			})
		}
		if !evaluation.Eligible {
			continue
		}
		decision.Scores = append(decision.Scores, scoreWorker(request.Task, worker, weights))
	}

	for _, score := range decision.Scores {
		if decision.SelectedWorkerID == "" {
			decision.SelectedWorkerID = score.WorkerID
			continue
		}
		best := scoreFor(decision.Scores, decision.SelectedWorkerID)
		// Evaluations are ordered by worker ID, so keeping the incumbent on a
		// tie selects the lowest ID and makes the choice reproducible.
		if score.Total > best.Total {
			decision.SelectedWorkerID = score.WorkerID
		}
	}
	if err := decision.Validate(); err != nil {
		return PlacementSelection{}, fmt.Errorf("select worker: %w", err)
	}
	return PlacementSelection{Placement: placement, Decision: decision}, nil
}

// WithSelectedWorker records which eligible worker a later planning phase
// chose. Placement ranks workers on capability and preference; a planner may
// still reject the top-ranked one for a reason placement does not own, such as
// a held resource lock or a closed quota pool, and the explanation must name
// the worker the plan actually used.
func (s PlacementSelection) WithSelectedWorker(workerID string) (PlacementSelection, error) {
	selection := s
	selection.Decision.SelectedWorkerID = workerID
	if err := selection.Decision.Validate(); err != nil {
		return PlacementSelection{}, err
	}
	return selection, nil
}

// WithReservation records the reservation and assignment a committed placement
// acquired, completing the explanation: rejected constraints, preference
// scores, the selected worker and the reservation are then all recoverable
// from one durable record.
func (s PlacementSelection) WithReservation(reservation domain.ResourceReservation) (domain.PlacementDecision, error) {
	decision := s.Decision
	if decision.SelectedWorkerID == "" {
		return domain.PlacementDecision{}, fmt.Errorf("placement decision has no selected worker to reserve for")
	}
	if reservation.WorkerID != decision.SelectedWorkerID {
		return domain.PlacementDecision{}, fmt.Errorf(
			"reservation %q is on worker %q but placement selected %q",
			reservation.ID, reservation.WorkerID, decision.SelectedWorkerID)
	}
	decision.ReservationID = reservation.ID
	decision.AssignmentID = reservation.AssignmentID
	decision.AttemptID = reservation.AttemptID
	if err := decision.Validate(); err != nil {
		return domain.PlacementDecision{}, err
	}
	return decision, nil
}

func scoreFor(scores []domain.PlacementScore, workerID string) domain.PlacementScore {
	for _, score := range scores {
		if score.WorkerID == workerID {
			return score
		}
	}
	return domain.PlacementScore{}
}

// scoreWorker ranks one surviving candidate. Every component is reported, with
// its weight, so that a placement trace shows why one eligible worker beat
// another rather than only which one won.
func scoreWorker(task domain.Task, worker domain.WorkerInventory, weights map[string]float64) domain.PlacementScore {
	values := map[string]float64{
		PreferenceCPUClassFit:  cpuClassFit(task.ResourceDemand, worker.CPUClass),
		PreferenceCPUHeadroom:  worker.Pressure.Headroom(),
		PreferenceDataLocality: dataLocality(task, worker.ID),
	}
	score := domain.PlacementScore{WorkerID: worker.ID}
	for _, name := range preferenceOrder {
		component := domain.PlacementScoreComponent{
			Name: name, Weight: weights[name], Value: values[name],
		}
		score.Components = append(score.Components, component)
		score.Total += component.Weight * component.Value
	}
	return score
}

// cpuClassFit scores how well a worker's class matches what the task wants. A
// worker at the target class scores 1 and each class above it loses a quarter,
// so work that does not need high performance ranks a suitable lower-class
// worker first and leaves high-class headroom for work that needs it.
//
// A task that declares no class at all targets the lowest class, which is the
// same rule stated for the case where nothing was asked for. A worker below
// the target still scores, because a preferred class is a preference: the
// hard floor is min_cpu_class and it was applied before scoring.
func cpuClassFit(demand domain.ResourceDemand, class domain.CPUClass) float64 {
	if !class.Valid() {
		return 0
	}
	target := demand.TargetCPUClass()
	targetRank := domain.CPUClassLow.Rank()
	if target.Valid() {
		targetRank = target.Rank()
	}
	distance := class.Rank() - targetRank
	if distance < 0 {
		distance = -distance
	}
	fit := 1 - 0.25*float64(distance)
	if fit < 0 {
		return 0
	}
	return fit
}

// dataLocality is the share of the task's approved directory bindings that
// already resolve to this worker. It is a preference only: a binding that must
// pin placement does so through the resource binding itself, not through this
// score.
func dataLocality(task domain.Task, workerID string) float64 {
	if len(task.DirectoryBindings) == 0 {
		return 0
	}
	local := 0
	for _, binding := range task.DirectoryBindings {
		if binding.Identity.Registration.WorkerID == workerID {
			local++
		}
	}
	return float64(local) / float64(len(task.DirectoryBindings))
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

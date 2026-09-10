package backlog

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

const (
	PlanningBlockerTaskClass        = "task-class"
	PlanningBlockerTaskNotBefore    = "task-not-before"
	PlanningBlockerTaskExpired      = "task-expired"
	PlanningBlockerEstimateMissing  = "admission-estimate-missing"
	PlanningBlockerQuotaAdmission   = "quota-admission"
	PlanningBlockerQuotaCapacity    = "quota-capacity"
	PlanningBlockerSurplusWindow    = "surplus-window"
	PlanningBlockerDeadlineRunway   = "deadline-runway"
	PlanningBlockerQuotaDrainRunway = "quota-drain-runway"
)

// QuotaWindowBudget is an immutable snapshot of one quota pool window. Costs
// use one normalized unit chosen by the caller; every field in a window must use
// that same unit.
type QuotaWindowBudget struct {
	QuotaPoolID                 string
	WindowID                    string
	Admission                   domain.AdmissionState
	Capacity                    float64
	CurrentUsage                float64
	ForecastInteractiveUsage    float64
	ActiveConsumption           float64
	PausedRequiredWorkRemainder float64
	CommittedReservations       float64
	SafetyMargin                float64
	SurplusStartsAt             time.Time
	DrainAt                     time.Time
	ResetsAt                    time.Time
}

// TaskAdmissionEstimate describes the remaining work for one attempt rather
// than the original whole-task estimate.
type TaskAdmissionEstimate struct {
	RemainingCost    float64
	ExpectedRuntime  time.Duration
	CheckpointMargin time.Duration
}

type QuotaAdmissionInput struct {
	Windows   []QuotaWindowBudget
	Estimates map[string]TaskAdmissionEstimate
}

// QuotaAdmissionPolicy is immutable and safe to reuse across planning cycles.
// Each cycle receives private reservation accounting through StartPlan.
type QuotaAdmissionPolicy struct {
	windows   []QuotaWindowBudget
	estimates map[string]TaskAdmissionEstimate
}

type quotaAdmissionSession struct {
	now           time.Time
	windows       []QuotaWindowBudget
	estimates     map[string]TaskAdmissionEstimate
	batchReserved []float64
}

func NewQuotaAdmissionPolicy(input QuotaAdmissionInput) (QuotaAdmissionPolicy, error) {
	if len(input.Windows) == 0 {
		return QuotaAdmissionPolicy{}, fmt.Errorf("quota admission requires at least one quota window")
	}
	windows := append([]QuotaWindowBudget(nil), input.Windows...)
	sort.Slice(windows, func(i, j int) bool {
		if windows[i].QuotaPoolID == windows[j].QuotaPoolID {
			return windows[i].WindowID < windows[j].WindowID
		}
		return windows[i].QuotaPoolID < windows[j].QuotaPoolID
	})
	seen := make(map[string]struct{}, len(windows))
	for _, window := range windows {
		if err := validateQuotaWindow(window); err != nil {
			return QuotaAdmissionPolicy{}, err
		}
		key := quotaWindowKey(window)
		if _, duplicate := seen[key]; duplicate {
			return QuotaAdmissionPolicy{}, fmt.Errorf("quota admission repeats window %q", key)
		}
		seen[key] = struct{}{}
	}
	estimates := make(map[string]TaskAdmissionEstimate, len(input.Estimates))
	for attemptID, estimate := range input.Estimates {
		if strings.TrimSpace(attemptID) != attemptID || attemptID == "" {
			return QuotaAdmissionPolicy{}, fmt.Errorf("quota admission estimate attempt IDs must be nonempty and trimmed")
		}
		if !positiveFinite(estimate.RemainingCost) {
			return QuotaAdmissionPolicy{}, fmt.Errorf("quota admission estimate %q remaining cost must be positive and finite", attemptID)
		}
		if estimate.ExpectedRuntime <= 0 {
			return QuotaAdmissionPolicy{}, fmt.Errorf("quota admission estimate %q runtime must be positive", attemptID)
		}
		if estimate.CheckpointMargin < 0 {
			return QuotaAdmissionPolicy{}, fmt.Errorf("quota admission estimate %q checkpoint margin cannot be negative", attemptID)
		}
		estimates[attemptID] = estimate
	}
	return QuotaAdmissionPolicy{windows: windows, estimates: estimates}, nil
}

func (policy QuotaAdmissionPolicy) StartPlan(now time.Time) PlanningConstraintSession {
	return &quotaAdmissionSession{
		now:           now,
		windows:       append([]QuotaWindowBudget(nil), policy.windows...),
		estimates:     cloneAdmissionEstimates(policy.estimates),
		batchReserved: make([]float64, len(policy.windows)),
	}
}

func (session *quotaAdmissionSession) Evaluate(candidate PlanningCandidate) []PlanningBlocker {
	class, validClass := planningTaskClass(candidate.Task.Class)
	if !validClass {
		return []PlanningBlocker{{
			Code:     PlanningBlockerTaskClass,
			Detail:   fmt.Sprintf("task class %q is not supported", candidate.Task.Class),
			WorkerID: candidate.WorkerID,
		}}
	}

	blockers := session.taskTimeBlockers(candidate)
	estimate, found := session.estimates[candidate.Attempt.ID]
	if !found {
		return append(blockers, PlanningBlocker{
			Code:     PlanningBlockerEstimateMissing,
			Detail:   fmt.Sprintf("attempt %q has no remaining-cost and runtime estimate", candidate.Attempt.ID),
			WorkerID: candidate.WorkerID,
		})
	}
	runway := estimate.ExpectedRuntime + estimate.CheckpointMargin
	if deadline := candidate.Task.Deadline; deadline != nil && session.now.Add(runway).After(*deadline) {
		blockers = append(blockers, PlanningBlocker{
			Code:       PlanningBlockerDeadlineRunway,
			Detail:     fmt.Sprintf("expected runtime and checkpoint margin exceed task deadline %s", deadline.UTC().Format(time.RFC3339)),
			WorkerID:   candidate.WorkerID,
			DeadlineAt: clonePlanningTime(deadline),
		})
	}

	for index, window := range session.windows {
		available := quotaAvailable(window) - session.batchReserved[index]
		common := PlanningBlocker{
			WorkerID:      candidate.WorkerID,
			QuotaPoolID:   window.QuotaPoolID,
			QuotaWindowID: window.WindowID,
			Admission:     window.Admission,
			RequiredCost:  estimate.RemainingCost,
			Available:     available,
		}
		if quotaAdmissionBlocked(window.Admission, class) {
			blocker := common
			blocker.Code = PlanningBlockerQuotaAdmission
			blocker.Detail = fmt.Sprintf("quota window %q admission is %q for %s work", quotaWindowKey(window), window.Admission, class)
			if !window.ResetsAt.IsZero() {
				blocker.EarliestAt = planningTimeValue(window.ResetsAt)
			}
			blockers = append(blockers, blocker)
		}
		if class == domain.TaskClassSurplus && !window.SurplusStartsAt.IsZero() && session.now.Before(window.SurplusStartsAt) {
			blocker := common
			blocker.Code = PlanningBlockerSurplusWindow
			blocker.Detail = fmt.Sprintf("surplus window %q opens at %s", quotaWindowKey(window), window.SurplusStartsAt.UTC().Format(time.RFC3339))
			blocker.EarliestAt = planningTimeValue(window.SurplusStartsAt)
			blockers = append(blockers, blocker)
		}
		if estimate.RemainingCost > available {
			blocker := common
			blocker.Code = PlanningBlockerQuotaCapacity
			blocker.Detail = fmt.Sprintf("quota window %q has %.3f available, below %.3f remaining cost", quotaWindowKey(window), available, estimate.RemainingCost)
			if !window.ResetsAt.IsZero() {
				blocker.EarliestAt = planningTimeValue(window.ResetsAt)
			}
			blockers = append(blockers, blocker)
		}
		if !window.DrainAt.IsZero() && session.now.Add(runway).After(window.DrainAt) {
			blocker := common
			blocker.Code = PlanningBlockerQuotaDrainRunway
			blocker.Detail = fmt.Sprintf("expected runtime and checkpoint margin exceed quota drain time %s", window.DrainAt.UTC().Format(time.RFC3339))
			blocker.DeadlineAt = planningTimeValue(window.DrainAt)
			blockers = append(blockers, blocker)
		}
	}
	return blockers
}

func (session *quotaAdmissionSession) Reserve(candidate PlanningCandidate) {
	estimate, found := session.estimates[candidate.Attempt.ID]
	if !found {
		return
	}
	for index := range session.batchReserved {
		session.batchReserved[index] += estimate.RemainingCost
	}
}

func (session *quotaAdmissionSession) taskTimeBlockers(candidate PlanningCandidate) []PlanningBlocker {
	var blockers []PlanningBlocker
	if notBefore := candidate.Task.NotBefore; notBefore != nil && session.now.Before(*notBefore) {
		blockers = append(blockers, PlanningBlocker{
			Code:       PlanningBlockerTaskNotBefore,
			Detail:     fmt.Sprintf("task is not eligible before %s", notBefore.UTC().Format(time.RFC3339)),
			WorkerID:   candidate.WorkerID,
			EarliestAt: clonePlanningTime(notBefore),
		})
	}
	if expiresAt := candidate.Task.ExpiresAt; expiresAt != nil && !session.now.Before(*expiresAt) {
		blockers = append(blockers, PlanningBlocker{
			Code:       PlanningBlockerTaskExpired,
			Detail:     fmt.Sprintf("task expired at %s", expiresAt.UTC().Format(time.RFC3339)),
			WorkerID:   candidate.WorkerID,
			DeadlineAt: clonePlanningTime(expiresAt),
		})
	}
	return blockers
}

func quotaAvailable(window QuotaWindowBudget) float64 {
	return window.Capacity -
		window.CurrentUsage -
		window.ForecastInteractiveUsage -
		window.ActiveConsumption -
		window.PausedRequiredWorkRemainder -
		window.CommittedReservations -
		window.SafetyMargin
}

func quotaAdmissionBlocked(admission domain.AdmissionState, class domain.TaskClass) bool {
	switch admission {
	case domain.AdmissionDraining, domain.AdmissionClosed:
		return true
	case domain.AdmissionConstrained, domain.AdmissionRecovering:
		return class == domain.TaskClassSurplus
	default:
		return false
	}
}

func planningTaskClass(class domain.TaskClass) (domain.TaskClass, bool) {
	switch class {
	case "", domain.TaskClassRequired:
		return domain.TaskClassRequired, true
	case domain.TaskClassSurplus:
		return domain.TaskClassSurplus, true
	default:
		return "", false
	}
}

func validateQuotaWindow(window QuotaWindowBudget) error {
	if strings.TrimSpace(window.QuotaPoolID) != window.QuotaPoolID || window.QuotaPoolID == "" ||
		strings.TrimSpace(window.WindowID) != window.WindowID || window.WindowID == "" {
		return fmt.Errorf("quota admission pool and window IDs must be nonempty and trimmed")
	}
	switch window.Admission {
	case domain.AdmissionOpen, domain.AdmissionConstrained, domain.AdmissionDraining, domain.AdmissionClosed, domain.AdmissionRecovering:
	default:
		return fmt.Errorf("quota admission window %q has invalid admission %q", quotaWindowKey(window), window.Admission)
	}
	if !positiveFinite(window.Capacity) {
		return fmt.Errorf("quota admission window %q capacity must be positive and finite", quotaWindowKey(window))
	}
	values := []struct {
		name  string
		value float64
	}{
		{"current usage", window.CurrentUsage},
		{"forecast interactive usage", window.ForecastInteractiveUsage},
		{"active consumption", window.ActiveConsumption},
		{"paused required-work remainder", window.PausedRequiredWorkRemainder},
		{"committed reservations", window.CommittedReservations},
		{"safety margin", window.SafetyMargin},
	}
	for _, value := range values {
		if !nonnegativeFinite(value.value) {
			return fmt.Errorf("quota admission window %q %s must be nonnegative and finite", quotaWindowKey(window), value.name)
		}
	}
	return nil
}

func quotaWindowKey(window QuotaWindowBudget) string {
	return window.QuotaPoolID + "/" + window.WindowID
}

func cloneAdmissionEstimates(source map[string]TaskAdmissionEstimate) map[string]TaskAdmissionEstimate {
	result := make(map[string]TaskAdmissionEstimate, len(source))
	for attemptID, estimate := range source {
		result[attemptID] = estimate
	}
	return result
}

func planningTimeValue(value time.Time) *time.Time {
	copied := value
	return &copied
}

func positiveFinite(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func nonnegativeFinite(value float64) bool {
	return value >= 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

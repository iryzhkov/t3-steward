package backlog

import (
	"math"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestQuotaAdmissionPolicyScenarios(t *testing.T) {
	tests := []struct {
		name      string
		class     domain.TaskClass
		task      func(domain.Task) domain.Task
		window    func(QuotaWindowBudget) QuotaWindowBudget
		estimate     TaskAdmissionEstimate
		omitEstimate bool
		wantCodes    []string
	}{
		{
			name:  "required deadline pressure stays eligible",
			class: domain.TaskClassRequired,
			task: func(task domain.Task) domain.Task {
				task.Deadline = planningTimeValue(plannerTestTime.Add(30 * time.Minute))
				return task
			},
		},
		{
			name:  "hard closure blocks required",
			class: domain.TaskClassRequired,
			window: func(window QuotaWindowBudget) QuotaWindowBudget {
				window.Admission = domain.AdmissionClosed
				return window
			},
			wantCodes: []string{PlanningBlockerQuotaAdmission},
		},
		{
			name:  "draining blocks required",
			class: domain.TaskClassRequired,
			window: func(window QuotaWindowBudget) QuotaWindowBudget {
				window.Admission = domain.AdmissionDraining
				return window
			},
			wantCodes: []string{PlanningBlockerQuotaAdmission},
		},
		{
			name:  "constrained allows required with runway",
			class: domain.TaskClassRequired,
			window: func(window QuotaWindowBudget) QuotaWindowBudget {
				window.Admission = domain.AdmissionConstrained
				return window
			},
		},
		{
			name:  "constrained blocks surplus",
			class: domain.TaskClassSurplus,
			window: func(window QuotaWindowBudget) QuotaWindowBudget {
				window.Admission = domain.AdmissionConstrained
				return window
			},
			wantCodes: []string{PlanningBlockerQuotaAdmission},
		},
		{
			name:  "late window surplus is eligible",
			class: domain.TaskClassSurplus,
		},
		{
			name:  "surplus waits for its window",
			class: domain.TaskClassSurplus,
			window: func(window QuotaWindowBudget) QuotaWindowBudget {
				window.SurplusStartsAt = plannerTestTime.Add(time.Hour)
				return window
			},
			wantCodes: []string{PlanningBlockerSurplusWindow},
		},
		{
			name:  "recovering reserves admission for required work",
			class: domain.TaskClassSurplus,
			window: func(window QuotaWindowBudget) QuotaWindowBudget {
				window.Admission = domain.AdmissionRecovering
				return window
			},
			wantCodes: []string{PlanningBlockerQuotaAdmission},
		},
		{
			name:  "forecast and paused required remainder protect surplus",
			class: domain.TaskClassSurplus,
			estimate: TaskAdmissionEstimate{
				RemainingCost: 25, ExpectedRuntime: 20 * time.Minute, CheckpointMargin: 5 * time.Minute,
			},
			wantCodes: []string{PlanningBlockerQuotaCapacity},
		},
		{
			name:  "committed reservations protect required capacity",
			class: domain.TaskClassRequired,
			estimate: TaskAdmissionEstimate{
				RemainingCost: 50, ExpectedRuntime: 20 * time.Minute, CheckpointMargin: 5 * time.Minute,
			},
			wantCodes: []string{PlanningBlockerQuotaCapacity},
		},
		{
			name:  "not before",
			class: domain.TaskClassRequired,
			task: func(task domain.Task) domain.Task {
				task.NotBefore = planningTimeValue(plannerTestTime.Add(time.Hour))
				return task
			},
			wantCodes: []string{PlanningBlockerTaskNotBefore},
		},
		{
			name:  "expired surplus does not become urgent",
			class: domain.TaskClassSurplus,
			task: func(task domain.Task) domain.Task {
				task.ExpiresAt = planningTimeValue(plannerTestTime)
				return task
			},
			wantCodes: []string{PlanningBlockerTaskExpired},
		},
		{
			name:  "runtime must fit before drain",
			class: domain.TaskClassRequired,
			window: func(window QuotaWindowBudget) QuotaWindowBudget {
				window.DrainAt = plannerTestTime.Add(20 * time.Minute)
				return window
			},
			wantCodes: []string{PlanningBlockerQuotaDrainRunway},
		},
		{
			name:  "runtime must fit before deadline",
			class: domain.TaskClassRequired,
			task: func(task domain.Task) domain.Task {
				task.Deadline = planningTimeValue(plannerTestTime.Add(20 * time.Minute))
				return task
			},
			wantCodes: []string{PlanningBlockerDeadlineRunway},
		},
		{
			name:         "missing estimate fails closed",
			class:        domain.TaskClassRequired,
			omitEstimate: true,
			wantCodes:    []string{PlanningBlockerEstimateMissing},
		},
		{
			name:      "unknown task class fails closed",
			class:     "best-effort",
			wantCodes: []string{PlanningBlockerTaskClass},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			task := testTask("alpha")
			task.Class = test.class
			if test.task != nil {
				task = test.task(task)
			}
			window := quotaTestWindow()
			if test.window != nil {
				window = test.window(window)
			}
			estimate := test.estimate
			if estimate.RemainingCost == 0 {
				estimate = quotaTestEstimate()
			}
			estimates := map[string]TaskAdmissionEstimate{"alpha-1": estimate}
			if test.omitEstimate {
				estimates = nil
			}
			policy, err := NewQuotaAdmissionPolicy(QuotaAdmissionInput{
				Windows:   []QuotaWindowBudget{window},
				Estimates: estimates,
			})
			if err != nil {
				t.Fatalf("NewQuotaAdmissionPolicy: %v", err)
			}
			candidate := PlanningCandidate{
				WorkflowRunID: "run",
				Task:          task,
				Attempt:       domain.Attempt{ID: "alpha-1"},
				WorkerID:      "worker-a",
			}
			got := planningBlockerCodes(policy.StartPlan(plannerTestTime).Evaluate(candidate))
			if !reflect.DeepEqual(got, test.wantCodes) {
				t.Fatalf("blocker codes = %v, want %v", got, test.wantCodes)
			}
		})
	}
}

func TestQuotaAdmissionPolicyUsesRemainingCostAndPrivateInput(t *testing.T) {
	window := quotaTestWindow()
	estimate := quotaTestEstimate()
	windows := []QuotaWindowBudget{window}
	estimates := map[string]TaskAdmissionEstimate{"alpha-1": estimate}
	policy, err := NewQuotaAdmissionPolicy(QuotaAdmissionInput{Windows: windows, Estimates: estimates})
	if err != nil {
		t.Fatalf("NewQuotaAdmissionPolicy: %v", err)
	}
	windows[0].Capacity = 1
	estimates["alpha-1"] = TaskAdmissionEstimate{RemainingCost: 99, ExpectedRuntime: time.Hour}

	originalCost := 90.0
	task := testTask("alpha")
	task.Class = domain.TaskClassRequired
	task.EstimatedCost = &originalCost
	candidate := PlanningCandidate{Task: task, Attempt: domain.Attempt{ID: "alpha-1"}, WorkerID: "worker-a"}
	if blockers := policy.StartPlan(plannerTestTime).Evaluate(candidate); len(blockers) != 0 {
		t.Fatalf("blockers = %#v, want remaining estimate admitted independently of original cost and mutated input", blockers)
	}
}

func TestBuildPlanQuotaAdmissionReservesBatchAndIsRepeatable(t *testing.T) {
	alpha := testTask("alpha")
	alpha.Class = domain.TaskClassSurplus
	beta := testTask("beta")
	beta.Class = domain.TaskClassSurplus
	policy, err := NewQuotaAdmissionPolicy(QuotaAdmissionInput{
		Windows: []QuotaWindowBudget{quotaTestWindow()},
		Estimates: map[string]TaskAdmissionEstimate{
			"alpha-1": quotaTestEstimate(),
			"beta-1":  quotaTestEstimate(),
		},
	})
	if err != nil {
		t.Fatalf("NewQuotaAdmissionPolicy: %v", err)
	}
	input := plannerInput(
		[]domain.Task{beta, alpha},
		[]domain.WorkerInventory{plannerWorker("worker-a")},
	)
	input.Constraints = []PlanningConstraint{policy}

	first, err := BuildPlan(input)
	if err != nil {
		t.Fatalf("BuildPlan(first): %v", err)
	}
	second, err := BuildPlan(input)
	if err != nil {
		t.Fatalf("BuildPlan(second): %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("reused policy produced different plans:\nfirst: %#v\nsecond: %#v", first, second)
	}
	if len(first.Proposals) != 1 || first.Proposals[0].TaskID != "task-alpha" {
		t.Fatalf("proposals = %#v, want only alpha", first.Proposals)
	}
	decision := plannerDecision(t, first, "beta")
	if !hasCandidatePlanningBlocker(decision.Candidates, PlanningBlockerQuotaCapacity, "pool/weekly", 15, 5) {
		t.Fatalf("beta candidates = %#v, want quota capacity blocker from alpha reservation", decision.Candidates)
	}
}

func TestQuotaAdmissionPolicyRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*QuotaAdmissionInput)
	}{
		{
			name: "no windows",
			mutate: func(input *QuotaAdmissionInput) {
				input.Windows = nil
			},
		},
		{
			name: "duplicate window",
			mutate: func(input *QuotaAdmissionInput) {
				input.Windows = append(input.Windows, input.Windows[0])
			},
		},
		{
			name: "invalid admission",
			mutate: func(input *QuotaAdmissionInput) {
				input.Windows[0].Admission = "unknown"
			},
		},
		{
			name: "negative accounting",
			mutate: func(input *QuotaAdmissionInput) {
				input.Windows[0].ForecastInteractiveUsage = -1
			},
		},
		{
			name: "nonfinite accounting",
			mutate: func(input *QuotaAdmissionInput) {
				input.Windows[0].Capacity = math.Inf(1)
			},
		},
		{
			name: "invalid estimate",
			mutate: func(input *QuotaAdmissionInput) {
				input.Estimates["alpha-1"] = TaskAdmissionEstimate{}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := QuotaAdmissionInput{
				Windows:   []QuotaWindowBudget{quotaTestWindow()},
				Estimates: map[string]TaskAdmissionEstimate{"alpha-1": quotaTestEstimate()},
			}
			test.mutate(&input)
			if _, err := NewQuotaAdmissionPolicy(input); err == nil {
				t.Fatal("NewQuotaAdmissionPolicy succeeded, want validation error")
			}
		})
	}
}

func quotaTestWindow() QuotaWindowBudget {
	return QuotaWindowBudget{
		QuotaPoolID:                 "pool",
		WindowID:                    "weekly",
		Admission:                   domain.AdmissionOpen,
		Capacity:                    100,
		CurrentUsage:                30,
		ForecastInteractiveUsage:    20,
		ActiveConsumption:           10,
		PausedRequiredWorkRemainder: 5,
		CommittedReservations:       10,
		SafetyMargin:                5,
		SurplusStartsAt:             plannerTestTime.Add(-time.Minute),
		DrainAt:                     plannerTestTime.Add(2 * time.Hour),
		ResetsAt:                    plannerTestTime.Add(7 * 24 * time.Hour),
	}
}

func quotaTestEstimate() TaskAdmissionEstimate {
	return TaskAdmissionEstimate{
		RemainingCost: 15, ExpectedRuntime: 20 * time.Minute, CheckpointMargin: 5 * time.Minute,
	}
}

func planningBlockerCodes(blockers []PlanningBlocker) []string {
	var codes []string
	for _, blocker := range blockers {
		codes = append(codes, blocker.Code)
	}
	sort.Strings(codes)
	return codes
}

func hasCandidatePlanningBlocker(candidates []CandidateEvaluation, code, window string, required, available float64) bool {
	for _, candidate := range candidates {
		for _, blocker := range candidate.Blockers {
			if blocker.Code == code &&
				blocker.QuotaPoolID+"/"+blocker.QuotaWindowID == window &&
				blocker.RequiredCost == required &&
				blocker.Available == available {
				return true
			}
		}
	}
	return false
}

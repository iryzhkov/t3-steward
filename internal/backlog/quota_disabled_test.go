package backlog

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestDisabledQuotaIsFleetWideAndRetainsConcurrency(t *testing.T) {
	bridge := QuotaBridge{Disabled: true, Now: func() time.Time { return plannerTestTime },
		Pools: []QuotaPoolBinding{{ID: "pool", Provider: "codex", ProviderInstanceIDs: []string{"codex"}, MaxConcurrent: 2}},
	}
	// No observation store is available, and retained throttle records are
	// contradictory. Neither is consulted while quota checks are disabled.
	report, err := bridge.ReconcileState(context.Background(), QuotaPlanningStateInput{
		Assignments:     []domain.Assignment{{ID: "busy", State: domain.AssignmentUnknown, Route: domain.ProviderRoute{QuotaPoolID: "pool"}}},
		ThrottleRecords: []domain.ThrottleAttemptRecord{{}, {}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !report.ChecksDisabled || len(report.Windows) != 0 || len(report.Directives) != 0 ||
		!report.Pools[0].ChecksDisabled || report.Pools[0].ActiveAssignments != 1 {
		t.Fatalf("disabled projection=%+v", report)
	}
	policy, err := NewQuotaAdmissionPolicy(QuotaAdmissionInput{Disabled: true, MaxObservationAge: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	for _, host := range []string{"homelab", "omarchy-pc"} {
		task := routingTask("alpha", "codex", "gpt")
		input := plannerInput([]domain.Task{task}, []domain.WorkerInventory{routingWorker(host, routingProvider("codex", "pool", true, "gpt"))})
		input.QuotaPools = report.Pools
		input.RouteEstimates = []RouteEstimate{routingEstimate("alpha-1", host, "codex", "gpt", nil, 1000)}
		input.Constraints = []PlanningConstraint{policy}
		plan, err := BuildPlan(input)
		if err != nil || len(plan.Proposals) != 1 {
			t.Fatalf("%s: plan=%+v err=%v", host, plan, err)
		}
		input.QuotaPools[0].ActiveAssignments = 2
		plan, err = BuildPlan(input)
		if err != nil || len(plan.Proposals) != 0 {
			t.Fatalf("concurrency bypassed: %+v %v", plan, err)
		}
		input.QuotaPools[0].ActiveAssignments = 1
	}
	admission := WorkerAdmissionPolicyFromQuotaReport(report)
	if !admission.QuotaChecksDisabled || !admission.AllowsNewWork("pool") || admission.AllowsNewWork("") {
		t.Fatalf("worker admission=%+v", admission)
	}
	bridge.Disabled = false
	if _, err := bridge.ReconcileState(context.Background(), QuotaPlanningStateInput{}); err == nil {
		t.Fatal("enabled checks accepted missing quota evidence")
	}
}

func TestDisabledQuotaPreservesTaskTimeAndDependencyChecks(t *testing.T) {
	policy, err := NewQuotaAdmissionPolicy(QuotaAdmissionInput{Disabled: true, MaxObservationAge: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	task := routingTask("alpha", "codex", "gpt")
	future := plannerTestTime.Add(time.Hour)
	task.NotBefore = &future
	estimate := quotaTestEstimate()
	blockers := policy.StartPlan(plannerTestTime).Evaluate(PlanningCandidate{
		Task: task, Attempt: domain.Attempt{ID: "alpha-1"}, WorkerID: "remote",
		Route: &domain.ProviderRoute{QuotaPoolID: "pool"}, Estimate: &estimate,
	})
	if len(blockers) == 0 {
		t.Fatal("disabled quota bypassed not-before")
	}
	task.NotBefore = nil
	task.Needs = []string{"missing"}
	input := plannerInput([]domain.Task{task}, []domain.WorkerInventory{routingWorker("remote", routingProvider("codex", "pool", true, "gpt"))})
	input.QuotaPools = []domain.QuotaPool{routingPool("pool", 2, 0, "codex")}
	input.RouteEstimates = []RouteEstimate{routingEstimate("alpha-1", "remote", "codex", "gpt", nil, 1000)}
	input.Constraints = []PlanningConstraint{policy}
	plan, err := BuildPlan(input)
	if err == nil && len(plan.Proposals) != 0 {
		t.Fatal("disabled quota bypassed dependency")
	}
}

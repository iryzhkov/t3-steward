package backlog

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// A coordinator with quota checks disabled enforces nothing, but its pools
// still name the buckets that govern them. A pool that named none was read
// from every window of its providers, so models gave a pool the age and
// staleness of an ignored window or of another model's window (09-25
// checkpoint, M7).
func TestDisabledQuotaStillNamesTheBucketsThatGovernEachPool(t *testing.T) {
	observed := plannerTestTime.Add(-time.Minute)
	key := func(window string) domain.BucketKey {
		return domain.BucketKey{ProviderInstanceID: "claude", LimitID: "claude", Window: window}
	}
	bridge := QuotaBridge{Disabled: true, Now: func() time.Time { return plannerTestTime },
		Pools: []QuotaPoolBinding{{
			ID: "pool-claude", Provider: "claude", ProviderInstanceIDs: []string{"claude"}, MaxConcurrent: 2,
			Models: []string{"claude-sonnet-5"}, IgnoredWindows: []string{"overage"},
		}},
	}
	report, err := bridge.ReconcileState(context.Background(), QuotaPlanningStateInput{
		WorkerSnapshots: []domain.WorkerSnapshot{{WorkerID: "homelab", QuotaObservations: []domain.WorkerQuotaObservation{
			{Key: key("seven_day"), Phase: domain.PhaseNormal, UsedPercent: 10, Healthy: true, ObservedAt: observed},
			{Key: key("five_hour"), Phase: domain.PhaseNormal, UsedPercent: 20, Healthy: true, ObservedAt: observed},
			{Key: key("overage"), Phase: domain.PhaseNormal, Healthy: true, ObservedAt: observed.Add(-72 * time.Hour)},
			{Key: key("seven_day_opus"), ModelSelector: "opus", Phase: domain.PhaseNormal, Healthy: true, ObservedAt: observed.Add(-72 * time.Hour)},
			{Key: domain.BucketKey{ProviderInstanceID: "codex", LimitID: "codex", Window: "weekly"}, Healthy: true, ObservedAt: observed},
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Pools) != 1 || !report.Pools[0].ChecksDisabled || report.Pools[0].Admission != domain.AdmissionOpen {
		t.Fatalf("disabled projection=%+v", report.Pools)
	}
	got := report.Pools[0].Buckets
	if len(got) != 2 || got[0] != key("five_hour") || got[1] != key("seven_day") {
		t.Fatalf("named buckets = %v, want five_hour and seven_day only", got)
	}
}

func TestDisabledQuotaIsFleetWideAndRetainsConcurrency(t *testing.T) {
	bridge := QuotaBridge{Disabled: true, Now: func() time.Time { return plannerTestTime },
		Pools: []QuotaPoolBinding{{ID: "pool", Provider: "codex", ProviderInstanceIDs: []string{"codex"}, MaxConcurrent: 2}},
	}
	// No observation store is available, and retained throttle records are
	// contradictory. Neither is consulted while quota checks are disabled.
	//
	// The assignment is presented with the attempt that owns it. Occupancy is
	// now reconstructed by the same derivation the enabled path uses, and that
	// derivation asks what the attempt is doing rather than only what state the
	// assignment row is in.
	report, err := bridge.ReconcileState(context.Background(), QuotaPlanningStateInput{
		Tasks: []domain.Task{{ID: "alpha", Class: domain.TaskClassRequired}},
		Attempts: []domain.Attempt{{
			ID: "alpha-1", TaskID: "alpha", AssignmentID: "busy",
			Progress: domain.ProgressActive, Control: domain.ControlRunning,
		}},
		Assignments: []domain.Assignment{{
			ID: "busy", AttemptID: "alpha-1", State: domain.AssignmentUnknown,
			Route: domain.ProviderRoute{QuotaPoolID: "pool"},
		}},
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

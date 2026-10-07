package backlog

import (
	"context"
	"encoding/json"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"testing"
	"time"
)

func TestWorkerDrainRunway(t *testing.T) {
	now := time.Date(2026, 10, 7, 0, 40, 0, 0, time.UTC)
	for _, tc := range []struct{ name, phase, extra, want string }{
		{"projected crossing", "warned", `,"ratePerMinute":0.8,"drainPercent":90,"drainsAt":"2026-10-07T00:45:00Z"`, PlanningBlockerQuotaDrainRunway},
		{"already draining", "draining", "", PlanningBlockerQuotaAdmission},
		{"older worker", "warned", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var observation domain.WorkerQuotaObservation
			payload := `{"key":{"providerInstanceId":"claudeAgent","limitId":"claude","window":"five_hour"},"phase":"` + tc.phase + `","usedPercent":86,"observedAt":"2026-10-07T00:40:00Z"` + tc.extra + `}`
			if err := json.Unmarshal([]byte(payload), &observation); err != nil {
				t.Fatal(err)
			}
			bridge := QuotaBridge{Store: &quotaBridgeStoreFake{}, MaxObservationAge: 5 * time.Minute, Now: func() time.Time { return now }, Pools: []QuotaPoolBinding{{ID: "claude-main", Provider: "claudeAgent", ProviderInstanceIDs: []string{"claudeAgent"}, MaxConcurrent: 2}}}
			report, err := bridge.ReconcileObservations(context.Background(), nil, []domain.WorkerSnapshot{{QuotaObservations: []domain.WorkerQuotaObservation{observation}}})
			if err != nil {
				t.Fatal(err)
			}
			policy, err := NewQuotaAdmissionPolicy(QuotaAdmissionInput{Windows: report.Windows, MaxObservationAge: 5 * time.Minute})
			if err != nil {
				t.Fatal(err)
			}
			candidate := PlanningCandidate{Task: domain.Task{Class: domain.TaskClassRequired}, Attempt: domain.Attempt{ID: "attempt"}, Route: &domain.ProviderRoute{QuotaPoolID: "claude-main"}, Estimate: &TaskAdmissionEstimate{ExpectedRuntime: 5 * time.Minute, CheckpointMargin: time.Minute}}
			blockers := policy.StartPlan(now).Evaluate(candidate)
			found := false
			for _, b := range blockers {
				if b.Code == tc.want {
					found = true
				}
			}
			if tc.want == "" && len(blockers) != 0 {
				t.Fatalf("old worker blockers: %+v", blockers)
			}
			if tc.want != "" && !found {
				t.Fatalf("missing %s: %+v windows=%+v", tc.want, blockers, report.Windows)
			}
			candidate.Attempt.AdminForceStart = true
			if forced := policy.StartPlan(now).Evaluate(candidate); len(forced) != 0 {
				t.Fatalf("force start changed: %+v", forced)
			}
		})
	}
}

func TestDrainRunwayUsesEarliestDeadline(t *testing.T) {
	now := time.Date(2026, 10, 7, 0, 40, 0, 0, time.UTC)
	state := domain.BucketState{Key: admissionBucket("claudeAgent", "five_hour"), ObservedAt: now}
	if err := json.Unmarshal([]byte(`{"drainsAt":"2026-10-07T00:45:00Z","drainDeadline":"2026-10-07T00:50:00Z"}`), &state); err != nil {
		t.Fatal(err)
	}
	exhaustion := 20 * time.Minute
	state.ExhaustsIn = &exhaustion
	windows, err := deriveQuotaPlanningWindows(now, []domain.QuotaPool{{ID: "pool", Admission: domain.AdmissionOpen}}, []domain.BucketState{state}, map[string]string{"claudeAgent": "pool"}, QuotaBridge{MaxObservationAge: 5 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if len(windows) != 1 || !windows[0].DrainAt.Equal(now.Add(5*time.Minute)) {
		t.Fatalf("earliest drain = %+v", windows)
	}
}

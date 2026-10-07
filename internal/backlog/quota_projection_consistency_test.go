package backlog

import (
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestWorkerDrainProjectionCannotExtendComputedRunway(t *testing.T) {
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name     string
		supplied *time.Time
		want     time.Time
	}{
		{"oversized timestamp", timePointer(time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)), now.Add(8 * time.Minute)},
		{"missing timestamp", nil, now.Add(8 * time.Minute)},
		{"earlier timestamp", timePointer(now.Add(5 * time.Minute)), now.Add(5 * time.Minute)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := domain.BucketKey{ProviderInstanceID: "worker", LimitID: "limit", Window: "five_hour"}
			observed := domain.WorkerQuotaObservation{Key: key, Phase: domain.PhaseNormal, UsedPercent: 80,
				Healthy: true, ObservedAt: now.Add(-2 * time.Minute), RatePerMinute: 1, DrainPercent: 90, DrainsAt: tc.supplied}
			merged := domain.MergeQuotaObservations(nil, []domain.WorkerSnapshot{{QuotaObservations: []domain.WorkerQuotaObservation{observed}}})
			windows, err := deriveQuotaPlanningWindows(now, []domain.QuotaPool{{ID: "pool", Admission: domain.AdmissionOpen}},
				merged, map[string]string{"worker": "pool"}, QuotaBridge{MaxObservationAge: 5 * time.Minute})
			if err != nil {
				t.Fatal(err)
			}
			if len(windows) != 1 || !windows[0].DrainAt.Equal(tc.want) {
				t.Fatalf("inconsistent runway accepted: windows=%+v want=%s", windows, tc.want)
			}
			policy, err := NewQuotaAdmissionPolicy(QuotaAdmissionInput{Windows: windows, MaxObservationAge: 5 * time.Minute})
			if err != nil {
				t.Fatal(err)
			}
			candidate := PlanningCandidate{Task: domain.Task{Class: domain.TaskClassRequired}, Attempt: domain.Attempt{ID: "attempt"},
				Route:    &domain.ProviderRoute{QuotaPoolID: "pool"},
				Estimate: &TaskAdmissionEstimate{ExpectedRuntime: 8 * time.Minute, CheckpointMargin: time.Minute}}
			blocked := false
			for _, b := range policy.StartPlan(now).Evaluate(candidate) {
				if b.Code == PlanningBlockerQuotaDrainRunway {
					blocked = true
				}
			}
			if !blocked {
				t.Fatal("inconsistent projection bypassed runway admission")
			}
		})
	}
}

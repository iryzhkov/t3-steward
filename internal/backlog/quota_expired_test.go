package backlog

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Reconciliation and planning budgets must agree on the governing windows.
func TestQuotaBridgeDropsExpiredSecondary(t *testing.T) {
	now := admissionDerivationTime
	primary := admissionBucket("codex", domain.WindowPrimary)
	secondary := admissionBucket("codex", domain.WindowSecondary)
	for _, tc := range []struct {
		name       string
		primaryAge time.Duration
		phase      domain.Phase
		used       float64
		want       domain.AdmissionState
		windows    int
	}{
		{"fresh primary", time.Minute, domain.PhaseNormal, 20, domain.AdmissionOpen, 1},
		{"all expired", 2 * time.Hour, domain.PhaseNormal, 20, domain.AdmissionClosed, 2},
		{"fresh exhausted", time.Minute, domain.PhaseStopped, 100, domain.AdmissionClosed, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fresh := admissionBucketState(primary, tc.phase, true)
			fresh.ObservedAt, fresh.UsedPercent = now.Add(-tc.primaryAge), tc.used
			old := admissionBucketState(secondary, domain.PhaseStopped, false)
			old.ObservedAt, old.UsedPercent = now.Add(-31*24*time.Hour), 100
			bridge := QuotaBridge{
				Store:             &quotaBridgeStoreFake{buckets: []domain.BucketState{old, fresh}},
				MaxObservationAge: time.Hour, Now: func() time.Time { return now },
				Pools: []QuotaPoolBinding{{ID: "codex-main", Provider: "codex", ProviderInstanceIDs: []string{"codex"}, MaxConcurrent: 2}},
			}
			report, err := bridge.Reconcile(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			if report.Pools[0].Admission != tc.want || len(report.Windows) != tc.windows {
				t.Fatalf("report = %+v", report)
			}
		})
	}
}

func TestSubmissionQuotaDropsExpiredSecondary(t *testing.T) {
	for _, mode := range []string{"fresh", "all expired", "zero headroom"} {
		t.Run(mode, func(t *testing.T) {
			admission, snapshot, records := quotaSubmissionFixture()
			old := snapshot.States[0]
			old.Key.Window = domain.WindowSecondary
			old.ObservedAt = admission.Now().Add(-31 * 24 * time.Hour)
			old.UsedPercent = 100
			old.Phase = domain.PhaseStopped
			snapshot.States = append(snapshot.States, old)
			switch mode {
			case "all expired":
				snapshot.States[0].ObservedAt = admission.Now().Add(-2 * time.Hour)
			case "zero headroom":
				snapshot.States[0].UsedPercent = 100
			}
			receipt, err := admission.evaluate(records, snapshot)
			if mode == "fresh" {
				if err != nil || len(receipt.Pools[0].Windows) != 1 || len(receipt.Pools[0].WindowSet.Windows) != 1 {
					t.Fatalf("receipt = %+v, err = %v", receipt, err)
				}
			} else if err == nil {
				t.Fatal("invalid quota admitted")
			}
		})
	}
}

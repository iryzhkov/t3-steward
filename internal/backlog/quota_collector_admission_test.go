package backlog

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"testing"
	"time"
)

func nativeAdmissionFixture(now time.Time) (QuotaBridge, []domain.BucketState) {
	coverage := &domain.QuotaCollectorCoverage{SourceID: "native", AccountID: "account", MaxObservationAge: 390 * time.Second, Origins: []domain.QuotaCollectorOrigin{{WorkerID: "collector", ProviderInstanceID: "claudeAgent"}}, RequiredWindows: []domain.QuotaCollectorWindow{{Window: "five_hour"}, {Window: "seven_day"}, {Window: "fable", Models: []string{"claude-fable-5-1"}}}}
	bridge := QuotaBridge{MaxObservationAge: time.Minute, Now: func() time.Time { return now }, Pools: []QuotaPoolBinding{{ID: "pool", Provider: "claude", AccountID: "account", Collector: coverage, ProviderInstanceIDs: []string{"claudeAgent"}, Models: []string{"claude-fable-5-1", "claude-haiku-5-5"}, MaxConcurrent: 2}}}
	at := now.Add(-300 * time.Second)
	reset := now.Add(time.Hour)
	var states []domain.BucketState
	for _, w := range []string{"five_hour", "seven_day", "fable"} {
		states = append(states, domain.BucketState{Key: domain.BucketKey{ProviderInstanceID: "claudeAgent", AccountID: "account", LimitID: "claude", Window: w}, Phase: domain.PhaseNormal, Healthy: true, UsedPercent: 20, ObservedAt: at, ResetsAt: &reset, Epoch: domain.EpochFor(&reset), Provenance: &domain.QuotaProvenance{SourceID: "native", AcquisitionID: "batch", OriginWorkerID: "collector", AcquiredAt: at}})
	}
	return bridge, states
}
func TestCollectorBridgeAdmissionPlanningShareScopedEvidence(t *testing.T) {
	now := time.Date(2026, 10, 13, 20, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		edit func(*QuotaBridge, *[]domain.BucketState)
		want domain.AdmissionState
	}{
		{"complete", func(*QuotaBridge, *[]domain.BucketState) {}, domain.AdmissionOpen},
		{"missing model", func(_ *QuotaBridge, s *[]domain.BucketState) { *s = (*s)[:2] }, domain.AdmissionClosed},
		{"mixed batch", func(_ *QuotaBridge, s *[]domain.BucketState) { (*s)[2].Provenance.AcquisitionID = "other" }, domain.AdmissionClosed},
		{"unavailable", func(_ *QuotaBridge, s *[]domain.BucketState) { *s = nil }, domain.AdmissionClosed},
		{"plain named log", func(_ *QuotaBridge, s *[]domain.BucketState) {
			for i := range *s {
				(*s)[i].Provenance = nil
			}
		}, domain.AdmissionClosed},
		{"partition", func(_ *QuotaBridge, s *[]domain.BucketState) {
			end := now.Add(-time.Second)
			for i := range *s {
				(*s)[i].OriginValidUntil = &end
			}
		}, domain.AdmissionClosed},
		{"model stops whole pool", func(_ *QuotaBridge, s *[]domain.BucketState) {
			(*s)[2].Phase = domain.PhaseStopped
			(*s)[2].Healthy = false
			(*s)[2].UsedPercent = 100
		}, domain.AdmissionClosed},
		{"unknown fresh warned", func(_ *QuotaBridge, s *[]domain.BucketState) {
			raw := (*s)[0]
			raw.Key.AccountID = ""
			raw.Provenance = nil
			raw.Phase = domain.PhaseWarned
			raw.ObservedAt = now.Add(-time.Second)
			*s = append(*s, raw)
		}, domain.AdmissionClosed},
		{"unknown history newer than source", func(_ *QuotaBridge, s *[]domain.BucketState) {
			raw := (*s)[0]
			raw.Key.AccountID = ""
			raw.Provenance = nil
			raw.Phase = domain.PhaseStopped
			raw.ObservedAt = now.Add(-120 * time.Second)
			*s = append(*s, raw)
		}, domain.AdmissionClosed},
		{"unmapped account restriction persists", func(_ *QuotaBridge, s *[]domain.BucketState) {
			raw := (*s)[0]
			raw.Key.AccountID = ""
			raw.Key.Window = "unmapped-account"
			raw.Provenance = nil
			raw.Phase = domain.PhaseStopped
			raw.ObservedAt = now.Add(-350 * time.Second)
			*s = append(*s, raw)
		}, domain.AdmissionClosed},
		{"different account", func(_ *QuotaBridge, s *[]domain.BucketState) {
			raw := (*s)[0]
			raw.Key.AccountID = "another"
			raw.Provenance = nil
			raw.Phase = domain.PhaseStopped
			raw.ObservedAt = now
			*s = append(*s, raw)
		}, domain.AdmissionOpen},
		{"older unknown superseded", func(_ *QuotaBridge, s *[]domain.BucketState) {
			raw := (*s)[0]
			raw.Key.AccountID = ""
			raw.Provenance = nil
			raw.Phase = domain.PhaseStopped
			raw.ObservedAt = now.Add(-350 * time.Second)
			*s = append(*s, raw)
		}, domain.AdmissionOpen},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			bridge, states := nativeAdmissionFixture(now)
			tt.edit(&bridge, &states)
			bridge.Store = &quotaBridgeStoreFake{buckets: states}
			report, err := bridge.Reconcile(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			if report.Pools[0].Admission != tt.want {
				t.Fatalf("admission=%s want%s issues=%+v", report.Pools[0].Admission, tt.want, report.Derived)
			}
			if tt.want == domain.AdmissionOpen {
				if len(report.Windows) != 3 {
					t.Fatalf("windows=%+v", report.Windows)
				}
				freshness := quotaAdmissionSession{now: now, maxObservationAge: time.Minute}
				for _, w := range report.Windows {
					if _, bad := freshness.quotaObservationStaleness(w); bad || w.MaxObservationAge != 390*time.Second {
						t.Fatalf("planning lost scoped age: %+v", w)
					}
				}
			}
		})
	}
}
func TestCollectorPlanningTiedUsageAndLegacyAgeIsolation(t *testing.T) {
	now := time.Date(2026, 10, 13, 20, 0, 0, 0, time.UTC)
	bridge, states := nativeAdmissionFixture(now)
	high := states[0]
	high.UsedPercent = 95
	high.Phase = domain.PhaseStopped
	high.Healthy = false
	for _, ordered := range [][]domain.BucketState{append(append([]domain.BucketState{}, states...), high), append([]domain.BucketState{high}, states...)} {
		bridge.Store = &quotaBridgeStoreFake{buckets: ordered}
		report, err := bridge.Reconcile(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if report.Pools[0].Admission != domain.AdmissionClosed {
			t.Fatal("tie opened pool")
		}
		for _, w := range report.Windows {
			if w.WindowID == high.Key.String() && w.CurrentUsage != 95 {
				t.Fatal("planning selected lower tied usage")
			}
		}
	}
	bridge.Pools[0].Collector = nil
	bridge.Store = &quotaBridgeStoreFake{buckets: states}
	report, err := bridge.Reconcile(context.Background(), nil)
	if err != nil || report.Pools[0].Admission != domain.AdmissionClosed {
		t.Fatalf("legacy age changed %v %+v", err, report)
	}
}

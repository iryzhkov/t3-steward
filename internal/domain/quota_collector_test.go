package domain

import (
	"testing"
	"time"
)

func collectorWindowFixture(now time.Time) (QuotaPool, []BucketState) {
	pool := QuotaPool{ID: "claude", Provider: "claude", AccountID: "account", ProviderInstanceIDs: []string{"claudeAgent"}, Models: []string{"claude-fable-5-1"}, Collector: &QuotaCollectorCoverage{SourceID: "native", AccountID: "account", MaxObservationAge: 390 * time.Second, Origins: []QuotaCollectorOrigin{{WorkerID: "collector", ProviderInstanceID: "claudeAgent"}}, RequiredWindows: []QuotaCollectorWindow{{Window: "five_hour"}, {Window: "seven_day"}, {Window: "fable_week", Models: []string{"claude-fable-5-1"}}}}}
	reset := now.Add(time.Hour)
	at := now.Add(-300 * time.Second)
	var states []BucketState
	for _, window := range []string{"five_hour", "seven_day", "fable_week"} {
		states = append(states, BucketState{Key: BucketKey{ProviderInstanceID: "claudeAgent", AccountID: "account", LimitID: "claude", Window: window}, ObservedAt: at, ResetsAt: &reset, Epoch: EpochFor(&reset), Phase: PhaseNormal, Healthy: true, UsedPercent: 20, Provenance: &QuotaProvenance{SourceID: "native", AcquisitionID: "batch", OriginWorkerID: "collector", AcquiredAt: at}})
	}
	return pool, states
}
func TestCollectorWindowCoverageFreshnessAndBatch(t *testing.T) {
	now := time.Date(2026, 10, 13, 20, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		mutate func(*QuotaPool, *[]BucketState)
		fresh  bool
	}{
		{"complete", func(*QuotaPool, *[]BucketState) {}, true},
		{"required reset expired", func(_ *QuotaPool, s *[]BucketState) {
			at := now.Add(-time.Second)
			(*s)[0].ResetsAt = &at
			(*s)[0].Epoch = EpochFor(&at)
		}, false},
		{"missing model", func(_ *QuotaPool, s *[]BucketState) { *s = (*s)[:2] }, false},
		{"mixed batch", func(_ *QuotaPool, s *[]BucketState) { (*s)[2].Provenance.AcquisitionID = "other" }, false},
		{"unproved account", func(_ *QuotaPool, s *[]BucketState) {
			for i := range *s {
				(*s)[i].Provenance = nil
			}
		}, false},
		{"false origin", func(_ *QuotaPool, s *[]BucketState) { (*s)[0].Provenance.OriginWorkerID = "other" }, false},
		{"scope stale", func(_ *QuotaPool, s *[]BucketState) {
			for i := range *s {
				(*s)[i].ObservedAt = now.Add(-391 * time.Second)
				(*s)[i].Provenance.AcquiredAt = (*s)[i].ObservedAt
			}
		}, false},
		{"partition", func(_ *QuotaPool, s *[]BucketState) {
			end := now.Add(-time.Second)
			for i := range *s {
				(*s)[i].OriginValidUntil = &end
			}
		}, false},
		{"future", func(_ *QuotaPool, s *[]BucketState) {
			for i := range *s {
				(*s)[i].ObservedAt = now.Add(time.Second)
				(*s)[i].Provenance.AcquiredAt = (*s)[i].ObservedAt
			}
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool, states := collectorWindowFixture(now)
			tt.mutate(&pool, &states)
			if got := ReadQuotaWindows(pool, states, now, time.Minute).Fresh(); got != tt.fresh {
				t.Fatalf("Fresh=%v want%v: %+v", got, tt.fresh, ReadQuotaWindows(pool, states, now, time.Minute))
			}
		})
	}
	pool, states := collectorWindowFixture(now)
	pool.Collector = nil
	if ReadQuotaWindows(pool, states, now, time.Minute).Fresh() {
		t.Fatal("unrelated legacy pool inherited native age")
	}
}
func TestCollectorUnknownRestrictionBarrierKeepsRawIdentity(t *testing.T) {
	now := time.Date(2026, 10, 13, 20, 0, 0, 0, time.UTC)
	pool, states := collectorWindowFixture(now)
	raw := states[0]
	raw.Key.AccountID = ""
	raw.Provenance = nil
	raw.Phase = PhaseWarned
	raw.ObservedAt = now.Add(-time.Second)
	states = append(states, raw)
	if ReadQuotaWindows(pool, states, now, time.Minute).Fresh() {
		t.Fatal("anonymous evidence counted as native coverage")
	}
	if !UnknownAccountRestricts(pool, raw, now, time.Minute) {
		t.Fatal("fresh warning not restrictive")
	}
	later := now.Add(61 * time.Second)
	if ReadQuotaWindows(pool, states, later, time.Minute).Fresh() {
		t.Fatal("old account acquisition reopened after anonymous expiry")
	}
	for i := 0; i < 3; i++ {
		states[i].ObservedAt = later
		states[i].Provenance.AcquiredAt = later
		states[i].Provenance.AcquisitionID = "after-stop"
	}
	if !ReadQuotaWindows(pool, states, later, time.Minute).Fresh() {
		t.Fatal("post-restriction full acquisition not accepted")
	}
	if states[3].Key.AccountID != "" {
		t.Fatal("anonymous key rewritten")
	}
	other := raw
	other.Key.AccountID = "other-account"
	if !ReadQuotaWindows(pool, append(states[:3], other), later, time.Minute).Fresh() {
		t.Fatal("known different account overlaid")
	}
}
func TestCollectorTiesAndAuthenticatedOriginAreOrderIndependent(t *testing.T) {
	now := time.Date(2026, 10, 13, 20, 0, 0, 0, time.UTC)
	pool, states := collectorWindowFixture(now)
	restrictive := states[0]
	restrictive.Phase = PhaseStopped
	restrictive.Healthy = false
	restrictive.UsedPercent = 100
	for _, ordered := range [][]BucketState{append(append([]BucketState{}, states...), restrictive), append([]BucketState{restrictive}, states...)} {
		set := ReadQuotaWindows(pool, ordered, now, time.Minute)
		if problem, bad := set.Problem(); !bad || problem.Code != QuotaWindowExhausted {
			t.Fatalf("tied restriction lost: %+v", set)
		}
	}
	o := WorkerQuotaObservation{Key: states[0].Key, ObservedAt: states[0].ObservedAt, ResetsAt: states[0].ResetsAt, Epoch: states[0].Epoch, Phase: PhaseNormal, Healthy: true, Provenance: CloneQuotaProvenance(states[0].Provenance)}
	worker := WorkerSnapshot{WorkerID: "impostor", ObservedAt: now, Connected: true, QuotaObservations: []WorkerQuotaObservation{o}}
	merged := MergeQuotaObservations(states, []WorkerSnapshot{worker})
	if len(merged) != 4 {
		t.Fatalf("tie discarded: %d", len(merged))
	}
	if merged[3].Provenance.OriginWorkerID != "impostor" || CollectorObservationQualified(pool, merged[3]) {
		t.Fatal("claimed origin trusted")
	}
	if o.Provenance.OriginWorkerID != "collector" {
		t.Fatal("merge mutated sender")
	}
	worker.WorkerID = "collector"
	worker.Connected = false
	merged = MergeQuotaObservations(nil, []WorkerSnapshot{worker})
	if QuotaReadingFresh(merged[0], now, 390*time.Second) {
		t.Fatal("disconnected source remained fresh")
	}
}
func TestCollectorQuotaWaitUsesSameScopedCompleteEvidence(t *testing.T) {
	now := time.Date(2026, 10, 13, 20, 0, 0, 0, time.UTC)
	pool, states := collectorWindowFixture(now)
	below := 50.0
	_, outcome, _, err := EvaluateQuotaWait(QuotaWaitCondition{Pool: pool.ID, Below: &below}, []QuotaPool{pool}, states, now, time.Minute)
	if err != nil || outcome != TaskWaitMet {
		t.Fatalf("complete scoped wait: %s %v", outcome, err)
	}
	_, outcome, _, err = EvaluateQuotaWait(QuotaWaitCondition{Pool: pool.ID, Below: &below}, []QuotaPool{pool}, states[:2], now, time.Minute)
	if err != nil || outcome != "" {
		t.Fatal("partial acquisition met wait")
	}
}

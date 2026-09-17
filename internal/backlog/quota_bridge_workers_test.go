package backlog

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// S-3: the coordinator's host is idle, so its own reading of the pool's bucket
// is stale, while a worker on the host that consumes the pool reports the
// bucket stopped at 97%. The pool must close on the worker's reading rather
// than report a stale snapshot that submission proceeds through.
func TestQuotaBridgeWorkerObservationClosesPoolWhenLocalReadingIsStale(t *testing.T) {
	now := time.Date(2026, 9, 17, 15, 0, 0, 0, time.UTC)
	key := domain.BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: "seven_day"}
	reset := now.Add(18 * time.Hour)
	stale := admissionBucketState(key, domain.PhaseNormal, true)
	stale.UsedPercent, stale.ResetsAt, stale.Epoch = 40, &reset, domain.EpochFor(&reset)
	stale.ObservedAt = now.Add(-2 * time.Hour)
	store := &quotaBridgeStoreFake{buckets: []domain.BucketState{stale}}
	bridge := QuotaBridge{
		Store: store, MaxObservationAge: 5 * time.Minute, Now: func() time.Time { return now },
		Pools: []QuotaPoolBinding{{ID: "claude-main", Provider: "claudeAgent", ProviderInstanceIDs: []string{"claudeAgent"}, MaxConcurrent: 2}},
	}
	worker := domain.WorkerSnapshot{
		WorkerID: "homelab", WorkerEpoch: "w1", CoordinatorEpoch: 1, Sequence: 3, Connected: true,
		ObservedAt: now.Add(-time.Minute), ValidUntil: now.Add(time.Minute),
		QuotaObservations: []domain.WorkerQuotaObservation{{
			Key: key, Phase: domain.PhaseStopped, UsedPercent: 97, Healthy: false,
			ObservedAt: now.Add(-time.Minute), ResetsAt: &reset, Epoch: domain.EpochFor(&reset),
		}},
	}
	// Without the worker's reading the pool fails closed on staleness.
	without, err := bridge.Reconcile(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(without.Derived) != 1 || without.Derived[0].Admission != domain.AdmissionClosed || !hasIssue(without.Derived[0], QuotaAdmissionIssueObservationStale) {
		t.Fatalf("stale-only derivation = %+v", without.Derived)
	}
	// With it the pool is closed on the observation itself: the reading is
	// fresh, and it says stopped at 97%.
	with, err := bridge.ReconcileObservations(context.Background(), nil, []domain.WorkerSnapshot{worker})
	if err != nil {
		t.Fatal(err)
	}
	if len(with.Derived) != 1 || with.Derived[0].Admission != domain.AdmissionClosed || hasIssue(with.Derived[0], QuotaAdmissionIssueObservationStale) ||
		!with.Derived[0].ObservedAt.Equal(now.Add(-time.Minute)) {
		t.Fatalf("merged derivation = %+v", with.Derived)
	}
	// The freshest reading wins in either direction: a worker reading older
	// than the local one is dropped.
	fresh := stale
	fresh.ObservedAt = now
	merged := MergeWorkerQuotaObservations([]domain.BucketState{fresh}, []domain.WorkerSnapshot{worker})
	if len(merged) != 1 || merged[0].UsedPercent != 40 {
		t.Fatalf("older worker reading replaced a fresher local one: %+v", merged)
	}
	// A worker without the field (an older build) changes nothing.
	if same := MergeWorkerQuotaObservations([]domain.BucketState{stale}, []domain.WorkerSnapshot{{WorkerID: "old"}}); len(same) != 1 || same[0].UsedPercent != 40 {
		t.Fatalf("an observation-less worker changed the states: %+v", same)
	}
}

func hasIssue(snapshot QuotaPoolAdmissionSnapshot, code string) bool {
	for _, issue := range snapshot.Issues {
		if issue.Code == code {
			return true
		}
	}
	return false
}

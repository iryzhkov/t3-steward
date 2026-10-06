package backlog

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// failingBucketStore is a coordinator store whose own observations cannot be
// read.
type failingBucketStore struct{}

func (failingBucketStore) LoadQuotaAdmissions(context.Context) ([]domain.QuotaAdmissionRecord, error) {
	return nil, nil
}
func (failingBucketStore) CommitQuotaAdmissionTransitions(context.Context, []domain.QuotaAdmissionTransition) error {
	return nil
}
func (failingBucketStore) ListBuckets(context.Context) ([]domain.BucketState, error) {
	return nil, errors.New("database is locked")
}

// Review of #42: with quota checks disabled, a quota wait is evaluated against
// the buckets that govern its pool, through the coordinator's own evaluator.
// A pool whose governing buckets were resolved and none applies is not read
// from every window, and a pool whose buckets could not be resolved is
// reported unknown rather than widened.
func TestQuotaWaitsWithChecksDisabledReadOnlyGoverningBuckets(t *testing.T) {
	now := plannerTestTime
	observed := now.Add(-time.Minute)
	key := func(account, window string) domain.BucketKey {
		return domain.BucketKey{ProviderInstanceID: "claude", AccountID: account, LimitID: "claude", Window: window}
	}
	reading := func(k domain.BucketKey, percent float64, selector string) domain.WorkerQuotaObservation {
		return domain.WorkerQuotaObservation{Key: k, Phase: domain.PhaseNormal, UsedPercent: percent, Healthy: true, ObservedAt: observed, ModelSelector: selector}
	}
	below := 50.0
	wait := domain.QuotaWaitCondition{Pool: "pool-claude", Below: &below}
	binding := QuotaPoolBinding{
		ID: "pool-claude", Provider: "claude", AccountID: "acct-1", ProviderInstanceIDs: []string{"claude"}, MaxConcurrent: 1,
		Models: []string{"claude-sonnet-5"}, IgnoredWindows: []string{"overage"},
	}
	// Excluded windows, each at 99% so that reading any of them would keep
	// the wait pending: an ignored window, another model's window, and
	// another account's window.
	excluded := []domain.WorkerQuotaObservation{
		reading(key("acct-1", "overage"), 99, ""),
		reading(key("acct-1", "seven_day_opus"), 99, "opus"),
		reading(key("acct-2", "seven_day"), 99, ""),
	}
	for _, tc := range []struct {
		name         string
		store        QuotaBridgeStore
		observations []domain.WorkerQuotaObservation
		wantBuckets  int
		wantMet      bool
		wantReason   string
	}{
		{name: "governing and excluded windows", observations: append([]domain.WorkerQuotaObservation{reading(key("acct-1", "seven_day"), 10, ""), reading(key("acct-1", "five_hour"), 5, "")}, excluded...),
			wantBuckets: 2, wantMet: true, wantReason: "is at 10%, below 50%"},
		{name: "a declared window missing among excluded ones", observations: append([]domain.WorkerQuotaObservation{reading(key("acct-1", "seven_day"), 10, "")}, excluded...),
			wantBuckets: 1, wantReason: "pending: pool pool-claude has missing five_hour"},
		{name: "only excluded windows", observations: excluded, wantReason: "pending: no observation of pool pool-claude yet"},
		{name: "no observations", wantReason: "pending: no observation of pool pool-claude yet"},
		{name: "a failing local store with worker observations", store: failingBucketStore{},
			observations: append([]domain.WorkerQuotaObservation{reading(key("acct-1", "five_hour"), 20, ""), reading(key("acct-1", "seven_day"), 15, "")}, excluded...),
			wantBuckets:  2, wantMet: true, wantReason: "is at 20%, below 50%"},
		{name: "a failing local store and no worker observation", store: failingBucketStore{},
			wantReason: "pending: the buckets that govern pool pool-claude are unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bridge := QuotaBridge{Disabled: true, Store: tc.store, Now: func() time.Time { return now }, Pools: []QuotaPoolBinding{binding}}
			workers := []domain.WorkerSnapshot{{WorkerID: "homelab", QuotaObservations: tc.observations}}
			report, err := bridge.ReconcileState(context.Background(), QuotaPlanningStateInput{WorkerSnapshots: workers})
			if err != nil {
				t.Fatal(err)
			}
			states := domain.MergeQuotaObservations(nil, workers)
			observation, outcome, reason, err := domain.EvaluateQuotaWait(wait, report.Pools, states, now, 0)
			if err != nil {
				t.Fatal(err)
			}
			if observation.Buckets != tc.wantBuckets || (outcome == domain.TaskWaitMet) != tc.wantMet || !strings.Contains(reason, tc.wantReason) {
				t.Fatalf("buckets=%d outcome=%q reason=%q, want %d buckets, met=%v, %q (pool %+v)",
					observation.Buckets, outcome, reason, tc.wantBuckets, tc.wantMet, tc.wantReason, report.Pools)
			}
		})
	}
}

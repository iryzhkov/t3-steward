package backlog

import (
	"context"
	"sort"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// QuotaRecoveryGrantAuthorizer is a pure source-qualified issuance gate.
type QuotaRecoveryGrantAuthorizer func(domain.QuotaPool, domain.ThrottleAttemptRecord, time.Time) *domain.QuotaRecoveryGrant

// CoordinatorQuotaRecoveryGrant binds issuance to this authenticated execution snapshot.
type CoordinatorQuotaRecoveryGrant func(context.Context, domain.QuotaPool, domain.ThrottleAttemptRecord, domain.WorkerSnapshot, time.Time) (*domain.QuotaRecoveryGrant, error)

// FoldQuotaRecoveryStates retains the most restrictive newest competitor before
// any map projection. Conflicting reset epochs cannot issue recovery authority.
func FoldQuotaRecoveryStates(pool domain.QuotaPool, states []domain.BucketState) ([]domain.BucketState, bool) {
	groups := make(map[domain.BucketKey][]domain.BucketState)
	var relevant []domain.BucketState
	for _, state := range states {
		if domain.PoolBucketMatcher(pool)(state) {
			relevant = append(relevant, state)
		}
	}
	for _, state := range domain.NewestQuotaStates(relevant) {
		if !domain.ValidQuotaPercent(state.UsedPercent) {
			return nil, false
		}
		groups[state.Key] = append(groups[state.Key], state)
	}
	result := make([]domain.BucketState, 0, len(groups))
	for key, group := range groups {
		sort.SliceStable(group, func(i, j int) bool { return group[i].ObservedAt.Before(group[j].ObservedAt) })
		state, issue, exists := reconcileAdmissionBucketStates(key, group)
		if issue != nil || !exists {
			return nil, false
		}
		result = append(result, state)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key.String() < result[j].Key.String() })
	return result, true
}

package domain

import (
	"math"
	"sort"
	"time"
)

// QuotaObservationEpoch supplies the legacy omitted epoch from its own reset.
// An explicitly reported epoch remains authoritative for conflict detection.
func QuotaObservationEpoch(state BucketState) string {
	if state.Epoch != "" {
		return state.Epoch
	}
	return EpochFor(state.ResetsAt)
}

func ValidQuotaPercent(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value >= 0 && value <= 100
}

// NewestQuotaStates retains all equally timed competitors. The downstream
// reader reconciles their restrictions instead of allowing input order to win.
func NewestQuotaStates(states []BucketState) []BucketState {
	latest := map[BucketKey]time.Time{}
	for _, state := range states {
		at, ok := latest[state.Key]
		if !ok || state.ObservedAt.After(at) {
			latest[state.Key] = state.ObservedAt
		}
	}
	out := make([]BucketState, 0, len(states))
	for _, state := range states {
		if state.ObservedAt.Equal(latest[state.Key]) {
			out = append(out, state)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Key.String() != b.Key.String() {
			return a.Key.String() < b.Key.String()
		}
		if a.Epoch != b.Epoch {
			return a.Epoch < b.Epoch
		}
		if a.Phase.Rank() != b.Phase.Rank() {
			return a.Phase.Rank() > b.Phase.Rank()
		}
		if a.UsedPercent != b.UsedPercent {
			return a.UsedPercent > b.UsedPercent
		}
		return a.Healthy != b.Healthy && !a.Healthy
	})
	return out
}

// CollectorBatchComplete requires the newest native acquisition to contain all
// governing required windows. Older complete batches cannot hide a partial one.
func CollectorBatchComplete(pool QuotaPool, states []BucketState) bool {
	if pool.Collector == nil {
		return true
	}
	var latest *QuotaProvenance
	for _, state := range states {
		if !CollectorObservationQualified(pool, state) {
			continue
		}
		if latest == nil || state.Provenance.AcquiredAt.After(latest.AcquiredAt) {
			latest = state.Provenance
		}
	}
	if latest == nil {
		return false
	}
	// A previous anonymous restriction cannot disappear merely by ageing out.
	// Only a subsequent account-bound acquisition can supersede its history.
	belongs := PoolBucketMatcher(pool)
	for _, state := range states {
		if belongs(state) && (state.Phase.Rank() > 0 || state.UsedPercent >= 100) && !CollectorWindowGoverns(pool, state.Key.Window) {
			// There is no proven native mapping that can resolve this constraint.
			return false
		}
		if belongs(state) && state.Key.AccountID == "" && (state.Phase.Rank() > 0 || state.UsedPercent >= 100) &&
			!latest.AcquiredAt.After(state.ObservedAt) {
			return false
		}
	}
	covered := map[string]bool{}
	for _, state := range states {
		if !CollectorObservationQualified(pool, state) {
			continue
		}
		if state.Provenance.AcquiredAt.Equal(latest.AcquiredAt) && !SameQuotaAcquisition(latest, state.Provenance) {
			return false
		}
		if SameQuotaAcquisition(latest, state.Provenance) {
			covered[state.Key.Window] = true
		}
	}
	for _, window := range pool.Collector.RequiredWindows {
		if CollectorWindowGoverns(pool, window.Window) && !covered[window.Window] {
			return false
		}
	}
	return true
}

// UnknownAccountRestricts is a fresh anonymous overlay, never positive account
// evidence. A known different account is separate rather than an overlay.
func UnknownAccountRestricts(pool QuotaPool, state BucketState, now time.Time, maxAge time.Duration) bool {
	if pool.AccountID == "" || state.Key.AccountID != "" || !PoolBucketMatcher(pool)(state) || state.ObservedAt.After(now) ||
		!QuotaReadingFresh(state, now, QuotaObservationMaxAge(pool, state, maxAge)) {
		return false
	}
	return state.Phase.Rank() > 0 || state.UsedPercent >= 100
}

package daemon

import (
	"strconv"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// IssueCollectorRecoveryGrant is the sole producer rule for collector resume
// authority. Callers supply conservatively folded authenticated observations and
// the actual stop acknowledgement, never a collection-time replacement.
func IssueCollectorRecoveryGrant(cfg config.Config, pool domain.QuotaPool, route domain.ProviderRoute, stopped []domain.QuotaBucketEpoch, stoppedAt time.Time, stopCommandID string, stopSequence int64, states []domain.BucketState, now time.Time) *domain.QuotaRecoveryGrant {
	if !CollectorPoolCovered(pool) || !domain.CollectorCoversRoute(pool, route) || stoppedAt.IsZero() || len(stopped) == 0 {
		return nil
	}
	// The whole pool closure is required, even when this execution uses one model.
	if !domain.ReadQuotaWindows(pool, states, now, QuotaStaleThreshold(cfg)).Fresh() {
		return nil
	}
	byKey := make(map[domain.BucketKey]domain.BucketState, len(states))
	for _, state := range states {
		if _, duplicate := byKey[state.Key]; duplicate {
			return nil
		}
		byKey[state.Key] = state
	}
	var source *domain.QuotaProvenance
	instance := ""
	expires := time.Time{}
	for _, state := range states {
		if !domain.CollectorObservationQualified(pool, state) {
			continue
		}
		if source != nil && (!domain.SameQuotaAcquisition(source, state.Provenance) || instance != state.Key.ProviderInstanceID) {
			return nil
		}
		source = state.Provenance
		instance = state.Key.ProviderInstanceID
		age := domain.QuotaObservationMaxAge(pool, state, QuotaStaleThreshold(cfg))
		until := state.ObservedAt.Add(age)
		if state.ResetsAt == nil {
			return nil
		}
		if state.ResetsAt.Before(until) {
			until = *state.ResetsAt
		}
		if state.OriginValidUntil != nil && state.OriginValidUntil.Before(until) {
			until = *state.OriginValidUntil
		}
		if expires.IsZero() || until.Before(expires) {
			expires = until
		}
	}
	if source == nil || !source.AcquiredAt.After(stoppedAt) || !expires.After(now) ||
		now.Before(source.AcquiredAt.Add(cfg.Resume.ResetSettleDelay.D())) {
		return nil
	}
	for _, epoch := range stopped {
		if epoch.Bucket.AccountID != "" && epoch.Bucket.AccountID != pool.AccountID || !domain.CollectorWindowGoverns(pool, epoch.Bucket.Window) {
			return nil
		}
		bound := false
		for _, provider := range pool.ProviderInstanceIDs {
			bound = bound || provider == epoch.Bucket.ProviderInstanceID
		}
		seconds, err := strconv.ParseInt(epoch.Epoch, 10, 64)
		if !bound || err != nil || seconds <= 0 || !source.AcquiredAt.After(time.Unix(seconds, 0)) {
			return nil
		}
	}
	for _, model := range pool.Models {
		if recovered, _ := CollectorBucketsRecovered(cfg, pool, nil, stoppedAt, domain.Thread{ProviderInstanceID: route.ProviderInstanceID, Model: model}, byKey, now); !recovered {
			return nil
		}
	}
	scope := domain.RecoveryEvidenceScope(pool)
	return &domain.QuotaRecoveryGrant{EvidenceScope: scope, EvidenceDigest: domain.QuotaRecoveryEvidenceDigest(pool.AccountID, scope, states, nil), OriginProviderInstanceID: instance,
		CoverageDigest: CollectorRecoveryDigest(cfg, pool, route), QuotaPoolID: pool.ID, AccountID: pool.AccountID,
		StopCommandID: stopCommandID, StoppedAt: stoppedAt, StopObservedSequence: stopSequence,
		Provenance: *source, ExpiresAt: expires}
}

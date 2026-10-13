package daemon

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// CollectorInferenceDisabled is declared coverage, not producer liveness.
// An ambiguous watchdog projection still forbids inference; it cannot prove recovery.
func CollectorInferenceDisabled(cfg config.Config, thread domain.Thread) bool {
	for workerID, worker := range cfg.BacklogV2.Workers {
		if provider, ok := worker.Providers[thread.ProviderInstanceID]; ok {
			pool, covered := cfg.CollectorCoverage(domain.ProviderRoute{
				WorkerID: workerID, ProviderInstanceID: thread.ProviderInstanceID,
				Model: thread.Model, QuotaPoolID: provider.QuotaPool,
			})
			if covered && collectorIsClaude(pool) {
				return true
			}
		}
	}
	return false
}

// CollectorPoolCovered limits this policy to the qualified Claude source.
func CollectorPoolCovered(pool domain.QuotaPool) bool { return collectorIsClaude(pool) }

func collectorIsClaude(pool domain.QuotaPool) bool {
	return pool.Collector != nil && strings.Contains(strings.ToLower(pool.Provider), "claude")
}

func CollectorBucketInferenceDisabled(cfg config.Config, state domain.BucketState) bool {
	for _, worker := range cfg.BacklogV2.Workers {
		if provider, ok := worker.Providers[state.Key.ProviderInstanceID]; ok {
			for _, model := range provider.Models {
				thread := domain.Thread{ProviderInstanceID: state.Key.ProviderInstanceID, Model: model}
				if thread.MatchesBucket(state.Key, state.ModelSelector) && CollectorInferenceDisabled(cfg, thread) {
					return true
				}
			}
		}
	}
	return false
}

// CollectorBucketsRecovered uses only a complete verified acquisition for the
// declared account. Historical anonymous keys stay stored; resolving their
// pause requires the declared window plus a real post-stop reset or explicit rearm.
func CollectorBucketsRecovered(cfg config.Config, pool domain.QuotaPool, stopped []domain.BucketKey, stoppedAt time.Time, thread domain.Thread, byKey map[domain.BucketKey]domain.BucketState, now time.Time) (bool, string) {
	cfg = cfg.CollectorRecoveryConfig()
	if !collectorIsClaude(pool) {
		return false, "no declared Claude collector coverage"
	}
	catalogPool := pool
	pool.Models = []string{thread.Model}
	qualified := make([]domain.BucketState, 0, len(byKey))
	for _, state := range byKey {
		if domain.CollectorObservationQualified(pool, state) {
			qualified = append(qualified, state)
		}
	}
	windows := domain.ReadQuotaWindows(pool, qualified, now, QuotaStaleThreshold(cfg))
	if !windows.Fresh() {
		problem, _ := windows.Problem()
		return false, "collector recovery waits: " + problem.String()
	}
	selected := make(map[string][]domain.BucketState)
	var acquisition time.Time
	var batch *domain.QuotaProvenance
	for _, window := range windows.Windows {
		state, exists := byKey[window.Key]
		if !exists || !domain.CollectorObservationQualified(pool, state) ||
			(batch != nil && !domain.SameQuotaAcquisition(batch, state.Provenance)) ||
			state.ObservedAt.After(now) || !state.ObservedAt.After(stoppedAt) ||
			state.Phase != domain.PhaseNormal || !state.Healthy ||
			math.IsNaN(state.UsedPercent) || math.IsInf(state.UsedPercent, 0) ||
			state.UsedPercent < 0 || state.UsedPercent >= cfg.Resume.BelowPercent ||
			state.ResetsAt == nil || !state.ResetsAt.After(now) ||
			state.Epoch != domain.EpochFor(state.ResetsAt) {
			return false, fmt.Sprintf("%s has no fresh complete below-threshold collector recovery", window.Key)
		}
		batch = state.Provenance
		acquisition = state.ObservedAt
		selected[state.Key.Window] = append(selected[state.Key.Window], state)
	}
	if batch == nil {
		return false, "collector recovery has no verified acquisition"
	}
	for _, key := range stopped {
		prior, exists := byKey[key]
		if !exists {
			return false, fmt.Sprintf("no historical state for %s", key)
		}
		recovered, at := collectorStopRecovered(pool, prior, selected, stoppedAt, acquisition)
		if !recovered {
			return false, fmt.Sprintf("%s has no confirmed collector recovery since the stop", key)
		}
		if now.Before(at.Add(cfg.Resume.ResetSettleDelay.D())) {
			return false, fmt.Sprintf("%s is waiting for the collector recovery settle delay", key)
		}
	}
	// A complete account reading cannot erase a later local stop, including
	// anonymous or older-source overlays. Equal timestamps stay restrictive.
	for _, state := range byKey {
		if !domain.CollectorWindowGoverns(pool, state.Key.Window) && domain.CollectorObservationQualified(catalogPool, state) {
			continue
		}
		if ignoredWindowIn(cfg.Policy.IgnoreWindows, state.Key.Window) ||
			!collectorStateApplies(pool, thread, state) ||
			(state.Healthy && state.Phase == domain.PhaseNormal) {
			continue
		}
		if domain.CollectorObservationQualified(pool, state) {
			continue // Each governing qualified reading was checked above.
		}
		if !state.ObservedAt.Before(acquisition) {
			return false, fmt.Sprintf("%s has newer or tied restrictive local evidence", state.Key)
		}
		recovered, recoveryAt := collectorStopRecovered(pool, state, selected, stoppedAt, acquisition)
		if !recovered || now.Before(recoveryAt.Add(cfg.Resume.ResetSettleDelay.D())) {
			return false, fmt.Sprintf("%s remains a restrictive local quota overlay", state.Key)
		}
	}
	return true, ""
}

// Shared account coverage follows the declared pool instances, while model
// selectors remain restricted to the exact execution model.
func collectorStateApplies(pool domain.QuotaPool, thread domain.Thread, state domain.BucketState) bool {
	return domain.PoolBucketMatcher(pool)(state) && (domain.Thread{ProviderInstanceID: state.Key.ProviderInstanceID, Model: thread.Model}).MatchesBucket(state.Key, state.ModelSelector)
}

func collectorStopRecovered(pool domain.QuotaPool, prior domain.BucketState, selected map[string][]domain.BucketState, stoppedAt, acquisition time.Time) (bool, time.Time) {
	candidates := selected[prior.Key.Window]
	if len(candidates) == 0 || !domain.CollectorWindowGoverns(pool, prior.Key.Window) {
		return false, time.Time{}
	}
	// Known accounts are never silently replaced by another account.
	if prior.Key.AccountID != "" && prior.Key.AccountID != pool.AccountID {
		return false, time.Time{}
	}
	if prior.Key.AccountID == pool.AccountID {
		for _, current := range candidates {
			if current.Key == prior.Key && current.RecoveredAt != nil && current.RecoveredAt.After(stoppedAt) {
				return true, *current.RecoveredAt
			}
		}
		return false, time.Time{}
	}
	// The old anonymous key must be a declared execution binding for this pool.
	bound := false
	for _, provider := range pool.ProviderInstanceIDs {
		bound = bound || provider == prior.Key.ProviderInstanceID
	}
	if !bound {
		return false, time.Time{}
	}
	if prior.RecoveredAt != nil && prior.RecoveredAt.After(stoppedAt) && !acquisition.Before(*prior.RecoveredAt) {
		return true, *prior.RecoveredAt
	}
	if prior.ResetsAt == nil || !acquisition.After(*prior.ResetsAt) || !acquisition.After(stoppedAt) {
		return false, time.Time{}
	}
	return true, acquisition
}

// CollectorCoordinatorVeto retains local restrictions newer than the acquisition
// cited by a coordinator recovery grant. It supplies no recovery permission.
func CollectorCoordinatorVeto(cfg config.Config, pool domain.QuotaPool, thread domain.Thread, states []domain.BucketState, acquiredAt time.Time) (bool, string) {
	cfg = cfg.CollectorRecoveryConfig()
	if !collectorIsClaude(pool) {
		return false, ""
	}
	if acquiredAt.IsZero() {
		return true, "collector coordinator resume has no acquisition timestamp"
	}
	for _, state := range states {
		if ignoredWindowIn(cfg.Policy.IgnoreWindows, state.Key.Window) ||
			!collectorStateApplies(pool, thread, state) ||
			(state.Phase == domain.PhaseNormal && state.Healthy) {
			continue
		}
		if !state.ObservedAt.Before(acquiredAt) || !state.UpdatedAt.Before(acquiredAt) ||
			(state.StoppedAt != nil && !state.StoppedAt.Before(acquiredAt)) {
			return true, fmt.Sprintf("%s has newer or tied restrictive local evidence", state.Key)
		}
	}
	return false, ""
}

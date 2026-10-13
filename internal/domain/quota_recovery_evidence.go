package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"time"
)

// QuotaRecoveryEvidenceScope is a quota-only pool projection, never a model catalog.
type QuotaRecoveryEvidenceScope struct {
	ProviderInstanceIDs []string `json:"providerInstanceIds"`
	Models []string `json:"models"`
	RequiredWindows []QuotaCollectorWindow `json:"requiredWindows"`
}

func RecoveryEvidenceScope(pool QuotaPool) QuotaRecoveryEvidenceScope {
	scope := QuotaRecoveryEvidenceScope{ProviderInstanceIDs: append([]string(nil), pool.ProviderInstanceIDs...), Models: append([]string(nil), pool.Models...)}
	if pool.Collector != nil {
		for _, window := range pool.Collector.RequiredWindows {
			copyWindow := QuotaCollectorWindow{Window: window.Window, Models: append([]string(nil), window.Models...)}
			sort.Strings(copyWindow.Models)
			scope.RequiredWindows = append(scope.RequiredWindows, copyWindow)
		}
	}
	sort.Strings(scope.ProviderInstanceIDs)
	sort.Strings(scope.Models)
	sort.Slice(scope.RequiredWindows, func(i,j int) bool { return scope.RequiredWindows[i].Window < scope.RequiredWindows[j].Window })
	return scope
}

// QuotaRecoveryEvidenceDigest binds only governing quota facts and authenticated
// source envelopes. It excludes credentials, prompts, log bodies and event IDs.
func QuotaRecoveryEvidenceDigest(account string, scope QuotaRecoveryEvidenceScope, local []BucketState, workers []WorkerSnapshot) string {
	matches := PoolBucketMatcher(QuotaPool{AccountID: account, ProviderInstanceIDs: scope.ProviderInstanceIDs, Models: scope.Models, Collector: &QuotaCollectorCoverage{RequiredWindows: scope.RequiredWindows}})
	rows := []string{}
	appendState := func(state BucketState) bool {
		if !matches(state) {
			return true
		}
		projected := struct {
			Key                                                BucketKey
			Phase                                              Phase
			UsedPercent                                        float64
			Healthy                                            bool
			Epoch, ModelSelector                               string
			ObservedAt, UpdatedAt                              time.Time
			ResetsAt, StoppedAt, RecoveredAt, OriginValidUntil *time.Time
			Provenance                                         *QuotaProvenance
		}{state.Key, state.Phase, state.UsedPercent, state.Healthy, state.Epoch, state.ModelSelector, state.ObservedAt, state.UpdatedAt,
			state.ResetsAt, state.StoppedAt, state.RecoveredAt, state.OriginValidUntil, state.Provenance}
		raw, err := json.Marshal(projected)
		if err != nil {
			return false
		}
		rows = append(rows, string(raw))
		return true
	}
	for _, state := range local {
		if !appendState(state) {
			return ""
		}
	}
	for _, worker := range workers {
		relevant := false
		for _, observed := range worker.QuotaObservations {
			relevant = relevant || matches(BucketState{Key: observed.Key, ModelSelector: observed.ModelSelector})
		}
		if !relevant {
			continue
		}
		envelope := struct {
			WorkerID, WorkerEpoch      string
			CoordinatorEpoch, Sequence int64
			Connected                  bool
			ObservedAt, ValidUntil     time.Time
		}{
			worker.WorkerID, worker.WorkerEpoch, worker.CoordinatorEpoch, worker.Sequence, worker.Connected, worker.ObservedAt, worker.ValidUntil}
		raw, _ := json.Marshal(envelope)
		rows = append(rows, string(raw))
		for _, state := range MergeQuotaObservations(nil, []WorkerSnapshot{worker}) {
			if !appendState(state) {
				return ""
			}
		}
	}
	sort.Strings(rows)
	raw, _ := json.Marshal(rows)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

package domain

import (
	"slices"
	"strings"
	"time"
)

// QuotaProvenance is constructed only after a native joint account/usage read.
// AcquiredAt is its real start, matching ObservedAt; completion remains local
// diagnostic status. Remote merges bind OriginWorkerID to the authenticated peer.
type QuotaProvenance struct {
	SourceID       string    `json:"sourceId"`
	AcquisitionID  string    `json:"acquisitionId"`
	OriginWorkerID string    `json:"originWorkerId"`
	AcquiredAt     time.Time `json:"acquiredAt"`
}

func CloneQuotaProvenance(p *QuotaProvenance) *QuotaProvenance {
	if p == nil {
		return nil
	}
	copy := *p
	return &copy
}

type QuotaCollectorOrigin struct {
	WorkerID           string `json:"workerId" yaml:"worker_id"`
	ProviderInstanceID string `json:"providerInstanceId" yaml:"provider_instance_id"`
}
type QuotaCollectorWindow struct {
	Window string `json:"window" yaml:"window"`
	// Empty Models declares an account-wide window. Exact IDs never grant
	// model authorization; they restrict which configured pool models it governs.
	Models []string `json:"models,omitempty" yaml:"models"`
}
type QuotaCollectorCoverage struct {
	SourceID          string                 `json:"sourceId"`
	AccountID         string                 `json:"accountId"`
	Origins           []QuotaCollectorOrigin `json:"origins"`
	MaxObservationAge time.Duration          `json:"maxObservationAge"`
	RequiredWindows   []QuotaCollectorWindow `json:"requiredWindows"`
}

func CollectorCoversRoute(pool QuotaPool, route ProviderRoute) bool {
	return pool.Collector != nil && pool.AccountID != "" && pool.Collector.AccountID == pool.AccountID &&
		(route.QuotaPoolID == "" || route.QuotaPoolID == pool.ID) &&
		slices.Contains(pool.ProviderInstanceIDs, route.ProviderInstanceID) &&
		(route.Model == "" || slices.Contains(pool.Models, route.Model))
}

func CollectorWindowGoverns(pool QuotaPool, window string) bool {
	if pool.Collector == nil {
		return true
	}
	for _, required := range pool.Collector.RequiredWindows {
		if required.Window == window {
			if len(required.Models) == 0 || len(pool.Models) == 0 {
				return true
			}
			for _, model := range required.Models {
				if slices.Contains(pool.Models, model) {
					return true
				}
			}
			return false
		}
	}
	return false
}

// CollectorBucketRelevant admits conservative restrictions for unmapped scopes,
// without treating them as required coverage or granting model authorization.
func CollectorBucketRelevant(pool QuotaPool, state BucketState) bool {
	if pool.Collector == nil {
		return false
	}
	if !slices.Contains(pool.ProviderInstanceIDs, state.Key.ProviderInstanceID) ||
		(state.Key.AccountID != "" && state.Key.AccountID != pool.AccountID) {
		return false
	}
	for _, required := range pool.Collector.RequiredWindows {
		if required.Window == state.Key.Window {
			return CollectorWindowGoverns(pool, state.Key.Window)
		}
	}
	selector := strings.ToLower(strings.TrimSpace(state.ModelSelector))
	if selector == "" {
		return true
	}
	for _, model := range pool.Models {
		if strings.Contains(strings.ToLower(model), selector) {
			return true
		}
	}
	return false
}

// CollectorObservationQualified never obtains authority from an account ID or
// a guessed event format. Both producer provenance and a declared origin must match.
func CollectorObservationQualified(pool QuotaPool, state BucketState) bool {
	c, p := pool.Collector, state.Provenance
	if c == nil || p == nil || c.SourceID == "" || c.AccountID == "" || c.AccountID != pool.AccountID ||
		state.Key.AccountID != c.AccountID || p.SourceID != c.SourceID ||
		p.AcquisitionID == "" || strings.TrimSpace(p.AcquisitionID) != p.AcquisitionID ||
		p.AcquiredAt.IsZero() || !p.AcquiredAt.Equal(state.ObservedAt) || !ValidQuotaPercent(state.UsedPercent) || state.ResetsAt == nil ||
		!CollectorWindowGoverns(pool, state.Key.Window) {
		return false
	}
	for _, origin := range c.Origins {
		if origin.WorkerID == p.OriginWorkerID && origin.ProviderInstanceID == state.Key.ProviderInstanceID {
			return true
		}
	}
	return false
}

// QuotaObservationMaxAge preserves legacy freshness for every unqualified
// observation, including anonymous restrictive overlays and old protocol peers.
func QuotaObservationMaxAge(pool QuotaPool, state BucketState, fallback time.Duration) time.Duration {
	if fallback <= 0 {
		fallback = DefaultQuotaStaleAfter
	}
	if CollectorObservationQualified(pool, state) && pool.Collector.MaxObservationAge > 0 {
		return pool.Collector.MaxObservationAge
	}
	if pool.Collector != nil && fallback > time.Minute {
		return time.Minute
	}
	return fallback
}

func SameQuotaAcquisition(a, b *QuotaProvenance) bool {
	return a != nil && b != nil && a.SourceID == b.SourceID && a.AcquisitionID == b.AcquisitionID &&
		a.OriginWorkerID == b.OriginWorkerID && a.AcquiredAt.Equal(b.AcquiredAt)
}

package backlog

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

type QuotaBridgeStore interface {
	QuotaAdmissionTransitionStore
	ListBuckets(context.Context) ([]domain.BucketState, error)
}

type QuotaPoolBinding struct {
	ID                  string
	Provider            string
	AccountID           string
	ProviderInstanceIDs []string
	MaxConcurrent       int
}

type QuotaBridge struct {
	Store                   QuotaBridgeStore
	Pools                   []QuotaPoolBinding
	MaxObservationAge       time.Duration
	SafetyMargin            float64
	FallbackForecastPerHour float64
	LongWindowCap           float64
	SurplusHorizon          time.Duration
	Now                     func() time.Time
}

type QuotaBridgeReport struct {
	Pools      []domain.QuotaPool
	Windows    []QuotaWindowBudget
	Derived    []QuotaPoolAdmissionSnapshot
	Directives []domain.ThrottleDirective
}

// Reconcile projects real provider bucket observations into configured fleet
// pools. It never sums observations: a bucket identity appears once in a pool,
// while duplicate reports remain available to conservative conflict handling.
func (b QuotaBridge) Reconcile(ctx context.Context, reservations []QuotaResumeReservation) (QuotaBridgeReport, error) {
	if b.Store == nil {
		return QuotaBridgeReport{}, fmt.Errorf("quota bridge store is required")
	}
	if b.MaxObservationAge <= 0 {
		return QuotaBridgeReport{}, fmt.Errorf("quota bridge maximum observation age must be positive")
	}
	now := time.Now().UTC()
	if b.Now != nil {
		now = b.Now().UTC()
	}
	pools, owners, err := quotaBridgePools(b.Pools)
	if err != nil {
		return QuotaBridgeReport{}, err
	}
	states, err := b.Store.ListBuckets(ctx)
	if err != nil {
		return QuotaBridgeReport{}, fmt.Errorf("load quota observations: %w", err)
	}
	sort.Slice(states, func(i, j int) bool {
		if states[i].Key.String() != states[j].Key.String() {
			return states[i].Key.String() < states[j].Key.String()
		}
		if !states[i].ObservedAt.Equal(states[j].ObservedAt) {
			return states[i].ObservedAt.Before(states[j].ObservedAt)
		}
		if states[i].Epoch != states[j].Epoch {
			return states[i].Epoch < states[j].Epoch
		}
		return states[i].Phase < states[j].Phase
	})
	poolIndex := make(map[string]int, len(pools))
	for index := range pools {
		poolIndex[pools[index].ID] = index
	}
	bucketSeen := make(map[string]map[domain.BucketKey]struct{}, len(pools))
	var relevant []domain.BucketState
	for _, state := range states {
		poolID, configured := owners[state.Key.ProviderInstanceID]
		if !configured {
			continue
		}
		pool := &pools[poolIndex[poolID]]
		if pool.AccountID != "" && state.Key.AccountID != pool.AccountID {
			continue
		}
		if bucketSeen[poolID] == nil {
			bucketSeen[poolID] = make(map[domain.BucketKey]struct{})
		}
		if _, exists := bucketSeen[poolID][state.Key]; !exists {
			pool.Buckets = append(pool.Buckets, state.Key)
			bucketSeen[poolID][state.Key] = struct{}{}
		}
		relevant = append(relevant, state)
	}
	for index := range pools {
		sort.Slice(pools[index].Buckets, func(i, j int) bool {
			return pools[index].Buckets[i].String() < pools[index].Buckets[j].String()
		})
	}
	derived, err := DeriveQuotaPoolAdmissions(QuotaAdmissionDerivationInput{
		Now: now, MaxObservationAge: b.MaxObservationAge, Pools: pools,
		BucketStates: relevant, ResumeReservations: reservations,
	})
	if err != nil {
		return QuotaBridgeReport{}, err
	}
	directives, err := ReconcileQuotaAdmissionTransitions(ctx, b.Store, derived, now)
	if err != nil {
		return QuotaBridgeReport{}, err
	}
	for index := range pools {
		for _, snapshot := range derived {
			if snapshot.QuotaPoolID == pools[index].ID {
				pools[index].Admission = snapshot.Admission
				pools[index].UpdatedAt = now
				break
			}
		}
	}
	windows, err := deriveQuotaPlanningWindows(now, pools, relevant, owners, b)
	if err != nil {
		return QuotaBridgeReport{}, err
	}
	return QuotaBridgeReport{
		Pools: pools, Windows: windows, Derived: derived, Directives: directives,
	}, nil
}

// ReconcileState reconstructs active concurrency and paused-attempt reservations
// from one durable coordinator snapshot before changing admission.
func (b QuotaBridge) ReconcileState(ctx context.Context, input QuotaPlanningStateInput) (QuotaBridgeReport, error) {
	pools, _, err := quotaBridgePools(b.Pools)
	if err != nil {
		return QuotaBridgeReport{}, err
	}
	input.QuotaPools = pools
	state, err := DeriveQuotaPlanningState(input)
	if err != nil {
		return QuotaBridgeReport{}, fmt.Errorf("reconstruct quota planning state: %w", err)
	}
	report, err := b.Reconcile(ctx, state.ResumeReservations)
	if err != nil {
		return QuotaBridgeReport{}, err
	}
	input.QuotaPools = report.Pools
	input.QuotaWindows = report.Windows
	state, err = DeriveQuotaPlanningState(input)
	if err != nil {
		return QuotaBridgeReport{}, fmt.Errorf("reconstruct quota planning windows: %w", err)
	}
	report.Pools = state.QuotaPools
	report.Windows = state.QuotaWindows
	return report, nil
}

func quotaBridgePools(bindings []QuotaPoolBinding) ([]domain.QuotaPool, map[string]string, error) {
	if len(bindings) == 0 {
		return nil, nil, fmt.Errorf("at least one quota pool binding is required")
	}
	sorted := append([]QuotaPoolBinding(nil), bindings...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })
	owners := make(map[string]string)
	pools := make([]domain.QuotaPool, 0, len(sorted))
	for _, binding := range sorted {
		if strings.TrimSpace(binding.ID) != binding.ID || binding.ID == "" ||
			strings.TrimSpace(binding.Provider) != binding.Provider || binding.Provider == "" {
			return nil, nil, fmt.Errorf("quota pool binding identity and provider must be nonempty and trimmed")
		}
		if binding.MaxConcurrent <= 0 {
			return nil, nil, fmt.Errorf("quota pool %q maximum concurrency must be positive", binding.ID)
		}
		instances := append([]string(nil), binding.ProviderInstanceIDs...)
		sort.Strings(instances)
		instances = uniqueStrings(instances)
		if len(instances) == 0 {
			return nil, nil, fmt.Errorf("quota pool %q has no provider instances", binding.ID)
		}
		for _, instance := range instances {
			if strings.TrimSpace(instance) != instance || instance == "" {
				return nil, nil, fmt.Errorf("quota pool %q has an invalid provider instance", binding.ID)
			}
			if owner, exists := owners[instance]; exists && owner != binding.ID {
				return nil, nil, fmt.Errorf("provider instance %q belongs to quota pools %q and %q", instance, owner, binding.ID)
			}
			owners[instance] = binding.ID
		}
		pools = append(pools, domain.QuotaPool{
			ID: binding.ID, Provider: binding.Provider, AccountID: binding.AccountID,
			ProviderInstanceIDs: instances, Admission: domain.AdmissionClosed,
			MaxConcurrent: binding.MaxConcurrent,
		})
	}
	return pools, owners, nil
}

func uniqueStrings(values []string) []string {
	if len(values) < 2 {
		return values
	}
	result := values[:1]
	for _, value := range values[1:] {
		if value != result[len(result)-1] {
			result = append(result, value)
		}
	}
	return result
}

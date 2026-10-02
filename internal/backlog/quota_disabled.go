package backlog

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// disabledReport grants admission by explicit operator policy, without claiming
// quota was observed healthy. Concurrency remains conservative and fleet-wide.
//
// Occupancy is reconstructed by the same derivation the enabled path uses,
// rather than by a rule of its own. Counting every assignment that was not
// completed or released ignored whether its attempt still holds a provider
// slot, so an attempt parked on an external wait kept the slot the H4 contract
// says it releases: on a one-slot pool a second campaign stayed ready and
// unassigned for the whole park, while explain called the task eligible. An
// operator had to debug that contradiction from scratch.
//
// This path is the one that actually runs. A synthetic provider produces no
// quota observations, so enabling checks closes admission entirely, and every
// disposable test and every quota-free deployment takes this branch. Agreeing
// with the enabled path by construction is the only way the two cannot drift.
func (b QuotaBridge) disabledReport(input QuotaPlanningStateInput) (QuotaBridgeReport, error) {
	pools, _, err := quotaBridgePools(b.Pools)
	if err != nil {
		return QuotaBridgeReport{}, err
	}
	now := time.Now().UTC()
	if b.Now != nil {
		now = b.Now().UTC()
	}
	// The identity check keeps this path's own refusal and its wording, which
	// is stricter than the derivation's about an assignment with no ID.
	seen := map[string]bool{}
	for _, a := range input.Assignments {
		if a.ID == "" || seen[a.ID] {
			return QuotaBridgeReport{}, fmt.Errorf("invalid or repeated assignment %q", a.ID)
		}
		seen[a.ID] = true
	}
	input.QuotaPools = pools
	// Retained throttle records are not consulted while checks are disabled, and
	// the coordinator does not load them on this path at all. Passing them on
	// would make a contradictory record refuse a report that does not depend on
	// it; occupancy is reconstructed from attempts and assignments alone.
	input.ThrottleRecords = nil
	state, err := DeriveQuotaPlanningState(input)
	if err != nil {
		return QuotaBridgeReport{}, fmt.Errorf("reconstruct quota planning state: %w", err)
	}
	report := QuotaBridgeReport{ChecksDisabled: true, Pools: state.QuotaPools}
	for i := range report.Pools {
		p := &report.Pools[i]
		p.ChecksDisabled = true
		p.Admission = domain.AdmissionOpen
		p.UpdatedAt = now
		report.Derived = append(report.Derived, QuotaPoolAdmissionSnapshot{
			QuotaPoolID: p.ID, Admission: domain.AdmissionOpen, ObservedAt: now,
			Reason: "quota checks disabled by coordinator policy",
		})
	}
	return report, nil
}

// nameGoverningBuckets names in each pool the observed buckets that govern
// it, as the enabled path does, although nothing is enforced from them while
// checks are disabled. The names are what a pool's state is read from: quota
// waits and models fold a pool from its named buckets, and a pool naming none
// is read from every window of its providers. That gave a pool the age and
// staleness of an ignored window (overage) or of a window scoped to a model
// the pool does not serve, which are exactly the windows the enabled path
// leaves out.
//
// The observations are informational here, so a store that cannot list them
// leaves the pools unnamed rather than failing a report that does not depend
// on them.
func (b QuotaBridge) nameGoverningBuckets(ctx context.Context, pools []domain.QuotaPool, workers []domain.WorkerSnapshot) {
	var local []domain.BucketState
	if b.Store != nil {
		listed, err := b.Store.ListBuckets(ctx)
		if err != nil {
			return
		}
		local = listed
	}
	states := MergeWorkerQuotaObservations(local, workers)
	bindings := make(map[string]QuotaPoolBinding, len(b.Pools))
	for _, binding := range b.Pools {
		bindings[binding.ID] = binding
	}
	for index := range pools {
		pool := &pools[index]
		instances := make(map[string]bool, len(pool.ProviderInstanceIDs))
		for _, id := range pool.ProviderInstanceIDs {
			instances[id] = true
		}
		named := make(map[domain.BucketKey]bool, len(pool.Buckets))
		for _, key := range pool.Buckets {
			named[key] = true
		}
		for _, state := range states {
			if !instances[state.Key.ProviderInstanceID] || named[state.Key] ||
				(pool.AccountID != "" && state.Key.AccountID != pool.AccountID) ||
				!bucketGovernsPool(bindings[pool.ID], state) {
				continue
			}
			named[state.Key] = true
			pool.Buckets = append(pool.Buckets, state.Key)
		}
		sort.Slice(pool.Buckets, func(i, j int) bool {
			return pool.Buckets[i].String() < pool.Buckets[j].String()
		})
	}
}

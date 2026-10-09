package backlogadmin

import (
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// RoleQuotaSnapshot is request-local ranking evidence, not submission authority.
// It is built from the same immutable view and clock used by viability admission.
type RoleQuotaSnapshot struct {
	Now            time.Time
	Pools          map[string]domain.RouteRankPool
	ChecksDisabled map[string]bool
}

func (v view) roleQuotaSnapshot() RoleQuotaSnapshot {
	snapshot := RoleQuotaSnapshot{Now: v.now, Pools: map[string]domain.RouteRankPool{}, ChecksDisabled: map[string]bool{}}
	states := domain.MergeQuotaObservations(nil, v.workers)
	for _, pool := range v.records.QuotaPools {
		state, observedAt, _, disabled := v.admissionState(pool.ID)
		windows := domain.ReadQuotaWindows(pool, states, v.now, v.runtime.MaxQuotaObservationAge)
		if disabled {
			// Retained history is neither a ranking gate nor live headroom.
			state = domain.AdmissionOpen
			windows = domain.QuotaWindowSet{Pool: pool.ID, Unknown: true}
			snapshot.ChecksDisabled[pool.ID] = true
		} else {
			maxAge := v.runtime.MaxQuotaObservationAge
			if maxAge <= 0 {
				maxAge = domain.DefaultQuotaStaleAfter
			}
			admissionFresh := !observedAt.IsZero() && !observedAt.After(v.now) && v.now.Sub(observedAt) <= maxAge
			admissionKnown := state == domain.AdmissionOpen || state == domain.AdmissionConstrained || state == domain.AdmissionRecovering || state == domain.AdmissionDraining || state == domain.AdmissionClosed
			if !admissionFresh || !admissionKnown {
				if len(windows.Windows) == 0 {
					windows.Unknown = true
				}
				for i := range windows.Windows {
					windows.Windows[i].Stale = true
				}
			}
			for i := range windows.Windows {
				if windows.Windows[i].ObservedAt.After(v.now) {
					windows.Windows[i].Stale = true
				}
			}
		}
		// Concurrency is ranking evidence too: a pool at its limit ranks in the
		// saturated band below every pool with room.
		snapshot.Pools[pool.ID] = domain.RouteRankPool{ID: pool.ID, Admission: state, Windows: windows,
			Active: pool.ActiveAssignments, MaxConcurrent: pool.MaxConcurrent}
	}
	return snapshot
}

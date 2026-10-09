package backlogadmin

import (
	"fmt"
	"sort"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// RoleQuotaSnapshot is request-local ranking evidence, not submission authority.
// It is built by BuildRoleQuotaSnapshot, the one quota view every role ranking
// reads: "task run" from the coordinator's quota and workers answers, and the
// coordinator itself for campaign check, submission and schedule occurrences.
type RoleQuotaSnapshot struct {
	Now time.Time
	// MaxObservationAge is the age past which a window reading is stale.
	MaxObservationAge time.Duration
	Pools             map[string]domain.RouteRankPool
	ChecksDisabled    map[string]bool
	// Freshness says, per pool, whether its readings are fresh, stale or
	// missing and how old they are, so that no pool is unknown silently.
	Freshness map[string]QuotaPoolFreshness
}

// The states a pool's quota readings are reported in.
const (
	QuotaFresh   = "fresh"
	QuotaStale   = "stale"
	QuotaMissing = "missing"
)

// QuotaPoolFreshness is how current one pool's readings are. ObservedAt is the
// oldest governing reading, because a pool is only as current as its least
// current window, and Age is measured from it.
type QuotaPoolFreshness struct {
	Pool           string
	State          string
	ObservedAt     time.Time
	Age            time.Duration
	MaxAge         time.Duration
	ChecksDisabled bool
}

// String is the freshness in the words ranking receipts use.
func (f QuotaPoolFreshness) String() string {
	text := f.Pool + " quota " + f.State
	if !f.ObservedAt.IsZero() {
		text += fmt.Sprintf(", observed %s ago", f.Age.Round(time.Second))
	}
	if f.State != QuotaFresh {
		text += fmt.Sprintf(" (maximum age %s)", f.MaxAge)
	}
	if f.ChecksDisabled {
		text += "; quota checks disabled, readings rank but do not gate"
	}
	return text
}

// RoleQuotaInput is everything the quota view is read from. Every field is
// something the coordinator's quota and workers answers carry, so that a
// client and the coordinator build the same view from the same records.
type RoleQuotaInput struct {
	Now               time.Time
	MaxObservationAge time.Duration
	Pools             []domain.QuotaPool
	Admissions        []domain.QuotaAdmissionRecord
	Workers           []domain.WorkerSnapshot
}

// BuildRoleQuotaSnapshot reads every pool's windows from the fleet-wide merged
// worker observations against one clock and one maximum age.
//
// Window freshness alone decides whether a pool's readings can supply
// headroom. The admission record is a gate (draining or closed) when present;
// its own age does not erase fresh windows, because admission is enforced
// again, atomically, at submission and occurrence time. A pool whose quota
// checks are disabled is never gated by retained admission, but its observed
// windows still rank: ranking is a preference and never grants admission.
func BuildRoleQuotaSnapshot(in RoleQuotaInput) RoleQuotaSnapshot {
	maxAge := in.MaxObservationAge
	if maxAge <= 0 {
		maxAge = domain.DefaultQuotaStaleAfter
	}
	snapshot := RoleQuotaSnapshot{
		Now: in.Now, MaxObservationAge: maxAge, Pools: map[string]domain.RouteRankPool{},
		ChecksDisabled: map[string]bool{}, Freshness: map[string]QuotaPoolFreshness{},
	}
	admissions := map[string]domain.QuotaAdmissionRecord{}
	for _, record := range in.Admissions {
		admissions[record.QuotaPoolID] = record
	}
	states := domain.MergeQuotaObservations(nil, in.Workers)
	pools := append([]domain.QuotaPool(nil), in.Pools...)
	sort.Slice(pools, func(i, j int) bool { return pools[i].ID < pools[j].ID })
	for _, pool := range pools {
		windows := domain.ReadQuotaWindows(pool, states, in.Now, maxAge)
		for i := range windows.Windows {
			// A reading from the future is not evidence about now.
			if windows.Windows[i].ObservedAt.After(in.Now) {
				windows.Windows[i].Stale = true
			}
		}
		admission := pool.Admission
		if record, ok := admissions[pool.ID]; ok {
			admission = record.Admission
		}
		if pool.ChecksDisabled {
			admission = domain.AdmissionOpen
			snapshot.ChecksDisabled[pool.ID] = true
		}
		snapshot.Pools[pool.ID] = domain.RouteRankPool{ID: pool.ID, Admission: admission, Windows: windows,
			Active: pool.ActiveAssignments, MaxConcurrent: pool.MaxConcurrent}
		snapshot.Freshness[pool.ID] = quotaPoolFreshness(pool, windows, in.Now, maxAge)
	}
	return snapshot
}

// RoleQuotaSnapshotFromAnswers builds the view from the coordinator's quota
// and workers answers, which carry the same records the coordinator reads.
func RoleQuotaSnapshotFromAnswers(now time.Time, maxAge time.Duration, quotas []Quota, workers []Worker) RoleQuotaSnapshot {
	in := RoleQuotaInput{Now: now, MaxObservationAge: maxAge}
	for _, q := range quotas {
		in.Pools = append(in.Pools, q.Pool)
		if q.Admission != nil {
			in.Admissions = append(in.Admissions, *q.Admission)
		}
	}
	for _, w := range workers {
		in.Workers = append(in.Workers, w.Snapshot)
	}
	return BuildRoleQuotaSnapshot(in)
}

func quotaPoolFreshness(pool domain.QuotaPool, windows domain.QuotaWindowSet, now time.Time, maxAge time.Duration) QuotaPoolFreshness {
	f := QuotaPoolFreshness{Pool: pool.ID, State: QuotaFresh, MaxAge: maxAge, ChecksDisabled: pool.ChecksDisabled}
	if !windows.Complete() {
		f.State = QuotaMissing
	}
	for _, w := range windows.Windows {
		if w.Missing || w.ObservedAt.IsZero() {
			continue
		}
		if f.ObservedAt.IsZero() || w.ObservedAt.Before(f.ObservedAt) {
			f.ObservedAt = w.ObservedAt
		}
		if w.Stale && f.State == QuotaFresh {
			f.State = QuotaStale
		}
	}
	if !f.ObservedAt.IsZero() {
		f.Age = now.Sub(f.ObservedAt)
	}
	return f
}

func (v view) roleQuotaSnapshot() RoleQuotaSnapshot {
	maxAge := v.runtime.RankingQuotaStaleAfter
	return BuildRoleQuotaSnapshot(RoleQuotaInput{
		Now: v.now, MaxObservationAge: maxAge, Pools: v.records.QuotaPools,
		Admissions: v.admissions, Workers: v.workers,
	})
}

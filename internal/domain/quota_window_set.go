package domain

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// DefaultQuotaStaleAfter is the age past which a quota reading is stale when
// backlog_v2.coordinator_client.defaults.quota_stale_after is not set.
const DefaultQuotaStaleAfter = time.Hour

// The codes a window set's first problem is reported with. They are part of
// the models JSON document and of quota wait reasons, so they are stable.
const (
	// QuotaWindowUnknown: the coordinator could not tell which buckets govern
	// the pool.
	QuotaWindowUnknown = "quota-unknown"
	// QuotaWindowMissing: a window the provider declares, or a bucket the pool
	// names, has no reading at all.
	QuotaWindowMissing = "missing-window"
	// QuotaWindowStale: a reading exists and is too old, or was taken before
	// its own reset, which has since passed.
	QuotaWindowStale = "stale-window"
	// QuotaWindowExhausted: a reading says the window is used up.
	QuotaWindowExhausted = "exhausted-window"
)

// QuotaReadingFresh reports whether a bucket reading can still be acted on: it
// was taken no longer than maxAge ago (zero means DefaultQuotaStaleAfter), and
// not before its own reset if that reset has since passed, because such a
// reading describes a window that is over.
func QuotaReadingFresh(state BucketState, now time.Time, maxAge time.Duration) bool {
	if maxAge <= 0 {
		maxAge = DefaultQuotaStaleAfter
	}
	if state.OriginValidUntil != nil && !now.Before(*state.OriginValidUntil) {
		return false
	}
	if state.ObservedAt.IsZero() || state.ObservedAt.After(now) || now.Sub(state.ObservedAt) > maxAge {
		return false
	}
	return state.ResetsAt == nil || !state.ObservedAt.Before(*state.ResetsAt) || now.Before(*state.ResetsAt)
}

// QuotaReadingExpired reports an observation whose age or own reset has ended.
// Missing timestamps remain invalid evidence, rather than expired windows.
func QuotaReadingExpired(state BucketState, now time.Time, maxAge time.Duration) bool {
	if state.ObservedAt.IsZero() {
		return false
	}
	return !QuotaReadingFresh(state, now, maxAge)
}

// QuotaReadingExhausted reports whether a reading says its window is used up:
// at 100% or in the stopped phase.
func QuotaReadingExhausted(state BucketState) bool {
	return state.UsedPercent >= 100 || state.Phase == PhaseStopped
}

// DeclaredQuotaWindows is the set of windows a provider always reports, so a
// pool of it is incomplete until each has a reading: Claude five_hour and
// seven_day; Codex primary, and secondary when it reports one. Any other
// provider declares none, and its pool is judged on the windows it was
// observed reporting. provider is the quota pool's configured provider name.
func DeclaredQuotaWindows(provider string, observed []BucketState) []string {
	name := strings.ToLower(provider)
	switch {
	case strings.Contains(name, "claude"):
		return []string{"five_hour", "seven_day"}
	case strings.Contains(name, "codex"):
		for _, state := range observed {
			if state.Key.Window == WindowSecondary {
				return []string{WindowPrimary, WindowSecondary}
			}
		}
		return []string{WindowPrimary}
	}
	return nil
}

// QuotaWindowReading is one window of a pool, every value read from the same
// observation; Missing marks a window with no observation at all.
type QuotaWindowReading struct {
	MaxObservationAge time.Duration `json:"maxObservationAge,omitempty"`
	Window            string        `json:"window"`
	Key               BucketKey     `json:"key"`
	Phase             Phase         `json:"phase,omitempty"`
	UsedPercent       float64       `json:"usedPercent"`
	ResetsAt          *time.Time    `json:"resetsAt,omitempty"`
	ObservedAt        time.Time     `json:"observedAt,omitempty"`
	// Declared marks a window the provider always reports.
	Declared  bool `json:"declared,omitempty"`
	Missing   bool `json:"missing,omitempty"`
	Stale     bool `json:"stale,omitempty"`
	Expired   bool `json:"expired,omitempty"`
	Exhausted bool `json:"exhausted,omitempty"`
}

// QuotaWindowSet is a pool read window by window against a clock and a
// maximum reading age.
type QuotaWindowSet struct {
	Pool    string               `json:"pool"`
	Unknown bool                 `json:"unknown,omitempty"`
	Windows []QuotaWindowReading `json:"windows"`
}

// ReadQuotaWindows keeps every newest competitor. Declared collector pools
// additionally require one complete native acquisition from a bound origin.
func ReadQuotaWindows(pool QuotaPool, states []BucketState, now time.Time, maxAge time.Duration) QuotaWindowSet {
	set := QuotaWindowSet{Pool: pool.ID, Unknown: pool.BucketSelection == BucketSelectionUnknown}
	if set.Unknown {
		return set
	}
	belongs := PoolBucketMatcher(pool)
	var relevant []BucketState
	for _, state := range states {
		if belongs(state) {
			relevant = append(relevant, state)
		}
	}
	latest := NewestQuotaStates(relevant)
	batchComplete := CollectorBatchComplete(pool, latest)
	declared := map[string]bool{}
	for _, window := range DeclaredQuotaWindows(pool.Provider, latest) {
		declared[window] = true
	}
	if pool.Collector != nil {
		for _, window := range pool.Collector.RequiredWindows {
			if CollectorWindowGoverns(pool, window.Window) {
				declared[window.Window] = true
			}
		}
	}
	covered := map[string]bool{}
	observed := map[BucketKey]bool{}
	competing := map[BucketKey]BucketState{}
	for _, state := range latest {
		if pool.Collector != nil && state.Key.AccountID == "" && !UnknownAccountRestricts(pool, state, now, maxAge) {
			continue
		}
		if pool.AccountID != "" && state.Key.AccountID != "" && state.Key.AccountID != pool.AccountID {
			continue
		}
		if !ValidQuotaPercent(state.UsedPercent) {
			set.Unknown = true
		}
		if current, ok := competing[state.Key]; ok {
			if pool.Collector != nil && !SameQuotaAcquisition(current.Provenance, state.Provenance) && current.Key.AccountID != "" {
				set.Unknown = true
			}
			if QuotaObservationEpoch(current) != QuotaObservationEpoch(state) || EpochFor(current.ResetsAt) != EpochFor(state.ResetsAt) {
				set.Unknown = true
			}
		}
		competing[state.Key] = state
		age := QuotaObservationMaxAge(pool, state, maxAge)
		reading := quotaWindowReading(state)
		reading.Declared = declared[state.Key.Window] && (pool.AccountID == "" || state.Key.AccountID == pool.AccountID)
		reading.Stale = !QuotaReadingFresh(state, now, age)
		reading.Expired = QuotaReadingExpired(state, now, age)
		reading.Exhausted = QuotaReadingExhausted(state) || UnknownAccountRestricts(pool, state, now, maxAge)
		if CollectorObservationQualified(pool, state) {
			reading.MaxObservationAge = age
		}
		set.Windows = append(set.Windows, reading)
		observed[state.Key] = true
		if pool.AccountID == "" || state.Key.AccountID == pool.AccountID {
			covered[state.Key.Window] = true
		}
	}
	for _, key := range pool.Buckets {
		if observed[key] || pool.Collector != nil && key.AccountID == "" {
			continue
		}
		set.Windows = append(set.Windows, QuotaWindowReading{Window: key.Window, Key: key, Declared: declared[key.Window], Missing: true})
		covered[key.Window] = true
	}
	for window := range declared {
		if !covered[window] || pool.Collector != nil && !batchComplete {
			set.Windows = append(set.Windows, QuotaWindowReading{Window: window, Key: BucketKey{AccountID: pool.AccountID, Window: window}, Declared: true, Missing: true})
		}
	}
	sort.SliceStable(set.Windows, func(i, j int) bool {
		a, b := set.Windows[i], set.Windows[j]
		if a.Window != b.Window {
			return a.Window < b.Window
		}
		if a.Key.String() != b.Key.String() {
			return a.Key.String() < b.Key.String()
		}
		if a.Phase.Rank() != b.Phase.Rank() {
			return a.Phase.Rank() > b.Phase.Rank()
		}
		if a.UsedPercent != b.UsedPercent {
			return a.UsedPercent > b.UsedPercent
		}
		return a.Missing && !b.Missing
	})
	return set
}

func quotaWindowReading(state BucketState) QuotaWindowReading {
	reading := QuotaWindowReading{
		Window: state.Key.Window, Key: state.Key, Phase: state.Phase,
		UsedPercent: state.UsedPercent, ObservedAt: state.ObservedAt,
	}
	if state.ResetsAt != nil {
		at := *state.ResetsAt
		reading.ResetsAt = &at
	}
	return reading
}

// Governing returns the windows that still govern ranking and admission.
// Retain the original set when no fresh window remains so it stays unknown;
// an expired reading never supplies headroom. The full set remains available
// for display and for quota waits that require every window to be fresh.
func (s QuotaWindowSet) Governing() QuotaWindowSet {
	fresh := false
	for _, w := range s.Windows {
		fresh = fresh || !w.Missing && !w.Stale && !w.Expired
	}
	if !fresh {
		return s
	}
	result := QuotaWindowSet{Pool: s.Pool, Unknown: s.Unknown}
	for _, w := range s.Windows {
		if !w.Expired || w.Declared && w.MaxObservationAge > 0 {
			result.Windows = append(result.Windows, w)
		}
	}
	return result
}

// Complete reports that the governing buckets are known, at least one window
// was read, and no named bucket or declared window lacks a reading.
func (s QuotaWindowSet) Complete() bool {
	if s.Unknown || len(s.Windows) == 0 {
		return false
	}
	for _, window := range s.Windows {
		if window.Missing {
			return false
		}
	}
	return true
}

// Fresh reports that the set is complete and every reading in it is fresh.
func (s QuotaWindowSet) Fresh() bool {
	if !s.Complete() {
		return false
	}
	for _, window := range s.Windows {
		if window.Stale || window.Expired {
			return false
		}
	}
	return true
}

// QuotaWindowProblem is the first reason a window set cannot be acted on.
type QuotaWindowProblem struct {
	Code       string     `json:"code"`
	Window     string     `json:"window,omitempty"`
	ResetsAt   *time.Time `json:"resetsAt,omitempty"`
	ObservedAt *time.Time `json:"observedAt,omitempty"`
}

// String is the problem in the words models and quota waits use: "quota
// unknown", "missing seven_day", "stale seven_day" or "exhausted seven_day
// until <reset>".
func (p QuotaWindowProblem) String() string {
	switch p.Code {
	case "":
		return ""
	case QuotaWindowUnknown:
		return "quota unknown"
	case QuotaWindowMissing:
		if p.Window == "" {
			return "missing quota reading"
		}
		return "missing " + p.Window
	case QuotaWindowStale:
		return "stale " + p.Window
	case QuotaWindowExhausted:
		if p.ResetsAt == nil {
			return "exhausted " + p.Window
		}
		return fmt.Sprintf("exhausted %s until %s", p.Window, p.ResetsAt.UTC().Format(time.RFC3339))
	}
	return p.Code
}

// Problem is the first reason the set cannot be acted on, in the order a
// caller needs it: unknown buckets, then a missing window, then a stale
// reading, then an exhausted window. Telemetry comes before exhaustion
// because an exhausted reading that is stale says nothing about now.
func (s QuotaWindowSet) Problem() (QuotaWindowProblem, bool) {
	if s.Unknown {
		return QuotaWindowProblem{Code: QuotaWindowUnknown}, true
	}
	if len(s.Windows) == 0 {
		return QuotaWindowProblem{Code: QuotaWindowMissing}, true
	}
	for _, window := range s.Windows {
		if window.Missing {
			return QuotaWindowProblem{Code: QuotaWindowMissing, Window: window.Window}, true
		}
	}
	for _, window := range s.Windows {
		if window.Stale || window.Expired {
			observed := window.ObservedAt
			return QuotaWindowProblem{Code: QuotaWindowStale, Window: window.Window, ObservedAt: &observed}, true
		}
	}
	for _, window := range s.Windows {
		if window.Exhausted {
			return QuotaWindowProblem{Code: QuotaWindowExhausted, Window: window.Window, ResetsAt: window.ResetsAt}, true
		}
	}
	return QuotaWindowProblem{}, false
}

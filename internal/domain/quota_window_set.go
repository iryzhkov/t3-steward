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
	if state.ObservedAt.IsZero() || now.Sub(state.ObservedAt) > maxAge {
		return false
	}
	return state.ResetsAt == nil || !state.ObservedAt.Before(*state.ResetsAt) || now.Before(*state.ResetsAt)
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
	Window      string     `json:"window"`
	Key         BucketKey  `json:"key"`
	Phase       Phase      `json:"phase,omitempty"`
	UsedPercent float64    `json:"usedPercent"`
	ResetsAt    *time.Time `json:"resetsAt,omitempty"`
	ObservedAt  time.Time  `json:"observedAt,omitempty"`
	// Declared marks a window the provider always reports.
	Declared  bool `json:"declared,omitempty"`
	Missing   bool `json:"missing,omitempty"`
	Stale     bool `json:"stale,omitempty"`
	Exhausted bool `json:"exhausted,omitempty"`
}

// QuotaWindowSet is a pool read window by window against a clock and a
// maximum reading age.
type QuotaWindowSet struct {
	Pool    string               `json:"pool"`
	Unknown bool                 `json:"unknown,omitempty"`
	Windows []QuotaWindowReading `json:"windows"`
}

// ReadQuotaWindows reads every governing bucket of a pool (PoolBucketMatcher
// says which govern), keeping the newest reading per bucket, and adds a
// missing entry for each bucket the pool names and each window its provider
// declares that has no reading.
func ReadQuotaWindows(pool QuotaPool, states []BucketState, now time.Time, maxAge time.Duration) QuotaWindowSet {
	set := QuotaWindowSet{Pool: pool.ID, Unknown: pool.BucketSelection == BucketSelectionUnknown}
	if set.Unknown {
		return set
	}
	belongs := PoolBucketMatcher(pool)
	newest := make(map[BucketKey]BucketState)
	for _, state := range states {
		if !belongs(state) {
			continue
		}
		if current, ok := newest[state.Key]; ok && !state.ObservedAt.After(current.ObservedAt) {
			continue
		}
		newest[state.Key] = state
	}
	governing := make([]BucketState, 0, len(newest))
	for _, state := range newest {
		governing = append(governing, state)
	}
	declared := make(map[string]bool)
	for _, window := range DeclaredQuotaWindows(pool.Provider, governing) {
		declared[window] = true
	}
	covered := make(map[string]bool)
	for _, state := range governing {
		reading := quotaWindowReading(state)
		reading.Declared = declared[state.Key.Window]
		reading.Stale = !QuotaReadingFresh(state, now, maxAge)
		reading.Exhausted = QuotaReadingExhausted(state)
		set.Windows = append(set.Windows, reading)
		covered[state.Key.Window] = true
	}
	for _, key := range pool.Buckets {
		if _, observed := newest[key]; observed {
			continue
		}
		set.Windows = append(set.Windows, QuotaWindowReading{Window: key.Window, Key: key, Declared: declared[key.Window], Missing: true})
		covered[key.Window] = true
	}
	for window := range declared {
		if !covered[window] {
			set.Windows = append(set.Windows, QuotaWindowReading{Window: window, Key: BucketKey{Window: window}, Declared: true, Missing: true})
		}
	}
	sort.Slice(set.Windows, func(i, j int) bool {
		if set.Windows[i].Window != set.Windows[j].Window {
			return set.Windows[i].Window < set.Windows[j].Window
		}
		return set.Windows[i].Key.String() < set.Windows[j].Key.String()
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
		if window.Stale {
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
		if window.Stale {
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

package quotatelemetry

import (
	"math"
	"sort"
	"time"
)

// DeltaMethod names how ComputeDeltas derives a finish's quota change. Deltas
// are computed when read, never stored, so a later method can re-derive them
// from the same retained readings.
const DeltaMethod = "window-before-after/v1"

const (
	// deltaReadingWindow bounds how far from the start and the finish a
	// reading may be, and how long a finish without a later reading is pending.
	deltaReadingWindow = 30 * time.Minute
	// overlapLookback is how far before the interval a concurrent start is
	// searched for.
	overlapLookback = 48 * time.Hour
	// maxConcurrentIDs caps the attempt ids a shared delta names.
	maxConcurrentIDs = 16
	// resetTolerance is how far two readings' reset times may differ and still
	// describe the same window.
	resetTolerance = time.Minute
)

// Delta absences.
const (
	AbsenceNoReadingBefore = "no-reading-before"
	AbsenceNoReadingAfter  = "no-reading-after"
	AbsencePending         = "pending"
	AbsenceResetCrossed    = "reset-crossed"
)

// Delta attributions.
const (
	AttributionExclusive   = "exclusive"
	AttributionShared      = "shared"
	AttributionUnavailable = "unavailable"
)

// Delta is the change of one quota window across one finished assignment's
// interval. It is the window's change over the interval, never a per-task
// share: with concurrent work it is the same number for every assignment
// that overlapped, labelled shared.
type Delta struct {
	Method    string        `json:"method"`
	BucketKey string        `json:"bucketKey"`
	Window    string        `json:"window"`
	Before    *DeltaReading `json:"before"`
	After     *DeltaReading `json:"after"`
	// DeltaPp is after minus before in percentage points; null with an
	// Absence when it cannot be measured.
	DeltaPp         *float64 `json:"deltaPp"`
	Absence         string   `json:"absence,omitempty"`
	Attribution     string   `json:"attribution"`
	ConcurrentCount int      `json:"concurrentCount"`
	Concurrent      []string `json:"concurrent"`
}

// DeltaReading is the reading one end of a delta was taken from.
type DeltaReading struct {
	Source      string     `json:"source"`
	ObservedAt  time.Time  `json:"observedAt"`
	UsedPercent float64    `json:"usedPercent"`
	ResetsAt    *time.Time `json:"resetsAt"`
}

// WorkSpan is one recorded assignment's run time, for overlap.
type WorkSpan struct {
	// Key is the assignment epoch, "assignment:epoch".
	Key       string
	AttemptID string
	// Pool is the quota pool, or the provider instance when the route names
	// no pool.
	Pool  string
	Start time.Time
	// End is nil while the assignment has no recorded finish.
	End *time.Time
}

// PoolOf is the overlap pool of a route: its quota pool, or its provider
// instance when it names none.
func PoolOf(route Route) string {
	if route.QuotaPoolID != "" {
		return route.QuotaPoolID
	}
	return route.ProviderInstanceID
}

// ComputeDeltas measures every window of the finish's provider instance. For
// each bucket key, before is the latest reading at or before the start and
// after the earliest at or after the finish, each from any source and within
// 30 minutes. A finish with no recorded start has no interval and no deltas.
func ComputeDeltas(finish Event, readings []Event, spans []WorkSpan, now time.Time) []Delta {
	work := finish.Work
	if work == nil || work.StartedAt == nil || work.FinishedAt == nil {
		return nil
	}
	start, end := *work.StartedAt, *work.FinishedAt
	type ends struct{ before, after *Reading }
	byKey := map[string]*ends{}
	for index := range readings {
		reading := readings[index].Reading
		if reading == nil || reading.ProviderInstanceID != work.Route.ProviderInstanceID {
			continue
		}
		observed := reading.ObservedAt
		inBefore := !observed.After(start) && !observed.Before(start.Add(-deltaReadingWindow))
		inAfter := !observed.Before(end) && !observed.After(end.Add(deltaReadingWindow))
		if !inBefore && !inAfter {
			continue
		}
		pair := byKey[reading.BucketKey]
		if pair == nil {
			pair = &ends{}
			byKey[reading.BucketKey] = pair
		}
		if inBefore && (pair.before == nil || observed.After(pair.before.ObservedAt)) {
			pair.before = reading
		}
		if inAfter && (pair.after == nil || observed.Before(pair.after.ObservedAt)) {
			pair.after = reading
		}
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	deltas := make([]Delta, 0, len(keys))
	for _, key := range keys {
		pair := byKey[key]
		delta := Delta{Method: DeltaMethod, BucketKey: key, Attribution: AttributionUnavailable, Concurrent: []string{}}
		if pair.before != nil {
			delta.Window = pair.before.Window
			delta.Before = deltaReading(pair.before)
		}
		if pair.after != nil {
			delta.Window = pair.after.Window
			delta.After = deltaReading(pair.after)
		}
		switch {
		case pair.before == nil:
			delta.Absence = AbsenceNoReadingBefore
		case pair.after == nil && now.Sub(end) < deltaReadingWindow:
			delta.Absence = AbsencePending
		case pair.after == nil:
			delta.Absence = AbsenceNoReadingAfter
		case resetCrossed(pair.before, pair.after):
			delta.Absence = AbsenceResetCrossed
		default:
			change := math.Round((pair.after.UsedPercent-pair.before.UsedPercent)*100) / 100
			delta.DeltaPp = &change
			attribute(&delta, work, spans, pair.before.ObservedAt, pair.after.ObservedAt)
		}
		deltas = append(deltas, delta)
	}
	return deltas
}

func deltaReading(reading *Reading) *DeltaReading {
	return &DeltaReading{Source: reading.Source, ObservedAt: reading.ObservedAt,
		UsedPercent: reading.UsedPercent, ResetsAt: reading.ResetsAt}
}

// resetCrossed reports a window that reset between the two readings: the
// reported reset time moved by more than a minute, or usage fell.
func resetCrossed(before, after *Reading) bool {
	if after.UsedPercent < before.UsedPercent {
		return true
	}
	if before.ResetsAt != nil && after.ResetsAt != nil {
		difference := after.ResetsAt.Sub(*before.ResetsAt)
		return difference > resetTolerance || difference < -resetTolerance
	}
	return false
}

// attribute labels a measured delta exclusive when no other recorded
// assignment of the same pool overlaps [from, to], and shared otherwise.
func attribute(delta *Delta, work *Work, spans []WorkSpan, from, to time.Time) {
	pool := PoolOf(work.Route)
	self := work.Key()
	seen := map[string]bool{}
	attempts := map[string]bool{}
	for _, span := range spans {
		if span.Key == self || span.Pool != pool || seen[span.Key] {
			continue
		}
		if span.Start.Before(from.Add(-overlapLookback)) || span.Start.After(to) {
			continue
		}
		if span.End != nil && span.End.Before(from) {
			continue
		}
		seen[span.Key] = true
		attempts[span.AttemptID] = true
	}
	if len(seen) == 0 {
		delta.Attribution = AttributionExclusive
		return
	}
	delta.Attribution = AttributionShared
	delta.ConcurrentCount = len(seen)
	ids := make([]string, 0, len(attempts))
	for id := range attempts {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if len(ids) > maxConcurrentIDs {
		ids = ids[:maxConcurrentIDs]
	}
	delta.Concurrent = ids
}

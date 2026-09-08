// Package report turns quota observations and token samples into a
// consumption breakdown: by peak versus off-peak hours, by hour of day, by
// model and by thread. It answers "when does the window fill up, and does
// it fill faster per token at some times of day".
//
// Consumption is measured as the rise in reported usage between two
// consecutive observations of the same reset window, attributed to the
// thread and model whose turn produced the later observation. Sums over a
// period are exact; the split between concurrently running threads is an
// approximation.
package report

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-quota-watchdog/internal/domain"
)

// Schedule is a weekly recurring set of hours, in local time.
type Schedule struct {
	Days  [7]bool // indexed by time.Weekday
	Start int     // inclusive hour
	End   int     // exclusive hour
	Label string
}

// Contains reports whether t (in its own location) falls in the schedule.
func (s Schedule) Contains(t time.Time) bool {
	if !s.Days[t.Weekday()] {
		return false
	}
	h := t.Hour()
	return h >= s.Start && h < s.End
}

var dayNames = map[string]time.Weekday{
	"sun": time.Sunday, "mon": time.Monday, "tue": time.Tuesday, "wed": time.Wednesday,
	"thu": time.Thursday, "fri": time.Friday, "sat": time.Saturday,
}

// ParseSchedule parses "Mon-Fri 09:00-17:00", "Sat,Sun 00:00-24:00" or
// "Mon-Wed,Fri 8:00-12:00".
func ParseSchedule(s string) (Schedule, error) {
	fields := strings.Fields(s)
	if len(fields) != 2 {
		return Schedule{}, fmt.Errorf("schedule %q must look like \"Mon-Fri 09:00-17:00\"", s)
	}
	var out Schedule
	out.Label = s
	for _, part := range strings.Split(fields[0], ",") {
		part = strings.ToLower(strings.TrimSpace(part))
		if a, b, ok := strings.Cut(part, "-"); ok {
			from, okA := dayNames[a]
			to, okB := dayNames[b]
			if !okA || !okB {
				return Schedule{}, fmt.Errorf("unknown day range %q", part)
			}
			for d := from; ; d = (d + 1) % 7 {
				out.Days[d] = true
				if d == to {
					break
				}
			}
			continue
		}
		d, ok := dayNames[part]
		if !ok {
			return Schedule{}, fmt.Errorf("unknown day %q", part)
		}
		out.Days[d] = true
	}
	a, b, ok := strings.Cut(fields[1], "-")
	if !ok {
		return Schedule{}, fmt.Errorf("hours %q must look like 09:00-17:00", fields[1])
	}
	var err error
	if out.Start, err = parseHour(a); err != nil {
		return Schedule{}, err
	}
	if out.End, err = parseHour(b); err != nil {
		return Schedule{}, err
	}
	if out.End <= out.Start {
		return Schedule{}, errors.New("schedule end must be after start")
	}
	return out, nil
}

func parseHour(s string) (int, error) {
	var h, m int
	if _, err := fmt.Sscanf(s, "%d:%d", &h, &m); err != nil {
		if _, err := fmt.Sscanf(s, "%d", &h); err != nil {
			return 0, fmt.Errorf("invalid hour %q", s)
		}
	}
	if h < 0 || h > 24 {
		return 0, fmt.Errorf("hour %q out of range", s)
	}
	return h, nil
}

// Band aggregates consumption over a set of hour slots.
type Band struct {
	// Percent is the quota consumed (sum of rises).
	Percent float64 `json:"percent"`
	// ActiveHours counts distinct clock hours with any activity.
	ActiveHours int `json:"activeHours"`
	// Rises counts the observations that contributed.
	Rises           int     `json:"rises"`
	FreshTokens     int64   `json:"freshTokens"`
	CacheReadTokens int64   `json:"cacheReadTokens"`
	CostUSD         float64 `json:"costUsd"`
	Samples         int     `json:"samples"`

	slots map[string]bool
}

func (b *Band) addRise(slot string, pct float64) {
	if b.slots == nil {
		b.slots = map[string]bool{}
	}
	b.slots[slot] = true
	b.Percent += pct
	b.Rises++
	b.ActiveHours = len(b.slots)
}

func (b *Band) addUsage(slot string, u domain.UsageSample) {
	if b.slots == nil {
		b.slots = map[string]bool{}
	}
	b.slots[slot] = true
	b.FreshTokens += u.FreshTokens()
	b.CacheReadTokens += u.CacheReadTokens
	b.CostUSD += u.CostUSD
	b.Samples++
	b.ActiveHours = len(b.slots)
}

// PercentPerActiveHour is the average rise per hour with activity.
func (b Band) PercentPerActiveHour() float64 {
	if b.ActiveHours == 0 {
		return 0
	}
	return b.Percent / float64(b.ActiveHours)
}

// PercentPerMillionFresh is quota consumed per million fresh tokens; NaN
// when no tokens were sampled.
func (b Band) PercentPerMillionFresh() float64 {
	if b.FreshTokens == 0 {
		return math.NaN()
	}
	return b.Percent / (float64(b.FreshTokens) / 1e6)
}

// PercentPerUSD is quota consumed per dollar of provider-reported cost;
// NaN when the provider reports no cost.
func (b Band) PercentPerUSD() float64 {
	if b.CostUSD == 0 {
		return math.NaN()
	}
	return b.Percent / b.CostUSD
}

// Share is one attribution row.
type Share struct {
	Name    string  `json:"name"`
	Percent float64 `json:"percent"`
}

// Window summarizes the most recent reset window of a bucket.
type Window struct {
	ResetsAt      *time.Time `json:"resetsAt"`
	FirstObserved time.Time  `json:"firstObserved"`
	LastObserved  time.Time  `json:"lastObserved"`
	// StartPercent is the usage at the first observation of the window,
	// consumed before the watchdog saw it (or by clients outside T3).
	StartPercent float64 `json:"startPercent"`
	// Percent is the latest usage reading.
	Percent float64 `json:"percent"`
	ByModel []Share `json:"byModel"`
}

// BucketReport is the breakdown of one bucket.
type BucketReport struct {
	Key           domain.BucketKey `json:"key"`
	Peak          Band             `json:"peak"`
	OffPeak       Band             `json:"offPeak"`
	WeekdayHours  [24]Band         `json:"weekdayHours"`
	WeekendHours  [24]Band         `json:"weekendHours"`
	ByModel       []Share          `json:"byModel"`
	ByThread      []Share          `json:"byThread"`
	Total         float64          `json:"total"`
	Observations  int              `json:"observations"`
	CurrentWindow *Window          `json:"currentWindow,omitempty"`
}

// Report is the full result.
type Report struct {
	From    time.Time      `json:"from"`
	To      time.Time      `json:"to"`
	Peak    Schedule       `json:"peak"`
	Buckets []BucketReport `json:"buckets"`
}

// Input is everything Build needs.
type Input struct {
	Observations []domain.Observation
	Usage        []domain.UsageSample
	Location     *time.Location
	Peak         Schedule
	From, To     time.Time
	// ResetTolerance decides when two observations belong to one window.
	ResetTolerance time.Duration
	// ThreadTitles maps thread ids to titles for display; optional.
	ThreadTitles map[string]string
}

func slotKey(t time.Time) string { return t.Format("2006-01-02T15") }

// Build computes the report.
func Build(in Input) Report {
	loc := in.Location
	if loc == nil {
		loc = time.Local
	}
	tol := in.ResetTolerance
	if tol <= 0 {
		tol = 5 * time.Minute
	}
	byBucket := map[domain.BucketKey][]domain.Observation{}
	for _, o := range in.Observations {
		if !in.From.IsZero() && o.ObservedAt.Before(in.From) {
			continue
		}
		if !in.To.IsZero() && !o.ObservedAt.Before(in.To) {
			continue
		}
		byBucket[o.Key] = append(byBucket[o.Key], o)
	}
	usageByProvider := map[string][]domain.UsageSample{}
	for _, u := range in.Usage {
		if !in.From.IsZero() && u.ObservedAt.Before(in.From) {
			continue
		}
		if !in.To.IsZero() && !u.ObservedAt.Before(in.To) {
			continue
		}
		usageByProvider[u.ProviderInstanceID] = append(usageByProvider[u.ProviderInstanceID], u)
	}
	keys := make([]domain.BucketKey, 0, len(byBucket))
	for k := range byBucket {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })

	rep := Report{From: in.From, To: in.To, Peak: in.Peak}
	for _, key := range keys {
		obs := byBucket[key]
		sort.SliceStable(obs, func(i, j int) bool { return obs[i].ObservedAt.Before(obs[j].ObservedAt) })
		br := BucketReport{Key: key, Observations: len(obs)}
		models := map[string]float64{}
		threads := map[string]float64{}
		var prev *domain.Observation
		// Window tracking for the current-window summary.
		var win *Window
		winModels := map[string]float64{}
		// Readings are rounded and can wobble by a point; only a new high
		// within the window counts as consumption.
		highWater := 0.0
		for i := range obs {
			o := obs[i]
			local := o.ObservedAt.In(loc)
			newWindow := prev == nil || !sameWindow(*prev, o, tol)
			if newWindow {
				w := &Window{ResetsAt: o.ResetsAt, FirstObserved: o.ObservedAt, LastObserved: o.ObservedAt, StartPercent: o.UsedPercent, Percent: o.UsedPercent}
				win = w
				winModels = map[string]float64{}
				highWater = o.UsedPercent
			} else {
				win.LastObserved = o.ObservedAt
				win.Percent = o.UsedPercent
				rise := o.UsedPercent - highWater
				if rise > 0 {
					highWater = o.UsedPercent
					slot := slotKey(local)
					if in.Peak.Contains(local) {
						br.Peak.addRise(slot, rise)
					} else {
						br.OffPeak.addRise(slot, rise)
					}
					if isWeekend(local) {
						br.WeekendHours[local.Hour()].addRise(slot, rise)
					} else {
						br.WeekdayHours[local.Hour()].addRise(slot, rise)
					}
					model := o.Model
					if model == "" {
						model = "unknown"
					}
					models[model] += rise
					winModels[model] += rise
					if o.ThreadID != "" {
						threads[o.ThreadID] += rise
					}
					br.Total += rise
				}
			}
			p := o
			prev = &p
		}
		if win != nil {
			win.ByModel = shares(winModels)
			br.CurrentWindow = win
		}
		for _, u := range usageByProvider[key.ProviderInstanceID] {
			if !modelApplies(key, u.Model) {
				continue
			}
			local := u.ObservedAt.In(loc)
			slot := slotKey(local)
			if in.Peak.Contains(local) {
				br.Peak.addUsage(slot, u)
			} else {
				br.OffPeak.addUsage(slot, u)
			}
			if isWeekend(local) {
				br.WeekendHours[local.Hour()].addUsage(slot, u)
			} else {
				br.WeekdayHours[local.Hour()].addUsage(slot, u)
			}
		}
		br.ByModel = shares(models)
		named := map[string]float64{}
		for id, pct := range threads {
			name := id
			if t, ok := in.ThreadTitles[id]; ok && t != "" {
				name = t + " (" + shortID(id) + ")"
			}
			named[name] += pct
		}
		br.ByThread = shares(named)
		rep.Buckets = append(rep.Buckets, br)
	}
	return rep
}

func sameWindow(a, b domain.Observation, tol time.Duration) bool {
	switch {
	case a.ResetsAt == nil && b.ResetsAt == nil:
		return true
	case a.ResetsAt == nil || b.ResetsAt == nil:
		return false
	default:
		d := b.ResetsAt.Sub(*a.ResetsAt)
		if d < 0 {
			d = -d
		}
		return d <= tol
	}
}

func isWeekend(t time.Time) bool {
	return t.Weekday() == time.Saturday || t.Weekday() == time.Sunday
}

// modelApplies filters token samples for model-specific windows such as
// Claude's seven_day_opus: the window name's suffix must appear in the
// model id. Account-wide windows accept every sample.
func modelApplies(key domain.BucketKey, model string) bool {
	for _, prefix := range []string{"five_hour_", "seven_day_"} {
		if strings.HasPrefix(key.Window, prefix) {
			suffix := strings.TrimPrefix(key.Window, prefix)
			if suffix == "" || strings.Contains(suffix, "overage") || strings.Contains(suffix, "oauth") {
				return true
			}
			return model == "" || strings.Contains(strings.ToLower(model), strings.ToLower(suffix))
		}
	}
	return true
}

func shares(m map[string]float64) []Share {
	out := make([]Share, 0, len(m))
	for k, v := range m {
		out = append(out, Share{Name: k, Percent: v})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Percent != out[j].Percent {
			return out[i].Percent > out[j].Percent
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

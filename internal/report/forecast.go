package report

import (
	"math"
	"sort"
	"time"

	"github.com/iryzhkov/t3-quota-watchdog/internal/domain"
)

// Slot is one hour of one weekday in local time.
type Slot struct {
	Weekday time.Weekday
	Hour    int
}

// Demand is the history of interactive consumption per slot: for every
// past occurrence of the slot inside the observed range, the percent of
// the window that interactive threads consumed during that hour.
type Demand struct {
	Key domain.BucketKey
	// Values holds one entry per past date for each slot, zero when the
	// slot saw no interactive consumption that day.
	Values map[Slot][]float64
	// FirstDate and LastDate bound the observed range (local dates).
	FirstDate, LastDate time.Time
	// Quantile is the quantile Forecast returns, 0.8 by default.
	Quantile float64
	// MinSamples is how many past occurrences a slot needs before its
	// forecast is trusted.
	MinSamples int
}

// Rise is one increase of a bucket reading, with its cause.
type Rise struct {
	Key         domain.BucketKey
	At          time.Time
	Percent     float64
	ThreadID    string
	Interactive bool
}

// Rises lists every rise of every bucket in the observations, using the
// high-water rule per window, and marks whether the reading came from an
// interactive thread (one the watchdog did not dispatch).
func Rises(observations []domain.Observation, dispatched map[string]string, tol time.Duration) []Rise {
	if tol <= 0 {
		tol = 5 * time.Minute
	}
	byBucket := map[domain.BucketKey][]domain.Observation{}
	for _, o := range observations {
		byBucket[o.Key] = append(byBucket[o.Key], o)
	}
	var out []Rise
	for key, obs := range byBucket {
		sort.SliceStable(obs, func(i, j int) bool { return obs[i].ObservedAt.Before(obs[j].ObservedAt) })
		var prev *domain.Observation
		high := 0.0
		for i := range obs {
			o := obs[i]
			if prev == nil || !sameWindow(*prev, o, tol) {
				high = o.UsedPercent
			} else if o.UsedPercent > high {
				_, isDispatched := dispatched[o.ThreadID]
				out = append(out, Rise{Key: key, At: o.ObservedAt, Percent: o.UsedPercent - high, ThreadID: o.ThreadID, Interactive: !isDispatched})
				high = o.UsedPercent
			}
			p := o
			prev = &p
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].At.Before(out[j].At) })
	return out
}

// BuildDemand aggregates interactive rises of one bucket into per-slot
// history over the observed date range.
func BuildDemand(key domain.BucketKey, rises []Rise, observations []domain.Observation, loc *time.Location) Demand {
	d := Demand{Key: key, Values: map[Slot][]float64{}, Quantile: 0.8, MinSamples: 3}
	if loc == nil {
		loc = time.Local
	}
	var first, last time.Time
	for _, o := range observations {
		if o.Key != key {
			continue
		}
		if first.IsZero() || o.ObservedAt.Before(first) {
			first = o.ObservedAt
		}
		if o.ObservedAt.After(last) {
			last = o.ObservedAt
		}
	}
	if first.IsZero() {
		return d
	}
	d.FirstDate = dateOf(first.In(loc))
	d.LastDate = dateOf(last.In(loc))
	// Per date and hour: interactive percent.
	type dayHour struct {
		date time.Time
		hour int
	}
	perDayHour := map[dayHour]float64{}
	for _, r := range rises {
		if r.Key != key || !r.Interactive {
			continue
		}
		local := r.At.In(loc)
		perDayHour[dayHour{dateOf(local), local.Hour()}] += r.Percent
	}
	for date := d.FirstDate; !date.After(d.LastDate); date = date.AddDate(0, 0, 1) {
		for h := 0; h < 24; h++ {
			// Skip hours outside the observed range on the boundary days.
			at := time.Date(date.Year(), date.Month(), date.Day(), h, 0, 0, 0, loc)
			if at.Add(time.Hour).Before(first) || at.After(last) {
				continue
			}
			slot := Slot{Weekday: date.Weekday(), Hour: h}
			d.Values[slot] = append(d.Values[slot], perDayHour[dayHour{date, h}])
		}
	}
	return d
}

func dateOf(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
}

// Forecast returns the slot's quantile consumption and the number of past
// occurrences it rests on. ok is false when there are too few.
func (d Demand) Forecast(slot Slot) (percent float64, samples int, ok bool) {
	q := d.Quantile
	if q <= 0 {
		q = 0.8
	}
	quantile := func(vals []float64) float64 {
		vals = append([]float64(nil), vals...)
		sort.Float64s(vals)
		idx := int(math.Ceil(q*float64(len(vals)))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(vals) {
			idx = len(vals) - 1
		}
		return vals[idx]
	}
	// Exact slot first; then the same hour pooled over the same kind of
	// day (weekdays or weekend); then the same hour over every day. Each
	// level needs MinSamples occurrences.
	own := d.Values[slot]
	if len(own) >= d.MinSamples {
		return quantile(own), len(own), true
	}
	weekend := slot.Weekday == time.Saturday || slot.Weekday == time.Sunday
	var pooled, all []float64
	for s, vals := range d.Values {
		if s.Hour != slot.Hour {
			continue
		}
		all = append(all, vals...)
		if (s.Weekday == time.Saturday || s.Weekday == time.Sunday) == weekend {
			pooled = append(pooled, vals...)
		}
	}
	if len(pooled) >= d.MinSamples {
		return quantile(pooled), len(pooled), true
	}
	if len(all) >= d.MinSamples {
		return quantile(all), len(all), true
	}
	if len(own) > 0 {
		return quantile(own), len(own), false
	}
	return 0, 0, false
}

// Expected sums the forecast over [from, to), pro rata for partial hours.
// fallback is used for slots with too few samples. The second return
// value reports how much of the span rested on the fallback.
func (d Demand) Expected(from, to time.Time, loc *time.Location, fallbackPerHour float64) (float64, time.Duration) {
	if loc == nil {
		loc = time.Local
	}
	total := 0.0
	var fallbackSpan time.Duration
	t := from.In(loc)
	end := to.In(loc)
	for t.Before(end) {
		next := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, loc).Add(time.Hour)
		if next.After(end) {
			next = end
		}
		frac := next.Sub(t).Hours()
		pct, _, ok := d.Forecast(Slot{Weekday: t.Weekday(), Hour: t.Hour()})
		if !ok {
			pct = fallbackPerHour
			fallbackSpan += next.Sub(t)
		}
		total += pct * frac
		t = next
	}
	return total, fallbackSpan
}

// Heatmap renders the forecast per weekday and hour as text.
func (d Demand) Heatmap() [7][24]struct {
	Percent float64
	Samples int
	OK      bool
} {
	var out [7][24]struct {
		Percent float64
		Samples int
		OK      bool
	}
	for wd := 0; wd < 7; wd++ {
		for h := 0; h < 24; h++ {
			p, n, ok := d.Forecast(Slot{Weekday: time.Weekday(wd), Hour: h})
			out[wd][h].Percent, out[wd][h].Samples, out[wd][h].OK = p, n, ok
		}
	}
	return out
}

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

	"github.com/iryzhkov/t3-steward/internal/domain"
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
	// EstimatedCost is the fitted cost of the calls, in quota percent.
	EstimatedCost float64 `json:"estimatedCost"`

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
	Key          domain.BucketKey `json:"key"`
	Peak         Band             `json:"peak"`
	OffPeak      Band             `json:"offPeak"`
	WeekdayHours [24]Band         `json:"weekdayHours"`
	WeekendHours [24]Band         `json:"weekendHours"`
	ByModel      []Share          `json:"byModel"`
	ByThread     []Share          `json:"byThread"`
	Total        float64          `json:"total"`
	// Unattributed is the part of Total that rose while no T3 call ran:
	// use outside T3, or readings without token data.
	Unattributed  float64 `json:"unattributed"`
	Fit           Fit     `json:"fit"`
	Observations  int     `json:"observations"`
	CurrentWindow *Window `json:"currentWindow,omitempty"`
}

// Report is the full result.
type Report struct {
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
	Peak Schedule  `json:"peak"`
	// Sources names the state databases or hosts the data came from.
	Sources []string       `json:"sources,omitempty"`
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
	// WindowModels maps a window name to the model selector configured for
	// it, overriding what the window name implies.
	WindowModels map[string]string
}

// callSlack is how long after a reading a call may be logged and still
// count toward the interval that ended with that reading.
const callSlack = 2 * time.Second

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
	inRange := func(t time.Time) bool {
		return (in.From.IsZero() || !t.Before(in.From)) && (in.To.IsZero() || t.Before(in.To))
	}
	byBucket := map[domain.BucketKey][]domain.Observation{}
	for _, o := range in.Observations {
		if inRange(o.ObservedAt) {
			byBucket[o.Key] = append(byBucket[o.Key], o)
		}
	}
	callsByProvider := map[string][]domain.UsageSample{}
	turnsByProvider := map[string][]domain.UsageSample{}
	for _, u := range in.Usage {
		if !inRange(u.ObservedAt) {
			continue
		}
		switch normalizeKind(u) {
		case domain.UsageKindCall:
			callsByProvider[u.ProviderInstanceID] = append(callsByProvider[u.ProviderInstanceID], u)
		default:
			turnsByProvider[u.ProviderInstanceID] = append(turnsByProvider[u.ProviderInstanceID], u)
		}
	}
	for p := range callsByProvider {
		callsByProvider[p] = dedupeCalls(callsByProvider[p])
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
		applies := func(model string) bool {
			if sel, ok := in.WindowModels[key.Window]; ok {
				return sel == "" || model == "" || strings.Contains(strings.ToLower(model), strings.ToLower(sel))
			}
			return modelApplies(key, model)
		}
		var calls []domain.UsageSample
		for _, u := range callsByProvider[key.ProviderInstanceID] {
			if applies(u.Model) {
				calls = append(calls, u)
			}
		}
		var turns []domain.UsageSample
		for _, u := range turnsByProvider[key.ProviderInstanceID] {
			if applies(u.Model) {
				turns = append(turns, u)
			}
		}
		rep.Buckets = append(rep.Buckets, buildBucket(key, obs, calls, turns, loc, tol, in))
	}
	return rep
}

// normalizeKind fills the kind of samples stored before kinds existed:
// Codex samples were always per call, Claude samples per turn.
func normalizeKind(u domain.UsageSample) string {
	if u.Kind != "" {
		return u.Kind
	}
	if strings.HasPrefix(strings.ToLower(u.ProviderInstanceID), "codex") {
		return domain.UsageKindCall
	}
	return domain.UsageKindTurn
}

// dedupeCalls drops repeated notifications of the same call, which Codex
// emits with an unchanged running total.
func dedupeCalls(samples []domain.UsageSample) []domain.UsageSample {
	sort.SliceStable(samples, func(i, j int) bool { return samples[i].ObservedAt.Before(samples[j].ObservedAt) })
	lastTotal := map[string]int64{}
	type sig struct {
		in, cw, cr, out int64
	}
	lastSig := map[string]sig{}
	lastAt := map[string]time.Time{}
	out := samples[:0]
	for _, u := range samples {
		if u.CumulativeTokens > 0 {
			if prev, ok := lastTotal[u.ThreadID]; ok && prev == u.CumulativeTokens {
				continue
			}
			lastTotal[u.ThreadID] = u.CumulativeTokens
		}
		// Claude occasionally logs the same message_delta twice within a
		// second; identical counts that close together are one call.
		s := sig{u.InputTokens, u.CacheWriteTokens, u.CacheReadTokens, u.OutputTokens}
		if prev, ok := lastSig[u.ThreadID]; ok && prev == s && u.ObservedAt.Sub(lastAt[u.ThreadID]) < time.Second {
			continue
		}
		lastSig[u.ThreadID] = s
		lastAt[u.ThreadID] = u.ObservedAt
		out = append(out, u)
	}
	return out
}

// buildBucket attributes every rise of one bucket to threads and models by
// splitting it across the calls made during the interval, weighted by the
// fitted (or default) cost per token type.
func buildBucket(key domain.BucketKey, obs []domain.Observation, calls, turns []domain.UsageSample, loc *time.Location, tol time.Duration, in Input) BucketReport {
	br := BucketReport{Key: key, Observations: len(obs)}

	// Pass 1: intervals between consecutive readings of one window, with
	// the calls that fall inside each.
	var spans []readingSpan
	var prev *domain.Observation
	highWater := 0.0
	ci := 0
	var win *Window
	for i := range obs {
		o := obs[i]
		if prev == nil || !sameWindow(*prev, o, tol) {
			win = &Window{ResetsAt: o.ResetsAt, FirstObserved: o.ObservedAt, LastObserved: o.ObservedAt, StartPercent: o.UsedPercent, Percent: o.UsedPercent}
			highWater = o.UsedPercent
			// Calls before the first reading of a window cannot be matched
			// to a rise.
			for ci < len(calls) && !calls[ci].ObservedAt.After(o.ObservedAt) {
				ci++
			}
		} else {
			win.LastObserved = o.ObservedAt
			win.Percent = o.UsedPercent
			s := readingSpan{prev: *prev, cur: o}
			if o.UsedPercent > highWater {
				s.rise = o.UsedPercent - highWater
				highWater = o.UsedPercent
			}
			// A call's usage is logged a few milliseconds around the reading
			// it triggered; allow a short slack so the call lands in the
			// interval it caused rather than the next one.
			for ci < len(calls) && !calls[ci].ObservedAt.After(o.ObservedAt.Add(callSlack)) {
				s.calls = append(s.calls, calls[ci])
				ci++
			}
			spans = append(spans, s)
		}
		p := o
		prev = &p
	}
	if win != nil {
		br.CurrentWindow = win
	}

	// Pass 1b: subagent work. Claude logs per-call usage only for the
	// parent agent; subagents surface solely in the per-turn totals. The
	// part of each turn's total that the parent's calls do not explain is
	// spread over the turn's duration so that intervals during which only
	// subagents ran still get tokens.
	spreadTurns(spans, obs, calls, turns, func(i int, u domain.UsageSample) { spans[i].calls = append(spans[i].calls, u) })

	// Pass 2: fit weights on the intervals.
	rows := make([]interval, 0, len(spans))
	for _, s := range spans {
		var r interval
		r.rise = s.rise
		r.bin = domain.EpochFor(s.cur.ResetsAt) + "/" + slotKey(s.cur.ObservedAt.In(loc))
		for _, c := range s.calls {
			r.tokens[0] += float64(c.InputTokens) / 1e6
			r.tokens[1] += float64(c.CacheWriteTokens) / 1e6
			r.tokens[2] += float64(c.CacheReadTokens) / 1e6
			r.tokens[3] += float64(c.OutputTokens) / 1e6
		}
		rows = append(rows, r)
	}
	br.Fit = fitWeights(rows)
	w := br.Fit.Weights

	// Model mix per thread from turn samples (Claude reports per-model
	// counts per turn), cost-weighted; falls back to the thread's model.
	modelMix := map[string]map[string]float64{}
	for _, t := range turns {
		if t.Model == "" {
			continue
		}
		if modelMix[t.ThreadID] == nil {
			modelMix[t.ThreadID] = map[string]float64{}
		}
		modelMix[t.ThreadID][t.Model] += w.Cost(t)
	}
	threadModel := map[string]string{}
	for _, c := range calls {
		if c.Model != "" {
			threadModel[c.ThreadID] = c.Model
		}
	}
	for _, o := range obs {
		if o.Model != "" && threadModel[o.ThreadID] == "" {
			threadModel[o.ThreadID] = o.Model
		}
	}

	// Pass 3: attribute rises.
	models := map[string]float64{}
	threads := map[string]float64{}
	winModels := map[string]float64{}
	var winStart time.Time
	if win != nil {
		winStart = win.FirstObserved
	}
	for _, s := range spans {
		local := s.cur.ObservedAt.In(loc)
		slot := slotKey(local)
		if s.rise > 0 {
			band := &br.OffPeak
			if in.Peak.Contains(local) {
				band = &br.Peak
			}
			band.addRise(slot, s.rise)
			if isWeekend(local) {
				br.WeekendHours[local.Hour()].addRise(slot, s.rise)
			} else {
				br.WeekdayHours[local.Hour()].addRise(slot, s.rise)
			}
			br.Total += s.rise
		}
		for _, c := range s.calls {
			band := &br.OffPeak
			if in.Peak.Contains(c.ObservedAt.In(loc)) {
				band = &br.Peak
			}
			cslot := slotKey(c.ObservedAt.In(loc))
			band.addUsage(cslot, c)
			band.EstimatedCost += w.Cost(c)
			if isWeekend(c.ObservedAt.In(loc)) {
				br.WeekendHours[c.ObservedAt.In(loc).Hour()].addUsage(cslot, c)
			} else {
				br.WeekdayHours[c.ObservedAt.In(loc).Hour()].addUsage(cslot, c)
			}
		}
		if s.rise <= 0 {
			continue
		}
		inWindow := !s.cur.ObservedAt.Before(winStart)
		// Cost share per thread inside the interval.
		costs := map[string]float64{}
		total := 0.0
		for _, c := range s.calls {
			cost := w.Cost(c)
			costs[c.ThreadID] += cost
			total += cost
		}
		if total <= 0 {
			br.Unattributed += s.rise
			models["outside T3 or no token data"] += s.rise
			if inWindow {
				winModels["outside T3 or no token data"] += s.rise
			}
			continue
		}
		for threadID, cost := range costs {
			share := s.rise * cost / total
			threads[threadID] += share
			mix := modelMix[threadID]
			if len(mix) == 0 {
				m := threadModel[threadID]
				if m == "" {
					m = "unknown"
				}
				models[m] += share
				if inWindow {
					winModels[m] += share
				}
				continue
			}
			mixTotal := 0.0
			for _, v := range mix {
				mixTotal += v
			}
			for m, v := range mix {
				models[m] += share * v / mixTotal
				if inWindow {
					winModels[m] += share * v / mixTotal
				}
			}
		}
	}
	for _, t := range turns {
		local := t.ObservedAt.In(loc)
		if in.Peak.Contains(local) {
			br.Peak.CostUSD += t.CostUSD
		} else {
			br.OffPeak.CostUSD += t.CostUSD
		}
	}
	if win != nil {
		win.ByModel = shares(winModels)
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
	return br
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

// readingSpan is the interval between two consecutive readings of one
// window, with the calls made during it.
type readingSpan struct {
	prev, cur domain.Observation
	rise      float64
	calls     []domain.UsageSample
}

// spreadTurns allocates, for every turn-level sample, the tokens that the
// thread's per-call samples during the turn do not account for, spread
// uniformly over the turn's duration across the overlapping spans. The
// synthetic samples are handed to add with the span index.
func spreadTurns(spans []readingSpan, obs []domain.Observation, calls, turns []domain.UsageSample, add func(i int, u domain.UsageSample)) {
	if len(spans) == 0 || len(turns) == 0 {
		return
	}
	byThread := map[string][]domain.UsageSample{}
	for _, t := range turns {
		byThread[t.ThreadID] = append(byThread[t.ThreadID], t)
	}
	callsByThread := map[string][]domain.UsageSample{}
	for _, c := range calls {
		callsByThread[c.ThreadID] = append(callsByThread[c.ThreadID], c)
	}
	readingsByThread := map[string][]time.Time{}
	for _, o := range obs {
		readingsByThread[o.ThreadID] = append(readingsByThread[o.ThreadID], o.ObservedAt)
	}
	for threadID, ts := range byThread {
		sort.SliceStable(ts, func(i, j int) bool { return ts[i].ObservedAt.Before(ts[j].ObservedAt) })
		tc := callsByThread[threadID]
		var prevEnd time.Time
		// Turns with several models share one result event: group by time.
		i := 0
		for i < len(ts) {
			end := ts[i].ObservedAt
			var total domain.UsageSample
			models := map[string]float64{}
			for i < len(ts) && ts[i].ObservedAt.Sub(end) < time.Second {
				total.InputTokens += ts[i].InputTokens
				total.CacheWriteTokens += ts[i].CacheWriteTokens
				total.CacheReadTokens += ts[i].CacheReadTokens
				total.OutputTokens += ts[i].OutputTokens
				models[ts[i].Model] += float64(ts[i].FreshTokens() + ts[i].CacheReadTokens)
				i++
			}
			// Turn start: the earliest call or quota reading of the thread
			// after the previous turn, within a 12 hour horizon; else a
			// minute before the result.
			horizon := end.Add(-12 * time.Hour)
			if prevEnd.After(horizon) {
				horizon = prevEnd
			}
			start := time.Time{}
			for _, at := range readingsByThread[threadID] {
				if at.After(horizon) && !at.After(end) && (start.IsZero() || at.Before(start)) {
					start = at
				}
			}
			for _, c := range tc {
				if c.ObservedAt.After(prevEnd) && !c.ObservedAt.After(end) {
					if c.ObservedAt.After(horizon) && (start.IsZero() || c.ObservedAt.Before(start)) {
						start = c.ObservedAt
					}
					total.InputTokens -= c.InputTokens
					total.CacheWriteTokens -= c.CacheWriteTokens
					total.CacheReadTokens -= c.CacheReadTokens
					total.OutputTokens -= c.OutputTokens
				}
			}
			if start.IsZero() {
				start = end.Add(-time.Minute)
			}
			prevEnd = end
			if total.InputTokens < 0 {
				total.InputTokens = 0
			}
			if total.CacheWriteTokens < 0 {
				total.CacheWriteTokens = 0
			}
			if total.CacheReadTokens < 0 {
				total.CacheReadTokens = 0
			}
			if total.OutputTokens < 0 {
				total.OutputTokens = 0
			}
			if total.FreshTokens()+total.CacheReadTokens == 0 {
				continue
			}
			// Dominant model of the turn for the synthetic samples.
			model := ""
			best := -1.0
			for m, v := range models {
				if v > best {
					model, best = m, v
				}
			}
			length := end.Sub(start).Seconds()
			if length <= 0 {
				length = 1
			}
			for si := range spans {
				s := &spans[si]
				lo := s.prev.ObservedAt
				if lo.Before(start) {
					lo = start
				}
				hi := s.cur.ObservedAt
				if hi.After(end) {
					hi = end
				}
				if !hi.After(lo) {
					continue
				}
				f := hi.Sub(lo).Seconds() / length
				add(si, domain.UsageSample{
					ProviderInstanceID: ts[0].ProviderInstanceID, ThreadID: threadID, Model: model,
					ObservedAt: hi, Kind: "spread",
					InputTokens:      int64(float64(total.InputTokens) * f),
					CacheWriteTokens: int64(float64(total.CacheWriteTokens) * f),
					CacheReadTokens:  int64(float64(total.CacheReadTokens) * f),
					OutputTokens:     int64(float64(total.OutputTokens) * f),
				})
			}
		}
	}
}

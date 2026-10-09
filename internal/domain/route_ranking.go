package domain

import (
	"fmt"
	"math"
	"sort"
	"time"
)

// RouteRankingV1 pins the bands, thresholds and lexicographic keys.
const RouteRankingV1 = "route-ranking/v1"

type RouteRankInput struct {
	Version    string
	Now        time.Time
	Candidates []RouteRankCandidate
}
type RouteRankCandidate struct {
	Route       string
	Ordinal     int
	Pools       []RouteRankPool
	WorkerScore *float64
}
type RouteRankPool struct {
	ID        string
	Admission AdmissionState
	Windows   QuotaWindowSet
	// Active counts the pool's active and planned assignments and
	// MaxConcurrent is its concurrency limit. A pool at its limit ranks in the
	// saturated band, below every pool with room and above gated pools. Zero
	// MaxConcurrent means the caller supplied no concurrency evidence, which
	// leaves the band to quota alone, as before.
	Active        int
	MaxConcurrent int
}

// RouteBandSaturated is the band of a pool at its concurrency limit.
const RouteBandSaturated = "saturated"

// RouteRankBand orders bands from best to worst: reset-soon, healthy,
// unknown, saturated, then gated and anything unrecognized.
func RouteRankBand(band string) int { return rankBand(band) }

// Saturated reports whether the pool is at its concurrency limit.
func (pool RouteRankPool) Saturated() bool {
	return pool.MaxConcurrent > 0 && pool.Active >= pool.MaxConcurrent
}

type RouteRankEntry struct {
	Route          string
	Ordinal        int
	Band           string
	Pool           string
	Reason         string
	MaxUsedPercent float64
	WorkerScore    *float64
}

// RankRoutes ranks already eligible routes without admission or input mutation.
// V1 uses raw percentages, not projected spendable budget.
func RankRoutes(in RouteRankInput) ([]RouteRankEntry, error) {
	if in.Version != RouteRankingV1 {
		return nil, fmt.Errorf("unsupported route ranking %q", in.Version)
	}
	entries := make([]RouteRankEntry, 0, len(in.Candidates))
	allUnknown := len(in.Candidates) > 0
	for _, candidate := range in.Candidates {
		e := RouteRankEntry{Route: candidate.Route, Ordinal: candidate.Ordinal, Band: "unknown", Reason: RouteRankingV1 + ": no pool binding; quota unknown"}
		if candidate.WorkerScore != nil {
			score := *candidate.WorkerScore
			if !math.IsNaN(score) && !math.IsInf(score, 0) {
				e.WorkerScore = &score
			}
		}
		best := false
		for _, pool := range candidate.Pools {
			p := rankRoutePool(pool, in.Now)
			if !best || rankBand(p.Band) < rankBand(e.Band) || rankBand(p.Band) == rankBand(e.Band) && (p.MaxUsedPercent < e.MaxUsedPercent || p.MaxUsedPercent == e.MaxUsedPercent && p.Pool < e.Pool) {
				e.Band, e.Pool, e.Reason, e.MaxUsedPercent = p.Band, p.Pool, p.Reason, p.MaxUsedPercent
				best = true
			}
		}
		allUnknown = allUnknown && e.Band == "unknown"
		entries = append(entries, e)
	}
	if allUnknown {
		for i := range entries {
			entries[i].Reason = RouteRankingV1 + ": quota unknown for every candidate; policy order"
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		a, b := entries[i], entries[j]
		if rankBand(a.Band) != rankBand(b.Band) {
			return rankBand(a.Band) < rankBand(b.Band)
		}
		if a.Ordinal != b.Ordinal {
			return a.Ordinal < b.Ordinal
		}
		if a.MaxUsedPercent != b.MaxUsedPercent {
			return a.MaxUsedPercent < b.MaxUsedPercent
		}
		// Missing scores contribute zero, keeping the comparator transitive
		// when a caller supplies scores for only some candidates.
		aScore, bScore := 0.0, 0.0
		if a.WorkerScore != nil {
			aScore = *a.WorkerScore
		}
		if b.WorkerScore != nil {
			bScore = *b.WorkerScore
		}
		if aScore != bScore {
			return aScore > bScore
		}
		return a.Route < b.Route
	})
	return entries, nil
}
func rankBand(band string) int {
	switch band {
	case "reset-soon":
		return 0
	case "healthy":
		return 1
	case "unknown":
		return 2
	case RouteBandSaturated:
		return 3
	default:
		return 4
	}
}

// rankRoutePool bands one pool by quota and then by concurrency. Saturation
// only ever makes a band worse: a gated pool stays gated, and a pool with room
// keeps the band its quota earns.
func rankRoutePool(pool RouteRankPool, now time.Time) RouteRankEntry {
	e := rankRoutePoolQuota(pool, now)
	if pool.Saturated() && rankBand(e.Band) < rankBand(RouteBandSaturated) {
		e.Band = RouteBandSaturated
		e.Reason = fmt.Sprintf("%s: %s has %d active or planned assignments at its concurrency limit of %d (saturated)",
			RouteRankingV1, pool.ID, pool.Active, pool.MaxConcurrent)
	}
	return e
}

func rankRoutePoolQuota(pool RouteRankPool, now time.Time) RouteRankEntry {
	e := RouteRankEntry{Pool: pool.ID, Band: "healthy"}
	reason := func(text string) RouteRankEntry { e.Reason = RouteRankingV1 + ": " + pool.ID + " " + text; return e }
	// Window usage is a tie-break in every band, including admission gates
	// and exhaustion; those early returns must not erase the metric.
	for _, w := range pool.Windows.Windows {
		if !math.IsNaN(w.UsedPercent) && !math.IsInf(w.UsedPercent, 0) && w.UsedPercent >= 0 && w.UsedPercent <= 100 && w.UsedPercent > e.MaxUsedPercent {
			e.MaxUsedPercent = w.UsedPercent
		}
	}
	if pool.Admission == AdmissionDraining || pool.Admission == AdmissionClosed {
		e.Band = "gated"
		return reason("admission " + string(pool.Admission) + " (gated)")
	}
	if problem, ok := pool.Windows.Problem(); ok {
		if problem.Code == QuotaWindowExhausted {
			e.Band = "gated"
		} else {
			e.Band = "unknown"
		}
		return reason(problem.String() + " (" + e.Band + ")")
	}
	// Malformed percentages cannot supply healthy headroom.
	for _, w := range pool.Windows.Windows {
		if math.IsNaN(w.UsedPercent) || math.IsInf(w.UsedPercent, 0) || w.UsedPercent < 0 || w.UsedPercent > 100 {
			e.Band = "unknown"
			return reason("invalid " + w.Window + " percentage (unknown)")
		}
		if w.UsedPercent > e.MaxUsedPercent {
			e.MaxUsedPercent = w.UsedPercent
		}
	}
	for _, w := range pool.Windows.Windows {
		short := w.Window == "five_hour" || w.Window == WindowPrimary
		if w.UsedPercent >= 100 || w.Phase == PhaseStopped || short && w.UsedPercent >= 90 {
			e.Band = "gated"
			return reason(fmt.Sprintf("%s %g%% >= %g (gated)", w.Window, w.UsedPercent, func() float64 {
				if short {
					return 90
				}
				return 100
			}()))
		}
	}
	// ReadQuotaWindows supplies windows in deterministic order, but callers of
	// this pure seam may not; choose a stable explanation among long windows.
	resetReasons := []string{}
	for _, w := range pool.Windows.Windows {
		long := w.Window == "seven_day" || w.Window == WindowSecondary
		if long && w.ResetsAt != nil && !w.ResetsAt.Before(now) && w.ResetsAt.Sub(now) <= 24*time.Hour && w.UsedPercent <= 95 {
			resetReasons = append(resetReasons, fmt.Sprintf("%s resets in %s with %g%% unused (reset-soon preference)", w.Window, w.ResetsAt.Sub(now), 100-w.UsedPercent))
		}
	}
	if len(resetReasons) > 0 {
		sort.Strings(resetReasons)
		e.Band = "reset-soon"
		return reason(resetReasons[0])
	}
	return reason(fmt.Sprintf("healthy headroom; maximum used %g%%", e.MaxUsedPercent))
}

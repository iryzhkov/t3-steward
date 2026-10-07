package domain

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func rankingPool(now time.Time, short, long float64, reset time.Duration) RouteRankPool {
	at := now.Add(reset)
	return RouteRankPool{ID: "p", Admission: AdmissionOpen, Windows: QuotaWindowSet{Pool: "p", Windows: []QuotaWindowReading{
		{Window: "five_hour", UsedPercent: short}, {Window: "seven_day", UsedPercent: long, ResetsAt: &at},
	}}}
}
func TestRankRoutesBands(t *testing.T) {
	now := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name        string
		short, long float64
		reset       time.Duration
		change      func(*RouteRankPool)
		band        string
	}{
		{"healthy", 89.999, 96, 24 * time.Hour, nil, "healthy"},
		{"short90", 90, 50, time.Hour, nil, "gated"},
		{"long95", 20, 95, 24 * time.Hour, nil, "reset-soon"},
		{"longOver95", 20, 95.001, time.Hour, nil, "healthy"},
		{"over24", 20, 50, 24*time.Hour + time.Nanosecond, nil, "healthy"},
		{"stale", 20, 50, time.Hour, func(p *RouteRankPool) { p.Windows.Windows[0].Stale = true }, "unknown"},
		{"missing", 20, 50, time.Hour, func(p *RouteRankPool) { p.Windows.Windows[1].Missing = true }, "unknown"},
		{"unknown", 20, 50, time.Hour, func(p *RouteRankPool) { p.Windows.Unknown = true }, "unknown"},
		{"exhausted", 20, 100, time.Hour, func(p *RouteRankPool) { p.Windows.Windows[1].Exhausted = true }, "gated"},
		{"draining", 20, 50, time.Hour, func(p *RouteRankPool) { p.Admission = AdmissionDraining }, "gated"},
		{"closed", 20, 50, time.Hour, func(p *RouteRankPool) { p.Admission = AdmissionClosed }, "gated"},
		{"codexPrimary", 20, 50, time.Hour, func(p *RouteRankPool) {
			p.Windows.Windows = []QuotaWindowReading{{Window: WindowPrimary, UsedPercent: 20}}
		}, "healthy"},
		{"codexReset", 20, 50, time.Hour, func(p *RouteRankPool) {
			p.Windows.Windows[0].Window = WindowPrimary
			p.Windows.Windows[1].Window = WindowSecondary
		}, "reset-soon"},
		{"noPool", 20, 50, time.Hour, func(p *RouteRankPool) { p.Windows = QuotaWindowSet{} }, "unknown"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := rankingPool(now, tc.short, tc.long, tc.reset)
			if tc.change != nil {
				tc.change(&p)
			}
			got, err := RankRoutes(RouteRankInput{Version: RouteRankingV1, Now: now, Candidates: []RouteRankCandidate{{Route: "a/m", Pools: []RouteRankPool{p}}}})
			if err != nil || len(got) != 1 || got[0].Band != tc.band || got[0].Reason == "" {
				t.Fatalf("%+v %v", got, err)
			}
		})
	}
	if _, err := RankRoutes(RouteRankInput{Version: "route-ranking/v2"}); err == nil {
		t.Fatal("unknown version accepted")
	}
}
func TestRankRoutesComparator(t *testing.T) {
	now := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	low, high := 1.0, 100.0
	healthy := rankingPool(now, 20, 40, 72*time.Hour)
	soon := rankingPool(now, 30, 55, 18*time.Hour)
	earlier := rankingPool(now, 30, 55, time.Hour)
	cases := []struct {
		name       string
		candidates []RouteRankCandidate
		want       string
	}{
		{"reset beats preference", []RouteRankCandidate{{Route: "a/m", Ordinal: 0, Pools: []RouteRankPool{healthy}}, {Route: "b/m", Ordinal: 1, Pools: []RouteRankPool{soon}}}, "b/m"},
		{"reset date never wins", []RouteRankCandidate{{Route: "a/m", Ordinal: 0, Pools: []RouteRankPool{soon}}, {Route: "b/m", Ordinal: 1, Pools: []RouteRankPool{earlier}}}, "a/m"},
		{"unknown behind healthy", []RouteRankCandidate{{Route: "a/m", Ordinal: 0}, {Route: "b/m", Ordinal: 1, Pools: []RouteRankPool{healthy}}}, "b/m"},
		{"gated last", []RouteRankCandidate{{Route: "a/m", Ordinal: 0, Pools: []RouteRankPool{rankingPool(now, 90, 20, time.Hour)}}, {Route: "b/m", Ordinal: 1}}, "b/m"},
		{"all unknown", []RouteRankCandidate{{Route: "b/m", Ordinal: 1}, {Route: "a/m", Ordinal: 0}}, "a/m"},
		{"windows before score", []RouteRankCandidate{{Route: "a/m", Pools: []RouteRankPool{healthy}, WorkerScore: &high}, {Route: "b/m", Pools: []RouteRankPool{rankingPool(now, 10, 30, 72*time.Hour)}, WorkerScore: &low}}, "b/m"},
		{"score before route", []RouteRankCandidate{{Route: "a/m", Pools: []RouteRankPool{healthy}, WorkerScore: &low}, {Route: "b/m", Pools: []RouteRankPool{healthy}, WorkerScore: &high}}, "b/m"},
		{"partial scores", []RouteRankCandidate{{Route: "a/m", WorkerScore: &low}, {Route: "b/m"}, {Route: "c/m", WorkerScore: &high}}, "c/m"},
		{"route final", []RouteRankCandidate{{Route: "b/m"}, {Route: "a/m"}}, "a/m"},
		{"best pool", []RouteRankCandidate{{Route: "a/m", Pools: []RouteRankPool{healthy, soon}}, {Route: "b/m", Pools: []RouteRankPool{healthy}}}, "a/m"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := RouteRankInput{Version: RouteRankingV1, Now: now, Candidates: tc.candidates}
			before := append([]RouteRankCandidate(nil), in.Candidates...)
			got, err := RankRoutes(in)
			if err != nil || got[0].Route != tc.want {
				t.Fatalf("%+v %v", got, err)
			}
			if !reflect.DeepEqual(before, in.Candidates) {
				t.Fatal("mutated input")
			}
			reversed := append([]RouteRankCandidate(nil), before...)
			for i, j := 0, len(reversed)-1; i < j; i, j = i+1, j-1 {
				reversed[i], reversed[j] = reversed[j], reversed[i]
			}
			in.Candidates = reversed
			again, _ := RankRoutes(in)
			if !reflect.DeepEqual(got, again) {
				t.Fatalf("shuffle changed result: %+v %+v", got, again)
			}
			if tc.name == "all unknown" && !strings.Contains(got[0].Reason, "quota unknown for every candidate; policy order") {
				t.Fatal(got[0].Reason)
			}
		})
	}
}

package domain

import (
	"testing"
	"time"
)

// Regression for M17-2: among same-band pools, choose the one with the
// lower maximum window usage after the comparator's band/policy keys tie.
func TestRankRoutesAdmissionStillUsesWindowMaximum(t *testing.T) {
	now := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	pool := func(id string, admission AdmissionState, short, long float64) RouteRankPool {
		return RouteRankPool{ID: id, Admission: admission, Windows: QuotaWindowSet{Pool: id, Windows: []QuotaWindowReading{
			{Window: "five_hour", UsedPercent: short},
			{Window: "seven_day", UsedPercent: long},
		}}}
	}
	got, err := RankRoutes(RouteRankInput{Version: RouteRankingV1, Now: now, Candidates: []RouteRankCandidate{{
		Route: "r/m", Pools: []RouteRankPool{
			pool("a-closed-99", AdmissionClosed, 99, 20),
			pool("z-open-90", AdmissionOpen, 90, 10),
		},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if got[0].Pool != "z-open-90" {
		t.Fatalf("selected pool %q (%s, max %.1f%%); want z-open-90, the gated pool with lower maximum usage",
			got[0].Pool, got[0].Band, got[0].MaxUsedPercent)
	}
}

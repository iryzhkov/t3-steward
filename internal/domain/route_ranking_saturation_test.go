package domain

import (
	"strings"
	"testing"
	"time"
)

func saturationCandidate(route string, ordinal int, pool RouteRankPool) RouteRankCandidate {
	return RouteRankCandidate{Route: route, Ordinal: ordinal, Pools: []RouteRankPool{pool}}
}

func healthyPool(id string, used float64) RouteRankPool {
	return RouteRankPool{ID: id, Admission: AdmissionOpen, Windows: QuotaWindowSet{Pool: id, Windows: []QuotaWindowReading{{
		Window: "five_hour", UsedPercent: used,
	}}}}
}

// Feedback 140: Claude at 1% and Codex at 43%. With Claude's pool at its
// concurrency limit, the Codex pool with room ranks first even though it has
// spent more quota and comes later in policy order.
func TestSaturatedPoolRanksBelowAPoolWithRoom(t *testing.T) {
	claude := healthyPool("claude-main", 1)
	claude.Active, claude.MaxConcurrent = 10, 10
	codex := healthyPool("codex-main", 43)
	codex.Active, codex.MaxConcurrent = 0, 5
	ranked, err := RankRoutes(RouteRankInput{Version: RouteRankingV1, Now: time.Unix(0, 0), Candidates: []RouteRankCandidate{
		saturationCandidate("claude/opus", 1, claude), saturationCandidate("codex/gpt", 2, codex),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if ranked[0].Route != "codex/gpt" || ranked[0].Band != "healthy" {
		t.Fatalf("first = %+v, want the pool with room", ranked[0])
	}
	if ranked[1].Band != RouteBandSaturated ||
		!strings.Contains(ranked[1].Reason, "claude-main has 10 active or planned assignments at its concurrency limit of 10 (saturated)") {
		t.Fatalf("second = %+v, want the saturated band with its reason", ranked[1])
	}
}

func TestSaturationOnlyMakesABandWorse(t *testing.T) {
	gated := healthyPool("gated", 95)
	gated.Active, gated.MaxConcurrent = 3, 3
	unknown := RouteRankPool{ID: "unknown", Windows: QuotaWindowSet{Pool: "unknown", Unknown: true}}
	saturated := healthyPool("saturated", 1)
	saturated.Active, saturated.MaxConcurrent = 2, 2
	unbounded := healthyPool("unbounded", 50)
	unbounded.Active = 40 // no limit supplied: quota alone decides
	ranked, err := RankRoutes(RouteRankInput{Version: RouteRankingV1, Now: time.Unix(0, 0), Candidates: []RouteRankCandidate{
		saturationCandidate("g/m", 1, gated), saturationCandidate("s/m", 2, saturated),
		saturationCandidate("u/m", 3, unknown), saturationCandidate("n/m", 4, unbounded),
	}})
	if err != nil {
		t.Fatal(err)
	}
	var order, bands []string
	for _, entry := range ranked {
		order = append(order, entry.Route)
		bands = append(bands, entry.Band)
	}
	if strings.Join(order, ",") != "n/m,u/m,s/m,g/m" || strings.Join(bands, ",") != "healthy,unknown,saturated,gated" {
		t.Fatalf("order = %v bands = %v", order, bands)
	}
	if RouteRankBand(RouteBandSaturated) <= RouteRankBand("unknown") || RouteRankBand(RouteBandSaturated) >= RouteRankBand("gated") {
		t.Fatal("the saturated band must sit between unknown and gated")
	}
}

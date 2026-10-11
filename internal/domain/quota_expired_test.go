package domain

import (
	"testing"
	"time"
)

// Ranking uses the remaining fresh windows without inventing headroom.
func TestRankRoutesExpiredQuotaWindows(t *testing.T) {
	now := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	primary := BucketKey{ProviderInstanceID: "codex", LimitID: "codex", Window: WindowPrimary}
	secondary := BucketKey{ProviderInstanceID: "codex", LimitID: "codex", Window: WindowSecondary}
	pool := QuotaPool{ID: "codex-main", Provider: "codex", Buckets: []BucketKey{primary, secondary}, Admission: AdmissionOpen}
	for _, tc := range []struct {
		name         string
		primaryAge   time.Duration
		primaryUsed  float64
		secondaryAge time.Duration
		reset        *time.Time
		band         string
		maxUsed      float64
	}{
		{"31 day secondary", time.Minute, 20, 31 * 24 * time.Hour, nil, "healthy", 20},
		{"expired reset", time.Minute, 20, time.Minute, ptrTime(now.Add(-time.Second)), "healthy", 20},
		{"only expired", 2 * time.Hour, 20, 31 * 24 * time.Hour, nil, "unknown", 0},
		{"fresh zero headroom", time.Minute, 100, 31 * 24 * time.Hour, nil, "gated", 100},
		{"age boundary still governs", time.Minute, 20, time.Hour, nil, "gated", 100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			states := []BucketState{
				{Key: primary, ObservedAt: now.Add(-tc.primaryAge), UsedPercent: tc.primaryUsed},
				{Key: secondary, ObservedAt: now.Add(-tc.secondaryAge), UsedPercent: 100, Phase: PhaseStopped, ResetsAt: tc.reset},
				// An older reading must not replace the newest primary.
				{Key: primary, ObservedAt: now.Add(-40 * 24 * time.Hour), UsedPercent: 100},
			}
			windows := ReadQuotaWindows(pool, states, now, time.Hour)
			got, err := RankRoutes(RouteRankInput{Version: RouteRankingV1, Now: now, Candidates: []RouteRankCandidate{
				{Route: "codex/model", Pools: []RouteRankPool{{ID: pool.ID, Admission: pool.Admission, Windows: windows}}},
			}})
			if err != nil || len(got) != 1 || got[0].Band != tc.band || got[0].MaxUsedPercent != tc.maxUsed {
				t.Fatalf("ranking = %+v, err = %v", got, err)
			}
			if len(windows.Windows) != 2 {
				t.Fatalf("display windows lost: %+v", windows)
			}
		})
	}
}

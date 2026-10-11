package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestModelsListsExpiredSecondaryWithAge(t *testing.T) {
	now := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	before := modelsNow
	modelsNow = func() time.Time { return now }
	defer func() { modelsNow = before }()
	primary := domain.BucketKey{ProviderInstanceID: "codex", LimitID: "codex", Window: domain.WindowPrimary}
	secondary := primary
	secondary.Window = domain.WindowSecondary
	pool := domain.QuotaPool{ID: "codex-main", Provider: "codex", ProviderInstanceIDs: []string{"codex"}, Admission: domain.AdmissionOpen}
	workers := []backlogadmin.Worker{{Health: string(domain.WorkerHealthReady), Snapshot: domain.WorkerSnapshot{
		WorkerID: "worker", Connected: true,
		Inventory: domain.WorkerInventory{AcceptBacklog: true, Providers: []domain.WorkerProviderInventory{{InstanceID: "codex", Available: true, Models: []string{"model"}}}},
		QuotaObservations: []domain.WorkerQuotaObservation{
			{Key: primary, ObservedAt: now, UsedPercent: 20, Phase: domain.PhaseNormal},
			{Key: secondary, ObservedAt: now.Add(-31 * 24 * time.Hour), UsedPercent: 2, Phase: domain.PhaseNormal},
		},
	}}}
	doc := buildModelsDocument("", workers, []backlogadmin.Quota{{Pool: pool}}, nil, time.Hour)
	for _, window := range doc.Instances[0].Windows {
		if window.Expired && window.Headroom != 0 {
			t.Fatalf("expired window reports headroom: %+v", window)
		}
	}
	var out bytes.Buffer
	if err := renderModels(&out, doc); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "secondary (codex): expired; age 31d") {
		t.Fatal(out.String())
	}
	if strings.Contains(out.String(), "secondary (codex): used") {
		t.Fatal("expired reading supplies displayed headroom")
	}
	snapshot := buildRouteRankView(workers, []backlogadmin.Quota{{Pool: pool}}, time.Hour)
	ranked, err := domain.RankRoutes(domain.RouteRankInput{Version: domain.RouteRankingV1, Now: now, Candidates: []domain.RouteRankCandidate{
		{Route: "codex/model", Pools: []domain.RouteRankPool{snapshot.Pools[pool.ID]}},
	}})
	if err != nil || ranked[0].Band != "healthy" {
		t.Fatalf("ranking = %+v, err = %v", ranked, err)
	}
}

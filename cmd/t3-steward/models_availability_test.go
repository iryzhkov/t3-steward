package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// availabilityFixture is one ready worker advertising one Claude route whose
// pool declares five_hour and seven_day, each read fresh at the given percent
// unless a case changes it. now is pinned so ages are exact.
func availabilityFixture(now time.Time) ([]backlogadmin.Worker, []backlogadmin.Quota) {
	reset := now.Add(72 * time.Hour)
	five := domain.BucketKey{ProviderInstanceID: "t3-primary", LimitID: "claude", Window: "five_hour"}
	week := domain.BucketKey{ProviderInstanceID: "t3-primary", LimitID: "claude", Window: "seven_day"}
	worker := backlogadmin.Worker{
		State: "observed", Enrolled: true, Health: string(domain.WorkerHealthReady),
		Snapshot: domain.WorkerSnapshot{
			WorkerID: "omarchy-pc", Connected: true,
			Inventory: domain.WorkerInventory{
				AcceptBacklog: true,
				Providers: []domain.WorkerProviderInventory{
					{InstanceID: "t3-primary", QuotaPoolID: "pool-claude", Available: true, Models: []string{"opus"}},
				},
			},
			QuotaObservations: []domain.WorkerQuotaObservation{
				{Key: five, Phase: domain.PhaseNormal, UsedPercent: 10, ObservedAt: now.Add(-time.Minute), Healthy: true},
				{Key: week, Phase: domain.PhaseNormal, UsedPercent: 20, ResetsAt: &reset, ObservedAt: now.Add(-time.Minute), Healthy: true},
			},
		},
	}
	quota := backlogadmin.Quota{Pool: domain.QuotaPool{
		ID: "pool-claude", Provider: "claude", ProviderInstanceIDs: []string{"t3-primary"},
		BucketSelection: domain.BucketSelectionResolved, Buckets: []domain.BucketKey{five, week},
		Admission: domain.AdmissionOpen, MaxConcurrent: 1,
	}}
	return []backlogadmin.Worker{worker}, []backlogadmin.Quota{quota}
}

func pinModelsNow(t *testing.T, now time.Time) {
	t.Helper()
	previous := modelsNow
	modelsNow = func() time.Time { return now }
	t.Cleanup(func() { modelsNow = previous })
}

// F2: models called a route available while its pool's telemetry was stale,
// missing a declared window or exhausted, or while no ready worker could take
// it. Each case now has its own reason, in the status column and as a code in
// the JSON document; a complete, fresh, unexhausted pool on a ready worker is
// still simply available.
func TestModelsAvailabilityNamesEachReason(t *testing.T) {
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name   string
		change func([]backlogadmin.Worker, []backlogadmin.Quota)
		code   string
		status string
	}{
		{name: "complete, fresh and ready", change: func([]backlogadmin.Worker, []backlogadmin.Quota) {}, code: modelsStatusAvailable, status: "available"},
		{name: "stale weekly window", change: func(w []backlogadmin.Worker, _ []backlogadmin.Quota) {
			w[0].Snapshot.QuotaObservations[1].ObservedAt = now.Add(-61 * time.Minute)
		}, code: domain.QuotaWindowStale, status: "stale seven_day"},
		{name: "weekly window exactly at the threshold", change: func(w []backlogadmin.Worker, _ []backlogadmin.Quota) {
			w[0].Snapshot.QuotaObservations[1].ObservedAt = now.Add(-time.Hour)
		}, code: modelsStatusAvailable, status: "available"},
		{name: "missing weekly window", change: func(w []backlogadmin.Worker, q []backlogadmin.Quota) {
			w[0].Snapshot.QuotaObservations = w[0].Snapshot.QuotaObservations[:1]
			q[0].Pool.Buckets = q[0].Pool.Buckets[:1]
		}, code: domain.QuotaWindowMissing, status: "missing seven_day"},
		{name: "exhausted weekly window", change: func(w []backlogadmin.Worker, _ []backlogadmin.Quota) {
			w[0].Snapshot.QuotaObservations[1].UsedPercent = 100
		}, code: domain.QuotaWindowExhausted, status: "exhausted seven_day until 2026-10-07T12:00:00Z"},
		{name: "weekly window at 99.9%", change: func(w []backlogadmin.Worker, _ []backlogadmin.Quota) {
			w[0].Snapshot.QuotaObservations[1].UsedPercent = 99.9
		}, code: modelsStatusAvailable, status: "available"},
		{name: "no ready worker", change: func(w []backlogadmin.Worker, _ []backlogadmin.Quota) {
			w[0].Health = string(domain.WorkerHealthDegraded)
		}, code: modelsAvailabilityNoReadyWorker, status: "no ready worker"},
		{name: "unknown buckets", change: func(_ []backlogadmin.Worker, q []backlogadmin.Quota) {
			q[0].Pool.BucketSelection = domain.BucketSelectionUnknown
		}, code: domain.QuotaWindowUnknown, status: "quota unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pinModelsNow(t, now)
			workers, quotas := availabilityFixture(now)
			tc.change(workers, quotas)
			document := buildModelsDocument("", workers, quotas, nil, time.Hour)
			instance := modelsInstanceByName(t, document, "t3-primary")
			if instance.Availability.Code != tc.code || modelsStatus(instance) != tc.status {
				t.Fatalf("availability = %+v, status %q; want code %q and status %q", instance.Availability, modelsStatus(instance), tc.code, tc.status)
			}
			var out bytes.Buffer
			if err := renderModels(&out, document); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(out.String(), tc.status) {
				t.Fatalf("the table does not say %q:\n%s", tc.status, out.String())
			}
			encoded := modelsDocumentKeys(t, mustModelsJSON(t, document))
			availability := encoded["instances"].([]any)[0].(map[string]any)["availability"].(map[string]any)
			if availability["code"] != tc.code {
				t.Fatalf("JSON availability = %v, want code %q", availability, tc.code)
			}
			scoped := scopeModelsDocument(document, modelsScope{Available: true})
			if (len(scoped.Instances) == 1) != (tc.code == modelsStatusAvailable) {
				t.Fatalf("--available kept %d instances for code %q", len(scoped.Instances), tc.code)
			}
		})
	}
}

// The 2026-10-04 incident as models saw it: the Claude seven-day window read
// 97% from a reading hours old while the window was still open. Nothing may
// call the route available on that reading. A manual refresh in the middle
// of the window then reports the real usage with the same reset time, and
// the route is available from that reading on.
func TestModelsStaleNinetySevenThenManualRefresh(t *testing.T) {
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	reset := time.Date(2026, 10, 8, 16, 0, 0, 0, time.UTC)
	pinModelsNow(t, now)
	workers, quotas := availabilityFixture(now)
	stale := &workers[0].Snapshot.QuotaObservations[1]
	stale.Phase, stale.UsedPercent, stale.ResetsAt, stale.ObservedAt, stale.Healthy = domain.PhaseDraining, 97, &reset, now.Add(-5*time.Hour), false
	before := buildModelsDocument("", workers, quotas, nil, time.Hour)
	if instance := modelsInstanceByName(t, before, "t3-primary"); instance.Availability.Code == modelsStatusAvailable || modelsStatus(instance) != "stale seven_day" {
		t.Fatalf("before the refresh: %+v status %q", instance.Availability, modelsStatus(instance))
	}
	if scoped := scopeModelsDocument(before, modelsScope{Available: true}); len(scoped.Instances) != 0 {
		t.Fatalf("--available reported a route on the stale reading: %+v", scoped.Instances)
	}
	// The refresh arrives from a second worker, newer than the stale reading,
	// with the reset time unchanged.
	refresher := workers[0]
	refresher.Snapshot.WorkerID = "normandy"
	refresher.Snapshot.QuotaObservations = []domain.WorkerQuotaObservation{{
		Key: stale.Key, Phase: domain.PhaseNormal, UsedPercent: 12, ResetsAt: &reset, ObservedAt: now, Healthy: true,
	}}
	after := buildModelsDocument("", append(workers, refresher), quotas, nil, time.Hour)
	instance := modelsInstanceByName(t, after, "t3-primary")
	if instance.Availability.Code != modelsStatusAvailable || instance.Percent == nil || *instance.Percent != 12 {
		t.Fatalf("after the refresh: %+v percent %v", instance.Availability, instance.Percent)
	}
}

// The routes the existing fixtures call available, each a complete and fresh
// pool on a ready worker, are still available, and the routes they call
// unavailable keep their reasons.
func TestModelsExistingAvailableRoutesStayAvailable(t *testing.T) {
	document := modelsReasonDocument(t)
	want := map[string]string{
		"t3-primary": modelsStatusAvailable,
		"codex":      modelsAvailabilityNotAdvertised,
		"opencode":   modelsAvailabilityMissingBinding,
	}
	for name, code := range want {
		if got := modelsInstanceByName(t, document, name).Availability.Code; got != code {
			t.Errorf("%s availability = %q, want %q", name, got, code)
		}
	}
	fixture := modelsFixture()
	if got := modelsInstanceByName(t, buildModelsDocument("", fixture.workers, fixture.quotas, nil, 0), "t3-primary").Availability.Code; got != modelsStatusAvailable {
		t.Errorf("modelsFixture t3-primary availability = %q, want available", got)
	}
}

func mustModelsJSON(t *testing.T, document modelsDocument) string {
	t.Helper()
	var out bytes.Buffer
	cli := modelsCLI{service: &modelsFixtureService{}, principal: backlogadmin.Principal{ID: "test"}, stdout: &out}
	if err := cli.encode(document); err != nil {
		t.Fatal(err)
	}
	return out.String()
}

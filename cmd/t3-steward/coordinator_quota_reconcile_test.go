package main

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func quotaReconcileTestBindings() []backlog.QuotaPoolBinding {
	return []backlog.QuotaPoolBinding{
		{ID: "first", Provider: "openai", ProviderInstanceIDs: []string{"codex"}, MaxConcurrent: 2},
		{ID: "second", Provider: "openai", ProviderInstanceIDs: []string{"other"}, MaxConcurrent: 2},
	}
}

func TestCoordinatorQuotaReconcilerIdleKeepsSnapshot(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		name := "enabled"
		if disabled {
			name = "disabled"
		}
		t.Run(name, func(t *testing.T) {
			store, loads, _ := openSnapshotCountingStore(t)
			now := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
			firstTime := now
			r := coordinatorQuotaReconciler{store: store, bridge: backlog.QuotaBridge{
				Store: store, Pools: quotaReconcileTestBindings(), Disabled: disabled,
				MaxObservationAge: time.Hour, Now: func() time.Time { return now },
			}}
			first, err := r.Tick(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(first.Pools) != 2 {
				t.Fatalf("first report pools = %#v", first.Pools)
			}
			now = now.Add(time.Minute)
			ctx, snapshot := sqlite.WithCoordinatorRecordsSnapshot(context.Background())
			defer snapshot.Finish()
			before := loads.Load()
			report, err := r.Tick(ctx)
			if err != nil {
				t.Fatal(err)
			}
			records, err := store.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if got := loads.Load() - before; got != 1 {
				t.Fatalf("idle scope full loads = %d, want 1", got)
			}
			if len(records.QuotaPools) != 2 || len(report.Pools) != 2 {
				t.Fatalf("stored pools = %#v; report = %#v", records.QuotaPools, report.Pools)
			}
			for i, pool := range records.QuotaPools {
				if !pool.UpdatedAt.Equal(firstTime) {
					t.Fatalf("stored pool %s updated at %s, want %s", pool.ID, pool.UpdatedAt, firstTime)
				}
				if !report.Pools[i].UpdatedAt.Equal(now) || quotaPoolChanged(pool, report.Pools[i]) {
					t.Fatalf("report changed by persistence: %#v", report.Pools[i])
				}
			}
		})
	}
}

func TestCoordinatorQuotaReconcilerWritesOnlyChangedPool(t *testing.T) {
	store, _, _ := openSnapshotCountingStore(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	firstTime := now
	r := coordinatorQuotaReconciler{store: store, bridge: backlog.QuotaBridge{
		Store: store, Pools: quotaReconcileTestBindings(), MaxObservationAge: time.Hour,
		Now: func() time.Time { return now },
	}}
	if _, err := r.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	if err := store.SaveBucket(ctx, domain.BucketState{
		Key:   domain.BucketKey{ProviderInstanceID: "codex", LimitID: "limit", Window: "primary"},
		Phase: domain.PhaseNormal, Healthy: true, ObservedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	report, err := r.Tick(ctx)
	if err != nil {
		t.Fatal(err)
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records.QuotaPools) != 2 {
		t.Fatalf("stored pools = %#v", records.QuotaPools)
	}
	for _, pool := range records.QuotaPools {
		wantTime, wantAdmission := firstTime, domain.AdmissionClosed
		if pool.ID == "first" {
			wantTime, wantAdmission = now, domain.AdmissionOpen
		}
		if !pool.UpdatedAt.Equal(wantTime) || pool.Admission != wantAdmission {
			t.Fatalf("stored pool = %#v, want time %s and admission %s", pool, wantTime, wantAdmission)
		}
	}
	for _, pool := range report.Pools {
		if !pool.UpdatedAt.Equal(now) {
			t.Fatalf("report timestamp changed: %#v", pool)
		}
	}
}

func TestCoordinatorQuotaReconcilerAddsPoolAndRetainsRemovedPool(t *testing.T) {
	store, _, _ := openSnapshotCountingStore(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	firstTime := now
	bindings := quotaReconcileTestBindings()
	r := coordinatorQuotaReconciler{store: store, bridge: backlog.QuotaBridge{
		Store: store, Pools: bindings[:1], Disabled: true, Now: func() time.Time { return now },
	}}
	if _, err := r.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	r.bridge.Pools = bindings[1:]
	report, err := r.Tick(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Pools) != 1 || report.Pools[0].ID != "second" {
		t.Fatalf("report = %#v", report)
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records.QuotaPools) != 2 {
		t.Fatalf("stored pools = %#v, want retained and new pool", records.QuotaPools)
	}
	for _, pool := range records.QuotaPools {
		wantTime := firstTime
		if pool.ID == "second" {
			wantTime = now
		}
		if !pool.UpdatedAt.Equal(wantTime) {
			t.Fatalf("pool %s updated at %s, want %s", pool.ID, pool.UpdatedAt, wantTime)
		}
	}
}

func TestQuotaPoolChangedIgnoresTimestampAndEmptySlices(t *testing.T) {
	previous := domain.QuotaPool{ID: "pool", UpdatedAt: time.Now()}
	current := previous
	current.UpdatedAt = previous.UpdatedAt.Add(time.Minute)
	current.ProviderInstanceIDs = []string{}
	current.Buckets = []domain.BucketKey{}
	if quotaPoolChanged(previous, current) {
		t.Fatal("timestamp and empty slices caused a change")
	}
	current.ActiveAssignments++
	if !quotaPoolChanged(previous, current) {
		t.Fatal("active assignment change was ignored")
	}
}

package main

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestCoordinatorMetricsDefaultDoesNotReadStoreOrListen(t *testing.T) {
	listener, err := startCoordinatorMetrics(context.Background(), config.Config{})
	if err != nil || listener != nil {
		t.Fatal(listener, err)
	}
}

func TestCoordinatorMetricsUsesAdmissionPoolProjection(t *testing.T) {
	cfg := config.Default()
	cfg.BacklogV2.QuotaPools = map[string]config.V2QuotaPool{"fixture-pool": {
		Provider: "claudeAgent", MaxConcurrent: 2, AccountID: "00000000-0000-0000-0000-000000000000", ObservationMaxAge: config.Duration(390 * time.Second),
		Collector: &config.V2QuotaCollectorCoverage{
			SourceID: "fixture-native", Origins: []domain.QuotaCollectorOrigin{{WorkerID: "fixture-worker", ProviderInstanceID: "fixture-instance"}},
			RequiredWindows: []domain.QuotaCollectorWindow{{Window: "five_hour"}, {Window: "seven_day"}, {Window: "weekly_fable", Models: []string{"fixture-model"}}},
		},
	}}
	cfg.BacklogV2.Workers = map[string]config.V2Worker{"fixture-worker": {Providers: map[string]config.V2Provider{
		"fixture-instance": {Models: []string{"fixture-model"}, QuotaPool: "fixture-pool"},
	}}}
	got, err := coordinatorMetricsQuotaPools(cfg)
	if err != nil {
		t.Fatal(err)
	}
	want, _, err := backlog.QuotaBridgePools(coordinatorQuotaPoolBindings(cfg))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) || len(got) != 1 || got[0].Collector == nil || got[0].Collector.MaxObservationAge != 390*time.Second {
		t.Fatalf("metrics/admission coverage differs: got=%#v want=%#v", got, want)
	}
	if got[0].Admission != domain.AdmissionClosed {
		t.Fatal("pure projection opened admission")
	}
	cfg.BacklogV2.QuotaPools = nil
	empty, err := coordinatorMetricsQuotaPools(cfg)
	if err != nil || len(empty) != 0 {
		t.Fatal(empty, err)
	}
}

func TestCoordinatorMetricsRefusesInvalidProjectionBeforeStoreOpen(t *testing.T) {
	cfg := config.Default()
	cfg.BacklogV2.Coordinator.Metrics.Bind = "127.0.0.1:9469"
	cfg.BacklogV2.QuotaPools = map[string]config.V2QuotaPool{"invalid": {Provider: "claudeAgent", MaxConcurrent: 0}}
	cfg.BacklogV2.Workers = map[string]config.V2Worker{"worker": {Providers: map[string]config.V2Provider{"instance": {QuotaPool: "invalid"}}}}
	if listener, err := startCoordinatorMetrics(context.Background(), cfg); err == nil || listener != nil {
		t.Fatal("invalid coverage attempted a listener", listener, err)
	}
}

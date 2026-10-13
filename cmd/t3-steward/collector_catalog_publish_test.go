package main

import (
	"context"
	"errors"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
	"testing"
	"time"
)

type collectorCatalogFake struct {
	requests  []workerruntime.CatalogRequest
	capable   bool
	failFinal bool
	wrongAck  bool
}

func (f *collectorCatalogFake) Catalog(_ context.Context, raw any) (map[string]string, error) {
	req := raw.(workerruntime.CatalogRequest)
	f.requests = append(f.requests, req)
	if f.failFinal && req.Projection.CollectorRecovery != nil {
		return nil, errors.New("enriched catalog refused")
	}
	ack := map[string]string{"revision": req.Projection.Revision, "workerId": req.Projection.WorkerID}
	if f.capable {
		ack["collectorProvenance"] = workerproto.CapabilityQuotaCollectorProvenance
	}
	if f.wrongAck && req.Projection.CollectorRecovery != nil {
		ack["revision"] = "different"
	}
	return ack, nil
}
func collectorCatalogSettings(t *testing.T) config.BacklogV2 {
	t.Helper()
	cfg := config.Default()
	cfg.BacklogV2.Coordinator.ID = "coordinator"
	cfg.BacklogV2.Workers = map[string]config.V2Worker{"worker": {Connection: "persistent-ssh", Address: "worker", Credential: "ref:worker", Epoch: "epoch", AcceptBacklog: true, Providers: map[string]config.V2Provider{"claudeAgent": {QuotaPool: "claude", Models: []string{"claude-fable-5-1"}}, "codex": {QuotaPool: "codex", Models: []string{"gpt"}}}}}
	cfg.BacklogV2.QuotaPools = map[string]config.V2QuotaPool{"claude": {Provider: "claude", AccountID: "11111111-2222-3333-4444-555555555555", ObservationMaxAge: config.Duration(390 * time.Second), MaxConcurrent: 1, Collector: &config.V2QuotaCollectorCoverage{SourceID: "native", Origins: []domain.QuotaCollectorOrigin{{WorkerID: "worker", ProviderInstanceID: "claudeAgent"}}, RequiredWindows: []domain.QuotaCollectorWindow{{Window: "five_hour"}, {Window: "seven_day"}}}}, "codex": {Provider: "codex", MaxConcurrent: 1}}
	cfg.BacklogV2.CollectorRecovery = cfg.CollectorRecoveryMetadataForWorker("")
	return cfg.BacklogV2
}
func TestCollectorCatalogEnrollmentRequiresCapabilityThenExactEnrichedACK(t *testing.T) {
	for _, tc := range []struct {
		name                        string
		capable, known, fail, wrong bool
		calls                       int
		success                     bool
	}{
		{"first capable enrollment", true, false, false, false, 2, true},
		{"known capable refresh", true, true, false, false, 1, true},
		{"old strict peer", false, false, false, false, 1, false},
		{"enriched refused", true, false, true, false, 2, false},
		{"wrong enriched revision", true, false, false, true, 2, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			settings := collectorCatalogSettings(t)
			fake := &collectorCatalogFake{capable: tc.capable, failFinal: tc.fail, wrongAck: tc.wrong}
			_, err := publishCollectorCatalog(context.Background(), fake, settings, "worker", "prior", tc.known)
			if (err == nil) != tc.success || len(fake.requests) != tc.calls {
				t.Fatalf("err=%v requests=%d", err, len(fake.requests))
			}
			if !tc.known {
				first := fake.requests[0].Projection
				if first.CollectorRecovery != nil || first.Worker.AcceptBacklog {
					t.Fatal("first enrollment usable before metadata")
				}
				if _, ok := first.Worker.Providers["claudeAgent"]; ok {
					t.Fatal("covered execution route exposed before capability")
				}
				if _, ok := first.Worker.Providers["codex"]; !ok {
					t.Fatal("unrelated route deleted from discovery catalog")
				}
			}
			if len(fake.requests) == 2 {
				first, last := fake.requests[0], fake.requests[1]
				if last.ExpectedRevision != first.Projection.Revision || last.Projection.CollectorRecovery == nil || last.Projection.Revision == first.Projection.Revision {
					t.Fatal("metadata publication did not fence prior restricted revision")
				}
			}
		})
	}
}
func TestCollectorCatalogNoncoveredEnrollmentUnchanged(t *testing.T) {
	settings := collectorCatalogSettings(t)
	worker := settings.Workers["worker"]
	delete(worker.Providers, "claudeAgent")
	settings.Workers["worker"] = worker
	fake := &collectorCatalogFake{}
	projection, err := publishCollectorCatalog(context.Background(), fake, settings, "worker", "prior", false)
	if err != nil || len(fake.requests) != 1 || projection.CollectorRecovery != nil || !projection.Worker.AcceptBacklog {
		t.Fatalf("noncovered enrollment changed %v %+v", err, projection)
	}
}

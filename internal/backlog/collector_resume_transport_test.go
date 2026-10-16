package backlog

import (
	"context"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestCollectorResumeTransportRequiresNegotiatedGrantCapability(t *testing.T) {
	pool := domain.QuotaPool{ID: "claude-main", Collector: &domain.QuotaCollectorCoverage{}}
	command := domain.ThrottleCommand{ID: "resume", Kind: domain.ThrottleCommandResume, WorkerID: "worker", QuotaPoolID: pool.ID, QuotaRecovery: &domain.QuotaRecoveryGrant{CoverageDigest: "binding"}}
	transport := &exchangeTransport{}
	adapter := workerThrottleTransport{workerID: "worker", pools: []domain.QuotaPool{pool}, transport: transport}
	if _, err := adapter.DeliverThrottleCommands(context.Background(), "worker", []domain.ThrottleCommand{command}); err != nil {
		t.Fatal(err)
	}
	if len(transport.throttles) != 0 {
		t.Fatal("old strict peer received a collector grant")
	}
	adapter.snapshot.Inventory.Capabilities = []string{workerproto.CapabilityQuotaCollectorProvenance}
	grantless := command
	grantless.QuotaRecovery = nil
	if _, err := adapter.DeliverThrottleCommands(context.Background(), "worker", []domain.ThrottleCommand{grantless}); err != nil {
		t.Fatal(err)
	}
	if len(transport.throttles) != 0 {
		t.Fatal("covered grantless pending resume reached worker")
	}
	if _, err := adapter.DeliverThrottleCommands(context.Background(), "worker", []domain.ThrottleCommand{command}); err != nil {
		t.Fatal(err)
	}
	if len(transport.throttles) != 1 {
		t.Fatal("negotiated source authority was not delivered")
	}
	legacy := grantless
	legacy.ID = "codex"
	legacy.QuotaPoolID = "codex-main"
	adapter.snapshot.Inventory.Capabilities = nil
	if _, err := adapter.DeliverThrottleCommands(context.Background(), "worker", []domain.ThrottleCommand{legacy}); err != nil {
		t.Fatal(err)
	}
	if len(transport.throttles) != 2 {
		t.Fatal("Codex legacy delivery changed")
	}
}

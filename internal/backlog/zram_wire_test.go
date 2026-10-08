package backlog

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestZramSwapRequestOnlyForCapableWorkers(t *testing.T) {
	for _, capable := range []bool{false, true} {
		source := &ackParkStore{t: t, snapshot: domain.WorkerSnapshot{WorkerID: "worker", WorkerEpoch: "epoch", Sequence: 1}}
		source.snapshot.Inventory.Capabilities = []string{workerproto.CapabilityResourceTelemetry}
		if capable {
			source.snapshot.Inventory.Capabilities = append(source.snapshot.Inventory.Capabilities, workerproto.CapabilityZramSwapTelemetry)
		}
		request, err := ParkedAssignmentsFor(context.Background(), source, "worker")
		if err != nil {
			t.Fatal(err)
		}
		if request.ZramSwapWanted != capable {
			t.Fatalf("capable=%v request=%+v", capable, request)
		}
		raw, _ := json.Marshal(request)
		if !capable && bytes.Contains(raw, []byte("zramSwapWanted")) {
			t.Fatalf("new field sent to strict old decoder: %s", raw)
		}
	}
}

// A placement trace carries every candidate's telemetry. A worker that does
// not decode the zram field must not receive another worker's.
func TestOfferPlacementStripsZramForOlderWorkers(t *testing.T) {
	zram := int64(4300)
	placement := &domain.PlacementDecision{TaskID: "task", ResourceEvaluations: []domain.ResourceEvaluation{
		{WorkerID: "omarchy-pc", Telemetry: &domain.WorkerTelemetry{ZramSwapUsedMB: &zram}},
		{WorkerID: "agent-a"},
	}}
	older := offerPlacement(placement, []string{workerproto.CapabilityResourceTelemetry})
	raw, err := json.Marshal(older)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("zram")) || len(older.ResourceEvaluations) != 2 {
		t.Fatalf("older worker receives %s", raw)
	}
	if placement.ResourceEvaluations[0].Telemetry.ZramSwapUsedMB == nil {
		t.Fatal("durable placement trace mutated")
	}
	current := offerPlacement(placement, []string{workerproto.CapabilityResourceTelemetry, workerproto.CapabilityZramSwapTelemetry})
	if current.ResourceEvaluations[0].Telemetry.ZramSwapUsedMB == nil {
		t.Fatal("capable worker lost the zram evidence")
	}
	if legacy := offerPlacement(placement, nil); legacy.ResourceEvaluations != nil {
		t.Fatal("pre-telemetry worker receives resource evaluations")
	}
	if offerPlacement(nil, nil) != nil {
		t.Fatal("absent placement invented")
	}
}

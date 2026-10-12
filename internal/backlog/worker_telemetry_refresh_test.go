package backlog

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestSnapshotTelemetryRefreshStoreSemantics(t *testing.T) {
	ctx := context.Background()
	store, db := snapshotChangeStore(t)
	seed := snapshotChangeAt(coordinatorSnapshot(1), 1, 0)
	seed.Inventory.Telemetry = resourceWorker("worker-a").Telemetry.Clone()
	seed.Inventory.Telemetry.ObservedAt = seed.ObservedAt
	snapshotChangeCheck(t, store.SaveWorkerSnapshot(ctx, seed))
	before := snapshotChangeRead(t, db, seed.WorkerID)
	incoming := snapshotChangeAt(seed, 2, 10*time.Second)
	incoming.Inventory.Telemetry = seed.Inventory.Telemetry.Clone()
	got, err := store.ObserveWorkerSnapshot(ctx, incoming, false)
	snapshotChangeCheck(t, err)
	if row := snapshotChangeRead(t, db, seed.WorkerID); row != before {
		t.Fatal("identical telemetry wrote the record")
	}
	// Timestamp-only changes preserve every non-telemetry field, including pressure.
	incoming.Inventory.Telemetry.ObservedAt = incoming.ObservedAt
	incoming.Inventory.Pressure = domain.CPUPressure{Normalized: .9, Load: 99}
	got, err = store.ObserveWorkerSnapshot(ctx, incoming, false)
	snapshotChangeCheck(t, err)
	want := seed
	want.Inventory.Telemetry = incoming.Inventory.Telemetry.Clone()
	row := snapshotChangeRead(t, db, seed.WorkerID)
	snapshotChangeAssertSnapshot(t, row, want)
	if !reflect.DeepEqual(got, want) || row.writes != before.writes+1 || row.audits != before.audits {
		t.Fatalf("timestamp refresh: got=%+v row=%+v before=%+v", got, row, before)
	}
	// Both caller and returned measurements are detached from the durable record.
	*incoming.Inventory.Telemetry.TempFreeMB = 1
	*got.Inventory.Telemetry.MemoryAvailableMB = 1
	snapshotChangeAssertSnapshot(t, snapshotChangeRead(t, db, seed.WorkerID), want)
	incoming.Inventory.Telemetry = nil
	incoming.Sequence++
	incoming.ObservedAt = seed.ObservedAt.Add(20 * time.Second)
	got, err = store.ObserveWorkerSnapshot(ctx, incoming, false)
	snapshotChangeCheck(t, err)
	want.Inventory.Telemetry = nil
	after := snapshotChangeRead(t, db, seed.WorkerID)
	snapshotChangeAssertSnapshot(t, after, want)
	if got.Inventory.Telemetry != nil || after.writes != row.writes+1 || after.audits != row.audits {
		t.Fatal("nil telemetry did not clear evidence without audit")
	}
	incoming.Sequence++
	incoming.ObservedAt = seed.ObservedAt.Add(21 * time.Second)
	_, err = store.ObserveWorkerSnapshot(ctx, incoming, false)
	snapshotChangeCheck(t, err)
	if snapshotChangeRead(t, db, seed.WorkerID) != after {
		t.Fatal("absent telemetry wrote the record")
	}
	incoming = snapshotChangeAt(seed, 5, 30*time.Second)
	incoming.Inventory.Telemetry = seed.Inventory.Telemetry.Clone()
	incoming.Inventory.Telemetry.ObservedAt = incoming.ObservedAt
	got, err = store.ObserveWorkerSnapshot(ctx, incoming, false)
	snapshotChangeCheck(t, err)
	row = snapshotChangeRead(t, db, seed.WorkerID)
	snapshotChangeAssertSnapshot(t, row, incoming)
	if !reflect.DeepEqual(got, incoming) || row.writes != after.writes+1 || row.audits != after.audits {
		t.Fatal("half-validity refresh did not advance full snapshot")
	}
}

func TestSnapshotTelemetryRefreshGuards(t *testing.T) {
	ctx := context.Background()
	store, db := snapshotChangeStore(t)
	seed := snapshotChangeAt(coordinatorSnapshot(4), 4, 0)
	seed.Inventory.Telemetry = &domain.WorkerTelemetry{ObservedAt: seed.ObservedAt}
	snapshotChangeCheck(t, store.SaveWorkerSnapshot(ctx, seed))
	before := snapshotChangeRead(t, db, seed.WorkerID)
	for _, kind := range []string{"coordinator-epoch", "sequence", "time", "worker-epoch"} {
		incoming := snapshotChangeAt(seed, 5, 10*time.Second)
		incoming.Inventory.Telemetry = &domain.WorkerTelemetry{ObservedAt: incoming.ObservedAt}
		switch kind {
		case "coordinator-epoch":
			incoming.CoordinatorEpoch++
		case "sequence":
			incoming.Sequence = seed.Sequence
		case "time":
			incoming.ObservedAt = seed.ObservedAt
		case "worker-epoch":
			incoming.WorkerEpoch = "old-epoch"
			incoming.ObservedAt = seed.ObservedAt.Add(-time.Second)
		}
		if _, err := store.ObserveWorkerSnapshot(ctx, incoming, false); err == nil {
			t.Fatalf("%s guard accepted observation", kind)
		}
		if snapshotChangeRead(t, db, seed.WorkerID) != before {
			t.Fatalf("%s guard changed telemetry", kind)
		}
	}
	wrongEpoch := snapshotChangeAt(seed, 5, 10*time.Second)
	wrongEpoch.WorkerEpoch = "wrong-epoch"
	wrongEpoch.Inventory.Telemetry = &domain.WorkerTelemetry{ObservedAt: wrongEpoch.ObservedAt}
	transport := &telemetryExchangeTransport{
		exchangeTransport: &exchangeTransport{workerID: seed.WorkerID, workerEpoch: seed.WorkerEpoch, snapshots: []domain.WorkerSnapshot{wrongEpoch}},
		telemetry:         wrongEpoch.Inventory.Telemetry,
	}
	coordinator := FleetCoordinator{Store: store, Now: func() time.Time { return wrongEpoch.ObservedAt }}
	if _, err := coordinator.ReconcileWorker(ctx, transport, testOfferBuilder{}, WorkerAdmissionPolicy{QuotaChecksDisabled: true}, nil, nil, time.Minute, 2*time.Minute); err == nil {
		t.Fatal("exchange accepted telemetry from a different worker epoch")
	}
	if snapshotChangeRead(t, db, seed.WorkerID) != before {
		t.Fatal("rejected exchange changed telemetry")
	}
	incoming := snapshotChangeAt(seed, 1, 10*time.Second)
	incoming.WorkerEpoch = "new-epoch"
	got, err := store.ObserveWorkerSnapshot(ctx, incoming, false)
	snapshotChangeCheck(t, err)
	snapshotChangeAssertSnapshot(t, snapshotChangeRead(t, db, seed.WorkerID), incoming)
	if got.Inventory.Telemetry != nil {
		t.Fatal("new epoch inherited old telemetry")
	}
}

func TestFleetCoordinatorTelemetryRefreshReachesNextPlan(t *testing.T) {
	ctx := context.Background()
	store, db := snapshotChangeStore(t)
	now := coordinatorTestTime
	coordinator := FleetCoordinator{Store: store, Now: func() time.Time { return now }}
	seed := snapshotChangeAt(coordinatorSnapshot(1), 1, 0)
	resources := resourceWorker(seed.WorkerID)
	seed.Inventory.Allocatable = resources.Allocatable
	seed.Inventory.CPUClass = resources.CPUClass
	seed.Inventory.Capabilities = []string{workerproto.CapabilityResourceTelemetry}
	seed.Inventory.Telemetry = resources.Telemetry.Clone()
	seed.Inventory.Telemetry.ObservedAt = now
	seed.Inventory.Telemetry.TempFreeMB = resourcePtr(int64(7985))
	snapshotChangeCheck(t, store.SaveWorkerSnapshot(ctx, seed))
	task := testTask("alpha")
	task.Routes = []domain.ProviderRoute{{ProviderInstanceID: "codex", Model: "gpt"}}
	task.ResourceDemand = domain.ResourceDemand{CPUUnits: 2, MemoryMB: 4096, ScratchMB: 8192}
	input := plannerInput([]domain.Task{task}, nil)
	input.Now = now
	input.QuotaPools = []domain.QuotaPool{routingPool("pool", 2, 0, "codex")}
	input.RouteEstimates = []RouteEstimate{routingEstimate("alpha-1", "worker-a", "codex", "gpt", nil, 10), routingEstimate("alpha-1", "worker-b", "codex", "gpt", nil, 10)}
	snapshotChangeCheck(t, store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{input.Workflows[0].Workflow}, WorkflowRuns: []domain.WorkflowRun{input.Workflows[0].State.Run},
		Tasks: []domain.Task{task}, Attempts: input.Workflows[0].State.Attempts, QuotaPools: input.QuotaPools,
	}))
	blocked, err := coordinator.PlanAndCommit(ctx, input)
	snapshotChangeCheck(t, err)
	if len(blocked.Assignments) != 0 || !hasPlacementExclusion(plannerDecision(t, blocked.Plan, "alpha").Placement, seed.WorkerID, "resource-temp-disk") {
		t.Fatalf("seed was not temp-disk excluded: %+v", blocked)
	}
	before := snapshotChangeRead(t, db, seed.WorkerID)
	now = now.Add(10 * time.Second)
	incoming := snapshotChangeAt(seed, 2, 10*time.Second)
	incoming.Inventory.Telemetry = seed.Inventory.Telemetry.Clone()
	incoming.Inventory.Telemetry.ObservedAt = now
	incoming.Inventory.Telemetry.TempFreeMB = resourcePtr(int64(61 * 1024))
	incoming.Inventory.Telemetry.MemoryAvailableMB = resourcePtr(int64(32768))
	incoming.Inventory.Telemetry.Load1 = resourcePtr(.5)
	incoming.Inventory.Telemetry.Load5 = resourcePtr(.5)
	report, transport := snapshotChangeExchange(t, coordinator, incoming, nil)
	if len(report.Renewed) != 0 || len(transport.offers) != 0 || len(report.Claimed) != 0 {
		t.Fatal("idle exchange offered, claimed or renewed")
	}
	row := snapshotChangeRead(t, db, seed.WorkerID)
	want := seed
	want.Inventory.Telemetry = incoming.Inventory.Telemetry.Clone()
	snapshotChangeAssertSnapshot(t, row, want)
	if row.writes != before.writes+1 || row.audits != before.audits || !reflect.DeepEqual(report.Snapshot, want) {
		t.Fatal("telemetry refresh changed snapshot fences or audits")
	}
	// A peer with higher measured load must rank behind the newly idle worker.
	peer := seed
	peer.WorkerID, peer.Inventory.ID = "worker-b", "worker-b"
	peer.Inventory.Telemetry = incoming.Inventory.Telemetry.Clone()
	peer.Inventory.Telemetry.Load1, peer.Inventory.Telemetry.Load5 = resourcePtr(4.0), resourcePtr(4.0)
	snapshotChangeCheck(t, store.SaveWorkerSnapshot(ctx, peer))
	input.Now = now
	planned, err := coordinator.PlanAndCommit(ctx, input)
	snapshotChangeCheck(t, err)
	decision := plannerDecision(t, planned.Plan, "alpha").Placement
	if len(planned.Assignments) != 1 || planned.Assignments[0].WorkerID != seed.WorkerID || hasPlacementExclusion(decision, seed.WorkerID, "resource-temp-disk") {
		t.Fatalf("next plan did not use refreshed resources: %+v", planned)
	}
	var ranks [2]int
	for _, evaluation := range planned.Plan.Proposals[0].Placement.ResourceEvaluations {
		if evaluation.WorkerID == seed.WorkerID {
			ranks[0] = evaluation.Rank
			if !reflect.DeepEqual(evaluation.Telemetry, incoming.Inventory.Telemetry) {
				t.Fatalf("placement evidence=%+v want=%+v", evaluation.Telemetry, incoming.Inventory.Telemetry)
			}
		} else {
			ranks[1] = evaluation.Rank
		}
	}
	if ranks[0] == 0 || ranks[1] <= ranks[0] {
		t.Fatalf("load ranking=%v", ranks)
	}
	now = now.Add(10 * time.Second)
	incoming = snapshotChangeAt(incoming, 3, 20*time.Second)
	incoming.Inventory.Telemetry = want.Inventory.Telemetry.Clone()
	incoming.Inventory.Telemetry.ObservedAt = now
	incoming.Inventory.Telemetry.MemoryAvailableMB = resourcePtr(int64(1024))
	// Finish the prior offer before collecting another idle exchange.
	finished := planned.Assignments[0]
	finished.State = domain.AssignmentReleased
	finished.UpdatedAt = now
	snapshotChangeCheck(t, store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Assignments: []domain.Assignment{finished}}))
	report, transport = snapshotChangeExchange(t, coordinator, incoming, nil)
	if len(report.Renewed) != 0 || len(transport.offers) != 0 || len(report.Claimed) != 0 {
		t.Fatal("reverse idle exchange offered, claimed or renewed")
	}
	input.Now = now
	reversed, err := coordinator.PlanAndCommit(ctx, input)
	snapshotChangeCheck(t, err)
	if !hasPlacementExclusion(plannerDecision(t, reversed.Plan, "alpha").Placement, seed.WorkerID, "resource-memory") {
		t.Fatalf("next plan missed reversed memory floor: %+v", reversed)
	}
	var durable domain.WorkerSnapshot
	snapshotChangeCheck(t, json.Unmarshal([]byte(snapshotChangeRead(t, db, seed.WorkerID).record), &durable))
	if durable.Sequence != seed.Sequence || !durable.ObservedAt.Equal(seed.ObservedAt) || !durable.ValidUntil.Equal(seed.ValidUntil) {
		t.Fatal("reverse telemetry observation advanced fences")
	}
}

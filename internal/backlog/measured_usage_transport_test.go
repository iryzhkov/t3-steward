package backlog

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/source/providerlog"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type measuredUsageTransport struct {
	store    *sqlite.Store
	snapshot domain.WorkerSnapshot
}

func (t measuredUsageTransport) SnapshotObservations(ctx context.Context, request workerproto.SnapshotRequest) (workerproto.Observations, error) {
	samples, err := t.store.WorkerUsageBatch(ctx, request.UsageAcknowledgements, workerproto.MaxUsageDelivery)
	return workerproto.Observations{
		Snapshot:                  t.snapshot,
		Usage:                     samples,
		AcknowledgedUsageEventIDs: append([]string(nil), request.UsageAcknowledgements...),
	}, err
}

func TestMeasuredUsageCrossesWorkerTransportWithReplayAndCoverage(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	workerPath := filepath.Join(root, "worker.db")
	worker, err := sqlite.OpenMigrated(workerPath)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := sqlite.OpenMigrated(filepath.Join(root, "coordinator.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer coordinator.Close()

	fixtures := []string{"measured-usage-claude.json", "measured-usage-codex.json"}
	var samples []domain.UsageSample
	for _, name := range fixtures {
		body, err := os.ReadFile(filepath.Join("..", "source", "providerlog", "testdata", name))
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := providerlog.ParseUsageJSON(body, time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		samples = append(samples, parsed...)
	}
	unknown := samples[0]
	unknown.SourceEventID = "unknown-event"
	unknown.ThreadID = "unknown-thread"
	samples = append(samples, unknown)
	for _, sample := range samples {
		if err := worker.RecordUsage(ctx, sample); err != nil {
			t.Fatal(err)
		}
	}

	now := samples[0].ObservedAt
	records := sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{{ID: "run-claude", WorkflowID: "workflow"}, {ID: "run-codex", WorkflowID: "workflow"}},
		Attempts: []domain.Attempt{
			{ID: "attempt-claude", WorkflowRunID: "run-claude", TaskID: "task", Number: 1},
			{ID: "attempt-codex", WorkflowRunID: "run-codex", TaskID: "task", Number: 1},
		},
	}
	if err := coordinator.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}
	assignments := []domain.Assignment{
		usageTransportAssignment("assignment-claude", "attempt-claude", samples[0], now),
		usageTransportAssignment("assignment-codex", "attempt-codex", samples[1], now),
	}
	for _, assignment := range assignments {
		if _, err := coordinator.PrepareAssignmentDispatch(ctx, assignment); err != nil {
			t.Fatal(err)
		}
	}

	snapshot := domain.WorkerSnapshot{WorkerID: "worker", WorkerEpoch: "worker-epoch", CoordinatorEpoch: 1, Sequence: 1}
	transport := measuredUsageTransport{store: worker, snapshot: snapshot}
	first, err := transport.SnapshotObservations(ctx, workerproto.SnapshotRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.ReceiveWorkerUsage(ctx, snapshot.WorkerID, first.Usage); err != nil {
		t.Fatal(err)
	}
	// A lost response causes the same source events to replay; coordinator dedupe is stable.
	replayed, err := transport.SnapshotObservations(ctx, workerproto.SnapshotRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.ReceiveWorkerUsage(ctx, snapshot.WorkerID, replayed.Usage); err != nil {
		t.Fatal(err)
	}
	acks, err := coordinator.WorkerUsageAcknowledgements(ctx, snapshot.WorkerID)
	if err != nil {
		t.Fatal(err)
	}
	settled, err := transport.SnapshotObservations(ctx, workerproto.SnapshotRequest{UsageAcknowledgements: acks})
	if err != nil {
		t.Fatal(err)
	}
	if len(settled.Usage) != 0 || len(settled.AcknowledgedUsageEventIDs) != len(acks) {
		t.Fatalf("settled delivery = %#v", settled)
	}
	if err := coordinator.ClearWorkerUsageAcknowledgements(ctx, snapshot.WorkerID, settled.AcknowledgedUsageEventIDs); err != nil {
		t.Fatal(err)
	}
	if err := worker.Close(); err != nil {
		t.Fatal(err)
	}
	worker, err = sqlite.OpenMigrated(workerPath)
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	remaining, err := worker.WorkerUsageBatch(ctx, nil, workerproto.MaxUsageDelivery)
	if err != nil || len(remaining) != 0 {
		t.Fatalf("worker restart pending=%#v err=%v", remaining, err)
	}

	claude, err := coordinator.AttributedUsage(ctx, "run-claude")
	if err != nil {
		t.Fatal(err)
	}
	codex, err := coordinator.AttributedUsage(ctx, "run-codex")
	if err != nil {
		t.Fatal(err)
	}
	if len(claude.Samples) != 1 || claude.Samples[0].Attribution.AttemptID != "attempt-claude" ||
		len(codex.Samples) != 1 || codex.Samples[0].Attribution.AttemptID != "attempt-codex" {
		t.Fatalf("claude=%#v codex=%#v", claude, codex)
	}
	if claude.Coverage.UnscopedUnattributedCount != 1 || claude.Coverage.Reason == "" {
		t.Fatalf("coverage = %#v", claude.Coverage)
	}
}

func usageTransportAssignment(id, attempt string, sample domain.UsageSample, now time.Time) domain.Assignment {
	return domain.Assignment{
		ID: id, AttemptID: attempt, WorkerID: "worker", WorkerEpoch: "worker-epoch",
		Route: domain.ProviderRoute{ProviderInstanceID: sample.ProviderInstanceID, Model: sample.Model},
		State: domain.AssignmentClaimed, Epoch: 1, LeaseToken: "lease-" + id,
		DispatchToken: "dispatch-" + id, ThreadID: sample.ThreadID,
		CreatedAt: now, UpdatedAt: now,
	}
}

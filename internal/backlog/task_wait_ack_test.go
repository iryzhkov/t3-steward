package backlog

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type ackParkStore struct {
	parkStore
	snapshot     domain.WorkerSnapshot
	readSnapshot bool
	t            *testing.T
}

func (s *ackParkStore) LoadWorkerSnapshots(context.Context) ([]domain.WorkerSnapshot, error) {
	s.readSnapshot = true
	return []domain.WorkerSnapshot{s.snapshot}, nil
}
func (s *ackParkStore) ParkedTaskWaitAttempts(context.Context) (map[string]string, error) {
	if !s.readSnapshot {
		s.t.Fatal("park state read before durable worker snapshot")
	}
	return nil, nil
}
func TestParkReportAcknowledgesOnlyCapableWorkersAndReadsSnapshotFirst(t *testing.T) {
	for _, capable := range []bool{false, true} {
		source := &ackParkStore{t: t, snapshot: domain.WorkerSnapshot{
			WorkerID: "worker", WorkerEpoch: "epoch-1", Sequence: 17,
		}}
		if capable {
			source.snapshot.Inventory.Capabilities = []string{workerproto.CapabilityTaskWaitCollectionFence}
		}
		request, err := ParkedAssignmentsFor(context.Background(), source, "worker")
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		if capable {
			if request.ObservedSequence != 17 || request.ObservedWorkerEpoch != "epoch-1" {
				t.Fatalf("%+v", request)
			}
		} else if strings.Contains(string(raw), "observedSequence") || strings.Contains(string(raw), "observedWorkerEpoch") {
			t.Fatalf("new fields sent to strict old decoder: %s", raw)
		}
	}
}

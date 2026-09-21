package workerruntime

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func authorizedWakeTaskWaits(t *testing.T, store *sqlite.Store, now time.Time) ([]domain.TaskWaitWakeContext, error) {
	t.Helper()
	snapshots, err := store.LoadWorkerSnapshots(context.Background())
	if err != nil {
		return nil, err
	}
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		return nil, err
	}
	for _, assignment := range records.Assignments {
		for index := range snapshots {
			if snapshots[index].WorkerID == assignment.WorkerID && snapshots[index].WorkerEpoch != assignment.WorkerEpoch {
				snapshots[index] = domain.WorkerSnapshot{WorkerID: assignment.WorkerID, WorkerEpoch: assignment.WorkerEpoch,
					CoordinatorEpoch: snapshots[index].CoordinatorEpoch, Sequence: 1, Connected: true,
					ObservedAt: now, ValidUntil: now.Add(time.Hour), Inventory: domain.WorkerInventory{ID: assignment.WorkerID, AcceptBacklog: true, Health: domain.WorkerHealthReady, ObservedAt: now}}
				if err := store.SaveWorkerSnapshot(context.Background(), snapshots[index]); err != nil {
					return nil, err
				}
			}
		}
	}
	if len(snapshots) == 0 {
		for _, assignment := range records.Assignments {
			snapshot := domain.WorkerSnapshot{WorkerID: assignment.WorkerID, WorkerEpoch: assignment.WorkerEpoch,
				CoordinatorEpoch: 1, Sequence: 1, Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Hour),
				Inventory: domain.WorkerInventory{ID: assignment.WorkerID, AcceptBacklog: true, Health: domain.WorkerHealthReady, ObservedAt: now}}
			if err := store.SaveWorkerSnapshot(context.Background(), snapshot); err != nil {
				return nil, err
			}
			snapshots = append(snapshots, snapshot)
		}
	}
	authorized := make(map[string]sqlite.TaskWakeWorkerAuthorization, len(snapshots))
	for _, snapshot := range snapshots {
		authorized[snapshot.WorkerID] = sqlite.TaskWakeWorkerAuthorization{
			WorkerEpoch: snapshot.WorkerEpoch, SnapshotSequence: snapshot.Sequence,
			CatalogRevision: snapshot.Inventory.CatalogRevision, ValidUntil: snapshot.ValidUntil,
			Providers: snapshot.Inventory.Providers, Projects: snapshot.Inventory.Projects,
		}
	}
	return store.WakeTaskWaitsBefore(context.Background(), now, sqlite.TaskWakeCutoffs{AuthorizedWorkers: authorized})
}

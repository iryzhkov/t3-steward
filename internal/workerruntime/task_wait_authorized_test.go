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

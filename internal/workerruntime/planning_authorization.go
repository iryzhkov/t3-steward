package workerruntime

import (
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"time"
)

// AuthorizedPlanningSnapshots excludes observations from removed workers or
// superseded catalogs. A fresh worker exchange must acknowledge the current
// catalog before its observations can authorize new placement.
func AuthorizedPlanningSnapshots(settings config.BacklogV2, snapshots []domain.WorkerSnapshot, now time.Time) []domain.WorkerSnapshot {
	result := make([]domain.WorkerSnapshot, 0, len(snapshots))
	for _, snapshot := range snapshots {
		binding, err := BuildWorkerBinding(settings, snapshot.WorkerID, now)
		if err != nil || snapshot.Inventory.CatalogRevision != binding.CatalogRevision {
			continue
		}
		result = append(result, snapshot)
	}
	return result
}

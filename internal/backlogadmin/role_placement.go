package backlogadmin

import (
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"time"
)

func (v view) roleWorkerEligible(settings ViabilitySettings, workers []viabilityWorker) RoleWorkerEligible {
	return func(task ViabilityTask, id string, ready bool) bool {
		for _, worker := range workers {
			if worker.id != id || !worker.hasSnapshot {
				continue
			}
			request := backlog.WorkerPlacementRequest{Task: domain.Task{Placement: domain.Placement{Hosts: task.Hosts, Capabilities: task.Capabilities}, ResourceDemand: task.Resources, ResourcePreset: task.ResourcePreset}, Project: task.Project, Now: v.now, MaxSnapshotAge: time.Duration(1 << 62), ResourcePolicy: settings.ResourcePolicy}
			inventory := worker.inventory
			if !ready {
				inventory.Reserved = domain.ReservedCapacity{}
			}
			matched, err := backlog.MatchWorkers(request, []domain.WorkerInventory{inventory})
			if err != nil || len(matched.Evaluations) != 1 {
				return false
			}
			for _, exclusion := range matched.Evaluations[0].Exclusions {
				switch exclusion.Code {
				case backlog.ExclusionWorkerHealth, backlog.ExclusionWorkerStale, "resource-memory", "resource-swap", "resource-workspace-disk", "resource-temp-disk":
					if ready {
						return false
					}
				default:
					return false
				}
			}
			return true
		}
		return false
	}
}

package backlogadmin

import (
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestWorkerExplanationHonorsEnrollmentAndDrain(t *testing.T) {
	now := time.Now().UTC()
	for _, scenario := range []string{"enrolled", "legacy", "unenrolled", "draining", "removed", "changed-catalog", "changed-epoch", "stale"} {
		t.Run(scenario, func(t *testing.T) {
			snapshot := domain.WorkerSnapshot{
				WorkerID: "homelab", WorkerEpoch: "worker-1", CoordinatorEpoch: 7, Connected: true,
				ObservedAt: now, ValidUntil: now.Add(time.Minute),
				Inventory: domain.WorkerInventory{ID: "homelab", AcceptBacklog: true, Health: domain.WorkerHealthReady, CatalogRevision: "catalog-1"},
			}
			v := newView(sqlite.CoordinatorRecords{}, []domain.WorkerSnapshot{snapshot}, nil, RuntimeInfo{Epoch: 7, MaxWorkerSnapshotAge: time.Minute}, now)
			v.requirements = []domain.WorkerRequirement{{WorkerID: "homelab", WorkerEpoch: "worker-1", CatalogRevision: "catalog-1", Connection: "persistent-ssh", CredentialRef: "secretref:f02-protocol/homelab"}}
			v.enrollments = []domain.WorkerEnrollment{{Request: domain.WorkerEnrollmentRequest{WorkerID: "homelab", CatalogRevision: "catalog-1"}, WorkerEpoch: "worker-1", Connection: "persistent-ssh", CredentialRef: "secretref:f02-protocol/homelab"}}
			switch scenario {
			case "legacy":
				v.requirements = nil
				v.enrollments = nil
			case "unenrolled":
				v.enrollments = nil
			case "draining":
				v.requirements[0].Draining = true
			case "removed":
				v.requirements[0].Connection = "removed"
			case "changed-catalog":
				v.workers[0].Inventory.CatalogRevision = "catalog-2"
			case "changed-epoch":
				v.workers[0].WorkerEpoch = "worker-2"
			case "stale":
				v.workers[0].ObservedAt = now.Add(-2 * time.Minute)
			}
			var explanation Explanation
			v.addWorkerBlocker(&explanation, domain.Task{Placement: domain.Placement{Hosts: []string{"homelab"}}})
			wantEligible := scenario == "enrolled" || scenario == "legacy"
			if (len(explanation.Blockers) == 0) != wantEligible {
				t.Fatalf("blockers = %+v, want eligible %v", explanation.Blockers, wantEligible)
			}
		})
	}
}

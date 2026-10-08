package backlog

import (
	"reflect"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// A task that requires a host capability is placed on the worker whose host
// reports it, and the worker without it is excluded with the capability named
// in the exclusion, so readiness can tell a host capability from a build one.
func TestMatchWorkersPlacesAHostCapabilityOnTheCapableWorker(t *testing.T) {
	task := placementTask()
	task.Placement.Capabilities = []string{"internet", workerproto.CapabilityAskRelay}
	inventory := []domain.WorkerInventory{
		placementWorker("agent-a", "internet"),
		placementWorker("homelab", "internet", workerproto.CapabilityAskRelay),
	}

	result := mustMatchWorkers(t, task, inventory)
	if want := []string{"homelab"}; !reflect.DeepEqual(result.EligibleWorkerIDs, want) {
		t.Fatalf("eligible workers = %#v, want %#v", result.EligibleWorkerIDs, want)
	}
	evaluation := evaluationFor(t, result, "agent-a")
	if len(evaluation.Exclusions) != 1 {
		t.Fatalf("agent-a exclusions = %+v", evaluation.Exclusions)
	}
	exclusion := evaluation.Exclusions[0]
	if exclusion.Code != ExclusionMissingCapability || exclusion.Capability != workerproto.CapabilityAskRelay {
		t.Fatalf("exclusion = %+v, want missing-capability naming %s", exclusion, workerproto.CapabilityAskRelay)
	}
}

// Only a missing-capability exclusion names a capability.
func TestMatchWorkersNamesNoCapabilityOnOtherExclusions(t *testing.T) {
	task := placementTask()
	worker := placementWorker("normandy", "internet")
	worker.Projects = nil
	result := mustMatchWorkers(t, task, []domain.WorkerInventory{worker})
	for _, exclusion := range evaluationFor(t, result, "normandy").Exclusions {
		if exclusion.Capability != "" {
			t.Fatalf("exclusion %+v names a capability", exclusion)
		}
	}
}

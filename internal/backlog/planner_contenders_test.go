package backlog

import (
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestBuildUnreservedProposalsIncludesContenderHiddenByBatchReservation(t *testing.T) {
	worker := plannerWorker("worker-a")
	worker.Allocatable.ExecutorSlots = 1
	input := plannerInput([]domain.Task{testTask("older"), testTask("newer")}, []domain.WorkerInventory{worker})
	input.Ordering.Attempts["older-1"] = PlanningAttemptOrdering{ReadySince: plannerTestTime.Add(-2)}
	input.Ordering.Attempts["newer-1"] = PlanningAttemptOrdering{ReadySince: plannerTestTime.Add(-1)}
	input.Constraints = []PlanningConstraint{NewCapacityConstraint(executorPools(input.Workers), nil)}

	plan, err := BuildPlan(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Proposals) != 1 {
		t.Fatalf("batch proposals=%d, want one slot", len(plan.Proposals))
	}
	contenders, err := BuildUnreservedProposals(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(contenders) != 2 || contenders[0].AttemptID != "older-1" || contenders[1].AttemptID != "newer-1" {
		t.Fatalf("unreserved contenders=%+v, want both in durable order", contenders)
	}
}

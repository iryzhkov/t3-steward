package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// The admission fence counts a sized demand against a worker that declares no
// cpu or memory capacity as one slot, as placement does, and checks a worker
// that declares cpu units on them.
func TestAssignmentPlanCountsSizedDemandAsSlotOnUnsizedWorker(t *testing.T) {
	for _, tc := range []struct {
		name        string
		allocatable domain.AllocatableCapacity
		want        int
	}{
		{"slots only", domain.AllocatableCapacity{ExecutorSlots: 2}, 2},
		{"cpu units", domain.AllocatableCapacity{ExecutorSlots: 2, CPUUnits: 6}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := openFleetTestStore(t)
			saveFleetAttempt(t, store, fleetAttempt("attempt-build-1"))
			secondAttempt := fleetAttempt("attempt-build-2")
			secondAttempt.Number = 2
			saveFleetAttempt(t, store, secondAttempt)
			snapshot := fleetSnapshot(1, "worker-epoch-1", 1, true, fleetTestTime.Add(time.Hour))
			snapshot.Inventory.CPUClass = domain.CPUClassHigh
			snapshot.Inventory.Allocatable = tc.allocatable
			saveFleetSnapshot(t, store, snapshot)

			build := domain.ResourceDemand{MinCPUClass: domain.CPUClassMedium, PreferredCPUClass: domain.CPUClassHigh, CPUUnits: 4, MemoryMB: 6000, ScratchMB: 8192}
			first := fleetPlanCommit(1, "assignment-build-1", "attempt-build-1", "worker-epoch-1", 1)
			first.Items[0].Assignment.ExecutorDemand = &build
			second := fleetPlanCommit(1, "assignment-build-2", "attempt-build-2", "worker-epoch-1", 1)
			second.Items[0].Assignment.ExecutorDemand = &build
			second.Items[0].Assignment.LeaseToken = "lease-token-2"
			second.Items[0].Assignment.DispatchToken = "dispatch-token-2"
			second.Items[0].Assignment.ThreadID = "thread-2"
			first.Items = append(first.Items, second.Items...)
			got, err := store.CommitAssignmentPlan(context.Background(), first)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != tc.want {
				t.Fatalf("committed builds = %d, want %d", len(got), tc.want)
			}
		})
	}
}

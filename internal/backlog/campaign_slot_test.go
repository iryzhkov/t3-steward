package backlog

import (
	"fmt"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestCampaignSlotsMatchExecutorAdmission(t *testing.T) {
	for _, cpu := range []float64{0, 3, 30} {
		for _, sized := range []bool{false, true} {
			t.Run(fmt.Sprintf("cpu=%g/sized=%t", cpu, sized), func(t *testing.T) {
				w := placementWorker("agent-b", "internet")
				w.CPUClass = domain.CPUClassMedium
				w.Allocatable = domain.AllocatableCapacity{ExecutorSlots: 2}
				if sized {
					w.Allocatable.CPUUnits = 100
				}
				demand := domain.ResourceDemand{CPUUnits: cpu}
				r, err := NewExecutorRegistry([]domain.ExecutorPool{{WorkerID: w.ID, Name: "default", CPUClass: w.CPUClass, Allocatable: w.Allocatable}}, nil)
				if err != nil {
					t.Fatal(err)
				}
				for occupied := 0; occupied <= w.Allocatable.ExecutorSlots; occupied++ {
					s := r.Snapshot(w.ID)
					w.Reserved = s.Reserved
					if s.FreeSlots() != w.Allocatable.ExecutorSlots-occupied {
						t.Fatalf("one reservation must consume one slot: %+v", s)
					}
					exclusions := capacityExclusions(demand, w)
					_, err := r.Reserve(ReservationRequest{ReservationID: fmt.Sprint(occupied), WorkerID: w.ID, Kind: ReservationKindAttempt, Demand: demand, ProviderAdmissionHeld: true})
					if (len(exclusions) == 0) != (err == nil) || (err == nil) != (s.FreeSlots() > 0) {
						t.Fatalf("placement/reservation parity: %+v, %v", exclusions, err)
					}
				}
				if _, err := r.Release("0", domain.ResourceReleaseSettled); err != nil {
					t.Fatal(err)
				}
				w.Reserved = r.Snapshot(w.ID).Reserved
				if got := capacityExclusions(demand, w); len(got) != 0 {
					t.Fatalf("released slot did not restore eligibility: %+v", got)
				}
			})
		}
	}
}

func TestCampaignSlotsPreserveOtherCapacityLimits(t *testing.T) {
	w := placementWorker("agent-b", "internet")
	w.CPUClass = domain.CPUClassMedium
	w.Allocatable = domain.AllocatableCapacity{ExecutorSlots: 2, CPUUnits: 2, MemoryMB: 100, ScratchMB: 100}
	for _, demand := range []domain.ResourceDemand{
		{CPUUnits: 3}, {MemoryMB: 101}, {ScratchMB: 101}, {MinCPUClass: domain.CPUClassHigh},
	} {
		if got := capacityExclusions(demand, w); len(got) != 1 {
			t.Fatalf("declared limit lost for %+v: %+v", demand, got)
		}
	}
	w.Allocatable = domain.AllocatableCapacity{}
	w.Reserved.ExecutorSlots = 2
	for _, demand := range []domain.ResourceDemand{{}, {CPUUnits: 3}} {
		if got := capacityExclusions(demand, w); len(got) != 0 {
			t.Fatalf("legacy zero slots became exhausted: %+v", got)
		}
	}
}

package backlogadmin

import (
	"context"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestViabilitySlotCapacityTransitions(t *testing.T) {
	task := viabilityTaskRequest()
	task.Resources = domain.ResourceDemand{CPUUnits: 3, MinCPUClass: domain.CPUClassMedium}
	for _, reserved := range []int{0, 1, 2, 1} {
		v := viabilityView(t, func(v *view) {
			v.workers[0].Inventory.CPUClass = domain.CPUClassMedium
			v.workers[0].Inventory.Allocatable = domain.AllocatableCapacity{ExecutorSlots: 2}
			v.workers[0].Inventory.Reserved.ExecutorSlots = reserved
		})
		matrix := v.viability(context.Background(), viabilityCatalog(t), ViabilityRequest{Tasks: []ViabilityTask{task}})
		want := ViabilityReady
		if reserved == 2 {
			want = ViabilityAcceptedWaiting
			reason, ok := candidateReason(t, matrix, ReasonWorkerAtCapacity)
			if !ok || reason.Permanent {
				t.Fatalf("full slot pool must be temporary: %+v", matrix)
			}
		}
		c := matrix.Tasks[0].Candidates[0]
		e := c.Capacity
		if matrix.Outcome != want || c.Outcome != want || e == nil || e.State != "current" {
			t.Fatalf("capacity transition: %+v", matrix)
		}
		if *e.Slots != (ViabilitySlotCapacity{Allocatable: 2, Reserved: reserved, Free: 2 - reserved, Required: 1}) ||
			*e.CPUUnits != (ViabilityCPUCapacity{DeclaredDemand: 3}) {
			t.Fatalf("slot-only evidence: %+v %+v", e.Slots, e.CPUUnits)
		}
	}
}

func TestViabilityCapacityFreshnessAndCPUEvidence(t *testing.T) {
	for _, state := range []string{"current", "stale", "unknown"} {
		v := viabilityView(t, func(v *view) {
			v.workers[0].Inventory.Allocatable = domain.AllocatableCapacity{ExecutorSlots: 2, CPUUnits: 4}
			v.workers[0].Inventory.Reserved = domain.ReservedCapacity{ExecutorSlots: 1, CPUUnits: 2}
			if state == "stale" {
				v.workers[0].ValidUntil = viabilityNow.Add(-1)
			}
			if state == "unknown" {
				v.workers = nil
			}
		})
		task := viabilityTaskRequest()
		task.Resources.CPUUnits = 3
		matrix := v.viability(context.Background(), viabilityCatalog(t), ViabilityRequest{Tasks: []ViabilityTask{task}})
		e := matrix.Tasks[0].Candidates[0].Capacity
		if e == nil || e.State != state || matrix.Outcome != ViabilityAcceptedWaiting {
			t.Fatalf("capacity freshness: %+v, evidence %+v", matrix, e)
		}
		if state == "unknown" {
			if e.Slots != nil || e.CPUUnits != nil {
				t.Fatalf("invented unknown counts: %+v", e)
			}
		} else if *e.CPUUnits != (ViabilityCPUCapacity{DeclaredDemand: 3, GoverningDemand: 3, Allocatable: 4, Reserved: 2, Free: 2, Governed: true}) {
			t.Fatalf("CPU evidence: %+v", e.CPUUnits)
		}
	}
}

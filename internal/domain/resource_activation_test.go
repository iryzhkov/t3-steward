package domain

import "testing"

func TestResourcePlacementRecognizesSlotOnlyActivation(t *testing.T) {
	activation := Attempt{TaskID: "activation-id", SupervisionActivationID: "activation-id", SupervisionActivationEpoch: 1}
	demand, known := AssignmentExecutorDemand(activation, Assignment{})
	if !known || !demand.IsZero() {
		t.Fatalf("activation demand=%+v known=%v", demand, known)
	}
	// Ordinary historical tasks with no frozen evidence must still fail closed.
	ordinary := Attempt{TaskID: "declared-task"}
	if _, known := AssignmentExecutorDemand(ordinary, Assignment{}); known {
		t.Fatal("ordinary unknown demand admitted")
	}
	frozen := ResourceDemand{CPUUnits: 2, MemoryMB: 4096, ScratchMB: 8192}
	demand, known = AssignmentExecutorDemand(ordinary, Assignment{ExecutorDemand: &frozen})
	if !known || demand != frozen {
		t.Fatalf("frozen demand lost: %+v known=%v", demand, known)
	}
}

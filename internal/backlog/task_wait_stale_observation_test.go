package backlog

import (
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// The worker still reports its old parked phase in the exchange that tells it
// a wake has landed. Only registration at the coordinator can start a new park.
func TestWorkerParkedObservationCannotUndoCoordinatorWake(t *testing.T) {
	for _, control := range []domain.ControlState{domain.ControlResuming, domain.ControlRunning} {
		t.Run(string(control), func(t *testing.T) {
			records, snapshot := parkedReleaseRecords()
			attempt := records.Attempts[0]
			attempt.Progress = domain.ProgressActive
			attempt.Control = control
			records.Attempts[0] = attempt
			snapshot.Assignments = []domain.WorkerAssignmentObservation{{
				AssignmentID: "assign-1", AssignmentEpoch: 2,
				State: domain.AssignmentClaimed, Control: domain.ControlWaitingExternal,
				ThreadID: "thread-1", ObservedAt: parkedReleaseTime,
			}}
			transitions, err := PlanWorkerStateTransitions(records, snapshot, nil, parkedReleaseTime)
			if err != nil {
				t.Fatal(err)
			}
			for _, transition := range transitions {
				if transition.Attempt.Progress != domain.ProgressActive || transition.Attempt.Control != control {
					t.Fatalf("stale parked observation undid the wake: %s/%s",
						transition.Attempt.Progress, transition.Attempt.Control)
				}
			}
		})
	}
}

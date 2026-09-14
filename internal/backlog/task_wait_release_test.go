package backlog

import (
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/directoryresource"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

var parkedReleaseTime = time.Date(2026, 9, 14, 17, 30, 44, 0, time.UTC)

func parkedReleaseRecords() (sqlite.CoordinatorRecords, domain.WorkerSnapshot) {
	attempt := domain.Attempt{
		ID: "a1", WorkflowRunID: "r", TaskID: "t", Number: 1, Revision: 5,
		Progress: domain.ProgressWaitingExternal, Control: domain.ControlWaitingExternal,
		AssignmentID: "assign-1", ThreadID: "thread-1",
	}
	assignment := domain.Assignment{
		ID: "assign-1", AttemptID: "a1", WorkerID: "normandy", WorkerEpoch: "worker-1", Epoch: 2,
		State: domain.AssignmentClaimed, LeaseToken: "lease", DispatchToken: "dispatch",
		ThreadID: "thread-1", LeaseExpiresAt: parkedReleaseTime.Add(time.Hour),
	}
	records := sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{{ID: "r", WorkflowID: "w"}},
		Tasks:        []domain.Task{{ID: "t", WorkflowID: "w", Name: "t"}},
		Attempts:     []domain.Attempt{attempt},
		Assignments:  []domain.Assignment{assignment},
	}
	snapshot := domain.WorkerSnapshot{
		WorkerID: "normandy", WorkerEpoch: "worker-1", CoordinatorEpoch: 1, Sequence: 1,
		Connected: true, ObservedAt: parkedReleaseTime, ValidUntil: parkedReleaseTime.Add(time.Minute),
	}
	return records, snapshot
}

// Every path that releases or completes an assignment must leave a parked
// attempt holding its assignment reference.
//
// Clearing it drops the directory writer binding the park is supposed to hold,
// and hides the release from the abandonment check, so the attempt wakes with
// no worker, is never dispatched, never becomes terminal, and its run never
// settles. The assignment itself does settle, because that is the evidence the
// wake needs in order to revoke the thread's authority and fail honestly.
func TestParkedAttemptKeepsItsAssignmentThroughEveryReleasePath(t *testing.T) {
	cases := map[string]func(*sqlite.CoordinatorRecords, *domain.WorkerSnapshot) []domain.WorkerCommandRecord{
		"worker epoch changed": func(records *sqlite.CoordinatorRecords, snapshot *domain.WorkerSnapshot) []domain.WorkerCommandRecord {
			snapshot.WorkerEpoch = "worker-2"
			return nil
		},
		"stop accepted": func(records *sqlite.CoordinatorRecords, _ *domain.WorkerSnapshot) []domain.WorkerCommandRecord {
			return []domain.WorkerCommandRecord{acceptedCommand(domain.WorkerCommandStop)}
		},
		"dispatch rejected": func(records *sqlite.CoordinatorRecords, _ *domain.WorkerSnapshot) []domain.WorkerCommandRecord {
			return []domain.WorkerCommandRecord{rejectedCommand(domain.WorkerCommandDispatch)}
		},
		"prepare rejected": func(records *sqlite.CoordinatorRecords, _ *domain.WorkerSnapshot) []domain.WorkerCommandRecord {
			return []domain.WorkerCommandRecord{rejectedCommand(domain.WorkerCommandPrepare)}
		},
		"assignment unknown to the worker": func(records *sqlite.CoordinatorRecords, _ *domain.WorkerSnapshot) []domain.WorkerCommandRecord {
			records.Assignments[0].State = domain.AssignmentUnknown
			return nil
		},
		"worker observed a release": func(_ *sqlite.CoordinatorRecords, snapshot *domain.WorkerSnapshot) []domain.WorkerCommandRecord {
			snapshot.Assignments = []domain.WorkerAssignmentObservation{{
				AssignmentID: "assign-1", AssignmentEpoch: 2, State: domain.AssignmentReleased,
				ThreadID: "thread-1", ObservedAt: parkedReleaseTime,
			}}
			return nil
		},
		"worker observed a completion": func(_ *sqlite.CoordinatorRecords, snapshot *domain.WorkerSnapshot) []domain.WorkerCommandRecord {
			snapshot.Assignments = []domain.WorkerAssignmentObservation{{
				AssignmentID: "assign-1", AssignmentEpoch: 2, State: domain.AssignmentCompleted,
				ThreadID: "thread-1", ObservedAt: parkedReleaseTime,
			}}
			return nil
		},
	}
	for name, arrange := range cases {
		t.Run(name, func(t *testing.T) {
			records, snapshot := parkedReleaseRecords()
			commands := arrange(&records, &snapshot)
			transitions, err := PlanWorkerStateTransitions(records, snapshot, commands, parkedReleaseTime)
			if err != nil {
				t.Fatal(err)
			}
			if len(transitions) == 0 {
				// Nothing changed, which is also a correct outcome: the parked
				// attempt still holds everything it held.
				return
			}
			attempt := transitions[0].Attempt
			if attempt.AssignmentID != "assign-1" {
				t.Fatalf("a parked attempt lost its assignment reference: %+v", attempt)
			}
			if attempt.Progress != domain.ProgressWaitingExternal {
				t.Fatalf("a parked attempt changed progress to %q", attempt.Progress)
			}
			if attempt.Control != domain.ControlWaitingExternal {
				t.Fatalf("a parked attempt changed control to %q", attempt.Control)
			}
			// The binding follows the attempt's assignment reference, so it is
			// still derivable: the park did not silently release it.
			owners := directoryOwners([]domain.Attempt{attempt},
				[]domain.Assignment{transitions[0].Assignment}, records.WorkflowRuns,
				[]domain.Task{{ID: "t", WorkflowID: "w", Name: "t",
					DirectoryBindings: parkedTestBindings()}})
			settled := transitions[0].Assignment.State == domain.AssignmentCompleted ||
				transitions[0].Assignment.State == domain.AssignmentReleased
			if !settled && len(owners) != 1 {
				t.Fatalf("a parked attempt on a live assignment lost its directory binding: %v", owners)
			}
		})
	}
}

// The abandonment check has to be reachable the way it actually happens: a
// worker reports the assignment finished while the coordinator has the attempt
// parked. Before this, the completed branches returned unchanged and the
// assignment stayed claimed forever, so the check could only be reached by
// writing a settled assignment into the store by hand.
func TestWorkerCompletionOfAParkedAttemptSettlesTheAssignment(t *testing.T) {
	records, snapshot := parkedReleaseRecords()
	snapshot.Assignments = []domain.WorkerAssignmentObservation{{
		AssignmentID: "assign-1", AssignmentEpoch: 2, State: domain.AssignmentCompleted,
		ThreadID: "thread-1", ObservedAt: parkedReleaseTime,
	}}
	commands := []domain.WorkerCommandRecord{acceptedCommand(domain.WorkerCommandCollect)}
	transitions, err := PlanWorkerStateTransitions(records, snapshot, commands, parkedReleaseTime)
	if err != nil {
		t.Fatal(err)
	}
	if len(transitions) != 1 {
		t.Fatalf("a completed parked attempt produced %d transitions", len(transitions))
	}
	if got := transitions[0].Assignment.State; got != domain.AssignmentCompleted {
		t.Fatalf("the assignment state is %q; the abandonment check can never see it", got)
	}
	attempt := transitions[0].Attempt
	if attempt.Progress != domain.ProgressWaitingExternal || attempt.Control != domain.ControlWaitingExternal {
		t.Fatalf("the worker un-parked the attempt: %q/%q", attempt.Progress, attempt.Control)
	}
	if attempt.AssignmentID != "assign-1" {
		t.Fatal("the parked attempt lost the assignment the abandonment check reads")
	}
	if attempt.CompletedAt != nil {
		t.Fatal("a parked attempt was given a completion time")
	}
}

func parkedTestBindings() []directoryresource.Binding {
	return []directoryresource.Binding{directoryTestBinding(directoryresource.ReadWrite)}
}

func acceptedCommand(kind domain.WorkerCommandKind) domain.WorkerCommandRecord {
	return domain.WorkerCommandRecord{
		Command:         domain.WorkerCommand{ID: string(kind) + "-1", Kind: kind, AssignmentID: "assign-1", AssignmentEpoch: 2},
		Acknowledgement: &domain.WorkerAcknowledgement{CommandID: string(kind) + "-1", Accepted: true},
	}
}

func rejectedCommand(kind domain.WorkerCommandKind) domain.WorkerCommandRecord {
	return domain.WorkerCommandRecord{
		Command:         domain.WorkerCommand{ID: string(kind) + "-1", Kind: kind, AssignmentID: "assign-1", AssignmentEpoch: 2},
		Acknowledgement: &domain.WorkerAcknowledgement{CommandID: string(kind) + "-1", Accepted: false},
	}
}

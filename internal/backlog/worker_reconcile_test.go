package backlog

import (
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestPlanWorkerStateTransitionsReconcilesObservationsAndAcknowledgements(t *testing.T) {
	now := coordinatorTestTime.Add(10 * time.Minute)
	baseSnapshot := coordinatorSnapshot(2)
	baseSnapshot.ObservedAt = now
	baseSnapshot.ValidUntil = now.Add(time.Hour)
	baseAssignment := domain.Assignment{
		ID: "assignment-1", AttemptID: "attempt-1", WorkerID: baseSnapshot.WorkerID,
		WorkerEpoch: baseSnapshot.WorkerEpoch, State: domain.AssignmentClaimed, Epoch: 1,
		LeaseToken: "lease", DispatchToken: "dispatch", LeaseExpiresAt: now.Add(time.Hour),
		UpdatedAt: coordinatorTestTime,
	}
	baseAttempt := domain.Attempt{
		ID: "attempt-1", AssignmentID: baseAssignment.ID, Progress: domain.ProgressActive,
		Control: domain.ControlPreparing, Revision: 3, UpdatedAt: coordinatorTestTime,
	}
	cases := []struct {
		name             string
		assignment       domain.Assignment
		attempt          domain.Attempt
		snapshot         domain.WorkerSnapshot
		commands         []domain.WorkerCommandRecord
		wantState        domain.AssignmentState
		wantControl      domain.ControlState
		wantProgress     domain.ProgressState
		wantWorkerEpoch  string
		wantAssignmentID string
		wantReason       string
		wantCount        int
	}{
		{
			name: "lost dispatch acknowledgement recovered from present observation",
			assignment: func() domain.Assignment {
				value := baseAssignment
				value.State = domain.AssignmentUnknown
				value.WorkerEpoch = "old-process"
				return value
			}(),
			attempt: baseAttempt,
			snapshot: func() domain.WorkerSnapshot {
				value := baseSnapshot
				value.WorkerEpoch = "new-process"
				value.Assignments = []domain.WorkerAssignmentObservation{{
					AssignmentID: baseAssignment.ID, AssignmentEpoch: 1,
					State: domain.AssignmentClaimed, Control: domain.ControlRunning,
					ThreadID: "thread-1", ObservedAt: now,
				}}
				return value
			}(),
			wantState: domain.AssignmentClaimed, wantControl: domain.ControlRunning,
			wantProgress: domain.ProgressActive, wantWorkerEpoch: "new-process",
			wantAssignmentID: baseAssignment.ID, wantReason: workerStateObservedPresent, wantCount: 1,
		},
		{
			name: "lease expired unknown is released by absent proof",
			assignment: func() domain.Assignment {
				value := baseAssignment
				value.State = domain.AssignmentUnknown
				return value
			}(),
			attempt: baseAttempt, snapshot: baseSnapshot,
			wantState: domain.AssignmentReleased, wantControl: domain.ControlUnassigned,
			wantProgress: domain.ProgressReady, wantWorkerEpoch: baseSnapshot.WorkerEpoch,
			wantReason: workerStateObservedAbsent, wantCount: 1,
		},
		{
			name: "lease expired unknown is re-claimed by a running observation",
			assignment: func() domain.Assignment {
				value := baseAssignment
				value.State = domain.AssignmentUnknown
				value.LeaseExpiresAt = now
				return value
			}(),
			attempt: baseAttempt,
			snapshot: func() domain.WorkerSnapshot {
				value := baseSnapshot
				value.Assignments = []domain.WorkerAssignmentObservation{{
					AssignmentID: baseAssignment.ID, AssignmentEpoch: 1,
					State: domain.AssignmentClaimed, Control: domain.ControlRunning,
					ThreadID: "thread-1", ObservedAt: now,
				}}
				return value
			}(),
			wantState: domain.AssignmentClaimed, wantControl: domain.ControlRunning,
			wantProgress: domain.ProgressActive, wantWorkerEpoch: baseSnapshot.WorkerEpoch,
			wantAssignmentID: baseAssignment.ID, wantReason: workerStateObservedPresent, wantCount: 1,
		},
		{
			name: "worker process epoch change makes omission authoritative",
			assignment: func() domain.Assignment {
				value := baseAssignment
				value.WorkerEpoch = "old-process"
				return value
			}(),
			attempt: baseAttempt,
			snapshot: func() domain.WorkerSnapshot {
				value := baseSnapshot
				value.WorkerEpoch = "new-process"
				return value
			}(),
			wantState: domain.AssignmentReleased, wantControl: domain.ControlUnassigned,
			wantProgress: domain.ProgressReady, wantWorkerEpoch: "old-process",
			wantReason: workerStateObservedAbsent, wantCount: 1,
		},
		{
			name:       "same epoch omission does not release claimed assignment",
			assignment: baseAssignment, attempt: baseAttempt, snapshot: baseSnapshot,
			wantCount: 0,
		},
		{
			name:       "accepted dispatch advances attempt without worker observation",
			assignment: baseAssignment, attempt: baseAttempt, snapshot: baseSnapshot,
			commands: []domain.WorkerCommandRecord{workerCommandRecord(
				baseSnapshot, baseAssignment, domain.WorkerCommandDispatch, true,
			)},
			wantState: domain.AssignmentClaimed, wantControl: domain.ControlRunning,
			wantProgress: domain.ProgressActive, wantWorkerEpoch: baseSnapshot.WorkerEpoch,
			wantAssignmentID: baseAssignment.ID, wantReason: workerStateDispatchAccepted, wantCount: 1,
		},
		{
			name:       "rejected prepare safely releases assignment",
			assignment: baseAssignment, attempt: baseAttempt, snapshot: baseSnapshot,
			commands: []domain.WorkerCommandRecord{workerCommandRecord(
				baseSnapshot, baseAssignment, domain.WorkerCommandPrepare, false,
			)},
			wantState: domain.AssignmentReleased, wantControl: domain.ControlUnassigned,
			wantProgress: domain.ProgressReady, wantWorkerEpoch: baseSnapshot.WorkerEpoch,
			wantReason: workerStateCommandRejected, wantCount: 1,
		},
		{
			name:       "accepted collection completes assignment",
			assignment: baseAssignment, attempt: baseAttempt, snapshot: baseSnapshot,
			commands: []domain.WorkerCommandRecord{workerCommandRecord(
				baseSnapshot, baseAssignment, domain.WorkerCommandCollect, true,
			)},
			wantState: domain.AssignmentCompleted, wantControl: domain.ControlStopped,
			wantProgress: domain.ProgressVerifying, wantWorkerEpoch: baseSnapshot.WorkerEpoch,
			wantAssignmentID: baseAssignment.ID, wantReason: workerStateCollectAccepted, wantCount: 1,
		},
		{
			name:       "completed observation advances collection projection",
			assignment: baseAssignment, attempt: baseAttempt,
			snapshot: func() domain.WorkerSnapshot {
				value := baseSnapshot
				value.Assignments = []domain.WorkerAssignmentObservation{{
					AssignmentID: baseAssignment.ID, AssignmentEpoch: 1,
					State: domain.AssignmentCompleted, ThreadID: "thread-1", ObservedAt: now,
				}}
				return value
			}(),
			wantState: domain.AssignmentClaimed, wantControl: domain.ControlStopped,
			wantProgress: domain.ProgressActive, wantWorkerEpoch: baseSnapshot.WorkerEpoch,
			wantAssignmentID: baseAssignment.ID, wantReason: workerStateObservedCompleted, wantCount: 1,
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			records := sqlite.CoordinatorRecords{
				Assignments: []domain.Assignment{test.assignment},
				Attempts:    []domain.Attempt{test.attempt},
			}
			transitions, err := PlanWorkerStateTransitions(records, test.snapshot, test.commands, now)
			if err != nil {
				t.Fatal(err)
			}
			if len(transitions) != test.wantCount {
				t.Fatalf("transitions = %#v, want %d", transitions, test.wantCount)
			}
			if test.wantCount == 0 {
				return
			}
			transition := transitions[0]
			if transition.Assignment.State != test.wantState ||
				transition.Assignment.WorkerEpoch != test.wantWorkerEpoch ||
				transition.Attempt.Control != test.wantControl ||
				transition.Attempt.Progress != test.wantProgress ||
				transition.Attempt.AssignmentID != test.wantAssignmentID ||
				transition.Reason != test.wantReason ||
				transition.Attempt.Revision != test.attempt.Revision+1 {
				t.Fatalf("transition = %#v", transition)
			}
		})
	}
}

func TestPlanWorkerStateTransitionsFencesNewUnknownObservation(t *testing.T) {
	now := coordinatorTestTime.Add(10 * time.Minute)
	snapshot := coordinatorSnapshot(2)
	snapshot.ObservedAt = now
	snapshot.ValidUntil = now.Add(time.Hour)
	assignment := domain.Assignment{
		ID: "assignment-1", AttemptID: "attempt-1", WorkerID: snapshot.WorkerID,
		WorkerEpoch: snapshot.WorkerEpoch, State: domain.AssignmentClaimed, Epoch: 1,
		LeaseToken: "lease", DispatchToken: "dispatch", LeaseExpiresAt: now.Add(time.Hour),
		UpdatedAt: coordinatorTestTime,
	}
	attempt := domain.Attempt{
		ID: "attempt-1", AssignmentID: assignment.ID, Progress: domain.ProgressActive,
		Control: domain.ControlPreparing, Revision: 3, UpdatedAt: coordinatorTestTime,
	}
	snapshot.Assignments = []domain.WorkerAssignmentObservation{{
		AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch,
		State: domain.AssignmentUnknown, Control: domain.ControlStopped, ObservedAt: now,
	}}
	transitions, err := PlanWorkerStateTransitions(sqlite.CoordinatorRecords{
		Assignments: []domain.Assignment{assignment}, Attempts: []domain.Attempt{attempt},
	}, snapshot, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(transitions) != 1 {
		t.Fatalf("transitions = %#v, want 1", transitions)
	}
	got := transitions[0]
	if got.Assignment.State != domain.AssignmentUnknown ||
		got.Attempt.Control != domain.ControlStopped ||
		got.Attempt.Progress != domain.ProgressActive ||
		got.Attempt.AssignmentID != assignment.ID ||
		got.Reason != workerStateObservedUnknown {
		t.Fatalf("transition = %#v", got)
	}
}

func TestPlanWorkerStateTransitionsKeepsUnknownEvidenceFencedWithoutBlockingOthers(t *testing.T) {
	now := coordinatorTestTime.Add(10 * time.Minute)
	snapshot := coordinatorSnapshot(2)
	snapshot.ObservedAt = now
	snapshot.ValidUntil = now.Add(time.Hour)
	unknown := domain.Assignment{
		ID: "assignment-1", AttemptID: "attempt-1", WorkerID: snapshot.WorkerID,
		WorkerEpoch: snapshot.WorkerEpoch, State: domain.AssignmentUnknown, Epoch: 1,
		UpdatedAt: coordinatorTestTime,
	}
	active := domain.Assignment{
		ID: "assignment-2", AttemptID: "attempt-2", WorkerID: snapshot.WorkerID,
		WorkerEpoch: snapshot.WorkerEpoch, State: domain.AssignmentClaimed, Epoch: 1,
		LeaseToken: "lease", DispatchToken: "dispatch", LeaseExpiresAt: now.Add(time.Hour),
		UpdatedAt: coordinatorTestTime,
	}
	snapshot.Assignments = []domain.WorkerAssignmentObservation{
		{AssignmentID: unknown.ID, AssignmentEpoch: unknown.Epoch, State: domain.AssignmentUnknown, ObservedAt: now},
		{AssignmentID: active.ID, AssignmentEpoch: active.Epoch, State: domain.AssignmentCompleted, ThreadID: "thread-2", ObservedAt: now},
	}
	transitions, err := PlanWorkerStateTransitions(sqlite.CoordinatorRecords{
		Assignments: []domain.Assignment{unknown, active},
		Attempts: []domain.Attempt{
			{ID: "attempt-1", AssignmentID: unknown.ID, Progress: domain.ProgressActive, Control: domain.ControlStopped, Revision: 2, UpdatedAt: coordinatorTestTime},
			{ID: "attempt-2", AssignmentID: active.ID, Progress: domain.ProgressActive, Control: domain.ControlRunning, Revision: 3, UpdatedAt: coordinatorTestTime},
		},
	}, snapshot, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(transitions) != 1 || transitions[0].Assignment.ID != active.ID {
		t.Fatalf("transitions = %#v, want only assignment-2", transitions)
	}
}

func TestPlanWorkerStateTransitionsNeverRevivesFinishedAttempt(t *testing.T) {
	now := coordinatorTestTime.Add(10 * time.Minute)
	completedAt := coordinatorTestTime.Add(5 * time.Minute)
	snapshot := coordinatorSnapshot(2)
	snapshot.ObservedAt = now
	snapshot.ValidUntil = now.Add(time.Hour)
	assignment := domain.Assignment{
		ID: "assignment-1", AttemptID: "attempt-1", WorkerID: snapshot.WorkerID,
		WorkerEpoch: snapshot.WorkerEpoch, State: domain.AssignmentClaimed, Epoch: 1,
		LeaseToken: "lease", DispatchToken: "dispatch", LeaseExpiresAt: now.Add(time.Hour),
		UpdatedAt: coordinatorTestTime,
	}

	t.Run("terminal progress wins over a running worker observation", func(t *testing.T) {
		attempt := domain.Attempt{
			ID: "attempt-1", AssignmentID: assignment.ID, Progress: domain.ProgressCancelled,
			Control: domain.ControlStopped, Revision: 3, UpdatedAt: coordinatorTestTime,
			CompletedAt: &completedAt,
		}
		observed := snapshot
		observed.Assignments = []domain.WorkerAssignmentObservation{{
			AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch,
			State: domain.AssignmentClaimed, Control: domain.ControlRunning,
			ThreadID: "thread-1", ObservedAt: now,
		}}
		transitions, err := PlanWorkerStateTransitions(sqlite.CoordinatorRecords{
			Assignments: []domain.Assignment{assignment}, Attempts: []domain.Attempt{attempt},
		}, observed, nil, now)
		if err != nil {
			t.Fatal(err)
		}
		if len(transitions) != 1 {
			t.Fatalf("transitions = %#v, want 1", transitions)
		}
		got := transitions[0]
		if got.Attempt.Progress != domain.ProgressCancelled ||
			got.Attempt.Control != domain.ControlStopped ||
			got.Reason != "terminal-attempt-stop-required" {
			t.Fatalf("transition revived terminal attempt: %#v", got)
		}
	})

	t.Run("completion marker repairs an active projection before dispatch acknowledgement", func(t *testing.T) {
		attempt := domain.Attempt{
			ID: "attempt-1", AssignmentID: assignment.ID, Progress: domain.ProgressActive,
			Control: domain.ControlRunning, Revision: 3, UpdatedAt: coordinatorTestTime,
			CompletedAt: &completedAt,
		}
		dispatch := workerCommandRecord(snapshot, assignment, domain.WorkerCommandDispatch, true)
		transitions, err := PlanWorkerStateTransitions(sqlite.CoordinatorRecords{
			Assignments: []domain.Assignment{assignment}, Attempts: []domain.Attempt{attempt},
		}, snapshot, []domain.WorkerCommandRecord{dispatch}, now)
		if err != nil {
			t.Fatal(err)
		}
		if len(transitions) != 1 {
			t.Fatalf("transitions = %#v, want 1", transitions)
		}
		got := transitions[0]
		if got.Attempt.Progress != domain.ProgressActive ||
			got.Attempt.Control != domain.ControlStopped ||
			got.Attempt.CompletedAt == nil ||
			got.Reason != "terminal-attempt-stop-required" {
			t.Fatalf("completion marker did not fence dispatch: %#v", got)
		}
	})
}

func TestPlanWorkerStateTransitionsRejectsStaleAndMalformedSnapshots(t *testing.T) {
	assignment := domain.Assignment{
		ID: "assignment-1", AttemptID: "attempt-1", WorkerID: "worker-a",
		WorkerEpoch: "worker-epoch-1", State: domain.AssignmentUnknown, Epoch: 1,
	}
	attempt := domain.Attempt{ID: "attempt-1", AssignmentID: assignment.ID, Revision: 1}
	records := sqlite.CoordinatorRecords{
		Assignments: []domain.Assignment{assignment}, Attempts: []domain.Attempt{attempt},
	}
	stale := coordinatorSnapshot(2)
	stale.ValidUntil = coordinatorTestTime
	if _, err := PlanWorkerStateTransitions(records, stale, nil, coordinatorTestTime); err == nil {
		t.Fatal("stale snapshot was accepted")
	}
	malformed := coordinatorSnapshot(2)
	malformed.Assignments = []domain.WorkerAssignmentObservation{{
		AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch,
		State: domain.AssignmentClaimed, ObservedAt: malformed.ObservedAt.Add(time.Minute),
	}}
	if _, err := PlanWorkerStateTransitions(records, malformed, nil, coordinatorTestTime); err == nil {
		t.Fatal("future assignment observation was accepted")
	}
}

func workerCommandRecord(
	snapshot domain.WorkerSnapshot,
	assignment domain.Assignment,
	kind domain.WorkerCommandKind,
	accepted bool,
) domain.WorkerCommandRecord {
	command := domain.WorkerCommand{
		ID:   stableCoordinatorID("command", workerCommandKey(assignment.ID, assignment.Epoch, kind)),
		Kind: kind, WorkerID: assignment.WorkerID, WorkerEpoch: assignment.WorkerEpoch,
		CoordinatorEpoch: snapshot.CoordinatorEpoch, AssignmentID: assignment.ID,
		AssignmentEpoch: assignment.Epoch, ExpectedWorkerSequence: 1,
		CreatedAt: coordinatorTestTime,
	}
	acknowledgement := domain.WorkerAcknowledgement{
		CommandID: command.ID, WorkerID: command.WorkerID, WorkerEpoch: command.WorkerEpoch,
		CoordinatorEpoch: command.CoordinatorEpoch, AssignmentID: command.AssignmentID,
		AssignmentEpoch: command.AssignmentEpoch, WorkerSequence: snapshot.Sequence,
		Accepted: accepted, AcknowledgedAt: snapshot.ObservedAt,
	}
	return domain.WorkerCommandRecord{Command: command, Acknowledgement: &acknowledgement}
}

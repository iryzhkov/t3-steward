package sqlite

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Every protected struct field is changed independently. This is a bounded
// handoff predicate audit, not a generic JSON corruption or schema audit.
func TestRuntimeTerminalFencesCurrentParkReleaseProof(t *testing.T) {
	for _, shape := range []string{"progress-only", "control-only", "both"} {
		for _, state := range []domain.AssignmentState{domain.AssignmentClaimed, domain.AssignmentUnknown} {
			t.Run(shape+"/"+string(state), func(t *testing.T) {
				ctx := context.Background()
				s := openFleetTestStore(t)
				defer func() { s.Close() }()
				claimFleetAssignment(t, s)
				records, err := s.LoadCoordinatorRecords(ctx)
				if err != nil {
					t.Fatal(err)
				}
				current := records.Attempts[0]
				custody := records.Assignments[0]
				current.Progress = domain.ProgressActive
				current.Control = domain.ControlRunning
				if shape != "control-only" {
					current.Progress = domain.ProgressWaitingExternal
				}
				if shape != "progress-only" {
					current.Control = domain.ControlWaitingExternal
				}
				current.ThreadID = "park-thread"
				custody.ThreadID = current.ThreadID
				custody.State = state
				if err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{current}, Assignments: []domain.Assignment{custody}}); err != nil {
					t.Fatal(err)
				}
				now := fleetTestTime.Add(3 * time.Second)
				next := current
				next.Revision++
				next.UpdatedAt = now
				released := custody
				released.State = domain.AssignmentReleased
				released.LeaseExpiresAt = time.Time{}
				released.UpdatedAt = now
				valid := domain.WorkerStateTransition{CoordinatorEpoch: 1, WorkerID: custody.WorkerID, WorkerEpoch: custody.WorkerEpoch, WorkerSequence: 1, TransitionedAt: now, ExpectedAssignment: custody, ExpectedAttemptRevision: current.Revision, Assignment: released, Attempt: next, Reason: "observed-released"}
				live := current
				live.ID = "live"
				live.TaskID = "live-task"
				live.AssignmentID = "live-assignment"
				live.Progress = domain.ProgressActive
				live.Control = domain.ControlPreparing
				la := custody
				la.ID = live.AssignmentID
				la.AttemptID = live.ID
				la.State = domain.AssignmentClaimed
				la.LeaseToken = "live-lease"
				la.DispatchToken = "live-token"
				if err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{live}, Assignments: []domain.Assignment{la}}); err != nil {
					t.Fatal(err)
				}
				first := valid
				first.ExpectedAssignment = la
				first.Assignment = la
				first.Attempt = live
				first.Attempt.Control = domain.ControlRunning
				first.Attempt.Revision++
				first.Attempt.UpdatedAt = now
				refuse := func(t *testing.T, forged domain.WorkerStateTransition) {
					t.Helper()
					before := cancellationSnapshot(t, s)
					if _, err = s.CommitWorkerStateTransitions(ctx, []domain.WorkerStateTransition{first, forged}); !errors.Is(err, ErrStaleWorkerStateTransition) {
						t.Fatalf("forged CURRENT handoff accepted: %v", err)
					}
					if before != cancellationSnapshot(t, s) {
						t.Fatal("earlier live sibling/tables/audits did not roll back")
					}
					s = reopenParkProof(t, s)
					if before != cancellationSnapshot(t, s) {
						t.Fatal("rollback lost after reopen")
					}
				}
				for _, part := range []string{"attempt", "assignment"} {
					var typ reflect.Type
					if part == "attempt" {
						typ = reflect.TypeOf(next)
					} else {
						typ = reflect.TypeOf(released)
					}
					for i := 0; i < typ.NumField(); i++ {
						field := typ.Field(i).Name
						if field == "UpdatedAt" || part == "attempt" && field == "Revision" || part == "assignment" && (field == "State" || field == "LeaseExpiresAt") {
							continue
						}
						t.Run(part+"/"+field, func(t *testing.T) {
							forged := valid
							var value reflect.Value
							if part == "attempt" {
								value = reflect.ValueOf(&forged.Attempt).Elem().Field(i)
							} else {
								value = reflect.ValueOf(&forged.Assignment).Elem().Field(i)
							}
							mutateParkProofField(value)
							refuse(t, forged)
						})
					}
				}
				for _, mutation := range []string{"live-current", "terminal-current", "completed-current", "cleared-current-binding", "foreign-current-binding", "current-revision", "different-current-assignment", "nonzero-release-lease", "attempt-time", "assignment-time"} {
					t.Run(mutation, func(t *testing.T) {
						changed := current
						changedCustody := custody
						forged := valid
						switch mutation {
						case "live-current":
							changed.Progress = domain.ProgressActive
							changed.Control = domain.ControlRunning
						case "terminal-current":
							changed.Progress = domain.ProgressFailed
						case "completed-current":
							changed.CompletedAt = &now
						case "cleared-current-binding":
							changed.AssignmentID = ""
						case "foreign-current-binding":
							changed.AssignmentID = "foreign"
						case "current-revision":
							changed.Revision++
						case "different-current-assignment":
							changedCustody.ThreadID = "different-current"
						case "nonzero-release-lease":
							forged.Assignment.LeaseExpiresAt = now
						case "attempt-time":
							forged.Attempt.UpdatedAt = now.Add(time.Hour)
						case "assignment-time":
							forged.Assignment.UpdatedAt = now.Add(time.Hour)
						}
						if err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{changed}, Assignments: []domain.Assignment{changedCustody}}); err != nil {
							t.Fatal(err)
						}
						refuse(t, forged)
						if err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{current}, Assignments: []domain.Assignment{custody}}); err != nil {
							t.Fatal(err)
						}
					})
				}
				if _, err = s.CommitWorkerStateTransitions(ctx, []domain.WorkerStateTransition{first, valid}); err != nil {
					t.Fatal(err)
				}
				before := cancellationSnapshot(t, s)
				s = reopenParkProof(t, s)
				if _, err = s.CommitWorkerStateTransitions(ctx, []domain.WorkerStateTransition{first, valid}); err != nil || before != cancellationSnapshot(t, s) {
					t.Fatalf("native audited exact replay: %v", err)
				}
				if _, err = s.db.ExecContext(ctx, "DELETE FROM coordinator_audit_events WHERE id = ?", workerStateAuditID(valid)); err != nil {
					t.Fatal(err)
				}
				before = cancellationSnapshot(t, s)
				if _, err = s.CommitWorkerStateTransitions(ctx, []domain.WorkerStateTransition{valid}); err == nil || before != cancellationSnapshot(t, s) {
					t.Fatalf("missing audit allowed replay: %v", err)
				}
			})
		}
	}
}

// Parent owns the reopened store: child-subtest cleanup must not close it.
func reopenParkProof(t *testing.T, s *Store) *Store {
	t.Helper()
	path := s.path
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	return reopened
}

func mutateParkProofField(value reflect.Value) {
	if !value.IsZero() {
		value.Set(reflect.Zero(value.Type()))
		return
	}
	switch value.Kind() {
	case reflect.String:
		value.SetString("forged")
	case reflect.Bool:
		value.SetBool(true)
	case reflect.Int, reflect.Int64:
		value.SetInt(1)
	case reflect.Struct:
		if value.Type() == reflect.TypeOf(time.Time{}) {
			value.Set(reflect.ValueOf(fleetTestTime))
			return
		}
		for i := 0; i < value.NumField(); i++ {
			if value.Field(i).CanSet() {
				mutateParkProofField(value.Field(i))
				return
			}
		}
	case reflect.Pointer:
		value.Set(reflect.New(value.Type().Elem()))
	default:
		panic("uncovered park proof field " + value.Type().String())
	}
}

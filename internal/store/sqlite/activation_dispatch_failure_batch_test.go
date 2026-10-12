package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func seedBatchDispatchFailure(t *testing.T, store *Store, runID, id string) domain.ActivationDispatchFailure {
	t.Helper()
	ctx := context.Background()
	failure := domain.ActivationDispatchFailure{ID: id, RunID: runID, AssignmentID: id, AssignmentEpoch: 1, ActivationID: id, ActivationEpoch: 1, GraphRevision: 1, AttemptID: id}
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{{ID: runID, GraphRevision: 1}},
		Assignments:  []domain.Assignment{{ID: id, AttemptID: id, DispatchToken: id, LeaseToken: id, State: domain.AssignmentOffered, Epoch: 1}},
		Attempts:     []domain.Attempt{{ID: id, WorkflowRunID: runID, TaskID: id, Number: 1, SupervisionActivationID: id, SupervisionActivationEpoch: 1}},
	}); err != nil {
		t.Fatal(err)
	}
	activation, _ := json.Marshal(domain.Activation{ID: id, RunID: runID, Epoch: 1, State: domain.ActivationActive})
	raw, _ := json.Marshal(failure)
	for _, insert := range []struct {
		query string
		args  []any
	}{
		{"INSERT INTO coordinator_supervision_activations(id, run_id, epoch, state, record) VALUES (?, ?, 1, ?, ?)", []any{id, runID, string(domain.ActivationActive), activation}},
		{"INSERT INTO coordinator_activation_dispatch_failures(id, assignment_id, assignment_epoch, run_id, activation_id, record) VALUES (?, ?, 1, ?, ?, ?)", []any{id, id, runID, id, raw}},
	} {
		if _, err := store.db.ExecContext(ctx, insert.query, insert.args...); err != nil {
			t.Fatal(err)
		}
	}
	return failure
}

func TestAllCurrentActivationDispatchFailures(t *testing.T) {
	for _, tc := range []struct {
		name, table, update string
		want                int
		wantError           bool
	}{
		{name: "current", want: 2},
		{"pending", "supervision_activations", "state = 'pending-dispatch'", 2, false},
		{"stale-graph", "workflow_runs", "record = json_set(record, '$.graphRevision', 2)", 0, false},
		{"assignment-epoch", "assignments", "assignment_epoch = 2", 1, false},
		{"attempt-identity", "attempts", "record = json_set(record, '$.supervisionActivationId', 'other')", 1, false},
		{"attempt-epoch", "attempts", "record = json_set(record, '$.supervisionActivationEpoch', 2)", 1, false},
		{"activation-epoch", "supervision_activations", "record = json_set(record, '$.epoch', 2)", 1, false},
		{"non-offered", "assignments", "record = json_set(record, '$.state', 'claimed')", 1, false},
		{"terminal", "supervision_activations", "state = 'closed'", 1, false},
		{"bad-run", "workflow_runs", `record = '"not-an-object"'`, 0, false},
		{"bad-assignment", "assignments", "record = '{'", 1, false},
		{"bad-attempt", "attempts", "record = '{'", 1, false},
		{"bad-activation", "supervision_activations", "record = '{'", 1, false},
		{"bad-failure", "activation_dispatch_failures", "record = '{'", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
			ctx := context.Background()
			z := seedBatchDispatchFailure(t, store, "run-b", "z")
			a := seedBatchDispatchFailure(t, store, "run-b", "a")
			c := seedBatchDispatchFailure(t, store, "run-a", "c")
			if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{{ID: "empty"}}}); err != nil {
				t.Fatal(err)
			}
			if tc.table != "" {
				id := "z"
				if tc.table == "workflow_runs" {
					id = "run-b"
				}
				if _, err := store.db.ExecContext(ctx, "UPDATE coordinator_"+tc.table+" SET "+tc.update+" WHERE id = ?", id); err != nil {
					t.Fatal(err)
				}
			}
			batch, err := store.ListAllCurrentActivationDispatchFailures(ctx)
			scoped, scopedErr := store.ListCurrentActivationDispatchFailures(ctx, "run-b")
			unaffected, otherErr := store.ListCurrentActivationDispatchFailures(ctx, "run-a")
			empty, emptyErr := store.ListCurrentActivationDispatchFailures(ctx, "empty")
			if otherErr != nil || emptyErr != nil || len(empty) != 0 || !reflect.DeepEqual(unaffected, []domain.ActivationDispatchFailure{c}) {
				t.Fatalf("other run = %v, %v", unaffected, otherErr)
			}
			if tc.wantError {
				if err == nil || batch != nil || scopedErr == nil {
					t.Fatalf("malformed failure: batch=%v err=%v scoped=%v", batch, err, scopedErr)
				}
				return
			}
			if err != nil || scopedErr != nil || len(batch["run-b"]) != tc.want || !reflect.DeepEqual(batch["run-b"], scoped) || !reflect.DeepEqual(batch["run-a"], unaffected) || len(batch["empty"]) != 0 {
				t.Fatalf("batch=%v scoped=%v errors=%v/%v", batch, scoped, err, scopedErr)
			}
			if tc.want == 2 && !reflect.DeepEqual(scoped, []domain.ActivationDispatchFailure{a, z}) {
				t.Fatalf("failure.id order = %v", scoped)
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if partial, err := store.ListAllCurrentActivationDispatchFailures(canceled); !errors.Is(err, context.Canceled) || partial != nil {
				t.Fatalf("canceled batch = %v, %v", partial, err)
			}
		})
	}
}

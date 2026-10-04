package sqlite

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestReviewChildArtifactAttemptBoundaries(t *testing.T) {
	for _, kind := range []string{"initial to other initial", "initial to other retry", "retry to other initial", "retry to other retry", "parent attempt", "foreign attempt", "dangling attempt", "retry-only foreign", "retry-only indexed", "retry-only JSON", "later-only foreign", "indexed attempt", "JSON attempt", "indexed id", "JSON id", "indexed run", "JSON run", "indexed task", "JSON task", "indexed hash", "JSON hash"} {
		t.Run(kind, func(t *testing.T) {
			s, f, cp, p, _ := childFixture(t)
			ctx := context.Background()
			r, err := s.MaterializeReviewChild(ctx, f, cp, p)
			if err != nil {
				t.Fatal(err)
			}
			retry := r.Graph.Attempts[0]
			retry.ID = "boundary-retry"
			retry.Number = 2
			later := retry
			later.ID = "boundary-later"
			later.Number = 3
			other := r.Graph.Attempts[1]
			other.ID = "other-retry"
			other.Number = 2
			foreign := retry
			foreign.ID = "foreign-attempt"
			foreign.WorkflowRunID = "foreign-run"
			foreign.TaskID = "foreign-task"
			if err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{retry, later, other, foreign}}); err != nil {
				t.Fatal(err)
			}
			a := domain.Artifact{ID: "boundary-output", WorkflowRunID: cp.RoundID, TaskID: retry.TaskID, AttemptID: retry.ID, Kind: domain.ArtifactOutput, Name: "review.md", SHA256: childDigest([]byte("output"))}
			mutation := ""
			crossMember := ""
			switch kind {
			case "initial to other initial", "retry to other initial":
				crossMember = r.Graph.Attempts[1].ID
			case "initial to other retry", "retry to other retry":
				crossMember = other.ID
			case "parent attempt":
				a.AttemptID = f.Parent.AttemptID
			case "foreign attempt":
				a.AttemptID = foreign.ID
			case "dangling attempt":
				a.AttemptID = "absent-attempt"
			case "retry-only foreign", "retry-only indexed", "retry-only JSON", "later-only foreign":
				a.WorkflowRunID = "foreign-run"
				a.TaskID = "foreign-task"
				if kind == "later-only foreign" {
					a.AttemptID = later.ID
				}
				if kind == "retry-only indexed" {
					a.AttemptID = foreign.ID
					mutation = "attempt_id='boundary-retry'"
				}
				if kind == "retry-only JSON" {
					a.AttemptID = foreign.ID
					mutation = "record=json_set(record,'$.attemptId','boundary-retry')"
				}
			case "indexed attempt":
				mutation = "attempt_id='absent-attempt'"
			case "JSON attempt":
				mutation = "record=json_set(record,'$.attemptId','absent-attempt')"
			case "indexed id":
				mutation = "id='changed-output'"
			case "JSON id":
				mutation = "record=json_set(record,'$.id','changed-output')"
			case "indexed run":
				mutation = "workflow_run_id='foreign-run'"
			case "JSON run":
				mutation = "record=json_set(record,'$.workflowRunId','foreign-run')"
			case "indexed task":
				mutation = "task_id='foreign-task'"
			case "JSON task":
				mutation = "record=json_set(record,'$.taskId','foreign-task')"
			case "indexed hash":
				mutation = "sha256='changed-hash'"
			case "JSON hash":
				mutation = "record=json_set(record,'$.sha256','changed-hash')"
			}
			// Retain a coherent output at the opposite attempt generation, too.
			coherent := domain.Artifact{ID: "coherent-output", WorkflowRunID: cp.RoundID, TaskID: retry.TaskID, AttemptID: r.Graph.Attempts[0].ID, Kind: domain.ArtifactOutput, Name: "review.md"}
			if kind == "initial to other initial" || kind == "initial to other retry" {
				a.ID = "initial-output"
				a.AttemptID = r.Graph.Attempts[0].ID
				coherent.AttemptID = later.ID
			}
			if err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Artifacts: []domain.Artifact{a, coherent}}); err != nil {
				t.Fatal(err)
			}
			if crossMember != "" {
				if _, err = s.MaterializeReviewChild(ctx, f, cp, p); err != nil {
					t.Fatalf("coherent initial/later fixture: %v", err)
				}
				if _, err = s.db.Exec("UPDATE coordinator_artifacts SET attempt_id=?,record=json_set(record,'$.attemptId',?) WHERE id=?", crossMember, crossMember, a.ID); err != nil {
					t.Fatal(err)
				}
			}
			if mutation != "" {
				if _, err = s.db.Exec("UPDATE coordinator_artifacts SET "+mutation+" WHERE id=?", a.ID); err != nil {
					t.Fatal(err)
				}
			}
			records, err := s.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			visible := false
			for _, loaded := range records.Artifacts {
				if loaded.ID == a.ID || (kind == "indexed id" || kind == "JSON id") && loaded.ID == "changed-output" {
					visible = true
				}
			}
			if !visible {
				t.Fatal("corrupt artifact not runtime-visible")
			}
			before := childOwnershipSnapshot(t, s)
			if _, err = s.MaterializeReviewChild(ctx, f, cp, p); err == nil {
				t.Fatal("accepted incoherent artifact")
			}
			if before != childOwnershipSnapshot(t, s) {
				t.Fatal("refusal changed indexed/JSON state, round or marker")
			}
		})
	}
}

func TestReviewChildArtifactCoherentControls(t *testing.T) {
	s, f, cp, p, _ := childFixture(t)
	ctx := context.Background()
	first, err := s.MaterializeReviewChild(ctx, f, cp, p)
	if err != nil {
		t.Fatal(err)
	}
	var attempts []domain.Attempt
	var outputs []domain.Artifact
	for i, initial := range first.Graph.Attempts {
		outputs = append(outputs, domain.Artifact{ID: initial.ID + "-output", WorkflowRunID: cp.RoundID, TaskID: initial.TaskID, AttemptID: initial.ID, Kind: domain.ArtifactOutput, Name: "review.md"})
		for _, number := range []int{2, 3} {
			a := initial
			a.ID = fmt.Sprintf("control-%d-%d", i, number)
			a.Number = number
			a.Revision = 3
			a.Progress = domain.ProgressSucceeded
			a.Control = domain.ControlStopped
			attempts = append(attempts, a)
			outputs = append(outputs, domain.Artifact{ID: a.ID + "-output", WorkflowRunID: cp.RoundID, TaskID: a.TaskID, AttemptID: a.ID, Kind: domain.ArtifactOutput, Name: "review.md"})
		}
	}
	foreign := attempts[0]
	foreign.ID = "control-foreign"
	foreign.WorkflowRunID = "foreign-run"
	foreign.TaskID = "foreign-task"
	attempts = append(attempts, foreign)
	outputs = append(outputs, domain.Artifact{ID: "control-unrelated", WorkflowRunID: foreign.WorkflowRunID, TaskID: foreign.TaskID, AttemptID: foreign.ID, Kind: domain.ArtifactOutput, Name: "review.md"})
	if err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: attempts, Artifacts: outputs}); err != nil {
		t.Fatal(err)
	}
	records, err := s.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	emptyInput := false
	for _, a := range records.Artifacts {
		if a.WorkflowRunID == cp.RoundID && a.Kind == domain.ArtifactInput && a.TaskID == "" && a.AttemptID == "" {
			emptyInput = true
		}
	}
	if !emptyInput {
		t.Fatal("missing empty run-level input control")
	}
	before := childOwnershipSnapshot(t, s)
	got, err := s.MaterializeReviewChild(ctx, f, cp, p)
	if err != nil || !reflect.DeepEqual(first, got) || before != childOwnershipSnapshot(t, s) {
		t.Fatalf("coherent output, empty input or unrelated artifact refused/reset: %v", err)
	}
}

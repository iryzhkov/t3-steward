package sqlite

import (
	"context"
	"reflect"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestIndependentChildArtifactAttemptCoherence(t *testing.T) {
	for _, kind := range []string{"foreign-attempt", "other-member", "retry-claim-only"} {
		t.Run(kind, func(t *testing.T) {
			s, f, cp, p, _ := childFixture(t)
			ctx := context.Background()
			receipt, err := s.MaterializeReviewChild(ctx, f, cp, p)
			if err != nil {
				t.Fatal(err)
			}
			retry := receipt.Graph.Attempts[0]
			retry.ID = "coherent-child-retry"
			retry.Number = 2
			if err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{retry}}); err != nil {
				t.Fatal(err)
			}
			artifact := domain.Artifact{ID: "incoherent-output", WorkflowRunID: cp.RoundID, TaskID: retry.TaskID, AttemptID: retry.ID, Kind: domain.ArtifactOutput, Name: "review.md"}
			switch kind {
			case "foreign-attempt":
				artifact.AttemptID = f.Parent.AttemptID
			case "other-member":
				artifact.AttemptID = receipt.Graph.Attempts[1].ID
			case "retry-claim-only":
				artifact.WorkflowRunID = "foreign-run"
				artifact.TaskID = "foreign-task"
			}
			if err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Artifacts: []domain.Artifact{artifact}}); err != nil {
				t.Fatal(err)
			}
			records, err := s.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			seen := false
			for _, a := range records.Artifacts {
				if reflect.DeepEqual(a, artifact) {
					seen = true
				}
			}
			if !seen {
				t.Fatal("fixture not runtime-visible")
			}
			before := childOwnershipSnapshot(t, s)
			_, err = s.MaterializeReviewChild(ctx, f, cp, p)
			if err == nil {
				t.Error("accepted cross-owned artifact")
			}
			if before != childOwnershipSnapshot(t, s) {
				t.Error("replay mutated state")
			}
		})
	}
}

func TestIndependentChildArtifactReadOnlyCorruption(t *testing.T) {
	s, f, cp, p, _ := childFixture(t)
	ctx := context.Background()
	receipt, err := s.MaterializeReviewChild(ctx, f, cp, p)
	if err != nil {
		t.Fatal(err)
	}
	retry := receipt.Graph.Attempts[0]
	retry.ID = "readonly-retry"
	retry.Number = 2
	if err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{retry}}); err != nil {
		t.Fatal(err)
	}
	a := domain.Artifact{ID: "readonly-output", WorkflowRunID: cp.RoundID, TaskID: retry.TaskID, AttemptID: retry.ID, Kind: domain.ArtifactOutput, Name: "review.md"}
	if err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Artifacts: []domain.Artifact{a}}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.MaterializeReviewChild(ctx, f, cp, p); err != nil {
		t.Fatalf("valid retry output: %v", err)
	}
	// Same run/task but another member's attempt is not a coherent output.
	if _, err = s.db.Exec("UPDATE coordinator_artifacts SET attempt_id=?,record=json_set(record,'$.attemptId',?) WHERE id=?", receipt.Graph.Attempts[1].ID, receipt.Graph.Attempts[1].ID, a.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec("UPDATE coordinator_attempts SET record=json_set(record,'$.progress','succeeded','$.control','stopped') WHERE id=?", f.Parent.AttemptID); err != nil {
		t.Fatal(err)
	}
	path := s.path
	s.Close()
	ro, err := OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	before := childOwnershipSnapshot(t, ro)
	if _, err = ro.MaterializeReviewChild(ctx, f, cp, p); err == nil {
		t.Error("accepted cross-member artifact after parent exit on read-only store")
	}
	if before != childOwnershipSnapshot(t, ro) {
		t.Error("read-only replay mutated state")
	}
}

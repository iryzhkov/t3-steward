package sqlite

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/pinnedinput"
	"github.com/iryzhkov/t3-steward/internal/review"
)

func reviewTickChanges(t *testing.T, s *Store) int64 {
	t.Helper()
	var n int64
	if err := s.db.QueryRow("SELECT total_changes()").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestReviewAuthorityWriterFenceWithoutChanges(t *testing.T) {
	s, f := reviewAuthorityFixture(t)
	other, err := Open(s.path)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, err := other.db.Exec("PRAGMA busy_timeout=20"); err != nil {
		t.Fatal(err)
	}
	before := reviewTickChanges(t, s)
	tx, err := s.reviewAuthorityWriteTx(context.Background(), f.Parent)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = other.db.ExecContext(ctx, "UPDATE coordinator_workflow_runs SET revision=revision WHERE id=?", f.Parent.RunID)
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "busy") && !strings.Contains(strings.ToLower(err.Error()), "locked") {
		t.Fatalf("second writer not fenced by busy error: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if got := reviewTickChanges(t, s); got != before {
		t.Fatalf("writer fence changed rows: %d -> %d", before, got)
	}
	if _, err := other.db.Exec("UPDATE coordinator_workflow_runs SET revision=revision WHERE id=?", f.Parent.RunID); err != nil {
		t.Fatal(err)
	}
}

// Create independent live parents so the fixture does not exceed round limits.
func reviewTickChild(t *testing.T, s *Store, id string) (review.FrozenAuthority, ReviewMaterialization) {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC()
	parent := domain.Attempt{ID: id + "-parent", WorkflowRunID: id + "-run", TaskID: id + "-task", Number: 1, Revision: 1, Progress: domain.ProgressActive, Control: domain.ControlRunning, AssignmentID: id + "-assignment", ThreadID: id + "-thread", UpdatedAt: now}
	assignment := domain.Assignment{ID: parent.AssignmentID, AttemptID: parent.ID, WorkerID: id + "-worker", WorkerEpoch: "session", Epoch: 1, State: domain.AssignmentClaimed, Route: domain.ProviderRoute{ProviderInstanceID: "codex", Model: "sol"}, ThreadID: parent.ThreadID, Project: "repo", LeaseToken: id + "-lease", DispatchToken: id + "-token"}
	task := domain.Task{ID: parent.TaskID, WorkflowID: id + "-workflow", Name: "parent", Class: domain.TaskClassRequired, Routes: []domain.ProviderRoute{assignment.Route}}
	run, err := domain.BindRunSink(domain.WorkflowRun{ID: parent.WorkflowRunID, WorkflowID: task.WorkflowID, Revision: 1, Progress: domain.ProgressActive, CreatedAt: now, UpdatedAt: now}, []domain.Task{task})
	if err != nil {
		t.Fatal(err)
	}
	workflow := domain.Workflow{ID: task.WorkflowID, Name: "parent", Version: 1, Project: "repo", Environment: domain.ExecutionEnvironment{Type: "repository", Scope: "repo"}, Class: domain.TaskClassRequired, TaskIDs: []string{task.ID}, CreatedAt: now}
	cancellationSave(t, s, CoordinatorRecords{Workflows: []domain.Workflow{workflow}, WorkflowRuns: []domain.WorkflowRun{run}, Tasks: []domain.Task{task}, Attempts: []domain.Attempt{parent}, Assignments: []domain.Assignment{assignment}})
	raw := []byte("tick review criteria")
	req, err := review.NewRequirements(review.RequirementsSpec{Risk: "routine", CriteriaDigest: childDigest(raw), PolicyDigest: strings.Repeat("a", 64), RequiredReviewers: 2, MinProviderFamilies: 2, Members: []review.MemberRequirement{
		{ID: "one", Role: "independent", Route: "codex/sol", ProviderFamily: "openai", Tier: "executor", Required: true},
		{ID: "two", Role: "independent", Route: "other/model", ProviderFamily: "other", Tier: "executor", Required: true},
	}})
	if err != nil {
		t.Fatal(err)
	}
	f, err := review.NewFrozenAuthority(review.ParentBinding{RunID: parent.WorkflowRunID, TaskID: parent.TaskID, AttemptID: parent.ID, ThreadID: parent.ThreadID, AssignmentID: assignment.ID, AssignmentEpoch: 1, IssuedRevision: 1, Repository: "repo", BaseCommit: strings.Repeat("b", 40), ExecutorRoute: "codex/sol"}, req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.FreezeReviewAuthority(ctx, f); err != nil {
		t.Fatal(err)
	}
	manifest, err := pinnedinput.NewManifest([]pinnedinput.Entry{{Name: "inputs/criteria.md", Size: int64(len(raw)), SHA256: childDigest(raw)}})
	if err != nil {
		t.Fatal(err)
	}
	cp, err := s.AllocateReviewCheckpoint(ctx, f, review.Checkpoint{ID: "tick", HeadCommit: strings.Repeat("c", 40), InputDigest: manifest.Digest})
	if err != nil {
		t.Fatal(err)
	}
	a := domain.Artifact{ID: review.ChildInputID(cp, "inputs/criteria.md"), WorkflowRunID: cp.RoundID, Kind: domain.ArtifactInput, Name: "inputs/criteria.md", Size: int64(len(raw)), SHA256: childDigest(raw), StoragePath: id + "/criteria.md", Producer: "submission", CreatedAt: now}
	files := []review.RetainedFile{{Artifact: a, Bytes: raw}}
	for _, m := range f.Requirements.Members {
		prompt, err := review.ChildPrompt(f, cp, m, manifest, a.Name)
		if err != nil {
			t.Fatal(err)
		}
		data := []byte(prompt)
		artifact := domain.Artifact{ID: review.ChildPromptID(cp, m.ID), WorkflowRunID: cp.RoundID, TaskID: cp.MemberTaskID(m.ID), Kind: domain.ArtifactInput, Name: review.ChildPromptName(m.ID), Size: int64(len(data)), SHA256: childDigest(data), StoragePath: id + "/" + m.ID + ".md", Producer: "submission", CreatedAt: now}
		files = append(files, review.RetainedFile{Artifact: artifact, Bytes: data})
	}
	prepared, err := review.PrepareChild(f, cp, a.Name, now.Add(time.Hour), files)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := s.MaterializeReviewChild(ctx, f, cp, prepared)
	if err != nil {
		t.Fatal(err)
	}
	return f, receipt
}

func reviewTickSettleRound(t *testing.T, s *Store, cp review.CheckpointAuthority) {
	t.Helper()
	round, err := s.GetReviewRound(context.Background(), cp.RoundID)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range round.Reviewers {
		round, err = s.RecordReviewResult(context.Background(), cp.RoundID, m.ID, round.Revision, review.Result{State: "failed", Failure: "finished fixture"})
		if err != nil {
			t.Fatal(err)
		}
	}
	if !round.Terminal() {
		t.Fatal("fixture round not terminal")
	}
}
func reviewTickFinishAttempts(t *testing.T, s *Store, receipt ReviewMaterialization) {
	t.Helper()
	records, err := finishedChildRecords(receipt, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	cancellationSave(t, s, records)
}

func TestReviewChildrenNoOpAndSettledWriterSkip(t *testing.T) {
	s, _, cp, active := parentWaitFixture(t)
	for i := 0; i < 3; i++ {
		_, receipt := reviewTickChild(t, s, fmt.Sprintf("settled-%d", i))
		reviewTickSettleRound(t, s, receipt.Checkpoint)
		reviewTickFinishAttempts(t, s, receipt)
	}
	before := reviewTickChanges(t, s)
	if err := s.ReconcileMaterializedReviewChildren(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := reviewTickChanges(t, s); got != before {
		t.Fatalf("no-op pass changed rows: %d -> %d", before, got)
	}

	// Settle the initial fixture too, then hold a writer on another connection.
	reviewTickSettleRound(t, s, cp)
	reviewTickFinishAttempts(t, s, active)
	other, err := Open(s.path)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, err := other.db.Exec("BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	defer other.db.Exec("ROLLBACK")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.ReconcileMaterializedReviewChildren(ctx); err != nil {
		t.Fatalf("settled pass took writer lock: %v", err)
	}
}

func TestReviewChildrenUnsettledEdges(t *testing.T) {
	for _, kind := range []string{"terminal-round-live-attempts", "active-round-terminal-attempts", "ended-parent-run"} {
		t.Run(kind, func(t *testing.T) {
			s, f, cp, receipt := parentWaitFixture(t)
			_, settled := reviewTickChild(t, s, "settled")
			reviewTickSettleRound(t, s, settled.Checkpoint)
			reviewTickFinishAttempts(t, s, settled)
			retained := childSnapshot(t, s, settled.Checkpoint)
			switch kind {
			case "terminal-round-live-attempts":
				reviewTickSettleRound(t, s, cp)
				cancellationEndParent(t, s, f, domain.ProgressSucceeded)
			case "active-round-terminal-attempts":
				reviewTickFinishAttempts(t, s, receipt)
				// A missing parent must still be detected: terminal child attempts alone
				// cannot classify a pending round as settled.
				if _, err := s.db.Exec("DELETE FROM coordinator_workflow_runs WHERE id=?", f.Parent.RunID); err != nil {
					t.Fatal(err)
				}
			case "ended-parent-run":
				for _, r := range cancellationRecords(t, s).WorkflowRuns {
					if r.ID == f.Parent.RunID {
						r.Progress = domain.ProgressSucceeded
						r.Revision++
						cancellationSave(t, s, CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{r}})
					}
				}
			}
			err := s.ReconcileMaterializedReviewChildren(context.Background())
			if kind == "active-round-terminal-attempts" {
				if err == nil {
					t.Fatal("pending round skipped")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				for _, a := range cancellationRecords(t, s).Attempts {
					if a.WorkflowRunID == cp.RoundID && a.Progress != domain.ProgressCancelled {
						t.Fatalf("stranded child not cancelled: %+v", a)
					}
				}
			}
			if childSnapshot(t, s, settled.Checkpoint) != retained {
				t.Fatal("settled neighbor changed")
			}
		})
	}
}

func TestReviewChildrenOncePerTick(t *testing.T) {
	t.Run("success skips all reads", func(t *testing.T) {
		s, _, _, _ := parentWaitFixture(t)
		ctx := WithReviewChildrenReconciledOncePerTick(context.Background())
		if err := s.ReconcileMaterializedReviewChildren(ctx); err != nil {
			t.Fatal(err)
		}
		// Closing the DB makes any later query fail, proving both calls read nothing.
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 2; i++ {
			if err := s.ReconcileMaterializedReviewChildren(ctx); err != nil {
				t.Fatal(err)
			}
		}
	})
	t.Run("unmarked runs every time", func(t *testing.T) {
		s, _, _, _ := parentWaitFixture(t)
		for i := 0; i < 3; i++ {
			// Each call must notice newly injected enumeration corruption.
			if i > 0 {
				if _, err := s.db.Exec("UPDATE coordinator_review_rounds SET record='invalid'"); err != nil {
					t.Fatal(err)
				}
			}
			err := s.ReconcileMaterializedReviewChildren(context.Background())
			if (err != nil) != (i > 0) {
				t.Fatalf("call %d: %v", i, err)
			}
		}
	})
	t.Run("failure permits retry", func(t *testing.T) {
		s, _, cp, _ := parentWaitFixture(t)
		var original []byte
		if err := s.db.QueryRow("SELECT record FROM coordinator_review_rounds WHERE id=?", cp.RoundID).Scan(&original); err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.Exec("UPDATE coordinator_review_rounds SET record='invalid' WHERE id=?", cp.RoundID); err != nil {
			t.Fatal(err)
		}
		ctx := WithReviewChildrenReconciledOncePerTick(context.Background())
		for i := 0; i < 2; i++ {
			if err := s.ReconcileMaterializedReviewChildren(ctx); err == nil {
				t.Fatal("failed pass suppressed a retry query")
			}
		}
		if _, err := s.db.Exec("UPDATE coordinator_review_rounds SET record=? WHERE id=?", original, cp.RoundID); err != nil {
			t.Fatal(err)
		}
		if err := s.ReconcileMaterializedReviewChildren(ctx); err != nil {
			t.Fatal(err)
		}
	})
}

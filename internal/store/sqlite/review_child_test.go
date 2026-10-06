package sqlite

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/pinnedinput"
	"github.com/iryzhkov/t3-steward/internal/review"
)

func childDigest(raw []byte) string { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }
func childFixture(t *testing.T) (*Store, review.FrozenAuthority, review.CheckpointAuthority, review.ChildPreparation, []review.RetainedFile) {
	t.Helper()
	s, f := reviewAuthorityFixture(t)
	raw := []byte("exact frozen criteria")
	spec := f.Requirements
	spec.CriteriaDigest = childDigest(raw)
	req, err := review.NewRequirements(spec)
	if err != nil {
		t.Fatal(err)
	}
	f, err = review.NewFrozenAuthority(f.Parent, req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.FreezeReviewAuthority(context.Background(), f); err != nil {
		t.Fatal(err)
	}
	manifest, err := pinnedinput.NewManifest([]pinnedinput.Entry{{Name: "inputs/criteria.md", Size: int64(len(raw)), SHA256: childDigest(raw)}})
	if err != nil {
		t.Fatal(err)
	}
	cp, err := s.AllocateReviewCheckpoint(context.Background(), f, review.Checkpoint{ID: "checkpoint", HeadCommit: strings.Repeat("d", 40), InputDigest: manifest.Digest})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	a := domain.Artifact{ID: review.ChildInputID(cp, "inputs/criteria.md"), WorkflowRunID: cp.RoundID, Kind: domain.ArtifactInput, Name: "inputs/criteria.md", Size: int64(len(raw)), SHA256: childDigest(raw), StoragePath: "child/criteria.md", Producer: "submission", CreatedAt: now}
	files := []review.RetainedFile{{Artifact: a, Bytes: raw}}
	for _, m := range f.Requirements.Members {
		prompt, err := review.ChildPrompt(f, cp, m, manifest, a.Name)
		if err != nil {
			t.Fatal(err)
		}
		bytes := []byte(prompt)
		a := domain.Artifact{ID: review.ChildPromptID(cp, m.ID), WorkflowRunID: cp.RoundID, TaskID: cp.MemberTaskID(m.ID), Kind: domain.ArtifactInput, Name: review.ChildPromptName(m.ID), Size: int64(len(bytes)), SHA256: childDigest(bytes), StoragePath: "child/" + m.ID + ".md", Producer: "submission", CreatedAt: now}
		files = append(files, review.RetainedFile{Artifact: a, Bytes: bytes})
	}
	p, err := review.PrepareChild(f, cp, "inputs/criteria.md", now.Add(time.Hour), files)
	if err != nil {
		t.Fatal(err)
	}
	return s, f, cp, p, files
}
func childSnapshot(t *testing.T, s *Store, cp review.CheckpointAuthority) string {
	t.Helper()
	var out []string
	for _, query := range []struct{ sql, id string }{
		{"SELECT record FROM coordinator_workflow_runs WHERE id=?", cp.RoundID},
		{"SELECT record FROM coordinator_attempts WHERE workflow_run_id=? ORDER BY id", cp.RoundID},
		{"SELECT record FROM coordinator_review_rounds WHERE id=?", cp.RoundID},
		{"SELECT record FROM coordinator_artifacts WHERE workflow_run_id=? ORDER BY id", cp.RoundID},
		{"SELECT record FROM coordinator_workflows WHERE id=?", review.ChildWorkflowID(cp)},
		{"SELECT record FROM coordinator_tasks WHERE workflow_id=? ORDER BY id", review.ChildWorkflowID(cp)},
		{"SELECT record FROM coordinator_graph_revisions WHERE run_id=? ORDER BY revision", cp.RoundID},
		{"SELECT record FROM coordinator_review_materializations WHERE checkpoint_id=?", cp.Key()},
	} {
		rows, err := s.db.Query(query.sql, query.id)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var raw string
			if err := rows.Scan(&raw); err != nil {
				t.Fatal(err)
			}
			out = append(out, raw)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
	}
	raw, _ := json.Marshal(out)
	return string(raw)
}

func TestReviewChildAllocationReopenProgressReplay(t *testing.T) {
	ctx := context.Background()
	s, f, cp, p, _ := childFixture(t)
	path := s.path
	s.Close()
	s, err := openMigratedFixture(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// Allocation-only restart and original authority after revision progression.
	if _, err = s.db.Exec("UPDATE coordinator_attempts SET revision=revision+1,record=json_set(record,'$.revision',revision+1) WHERE id=?", f.Parent.AttemptID); err != nil {
		t.Fatal(err)
	}
	first, err := s.MaterializeReviewChild(ctx, f, cp, p)
	if err != nil {
		t.Fatal(err)
	}
	if first.Graph.Run.ID != cp.RoundID || len(first.Graph.Tasks) != len(f.Requirements.Members) || first.Graph.Run.Sink == nil {
		t.Fatal("reserved graph/sink absent")
	}
	round, err := s.GetReviewRound(ctx, cp.RoundID)
	if err != nil {
		t.Fatal(err)
	}
	for i, m := range f.Requirements.Members {
		task := first.Graph.Tasks[i]
		if task.ID != cp.MemberTaskID(m.ID) || task.Routes[0].ProviderInstanceID+"/"+task.Routes[0].Model != m.Route || !task.Deadline.Equal(round.Deadline) || task.InputArtifactIDs[0] != review.ChildInputID(cp, "inputs/criteria.md") {
			t.Fatal("wrong derived task")
		}
	}
	before := childSnapshot(t, s, cp)
	replay, err := s.MaterializeReviewChild(ctx, f, cp, p)
	if err != nil || !reflect.DeepEqual(first, replay) || before != childSnapshot(t, s, cp) {
		t.Fatalf("initial replay changed state: %v", err)
	}
	a := first.Graph.Attempts[0]
	a.Progress = domain.ProgressSucceeded
	a.Control = domain.ControlStopped
	a.Revision = 4
	a.Failure = "retained sentinel"
	ended := time.Now().UTC()
	a.CompletedAt = &ended
	run := first.Graph.Run
	run.Progress = domain.ProgressActive
	run.Revision = 7
	run.UpdatedAt = ended
	if err := s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{a}, WorkflowRuns: []domain.WorkflowRun{run}}); err != nil {
		t.Fatal(err)
	}
	round, err = s.RecordReviewResult(ctx, round.ID, round.Reviewers[0].ID, round.Revision, review.Result{State: "succeeded", ReviewMD: "Reviewed", VerdictJSON: []byte(`{"schema":"review-verdict/v1","verdict":"accept","findings":[],"inputManifestDigest":"` + cp.Checkpoint.InputDigest + `","reviewerRoute":"` + round.Reviewers[0].Route + `"}`)})
	if err != nil {
		t.Fatal(err)
	}
	// A progressed later retry also survives read-only replay after parent exit.
	retry := a
	retry.ID += "-retry"
	retry.Number = 2
	retry.ThreadID = "retained-retry-thread"
	if err := s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{retry}}); err != nil {
		t.Fatal(err)
	}
	// End the parent; bound replay must still be read-only.
	if _, err := s.db.Exec("UPDATE coordinator_attempts SET record=json_set(record,'$.progress','succeeded','$.control','stopped') WHERE id=?", f.Parent.AttemptID); err != nil {
		t.Fatal(err)
	}
	before = childSnapshot(t, s, cp)
	s.Close()
	s, err = OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	replay, err = s.MaterializeReviewChild(ctx, f, cp, p)
	if err != nil || !reflect.DeepEqual(first, replay) || before != childSnapshot(t, s, cp) {
		t.Fatalf("progressed read-only replay changed state: %v", err)
	}
}

func TestReviewChildConcurrentWriters(t *testing.T) {
	s, f, cp, p, _ := childFixture(t)
	other, err := Open(s.path)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	start := make(chan struct{})
	var wg sync.WaitGroup
	receipts := make([]ReviewMaterialization, 2)
	errs := make([]error, 2)
	for i, store := range []*Store{s, other} {
		wg.Add(1)
		go func(i int, store *Store) {
			defer wg.Done()
			<-start
			receipts[i], errs[i] = store.MaterializeReviewChild(context.Background(), f, cp, p)
		}(i, store)
	}
	close(start)
	wg.Wait()
	if errs[0] != nil || errs[1] != nil || !reflect.DeepEqual(receipts[0], receipts[1]) {
		t.Fatalf("concurrent: %v", errs)
	}
	var count int
	if err := s.db.QueryRow("SELECT count(*) FROM coordinator_attempts WHERE workflow_run_id=?", cp.RoundID).Scan(&count); err != nil || count != len(f.Requirements.Members) {
		t.Fatalf("duplicate reviewers: %d %v", count, err)
	}
}

func TestReviewChildCreationRollbackAndFences(t *testing.T) {
	for _, kind := range []string{"late failure", "workflow collision", "run collision", "attempt collision", "task collision", "artifact collision", "graph collision", "ended parent", "ended run", "superseded", "epoch", "stale revision", "unbound result", "unbound deadline", "authority forgery", "checkpoint forgery"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			s, f, cp, p, _ := childFixture(t)
			var err error
			switch kind {
			case "late failure":
				_, err = s.db.Exec("CREATE TRIGGER fail_child BEFORE INSERT ON coordinator_review_materializations BEGIN SELECT RAISE(ABORT,'injected final failure'); END")
			case "workflow collision":
				err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Workflows: []domain.Workflow{{ID: review.ChildWorkflowID(cp), Name: "foreign"}}})
			case "run collision":
				err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{{ID: cp.RoundID, WorkflowID: "foreign", Progress: domain.ProgressQueued}}})
			case "attempt collision":
				err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{{ID: review.ChildAttemptID(cp, "one"), WorkflowRunID: "foreign", TaskID: "foreign", Number: 1}}})
			case "task collision":
				err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Tasks: []domain.Task{{ID: cp.MemberTaskID("one"), WorkflowID: "foreign", Name: "foreign"}}})
			case "artifact collision":
				err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Artifacts: []domain.Artifact{{ID: review.ChildInputID(cp, "inputs/criteria.md"), WorkflowRunID: "foreign", Kind: domain.ArtifactInput}}})
			case "graph collision":
				_, err = s.db.Exec("INSERT INTO coordinator_graph_revisions(run_id,revision,record) VALUES(?,1,'{}')", cp.RoundID)
			case "ended parent":
				_, err = s.db.Exec("UPDATE coordinator_attempts SET record=json_set(record,'$.progress','succeeded','$.control','stopped') WHERE id=?", f.Parent.AttemptID)
			case "ended run":
				_, err = s.db.Exec("UPDATE coordinator_workflow_runs SET record=json_set(record,'$.progress','succeeded') WHERE id=?", f.Parent.RunID)
			case "superseded":
				a := loadAttempt(t, s, f.Parent.AttemptID)
				a.ID = "new-attempt"
				a.Number++
				err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{a}})
			case "epoch":
				_, err = s.db.Exec("UPDATE coordinator_assignments SET record=json_set(record,'$.epoch',2) WHERE id=?", f.Parent.AssignmentID)
			case "stale revision":
				_, err = s.db.Exec("UPDATE coordinator_attempts SET record=json_set(record,'$.revision',0) WHERE id=?", f.Parent.AttemptID)
			case "unbound result":
				round, e := s.GetReviewRound(ctx, cp.RoundID)
				if e != nil {
					t.Fatal(e)
				}
				_, err = s.RecordReviewResult(ctx, cp.RoundID, "one", round.Revision, review.Result{State: "failed", Failure: "sentinel"})
			case "unbound deadline":
				_, err = s.db.Exec("UPDATE coordinator_review_rounds SET record=json_set(record,'$.deadline','2027-01-01T00:00:00Z') WHERE id=?", cp.RoundID)
			case "authority forgery":
				f.Requirements.Members[0].Route = "forged/model"
				r, e := review.NewRequirements(f.Requirements)
				if e != nil {
					t.Fatal(e)
				}
				f.RequirementsDigest = r.Digest()
			case "checkpoint forgery":
				cp.Checkpoint.HeadCommit = strings.Repeat("f", 40)
			}
			if err != nil {
				t.Fatal(err)
			}
			before := childSnapshot(t, s, cp)
			if _, err = s.MaterializeReviewChild(ctx, f, cp, p); err == nil {
				t.Fatal("unsafe creation accepted")
			}
			if before != childSnapshot(t, s, cp) {
				t.Fatal("rollback changed child")
			}
			var count int
			if err := s.db.QueryRow("SELECT count(*) FROM coordinator_review_materializations").Scan(&count); err != nil || count != 0 {
				t.Fatal("partial binding")
			}
			round, e := s.GetReviewRound(ctx, cp.RoundID)
			if e != nil {
				t.Fatal(e)
			}
			if kind == "late failure" && (!round.Deadline.IsZero() || round.Revision != 1) {
				t.Fatal("deadline survived rollback")
			}
		})
	}
}

func TestReviewChildBoundCorruptionAndConflicts(t *testing.T) {
	for _, kind := range []string{"workflow", "run", "task", "attempt", "artifact", "graph", "sink", "round route", "round deadline", "deadline conflict", "input bytes", "criteria", "prompt", "foreign metadata"} {
		t.Run(kind, func(t *testing.T) {
			s, f, cp, p, files := childFixture(t)
			ctx := context.Background()
			receipt, err := s.MaterializeReviewChild(ctx, f, cp, p)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "workflow":
				_, err = s.db.Exec("DELETE FROM coordinator_workflows WHERE id=?", receipt.Graph.Workflow.ID)
			case "run":
				_, err = s.db.Exec("DELETE FROM coordinator_workflow_runs WHERE id=?", cp.RoundID)
			case "task":
				_, err = s.db.Exec("DELETE FROM coordinator_tasks WHERE id=?", receipt.Graph.Tasks[0].ID)
			case "attempt":
				_, err = s.db.Exec("DELETE FROM coordinator_attempts WHERE id=?", receipt.Graph.Attempts[0].ID)
			case "artifact":
				_, err = s.db.Exec("DELETE FROM coordinator_artifacts WHERE id=?", receipt.Graph.Artifacts[0].ID)
			case "graph":
				_, err = s.db.Exec("DELETE FROM coordinator_graph_revisions WHERE run_id=?", cp.RoundID)
			case "sink":
				_, err = s.db.Exec("UPDATE coordinator_workflow_runs SET record=json_remove(record,'$.sink') WHERE id=?", cp.RoundID)
			case "round route":
				_, err = s.db.Exec("UPDATE coordinator_review_rounds SET record=json_set(record,'$.reviewers[0].route','forged/model') WHERE id=?", cp.RoundID)
			case "round deadline":
				_, err = s.db.Exec("UPDATE coordinator_review_rounds SET record=json_set(record,'$.deadline','2027-01-01T00:00:00Z') WHERE id=?", cp.RoundID)
			default:
				deadline := p.Deadline()
				switch kind {
				case "deadline conflict":
					deadline = deadline.Add(time.Hour)
				case "input bytes":
					files[0].Bytes = []byte("changed")
				case "criteria":
					f.Requirements.CriteriaDigest = strings.Repeat("f", 64)
				case "prompt":
					files[1].Bytes = []byte("altered instructions")
					files[1].Artifact.Size = int64(len(files[1].Bytes))
					files[1].Artifact.SHA256 = childDigest(files[1].Bytes)
				case "foreign metadata":
					files[1].Artifact.WorkflowRunID = "foreign"
				}
				candidate, e := review.PrepareChild(f, cp, "inputs/criteria.md", deadline, files)
				if kind != "deadline conflict" {
					if e == nil {
						t.Fatal("invalid preparation accepted")
					}
					return
				}
				if e != nil {
					t.Fatal(e)
				}
				p = candidate
			}
			if err != nil {
				t.Fatal(err)
			}
			before := childSnapshot(t, s, cp)
			if _, err := s.MaterializeReviewChild(ctx, f, cp, p); err == nil {
				t.Fatal("corrupt/conflicting replay accepted")
			}
			if before != childSnapshot(t, s, cp) {
				t.Fatal("replay repaired or reset state")
			}
		})
	}
}

func TestReviewChildV34Migration(t *testing.T) {
	s, err := open(filepath.Join(t.TempDir(), "state.db"), true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.migrateThrough(33); err != nil {
		t.Fatal(err)
	}
	var old string
	query := "SELECT group_concat(sql) FROM sqlite_master WHERE name LIKE 'coordinator_review_%' ORDER BY name"
	if err = s.db.QueryRow(query).Scan(&old); err != nil {
		t.Fatal(err)
	}
	if err = s.Migrate(); err != nil {
		t.Fatal(err)
	}
	if schemaVersionOf(t, s) != 34 {
		t.Fatal("wrong schema")
	}
	var kept string
	if err = s.db.QueryRow("SELECT group_concat(sql) FROM sqlite_master WHERE name LIKE 'coordinator_review_%' AND name!='coordinator_review_materializations' ORDER BY name").Scan(&kept); err != nil {
		t.Fatal(err)
	}
	if kept != old {
		t.Fatal("foundation changed")
	}
	if err = s.Migrate(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec("INSERT INTO coordinator_review_materializations(checkpoint_id,workflow_id,run_id,record) VALUES('cp','w','r','{}')"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec("UPDATE coordinator_review_materializations SET record='null'"); err == nil {
		t.Fatal("binding mutable")
	}
	if _, err = s.db.Exec("DELETE FROM coordinator_review_materializations"); err == nil {
		t.Fatal("binding deletable")
	}
}

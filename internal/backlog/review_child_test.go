package backlog

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// The pre-publication catalog exposes only trusted staged metadata, never rows
// prematurely saved through the generic graph API.
type childStagedCatalog struct {
	ArtifactCatalog
	artifacts []domain.Artifact
}

func (c childStagedCatalog) LoadArtifacts(_ context.Context, ids []string) ([]domain.Artifact, error) {
	var out []domain.Artifact
	for _, id := range ids {
		for _, a := range c.artifacts {
			if a.ID == id {
				out = append(out, a)
			}
		}
	}
	return out, nil
}

type retainedChildFixture struct {
	parent     admissionFixture
	frozen     review.FrozenAuthority
	checkpoint review.CheckpointAuthority
	artifacts  []domain.Artifact
	retained   CoordinatorArtifactStore
	deadline   time.Time
}

func newRetainedChild(t *testing.T, swarm bool) retainedChildFixture {
	t.Helper()
	ctx := context.Background()
	f := newAdmissionFixture(t)
	if swarm {
		f.request.Policy.Members[1].Role = "judge"
		f.catalog.catalog.Classifications[1].Tier = "executor"
		f.request.Policy.Members = append(f.request.Policy.Members, AdmissionMember{"lens", "swarm:correctness", "codex/economy", false})
		f.catalog.catalog.Classifications = append(f.catalog.catalog.Classifications, AdmissionClassification{"codex/economy", "openai", "economy"})
		f.catalog.catalog.AuthoredWorkers[0].Providers[0].Models = append(f.catalog.catalog.AuthoredWorkers[0].Providers[0].Models, "economy")
	}
	snap, err := f.service.Freeze(ctx, f.request)
	if err != nil {
		t.Fatal(err)
	}
	cp, err := f.store.AllocateReviewCheckpoint(ctx, snap.Authority, review.Checkpoint{ID: "child", HeadCommit: strings.Repeat("d", 40), InputDigest: f.records.Workflows[0].InputManifest.Digest})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	now := time.Now().UTC()
	var artifacts []domain.Artifact
	stage := func(a domain.Artifact, raw []byte) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(filepath.Join(root, a.StoragePath)), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, a.StoragePath), raw, 0400); err != nil {
			t.Fatal(err)
		}
		artifacts = append(artifacts, a)
	}
	for _, entry := range f.records.Workflows[0].InputManifest.Entries {
		for _, a := range f.records.Artifacts {
			if a.Name != entry.Name {
				continue
			}
			_, file, err := f.service.Artifacts.Open(ctx, a.ID)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := io.ReadAll(file)
			file.Close()
			if err != nil {
				t.Fatal(err)
			}
			a.ID = review.ChildInputID(cp, a.Name)
			a.WorkflowRunID = cp.RoundID
			a.TaskID = ""
			a.AttemptID = ""
			a.StoragePath = "child/" + a.Name
			a.CreatedAt = now
			stage(a, raw)
		}
	}
	for _, m := range snap.Authority.Requirements.Members {
		prompt, err := review.ChildPrompt(snap.Authority, cp, m, *f.records.Workflows[0].InputManifest, "inputs/criteria.md")
		if err != nil {
			t.Fatal(err)
		}
		raw := []byte(prompt)
		stage(domain.Artifact{ID: review.ChildPromptID(cp, m.ID), WorkflowRunID: cp.RoundID, TaskID: cp.MemberTaskID(m.ID), Kind: domain.ArtifactInput, Name: review.ChildPromptName(m.ID), Size: int64(len(raw)), SHA256: admissionDigestBytes(raw), StoragePath: "child/prompts/" + m.ID + ".md", Producer: "submission", CreatedAt: now}, raw)
	}
	catalog := childStagedCatalog{ArtifactCatalog: f.store.Store, artifacts: append([]domain.Artifact(nil), artifacts...)}
	return retainedChildFixture{f, snap.Authority, cp, artifacts, CoordinatorArtifactStore{Root: root, SubmissionRoot: root, Catalog: catalog}, now.Add(time.Hour)}
}
func (f retainedChildFixture) prepare(ctx context.Context) (review.ChildPreparation, error) {
	return PrepareReviewChild(ctx, f.retained, f.frozen, f.checkpoint, "inputs/criteria.md", f.deadline, f.artifacts)
}

func TestReviewChildRetainedSQLiteCollectorReplay(t *testing.T) {
	ctx := context.Background()
	f := newRetainedChild(t, false)
	p, err := f.prepare(ctx)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := f.parent.store.MaterializeReviewChild(ctx, f.frozen, f.checkpoint, p)
	if err != nil {
		t.Fatal(err)
	}
	var artifacts []domain.Artifact
	attempts := append([]domain.Attempt(nil), receipt.Graph.Attempts...)
	now := time.Now().UTC()
	for i, m := range f.frozen.Requirements.Members {
		attempts[i].Progress = domain.ProgressSucceeded
		attempts[i].Control = domain.ControlStopped
		attempts[i].Revision = 3
		attempts[i].CompletedAt = &now
		resultAttempt := attempts[i]
		if i == 0 {
			// The collector must consume the latest retry, keeping the reserved task.
			attempts[i].Progress = domain.ProgressFailed
			attempts[i].Failure = "initial failure retained"
			resultAttempt.ID += "-retry"
			resultAttempt.Number = 2
			attempts = append(attempts, resultAttempt)
		}
		verdict := `{"schema":"review-verdict/v1","verdict":"accept","findings":[],"inputManifestDigest":"` + f.checkpoint.Checkpoint.InputDigest + `","reviewerRoute":"` + m.Route + `"}`
		for _, out := range []struct{ name, raw string }{{"review.md", "Reviewed exact retained criteria"}, {"verdict.json", verdict}} {
			a := domain.Artifact{ID: resultAttempt.ID + "-" + out.name, WorkflowRunID: f.checkpoint.RoundID, TaskID: f.checkpoint.MemberTaskID(m.ID), AttemptID: resultAttempt.ID, Kind: domain.ArtifactOutput, Name: out.name, Size: int64(len(out.raw)), SHA256: admissionDigestBytes([]byte(out.raw)), StoragePath: "outputs/" + m.ID + "/" + out.name, Producer: "worker", CreatedAt: now}
			if err := os.MkdirAll(filepath.Dir(filepath.Join(f.retained.Root, a.StoragePath)), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(f.retained.Root, a.StoragePath), []byte(out.raw), 0400); err != nil {
				t.Fatal(err)
			}
			artifacts = append(artifacts, a)
		}
	}
	if err := f.parent.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: attempts, Artifacts: artifacts}); err != nil {
		t.Fatal(err)
	}
	persisted := f.retained
	persisted.Catalog = f.parent.store.Store
	collector := ReviewCollector{Store: f.parent.store.Store, Results: t.TempDir(), Open: func(ctx context.Context, id string) (domain.Artifact, io.ReadCloser, error) {
		return persisted.Open(ctx, id)
	}}
	if err := collector.Tick(ctx, now); err != nil {
		t.Fatal(err)
	}
	round, err := f.parent.store.GetReviewRound(ctx, f.checkpoint.RoundID)
	if err != nil {
		t.Fatal(err)
	}
	if round.Combined != "accept" || round.ReplyText == "" || !round.Deadline.Equal(f.deadline) || round.BaseCommit != f.frozen.Parent.BaseCommit || round.HeadCommit != f.checkpoint.Checkpoint.HeadCommit {
		t.Fatalf("collector incompatible: %#v", round)
	}
	beforeRecords, err := f.parent.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(beforeRecords)
	roundBytes, _ := json.Marshal(round)
	replay, err := f.parent.store.MaterializeReviewChild(ctx, f.frozen, f.checkpoint, p)
	if err != nil || !reflect.DeepEqual(receipt, replay) {
		t.Fatalf("receipt changed: %v", err)
	}
	afterRecords, err := f.parent.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(afterRecords)
	afterRound, err := f.parent.store.GetReviewRound(ctx, round.ID)
	if err != nil {
		t.Fatal(err)
	}
	afterRoundBytes, _ := json.Marshal(afterRound)
	if string(before) != string(after) || string(roundBytes) != string(afterRoundBytes) {
		t.Fatal("collected results/attempts reset")
	}
	for _, a := range receipt.Graph.Artifacts {
		if _, reader, err := persisted.Open(ctx, a.ID); err != nil {
			t.Fatal(err)
		} else {
			reader.Close()
		}
	}
}

func TestReviewChildRetainedOrphanOutputsRefused(t *testing.T) {
	testRetainedOrphanOutputs(t, "canonical")
}
func TestReviewChildRetainedAmbiguousOutputsRefused(t *testing.T) {
	for _, shape := range []string{"duplicate", "case", "artifact-duplicate", "artifact-case"} {
		t.Run(shape, func(t *testing.T) { testRetainedOrphanOutputs(t, shape) })
	}
}
func testRetainedOrphanOutputs(t *testing.T, shape string) {
	ctx := context.Background()
	f := newRetainedChild(t, false)
	p, err := f.prepare(ctx)
	if err != nil {
		t.Fatal(err)
	}
	g, err := p.Build(f.frozen, f.checkpoint, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	a := g.Attempts[0]
	a.ID = "orphan-retry"
	a.Number = 2
	a.Progress = domain.ProgressSucceeded
	a.Control = domain.ControlStopped
	member := f.frozen.Requirements.Members[0]
	verdict := `{"schema":"review-verdict/v1","verdict":"accept","findings":[],"inputManifestDigest":"` + f.checkpoint.Checkpoint.InputDigest + `","reviewerRoute":"` + member.Route + `"}`
	var artifacts []domain.Artifact
	for _, out := range []struct{ name, raw string }{{"review.md", "orphan retained evidence"}, {"verdict.json", verdict}} {
		path := "orphan/" + out.name
		if err = os.MkdirAll(filepath.Dir(filepath.Join(f.retained.Root, path)), 0700); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(f.retained.Root, path), []byte(out.raw), 0400); err != nil {
			t.Fatal(err)
		}
		artifacts = append(artifacts, domain.Artifact{ID: a.ID + "-" + out.name, WorkflowRunID: a.WorkflowRunID, TaskID: a.TaskID, AttemptID: a.ID, Kind: domain.ArtifactOutput, Name: out.name, StoragePath: path, Size: int64(len(out.raw)), SHA256: admissionDigestBytes([]byte(out.raw)), Producer: "worker", CreatedAt: time.Now().UTC()})
	}
	if err = f.parent.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{a}, Artifacts: artifacts}); err != nil {
		t.Fatal(err)
	}

	if shape != "canonical" {
		raw, e := json.Marshal(a)
		if e != nil {
			t.Fatal(e)
		}
		record := string(raw)
		if strings.HasSuffix(shape, "duplicate") {
			record = `{"workflowRunId":"foreign-run","taskId":"foreign-task",` + record[1:]
		} else {
			record = strings.ReplaceAll(strings.ReplaceAll(record, `"workflowRunId":`, `"WORKFLOWRUNID":`), `"taskId":`, `"TASKID":`)
		}
		rawDB, e := sql.Open("sqlite", f.parent.store.dbPath)
		if e != nil {
			t.Fatal(e)
		}
		defer rawDB.Close()
		if strings.HasPrefix(shape, "artifact-") {
			// Isolate output-only ownership: the attempt is canonical and unrelated.
			if _, e = rawDB.Exec("UPDATE coordinator_attempts SET workflow_run_id='foreign-run',task_id='foreign-task',record=json_set(record,'$.workflowRunId','foreign-run','$.taskId','foreign-task') WHERE id=?", a.ID); e != nil {
				t.Fatal(e)
			}
			for _, artifact := range artifacts {
				raw, e = json.Marshal(artifact)
				if e != nil {
					t.Fatal(e)
				}
				record = string(raw)
				if strings.HasSuffix(shape, "duplicate") {
					record = `{"workflowRunId":"foreign-run","taskId":"foreign-task",` + record[1:]
				} else {
					record = strings.ReplaceAll(strings.ReplaceAll(record, `"workflowRunId":`, `"WORKFLOWRUNID":`), `"taskId":`, `"TASKID":`)
				}
				if _, e = rawDB.Exec("UPDATE coordinator_artifacts SET workflow_run_id='foreign-run',task_id='foreign-task',record=? WHERE id=?", record, artifact.ID); e != nil {
					t.Fatal(e)
				}
			}
		} else if _, e = rawDB.Exec("UPDATE coordinator_attempts SET workflow_run_id='foreign-run',task_id='foreign-task',record=? WHERE id=?", record, a.ID); e != nil {
			t.Fatal(e)
		}
	}
	before, err := f.parent.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}

	seen := false
	for _, got := range before.Attempts {
		if got.ID == a.ID && got.WorkflowRunID == g.Run.ID && got.TaskID == a.TaskID {
			seen = true
		}
	}
	if strings.HasPrefix(shape, "artifact-") {
		seen = false
		for _, got := range before.Artifacts {
			if got.ID == artifacts[0].ID && got.WorkflowRunID == g.Run.ID && got.TaskID == a.TaskID {
				seen = true
			}
		}
	}
	if !seen {
		t.Fatal("runtime loader missed orphan")
	}
	round, err := f.parent.store.GetReviewRound(ctx, f.checkpoint.RoundID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.parent.store.MaterializeReviewChild(ctx, f.frozen, f.checkpoint, p); err == nil {
		t.Fatal("orphan collector evidence adopted")
	}
	after, err := f.parent.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	afterRound, err := f.parent.store.GetReviewRound(ctx, f.checkpoint.RoundID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(round, afterRound) || !afterRound.Deadline.IsZero() {
		t.Fatal("orphan refusal changed execution or round")
	}
	persisted := f.retained
	persisted.Catalog = f.parent.store.Store
	for _, artifact := range artifacts {
		_, reader, err := persisted.Open(ctx, artifact.ID)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := io.ReadAll(reader)
		reader.Close()
		if err != nil || admissionDigestBytes(raw) != artifact.SHA256 {
			t.Fatal("orphan output altered")
		}
	}
}

func TestReviewChildRetainedPreparationRefusals(t *testing.T) {
	for _, kind := range []string{"missing file", "altered bytes", "metadata", "foreign ownership", "prompt bytes", "criteria substitution", "zero deadline", "manifest conflict"} {
		t.Run(kind, func(t *testing.T) {
			f := newRetainedChild(t, false)
			path := filepath.Join(f.retained.SubmissionRoot, f.artifacts[0].StoragePath)
			switch kind {
			case "missing file":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			case "altered bytes":
				if err := os.Chmod(path, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte("bad"), 0400); err != nil {
					t.Fatal(err)
				}
			case "metadata":
				f.artifacts[0].CreatedAt = f.artifacts[0].CreatedAt.Add(time.Second)
			case "foreign ownership":
				f.artifacts[0].WorkflowRunID = "foreign"
				f.retained.Catalog = childStagedCatalog{ArtifactCatalog: f.parent.store.Store, artifacts: f.artifacts}
			case "prompt bytes":
				for i, a := range f.artifacts {
					if a.TaskID == "" {
						continue
					}
					raw := []byte("forged control instructions")
					p := filepath.Join(f.retained.Root, a.StoragePath)
					os.Chmod(p, 0600)
					if err := os.WriteFile(p, raw, 0400); err != nil {
						t.Fatal(err)
					}
					f.artifacts[i].Size = int64(len(raw))
					f.artifacts[i].SHA256 = admissionDigestBytes(raw)
					break
				}
				f.retained.Catalog = childStagedCatalog{ArtifactCatalog: f.parent.store.Store, artifacts: f.artifacts}
			case "criteria substitution":
				f.frozen.Requirements.CriteriaDigest = strings.Repeat("f", 64)
			case "zero deadline":
				f.deadline = time.Time{}
			case "manifest conflict":
				f.checkpoint.Checkpoint.InputDigest = strings.Repeat("f", 64)
			}
			if _, err := f.prepare(context.Background()); err == nil {
				t.Fatal("unverified preparation accepted")
			}
			records, err := f.parent.store.LoadCoordinatorRecords(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(records.WorkflowRuns) != 1 {
				t.Fatal("premature database graph")
			}
		})
	}
}

func TestReviewChildSwarmAndLosingCallerRetention(t *testing.T) {
	f := newRetainedChild(t, true)
	ctx := context.Background()
	p, err := f.prepare(ctx)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := f.parent.store.MaterializeReviewChild(ctx, f.frozen, f.checkpoint, p)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range receipt.Graph.Tasks {
		if task.Name == "two" {
			if !task.ReviewJudge || !reflect.DeepEqual(task.Needs, []string{"lens"}) || !reflect.DeepEqual(task.DependencyInputs, map[string][]string{"lens": {"verdict.json"}}) {
				t.Fatal("wrong judge visibility")
			}
		} else if len(task.Needs) != 0 || len(task.DependencyInputs) != 0 {
			t.Fatal("reviewer isolation lost")
		}
	}
	f.deadline = f.deadline.Add(time.Hour)
	losing, err := f.prepare(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.parent.store.MaterializeReviewChild(ctx, f.frozen, f.checkpoint, losing); err == nil {
		t.Fatal("deadline extended")
	}
	for _, a := range f.artifacts {
		_, reader, err := f.retained.Open(ctx, a.ID)
		if err != nil {
			t.Fatal("loser removed retained content", err)
		}
		reader.Close()
	}
}

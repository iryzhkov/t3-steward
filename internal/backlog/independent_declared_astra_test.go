package backlog

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"

	"github.com/iryzhkov/t3-steward/internal/review"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

type independentDeclaredStore struct {
	*sqlite.Store
	beforeFreeze func()
}

func (s independentDeclaredStore) FreezeDeclaredReviewAuthority(ctx context.Context, f review.FrozenAuthority) (review.FrozenAuthority, error) {
	s.beforeFreeze()
	return s.Store.FreezeDeclaredReviewAuthority(ctx, f)
}

// Complete logical table contents, including native audit; no physical page claim.
func independentDeclaredTables(t *testing.T, db *sql.DB) string {
	t.Helper()
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	var all []string
	for _, name := range names {
		rows, err := db.Query("SELECT * FROM \"" + strings.ReplaceAll(name, "\"", "\"\"") + "\"")
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		var table []string
		for rows.Next() {
			values := make([]any, len(columns))
			dest := make([]any, len(columns))
			for i := range values {
				dest[i] = &values[i]
			}
			if err := rows.Scan(dest...); err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(values)
			if err != nil {
				t.Fatal(err)
			}
			table = append(table, string(raw))
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		sort.Strings(table)
		all = append(all, name+":"+strings.Join(table, "\n"))
	}
	return strings.Join(all, "\n")
}

// Production ResolveDeclared obtains policy and bytes. A separate connection
// then invalidates current records immediately before the real owning writer.
func TestIndependentDeclaredCurrentTransaction(t *testing.T) {
	for _, kind := range []string{"healthy", "revision", "workflow-membership", "assignment-project", "assignment-activation", "criteria-owner"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			f := newDeclaredAdmissionFixture(t)
			db, err := sql.Open("sqlite", f.store.dbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			fired := false
			var before string
			var freshErr error
			f.service.Store = independentDeclaredStore{f.store.Store, func() {
				fired = true
				var query, id string
				switch kind {
				case "revision":
					query = "UPDATE coordinator_attempts SET revision=revision+1,record=json_set(record,'$.revision',revision+1) WHERE id=?"
					id = f.request.AttemptID
				case "workflow-membership":
					query = "UPDATE coordinator_workflows SET record=json_set(record,'$.taskIds',json('[]')) WHERE id=?"
					id = f.records.Workflows[0].ID
				case "assignment-project":
					query = "UPDATE coordinator_assignments SET record=json_set(record,'$.project','foreign') WHERE id=?"
					id = f.records.Assignments[0].ID
				case "assignment-activation":
					query = "UPDATE coordinator_assignments SET record=json_set(record,'$.activationId','foreign') WHERE id=?"
					id = f.records.Assignments[0].ID
				case "criteria-owner":
					query = "UPDATE coordinator_artifacts SET record=json_set(record,'$.workflowRunId','foreign') WHERE id=?"
					id = f.records.Tasks[0].ReviewRequirements.Criteria.ArtifactID
				}
				if query != "" {
					if _, err := db.Exec(query, id); err != nil {
						t.Fatal(err)
					}
				}
				before = independentDeclaredTables(t, db)
				// Still no frozen authority: fresh resolution sees the invalidity.
				_, freshErr = f.service.ResolveDeclared(ctx, declaredRequest(f))
			}}
			got, err := f.service.FreezeDeclared(ctx, declaredRequest(f))
			if !fired {
				t.Fatal("did not reach owning writer")
			}
			t.Logf("kind=%s freeze_error=%v fresh_before_freeze_error=%v issued=%d", kind, err, freshErr, got.Authority.Parent.IssuedRevision)
			if kind == "healthy" {
				if err != nil || freshErr != nil {
					t.Fatal(err, freshErr)
				}
				return
			}
			after := independentDeclaredTables(t, db)
			if err == nil {
				t.Errorf("owning freeze accepted invalidated %s", kind)
			}
			if kind != "revision" && freshErr == nil {
				t.Error("fresh pre-freeze resolution did not refuse")
			}
			if err != nil && before != after {
				t.Error("refusal leaked logical tables/native audit")
			}
			reopened, e := sqlite.OpenMigrated(f.store.dbPath)
			if e != nil {
				t.Fatal(e)
			}
			defer reopened.Close()
			if after != independentDeclaredTables(t, db) {
				t.Fatal("reopen changed logical state")
			}
			f.service.Store = reopened
			if kind == "revision" {
				if freshErr != nil {
					t.Fatal(freshErr)
				}
				retry, e := f.service.FreezeDeclared(ctx, declaredRequest(f))
				if e != nil || retry.Authority.Parent.IssuedRevision != 2 {
					t.Fatal("fresh retry", e)
				}
			} else if err == nil {
				replay, e := f.service.FreezeDeclared(ctx, declaredRequest(f))
				t.Logf("invalid original-authority replay after reopen: error=%v equal=%v", e, reflect.DeepEqual(got, replay))
				if e == nil {
					t.Error("invalid authority remained replayable after reopen")
				}
			}
		})
	}
}

func TestIndependentDeclaredReceipt(t *testing.T) {
	f := newDeclaredAdmissionFixture(t)
	ctx := context.Background()
	service := SubmissionService{Permanent: declarationValidator{f.catalog}}
	receipt, err := service.validatePermanent(ctx, f.source)
	if err != nil {
		t.Fatal(err)
	}
	m, err := LoadManifest(f.source)
	if err != nil {
		t.Fatal(err)
	}
	if err = receipt.ValidatePermanent(ctx, m); err != nil {
		t.Fatal(err)
	}
	m.Tasks["inspect"].ReviewRequirements.Members[0].Route = "forged/model"
	if err = receipt.ValidatePermanent(ctx, m); err == nil {
		t.Fatal("receipt accepted mutated policy")
	}
	// Actual ingestion of changed, still schema-valid bytes must refuse before publication.
	rewriteBundleManifest(t, f.source, strings.Replace(declaredManifestYAML(), "codex/org/sol", "forged/model", 1))
	before, err := f.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = (BundleIngester{Store: f.store, StorageRoot: f.service.Artifacts.SubmissionRoot, Permanent: receipt}).Ingest(ctx, f.source)
	if err == nil || !strings.Contains(err.Error(), "manifest changed") {
		t.Fatal("changed receipt ingestion", err)
	}
	after, e := f.store.LoadCoordinatorRecords(ctx)
	if e != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("refusal published records", e)
	}
	rewriteBundleManifest(t, f.source, declaredManifestYAML())
	if _, err = (BundleIngester{Store: f.store, StorageRoot: f.service.Artifacts.SubmissionRoot, Permanent: receipt}).Ingest(ctx, f.source); err != nil {
		t.Fatal(fmt.Errorf("unchanged receipt ingestion: %w", err))
	}
}

func TestIndependentDeclaredGraphCustody(t *testing.T) {
	for _, operation := range []string{"clone", "rerun"} {
		for _, kind := range []string{"healthy", "weaken", "old-custody", "wrong-reference"} {
			t.Run(operation+"/"+kind, func(t *testing.T) {
				ctx := context.Background()
				f := newDeclaredAdmissionFixture(t)
				source := f.records.WorkflowRuns[0]
				if operation == "rerun" {
					source.Progress = domain.ProgressFailed
					source.Revision++
					if err := f.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{source}}); err != nil {
						t.Fatal(err)
					}
				}
				runID := "run:" + operation + ":independent"
				raw, _ := json.Marshal(f.records.Tasks)
				var tasks []domain.Task
				if err := json.Unmarshal(raw, &tasks); err != nil {
					t.Fatal(err)
				}
				remap := map[string]string{}
				for i := range tasks {
					old := tasks[i].ID
					tasks[i].ID = "copy-" + old
					remap[old] = tasks[i].ID
					tasks[i].RunID = runID
					tasks[i].DefinitionRevision = 1
				}
				artifacts := map[string]string{}
				var inputs []domain.Artifact
				for _, a := range f.records.Artifacts {
					old := a.ID
					a.ID = "copy-" + old
					artifacts[old] = a.ID
					a.WorkflowRunID = runID
					if a.TaskID != "" {
						a.TaskID = remap[a.TaskID]
					}
					inputs = append(inputs, a)
				}
				for i := range tasks {
					task := &tasks[i]
					task.PromptArtifactID = artifacts[task.PromptArtifactID]
					// The resolver fixture omits executor routes; graph commits require one.
					task.Routes = []domain.ProviderRoute{f.records.Assignments[0].Route}
					if task.MaxTurns < 1 {
						task.MaxTurns = 1
					}
					for j, id := range task.InputArtifactIDs {
						task.InputArtifactIDs[j] = artifacts[id]
					}
					d := task.ReviewRequirements
					d.Criteria.ArtifactID = artifacts[d.Criteria.ArtifactID]
					d.Criteria.RunID = runID
					d.Criteria.TaskID = task.ID
				}
				switch kind {
				case "weaken":
					tasks[0].ReviewRequirements.RoundLimit = 1
				case "old-custody":
					tasks[0].ReviewRequirements.Criteria.RunID = source.ID
				case "wrong-reference":
					for i := range inputs {
						if inputs[i].ID == tasks[0].ReviewRequirements.Criteria.ArtifactID {
							inputs[i].Producer = "worker"
						}
					}
				}
				c := sqlite.GraphCommit{Request: domain.GraphAmendment{ID: "independent", RunID: source.ID, ExpectedRevision: source.GraphRevision, Operation: operation, Reason: "independent custody probe"}, Actor: "reviewer", Before: source, Tasks: tasks, Inputs: inputs, TaskIDRemap: remap, Now: time.Now().UTC()}
				if operation == "rerun" {
					c.Request.TaskID = f.records.Tasks[0].ID
					c.Rerun = &domain.RerunProvenance{SourceRunID: source.ID, SourceTaskID: c.Request.TaskID, SourceAttemptID: f.request.AttemptID, IdempotencyKey: c.Request.ID, Reason: c.Request.Reason}
				}
				db, err := sql.Open("sqlite", f.store.dbPath)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				before := independentDeclaredTables(t, db)
				var result domain.GraphAmendmentResult
				if operation == "clone" {
					result, err = f.store.CommitGraphClone(ctx, c)
				} else {
					result, err = f.store.CommitGraphRerun(ctx, c)
				}
				if kind == "healthy" {
					if err != nil {
						t.Fatal(err)
					}
					if result.Graph.Tasks[0].ReviewRequirements.Criteria.RunID != runID {
						t.Fatal("custody not rebound")
					}
				} else {
					if err == nil || !strings.Contains(err.Error(), "review") {
						t.Fatal("forged graph did not reach declaration guard", err)
					}
					t.Logf("owning graph writer refused: %v", err)
					if before != independentDeclaredTables(t, db) {
						t.Fatal("graph refusal leaked logical tables/audit")
					}
					reopened, e := sqlite.OpenMigrated(f.store.dbPath)
					if e != nil {
						t.Fatal(e)
					}
					defer reopened.Close()
					if before != independentDeclaredTables(t, db) {
						t.Fatal("graph rollback lost on reopen")
					}
				}
			})
		}
	}
}

func TestIndependentDeclaredSchemaGlobAndCopy(t *testing.T) {
	ctx := context.Background()
	f := newAdmissionFixture(t)
	raw := strings.Replace(declaredManifestYAML(), "inputs: [inputs/criteria.md]", "inputs: [inputs/*.md]", 1)
	raw += "  ordinary:\n    prompt_file: prompts/inspect.md\n"
	rewriteBundleManifest(t, f.source, raw)
	m, err := LoadManifest(f.source)
	if err != nil {
		t.Fatal(err)
	}
	if m.Tasks["ordinary"].ReviewRequirements != nil {
		t.Fatal("implicit review inheritance")
	}
	result, err := (BundleIngester{Store: f.store, StorageRoot: f.service.Artifacts.SubmissionRoot, Permanent: declarationValidator{f.catalog}}).Ingest(ctx, f.source)
	if err != nil {
		t.Fatal(err)
	}
	var reviewed domain.Task
	for _, task := range result.Records.Tasks {
		if task.Name == "ordinary" && task.ReviewRequirements != nil {
			t.Fatal("ingestion inherited policy")
		}
		if task.Name == "inspect" {
			reviewed = task
		}
	}
	if reviewed.ReviewRequirements == nil || reviewed.ReviewRequirements.Criteria.SHA256 != admissionDigestBytes([]byte("retained criteria")) {
		t.Fatal("glob criteria lost")
	}
	run := result.Records.WorkflowRuns[0]
	run.Graph = &domain.GraphDefinition{Tasks: []domain.Task{reviewed}}
	original := domain.GraphDigest(run.Graph.Tasks)
	selected := domain.TasksForRun(run, nil)
	selected[0].ReviewRequirements.Members[0].Role = "forged"
	selected[0].ReviewRequirements.Criteria.RunID = "foreign"
	if domain.GraphDigest(run.Graph.Tasks) != original || domain.GraphDigest(selected) == original {
		t.Fatal("declaration copy/digest failed")
	}
	for _, field := range []string{"provider_family", "tier", "digest", "input_digest", "base_commit"} {
		invalid := strings.Replace(raw, "version: 1", "version: 1\n      "+field+": forged", 1)
		if _, err := ParseManifest([]byte(invalid)); err == nil {
			t.Fatal("unknown authority field admitted", field)
		}
	}
	if err := domain.ValidateGraphAmendment(domain.GraphAmendment{ID: "add", RunID: run.ID, ExpectedRevision: 1, Operation: "task-add", Reason: "probe", Prompt: "prompt", Task: &reviewed}); err == nil || !strings.Contains(err.Error(), "compiled review authority") {
		t.Fatal("public task-add admitted compiled policy", err)
	}
}

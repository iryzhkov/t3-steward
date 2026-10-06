package sqlite

import (
	"context"
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/pinnedinput"
	"github.com/iryzhkov/t3-steward/internal/review"
)

func executionChildFixture(t *testing.T) (*Store, review.FrozenAuthority, review.CheckpointAuthority, review.ChildPreparation, []review.RetainedFile) {
	t.Helper()
	s, f := reviewAuthorityFixture(t)
	raw := []byte("exact frozen criteria")
	spec := f.Requirements
	for i := range spec.Members {
		spec.Members[i].Execution = &domain.ReviewExecutionProfile{Effort: "medium", QuotaPoolID: "review-pool", MaxTurns: 9, Resources: domain.ResourceDemand{MinCPUClass: domain.CPUClassMedium, PreferredCPUClass: domain.CPUClassHigh, CPUUnits: 1.5, MemoryMB: 512, ScratchMB: 64}, ResourcePreset: "build"}
	}
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

func executionSQLSnapshot(t *testing.T, s *Store) string {
	t.Helper()
	rows, err := s.db.Query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	var result []string
	for _, name := range names {
		rows, err := s.db.Query("SELECT * FROM \"" + strings.ReplaceAll(name, "\"", "\"\"") + "\"")
		if err != nil {
			t.Fatal(err)
		}
		cols, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		var table []string
		for rows.Next() {
			values := make([]any, len(cols))
			dest := make([]any, len(cols))
			for i := range values {
				dest[i] = &values[i]
			}
			if err = rows.Scan(dest...); err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(values)
			if err != nil {
				t.Fatal(err)
			}
			table = append(table, string(raw))
		}
		if err = rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		sort.Strings(table)
		result = append(result, name+":"+strings.Join(table, "\n"))
	}
	return strings.Join(result, "\n")
}
func TestReviewExecutionProfileSQLiteFreshReopenedTerminal(t *testing.T) {
	for _, reopen := range []bool{false, true} {
		t.Run(map[bool]string{false: "fresh", true: "reopened"}[reopen], func(t *testing.T) {
			ctx := context.Background()
			s, f, cp, p, _ := executionChildFixture(t)
			path := s.path
			if reopen {
				s.Close()
				var err error
				s, err = OpenMigrated(path)
				if err != nil {
					t.Fatal(err)
				}
				defer s.Close()
			}
			first, err := s.MaterializeReviewChild(ctx, f, cp, p)
			if err != nil {
				t.Fatal(err)
			}
			for i, task := range first.Graph.Tasks {
				profile := f.Requirements.Members[i].Execution
				if task.MaxTurns != profile.MaxTurns || task.ResourceDemand != profile.Resources || task.ResourcePreset != "build" || task.Routes[0].QuotaPoolID != profile.QuotaPoolID || !reflect.DeepEqual(task.Routes[0].Options, map[string]string{"effort": profile.Effort}) {
					t.Fatal("lost exact child profile")
				}
			}
			before := executionSQLSnapshot(t, s)
			next, err := s.MaterializeReviewChild(ctx, f, cp, p)
			if err != nil || !reflect.DeepEqual(first, next) || before != executionSQLSnapshot(t, s) {
				t.Fatal("replay mutated SQL", err)
			}
			// Build and reply maps are independent of original authority and each other.
			next.Graph.Tasks[0].Routes[0].Options["effort"] = "high"
			if first.Graph.Tasks[0].Routes[0].Options["effort"] != "medium" || first.Graph.Tasks[1].Routes[0].Options["effort"] != "medium" || f.Requirements.Members[0].Execution.Effort != "medium" {
				t.Fatal("Build options alias")
			}
			a := first.Graph.Attempts[0]
			a.Progress = domain.ProgressSucceeded
			a.Control = domain.ControlStopped
			a.Revision++
			ended := time.Now().UTC()
			a.CompletedAt = &ended
			run := first.Graph.Run
			run.Progress = domain.ProgressActive
			run.Revision++
			if err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{a}, WorkflowRuns: []domain.WorkflowRun{run}}); err != nil {
				t.Fatal(err)
			}
			round, err := s.GetReviewRound(ctx, cp.RoundID)
			if err != nil {
				t.Fatal(err)
			}
			_, err = s.RecordReviewResult(ctx, cp.RoundID, round.Reviewers[0].ID, round.Revision, review.Result{State: "succeeded", ReviewMD: "Reviewed", VerdictJSON: []byte(`{"schema":"review-verdict/v1","verdict":"accept","findings":[],"inputManifestDigest":"` + cp.Checkpoint.InputDigest + `","reviewerRoute":"` + round.Reviewers[0].Route + `"}`)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.db.Exec("UPDATE coordinator_attempts SET record=json_set(record,'$.progress','succeeded','$.control','stopped') WHERE id=?", f.Parent.AttemptID); err != nil {
				t.Fatal(err)
			}
			before = executionSQLSnapshot(t, s)
			s.Close()
			s, err = OpenReadOnly(path)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			next, err = s.MaterializeReviewChild(ctx, f, cp, p)
			if err != nil || !reflect.DeepEqual(first, next) || before != executionSQLSnapshot(t, s) {
				t.Fatal("terminal read-only replay changed SQL", err)
			}
		})
	}
}
func TestReviewExecutionProfileSQLiteEveryFieldTamperRefused(t *testing.T) {
	paths := map[string]any{"$.maxTurns": 10, "$.routes[0].options.effort": "high", "$.routes[0].quotaPoolId": "different", "$.resourceDemand.minCpuClass": "low", "$.resourceDemand.preferredCpuClass": "medium", "$.resourceDemand.cpuUnits": 2.5, "$.resourceDemand.memoryMb": 513, "$.resourceDemand.scratchMb": 65}
	for _, target := range []string{"template", "graph", "run-graph", "receipt"} {
		for path, value := range paths {
			t.Run(target+"/"+path, func(t *testing.T) {
				s, f, cp, p, _ := executionChildFixture(t)
				ctx := context.Background()
				first, err := s.MaterializeReviewChild(ctx, f, cp, p)
				if err != nil {
					t.Fatal(err)
				}
				switch target {
				case "template":
					_, err = s.db.Exec("UPDATE coordinator_tasks SET record=json_set(record,?,?) WHERE id=?", path, value, first.Graph.Tasks[0].ID)
				case "graph":
					before := executionSQLSnapshot(t, s)
					if _, e := s.db.Exec("UPDATE coordinator_graph_revisions SET record=json_set(record,?,?) WHERE run_id=?", "$.tasks[0]"+strings.TrimPrefix(path, "$"), value, cp.RoundID); e == nil {
						t.Fatal("original graph writable")
					}
					if before != executionSQLSnapshot(t, s) {
						t.Fatal("graph refusal mutated SQL")
					}
					if _, err = s.db.Exec("DROP TRIGGER immutable_graph_revision"); err != nil {
						t.Fatal(err)
					}
					_, err = s.db.Exec("UPDATE coordinator_graph_revisions SET record=json_set(record,?,?) WHERE run_id=?", "$.tasks[0]"+strings.TrimPrefix(path, "$"), value, cp.RoundID)
				case "run-graph":
					_, err = s.db.Exec("UPDATE coordinator_workflow_runs SET record=json_set(record,?,?) WHERE id=?", "$.graph.tasks[0]"+strings.TrimPrefix(path, "$"), value, cp.RoundID)
				case "receipt":
					// First prove immutable marker update is refused. Then emulate corrupt
					// persisted bytes in this disposable database, without changing production.
					before := executionSQLSnapshot(t, s)
					if _, e := s.db.Exec("UPDATE coordinator_review_materializations SET record=json_set(record,?,?) WHERE checkpoint_id=?", "$.Graph.Tasks[0]"+strings.TrimPrefix(path, "$"), value, cp.Key()); e == nil {
						t.Fatal("immutable marker writable")
					}
					if before != executionSQLSnapshot(t, s) {
						t.Fatal("marker refusal mutated SQL")
					}
					if _, err = s.db.Exec("DROP TRIGGER immutable_review_materialization_update"); err == nil {
						_, err = s.db.Exec("UPDATE coordinator_review_materializations SET record=json_set(record,?,?) WHERE checkpoint_id=?", "$.Graph.Tasks[0]"+strings.TrimPrefix(path, "$"), value, cp.Key())
					}
				}
				if err != nil {
					t.Fatal(err)
				}
				before := executionSQLSnapshot(t, s)
				if _, err = s.MaterializeReviewChild(ctx, f, cp, p); err == nil {
					t.Fatal("changed complete definition accepted")
				}
				if before != executionSQLSnapshot(t, s) {
					t.Fatal("tamper refusal repaired or mutated logical SQL/native audit")
				}
			})
		}
	}
}
func TestReviewExecutionProfileSQLiteAuthorityChangeRefused(t *testing.T) {
	changes := []func(*domain.ReviewExecutionProfile){
		func(p *domain.ReviewExecutionProfile) { p.Effort = "high" }, func(p *domain.ReviewExecutionProfile) { p.QuotaPoolID = "other-pool" }, func(p *domain.ReviewExecutionProfile) { p.MaxTurns++ },
		func(p *domain.ReviewExecutionProfile) { p.Resources.MinCPUClass = domain.CPUClassLow }, func(p *domain.ReviewExecutionProfile) { p.Resources.PreferredCPUClass = domain.CPUClassMedium },
		func(p *domain.ReviewExecutionProfile) { p.Resources.CPUUnits++ }, func(p *domain.ReviewExecutionProfile) { p.Resources.MemoryMB++ }, func(p *domain.ReviewExecutionProfile) { p.Resources.ScratchMB++ }}
	for _, materialized := range []bool{false, true} {
		for i, change := range changes {
			t.Run(fmtProfileCase(materialized, i), func(t *testing.T) {
				s, f, cp, p, _ := executionChildFixture(t)
				ctx := context.Background()
				if materialized {
					if _, err := s.MaterializeReviewChild(ctx, f, cp, p); err != nil {
						t.Fatal(err)
					}
				}
				changed, err := f.Canonical()
				if err != nil {
					t.Fatal(err)
				}
				change(changed.Requirements.Members[0].Execution)
				r, err := review.NewRequirements(changed.Requirements)
				if err != nil {
					t.Fatal(err)
				}
				changed.RequirementsDigest = r.Digest()
				before := executionSQLSnapshot(t, s)
				if _, err = s.MaterializeReviewChild(ctx, changed, cp, p); err == nil {
					t.Fatal("rehashed profile replaced original")
				}
				if _, err = s.AllocateReviewCheckpoint(ctx, changed, cp.Checkpoint); err == nil {
					t.Fatal("same-checkpoint authority replaced")
				}
				if before != executionSQLSnapshot(t, s) {
					t.Fatal("authority refusal mutated SQL")
				}
			})
		}
	}
}
func TestReviewExecutionProfileLegacyBuildReplay(t *testing.T) {
	s, f, cp, p, _ := childFixture(t)
	ctx := context.Background()
	first, err := s.MaterializeReviewChild(ctx, f, cp, p)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range first.Graph.Tasks {
		if task.MaxTurns != 12 || !task.ResourceDemand.IsZero() || task.Routes[0].QuotaPoolID != "" || task.Routes[0].Options != nil {
			t.Fatal("historical default graph changed")
		}
	}
	before := executionSQLSnapshot(t, s)
	path := s.path
	s.Close()
	s, err = OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	next, err := s.MaterializeReviewChild(ctx, f, cp, p)
	if err != nil || !reflect.DeepEqual(first, next) || before != executionSQLSnapshot(t, s) {
		t.Fatal("historical replay changed", err)
	}
}
func fmtProfileCase(materialized bool, i int) string {
	return map[bool]string{false: "allocation", true: "materialized"}[materialized] + "/" + string(rune('a'+i))
}

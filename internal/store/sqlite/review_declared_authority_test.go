package sqlite

import (
	"context"
	"encoding/json"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/pinnedinput"
	"github.com/iryzhkov/t3-steward/internal/review"
	"reflect"
	"testing"
)

func declaredAuthorityFixture(t *testing.T) (*Store, review.FrozenAuthority) {
	s, f := reviewAuthorityFixture(t)
	ctx := context.Background()
	records, err := s.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	task := records.Tasks[0]
	run := records.WorkflowRuns[0]
	workflow := records.Workflows[0]
	// This declared fixture needs current run membership and project custody;
	// the trusted legacy fixture deliberately leaves those fields unspecified.
	workflow.TaskIDs = []string{task.ID}
	task.RunID = run.ID
	a := domain.Artifact{ID: "declared-criteria", WorkflowRunID: run.ID, Kind: domain.ArtifactInput, Producer: "submission", Name: "criteria.md", Size: 5, SHA256: f.Requirements.CriteriaDigest}
	m, err := pinnedinput.NewManifest([]pinnedinput.Entry{{Name: a.Name, Size: a.Size, SHA256: a.SHA256}})
	if err != nil {
		t.Fatal(err)
	}
	workflow.Environment = domain.ExecutionEnvironment{Type: "git", Ref: f.Parent.BaseCommit}
	workflow.InputManifest = &m
	workflow.InputArtifactIDs = []string{a.ID}
	run.InputArtifactIDs = []string{a.ID}
	task.InputArtifactIDs = []string{a.ID}
	d := &domain.TaskReviewRequirements{Version: 1, Risk: f.Requirements.Risk, RequiredReviewers: f.Requirements.RequiredReviewers, MinProviderFamilies: f.Requirements.MinProviderFamilies, RoundLimit: f.Requirements.RoundLimit, Criteria: domain.ReviewCriteria{ArtifactID: a.ID, RunID: run.ID, WorkflowID: workflow.ID, TaskID: task.ID, Name: a.Name, Size: a.Size, SHA256: a.SHA256}}
	for _, member := range f.Requirements.Members {
		d.Members = append(d.Members, domain.TaskReviewMember{ID: member.ID, Role: member.Role, Route: member.Route, Required: member.Required})
	}
	task.ReviewRequirements = d
	f.DeclarationDigest = domain.TaskDigest(task)
	f.AdmissionProvenance, _ = json.Marshal(struct {
		InputManifest pinnedinput.Manifest
		PolicyDigest  string
	}{m, f.Requirements.PolicyDigest})
	as := records.Assignments[0]
	as.Project = workflow.Project
	as.TaskDigest = domain.TaskDigest(task)
	as.TaskRevision = task.DefinitionRevision
	as.GraphRevision = run.GraphRevision
	if err = s.SaveCoordinatorRecords(ctx, CoordinatorRecords{Workflows: []domain.Workflow{workflow}, WorkflowRuns: []domain.WorkflowRun{run}, Tasks: []domain.Task{task}, Artifacts: []domain.Artifact{a}, Assignments: []domain.Assignment{as}}); err != nil {
		t.Fatal(err)
	}
	return s, f
}
func TestReviewDeclaredAuthorityOwningTransactionRollbackReopen(t *testing.T) {
	ctx := context.Background()
	for _, kind := range []string{"healthy", "advanced before initial freeze", "ended", "assignment epoch", "source", "declaration", "input manifest", "input membership", "policy forgery"} {
		t.Run(kind, func(t *testing.T) {
			s, f := declaredAuthorityFixture(t)
			other, err := openMigratedFixture(s.path)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "advanced before initial freeze":
				_, err = other.db.Exec("UPDATE coordinator_attempts SET revision=revision+1,record=json_set(record,'$.revision',revision+1) WHERE id=?", f.Parent.AttemptID)
			case "ended":
				_, err = other.db.Exec("UPDATE coordinator_attempts SET record=json_set(record,'$.progress','failed','$.control','stopped') WHERE id=?", f.Parent.AttemptID)
			case "assignment epoch":
				_, err = other.db.Exec("UPDATE coordinator_assignments SET record=json_set(record,'$.epoch',2) WHERE id=?", f.Parent.AssignmentID)
			case "source":
				_, err = other.db.Exec("UPDATE coordinator_workflows SET record=json_set(record,'$.environment.ref','main') WHERE id='w'")
			case "declaration":
				_, err = other.db.Exec("UPDATE coordinator_tasks SET record=json_set(record,'$.reviewRequirements.roundLimit',1) WHERE id=?", f.Parent.TaskID)
			case "input manifest":
				_, err = other.db.Exec("UPDATE coordinator_workflows SET record=json_set(record,'$.inputManifest.digest','bad') WHERE id='w'")
			case "input membership":
				_, err = other.db.Exec("UPDATE coordinator_workflows SET record=json_set(record,'$.inputArtifactIds',json('[]')) WHERE id='w'")
			case "policy forgery":
				f.Requirements.RoundLimit = 1
				r, e := review.NewRequirements(f.Requirements)
				if e != nil {
					t.Fatal(e)
				}
				f.RequirementsDigest = r.Digest()
			}
			other.Close()
			if err != nil {
				t.Fatal(err)
			}
			before := cancellationSnapshot(t, s)
			got, err := s.FreezeDeclaredReviewAuthority(ctx, f)
			if kind == "healthy" {
				if err != nil || !reflect.DeepEqual(got, f) {
					t.Fatal("healthy freeze failed", err)
				}
			} else {
				if err == nil {
					t.Fatal("stale/forged declaration accepted")
				}
				t.Logf("owning transaction refused: %v", err)
				if before != cancellationSnapshot(t, s) {
					t.Fatal("refusal leaked full tables/native audit")
				}
				path := s.path
				if err = s.Close(); err != nil {
					t.Fatal(err)
				}
				s, err = openMigratedFixture(path)
				if err != nil {
					t.Fatal(err)
				}
				defer s.Close()
				if before != cancellationSnapshot(t, s) {
					t.Fatal("rollback changed on reopen")
				}
			}
		})
	}
}

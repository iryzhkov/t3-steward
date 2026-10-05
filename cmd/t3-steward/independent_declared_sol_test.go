package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

func independentDeclaredProduction(t *testing.T) (*backlogadmin.Service, *sqlite.Store, coordinatorDeclaredReviewAdmission, sqlite.CoordinatorRecords, string) {
	t.Helper()
	ctx := context.Background()
	admin, db := probeReadinessService(t, nil, "homelab")
	submissions := probeSubmissions(t, admin, db)
	cfg := declaredCoordinatorSettings(submissions.StorageRoot)
	catalog, err := workerruntime.NewConfiguredAdmissionCatalog(cfg)
	if err != nil {
		t.Fatal(err)
	}
	submissions.Permanent = coordinatorPermanentValidator{admin: admin, reviews: catalog}
	bundle := probeCampaignFixture(t)
	raw := strings.Replace(probeCampaignManifest, "project: dev-fleet", "project: dev-fleet\n  ref: "+strings.Repeat("c", 40), 1)
	raw += "    review_requirements:\n      version: 1\n      risk: routine\n      criteria_file: inputs/plan.md\n      required_reviewers: 2\n      min_provider_families: 2\n      members:\n        - {id: a, role: independent, route: t3-primary/opus, required: true}\n        - {id: b, role: independent, route: other/model, required: true}\n"
	if err = os.WriteFile(filepath.Join(bundle, "workflow.yaml"), []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = submissions.SubmitDirectory(ctx, backlog.DirectorySubmission{IdempotencyKey: "independent", BundleDir: bundle}); err != nil {
		t.Fatal(err)
	}
	entry, err := newCoordinatorDeclaredReviewAdmission(cfg, db)
	if err != nil {
		t.Fatal(err)
	}
	admin.SetGraphAmendmentSupport(filepath.Join(t.TempDir(), "graph-inputs"), func(w domain.Workflow, t domain.Task) error { _, e := entry.service.Projects.Resolve(w, t); return e })
	admin.SetArtifactOpener(func(ctx context.Context, id string) (domain.Artifact, io.ReadCloser, error) {
		return entry.service.Artifacts.Open(ctx, id)
	})
	records, err := db.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return admin, db, entry, records, bundle
}
func TestIndependentDeclaredProductionCloneRerunCustody(t *testing.T) {
	for _, operation := range []string{"clone", "rerun"} {
		t.Run(operation, func(t *testing.T) {
			ctx := context.Background()
			admin, db, entry, records, bundle := independentDeclaredProduction(t)
			source := records.WorkflowRuns[0]
			original := records.Tasks[0]
			if operation == "rerun" {
				source.Progress = domain.ProgressFailed
				source.Revision++
				a := records.Attempts[0]
				a.Progress = domain.ProgressFailed
				a.Control = domain.ControlStopped
				a.Revision = 1
				if err := db.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{source}, Attempts: []domain.Attempt{a}}); err != nil {
					t.Fatal(err)
				}
			}
			principal := backlogadmin.Principal{ID: "local:1000", Roles: []string{backlogadmin.LocalAdminRole}}
			request := domain.GraphAmendment{ID: "independent-" + operation, RunID: source.ID, TaskID: original.ID, ExpectedRevision: source.GraphRevision, Operation: operation, Reason: "independent custody"}
			if operation == "clone" {
				request.TaskID = ""
			}
			result, err := admin.AmendGraph(ctx, principal, request)
			if err != nil {
				t.Fatal(err)
			}
			task := result.Graph.Tasks[0]
			d := task.ReviewRequirements
			if d.Criteria.RunID != result.Run.ID || d.Criteria.TaskID != task.ID || d.Criteria.ArtifactID == original.ReviewRequirements.Criteria.ArtifactID {
				t.Fatal("old criteria identity reused")
			}
			// Original submitted mutable file has no authority after retention.
			if err = os.WriteFile(filepath.Join(bundle, "inputs/plan.md"), []byte("untrusted changed source"), 0600); err != nil {
				t.Fatal(err)
			}
			loaded, err := db.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var parent domain.Attempt
			for _, a := range loaded.Attempts {
				if a.WorkflowRunID == result.Run.ID {
					parent = a
				}
			}
			parent.Progress = domain.ProgressActive
			parent.Control = domain.ControlRunning
			parent.ThreadID = "independent-thread"
			parent.AssignmentID = "independent-assignment"
			assignment := domain.Assignment{ID: parent.AssignmentID, DispatchToken: "independent-token", AttemptID: parent.ID, Epoch: 1, State: domain.AssignmentClaimed, ThreadID: parent.ThreadID, Project: "dev-fleet", TaskDigest: domain.TaskDigest(task), TaskRevision: task.DefinitionRevision, GraphRevision: result.Run.GraphRevision, Route: domain.ProviderRoute{ProviderInstanceID: "t3-primary", Model: "opus"}}
			if err = db.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{parent}, Assignments: []domain.Assignment{assignment}}); err != nil {
				t.Fatal(err)
			}
			frozen, err := entry.FreezeDeclared(ctx, backlog.DeclaredAdmissionRequest{RunID: result.Run.ID, TaskID: task.ID, AttemptID: parent.ID})
			if err != nil {
				t.Fatal(err)
			}
			if frozen.Provenance.Criteria.SHA256 != original.ReviewRequirements.Criteria.SHA256 || frozen.Provenance.Criteria.RunID != result.Run.ID {
				t.Fatal("custody/content lost")
			}
			// Returned graph/declaration mutation must not affect immutable reload or replay.
			result.Graph.Tasks[0].ReviewRequirements.Members[0].Route = "forged/model"
			again, err := admin.AmendGraph(ctx, principal, request)
			if err != nil {
				t.Fatal(err)
			}
			if !again.Replay || again.Graph.Tasks[0].ReviewRequirements.Members[0].Route == "forged/model" {
				t.Fatal("graph reply aliases persistent authority")
			}
			fresh, err := entry.FreezeDeclared(ctx, backlog.DeclaredAdmissionRequest{RunID: again.Run.ID, TaskID: task.ID, AttemptID: parent.ID})
			if err != nil || !reflect.DeepEqual(frozen, fresh) {
				t.Fatalf("frozen custody replay: %v", err)
			}
		})
	}
}

func TestIndependentDeclaredProductionRejectChangedCloneRerun(t *testing.T) {
	for _, operation := range []string{"clone", "rerun"} {
		for _, drift := range []string{"policy", "criteria", "custody"} {
			t.Run(operation+"/"+drift, func(t *testing.T) {
				ctx := context.Background()
				admin, db, _, records, _ := independentDeclaredProduction(t)
				run := records.WorkflowRuns[0]
				task := records.Tasks[0]
				if operation == "rerun" {
					run.Progress = domain.ProgressFailed
					run.Revision++
					a := records.Attempts[0]
					a.Progress = domain.ProgressFailed
					a.Control = domain.ControlStopped
					a.Revision = 1
					if err := db.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}, Attempts: []domain.Attempt{a}}); err != nil {
						t.Fatal(err)
					}
				}
				admin.SetGraphAmendmentSupport(filepath.Join(t.TempDir(), "graph-inputs"), func(_ domain.Workflow, t domain.Task) error {
					switch drift {
					case "policy":
						t.ReviewRequirements.RoundLimit = 1
					case "criteria":
						t.ReviewRequirements.Criteria.SHA256 = strings.Repeat("0", 64)
					case "custody":
						t.ReviewRequirements.Criteria.RunID = "foreign"
					}
					return nil
				})
				before, err := db.LoadCoordinatorRecords(ctx)
				if err != nil {
					t.Fatal(err)
				}
				audit, err := db.LoadAuditEvents(ctx, "")
				if err != nil {
					t.Fatal(err)
				}
				request := domain.GraphAmendment{ID: "bad-" + operation + "-" + drift, RunID: run.ID, TaskID: task.ID, ExpectedRevision: run.GraphRevision, Operation: operation, Reason: "adversarial candidate"}
				if operation == "clone" {
					request.TaskID = ""
				}
				if _, err = admin.AmendGraph(ctx, backlogadmin.Principal{ID: "local:1000", Roles: []string{backlogadmin.LocalAdminRole}}, request); err == nil {
					t.Fatal("changed immutable declaration published")
				}
				t.Logf("owning graph writer refused: %v", err)
				after, err := db.LoadCoordinatorRecords(ctx)
				if err != nil {
					t.Fatal(err)
				}
				afterAudit, err := db.LoadAuditEvents(ctx, "")
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(audit, afterAudit) {
					t.Fatal("refusal leaked candidate records/audit")
				}
			})
		}
	}
}

type independentDeclaredProductionInterleave struct {
	*sqlite.Store
	mutate func()
}

func (s independentDeclaredProductionInterleave) FreezeDeclaredReviewAuthority(ctx context.Context, f review.FrozenAuthority) (review.FrozenAuthority, error) {
	s.mutate()
	return s.Store.FreezeDeclaredReviewAuthority(ctx, f)
}
func TestIndependentDeclaredProductionCurrentAssignment(t *testing.T) {
	for _, drift := range []string{"healthy", "project", "activation", "released", "epoch"} {
		t.Run(drift, func(t *testing.T) {
			ctx := context.Background()
			_, db, entry, records, _ := independentDeclaredProduction(t)
			task := records.Tasks[0]
			run := records.WorkflowRuns[0]
			parent := records.Attempts[0]
			parent.Progress = domain.ProgressActive
			parent.Control = domain.ControlRunning
			parent.ThreadID = "parent-thread"
			parent.AssignmentID = "parent-assignment"
			parent.Revision = 1
			as := domain.Assignment{ID: parent.AssignmentID, DispatchToken: "parent-token", AttemptID: parent.ID, Epoch: 1, State: domain.AssignmentClaimed, ThreadID: parent.ThreadID, Project: "dev-fleet", TaskDigest: domain.TaskDigest(task), TaskRevision: task.DefinitionRevision, GraphRevision: run.GraphRevision, Route: domain.ProviderRoute{ProviderInstanceID: "t3-primary", Model: "opus"}}
			if err := db.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{parent}, Assignments: []domain.Assignment{as}}); err != nil {
				t.Fatal(err)
			}
			entry.service.Store = independentDeclaredProductionInterleave{db, func() {
				switch drift {
				case "project":
					as.Project = "foreign"
				case "activation":
					as.ActivationID = "foreign-activation"
				case "released":
					as.State = domain.AssignmentReleased
				case "epoch":
					as.Epoch++
				}
				if err := db.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Assignments: []domain.Assignment{as}}); err != nil {
					t.Fatal(err)
				}
			}}
			_, err := entry.FreezeDeclared(ctx, backlog.DeclaredAdmissionRequest{RunID: run.ID, TaskID: task.ID, AttemptID: parent.ID})
			_, found, e := db.GetFrozenReviewAuthority(ctx, run.ID, task.ID)
			if e != nil {
				t.Fatal(e)
			}
			if drift == "healthy" {
				if err != nil || !found {
					t.Fatalf("healthy factory failed: %v stored=%v", err, found)
				}
			} else if err == nil || found {
				t.Errorf("configured factory froze after CURRENT %s drift: err=%v stored=%v", drift, err, found)
			}
		})
	}
}

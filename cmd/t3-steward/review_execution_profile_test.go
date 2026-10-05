package main

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestReviewExecutionProfileConfiguredCloneRerunCustody(t *testing.T) {
	for _, operation := range []string{"clone", "rerun"} {
		t.Run(operation, func(t *testing.T) {
			ctx := context.Background()
			admin, db, entry, records, bundle := executionDeclaredProduction(t)
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
			if d.Version != 2 || !reflect.DeepEqual(d.Members, original.ReviewRequirements.Members) {
				t.Fatal("clone/rerun lost profile")
			}
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
			cfg := declaredCoordinatorSettings(entry.service.Artifacts.SubmissionRoot)
			cfg.Storage.Artifacts = reviewInputTempDir(t)
			owner, e := newCoordinatorDeclaredReviewStaging(cfg, db)
			if e != nil {
				t.Fatal(e)
			}
			req := backlog.DeclaredChildStageRequest{Parent: backlog.DeclaredAdmissionRequest{RunID: result.Run.ID, TaskID: task.ID, AttemptID: parent.ID}, CheckpointID: "cloned", HeadCommit: strings.Repeat("d", 40), Deadline: time.Now().UTC().Add(time.Hour)}
			staged, e := owner.StageDeclared(ctx, req)
			if e != nil {
				t.Fatal(e)
			}
			if !reflect.DeepEqual(staged.Admission, frozen) {
				t.Fatal("clone staging lost original authority")
			}
			graph, e := staged.Preparation.Build(staged.Admission.Authority, staged.Checkpoint, staged.Receipt.CreatedAt)
			for _, child := range graph.Tasks {
				if child.MaxTurns != 7 || child.Routes[0].QuotaPoolID != "pool-1" || child.Routes[0].Options["effort"] != "medium" || child.ResourceDemand.MinCPUClass != domain.CPUClassLow || child.ResourceDemand.MemoryMB != 256 {
					t.Fatal("clone/rerun child lost exact profile")
				}
			}
			if e != nil {
				t.Fatal(e)
			}
			repeated, e := owner.StageDeclared(ctx, req)
			if e != nil || !reflect.DeepEqual(staged, repeated) {
				t.Fatal("clone retained stage changed", e)
			}
			// Returned graph/declaration mutation must not affect immutable reload or replay.
			result.Graph.Tasks[0].ReviewRequirements.Members[0].Execution.Effort = "high"
			again, err := admin.AmendGraph(ctx, principal, request)
			if err != nil {
				t.Fatal(err)
			}
			if !again.Replay || again.Graph.Tasks[0].ReviewRequirements.Members[0].Execution.Effort != "medium" {
				t.Fatal("graph reply aliases persistent authority")
			}
			fresh, err := entry.FreezeDeclared(ctx, backlog.DeclaredAdmissionRequest{RunID: again.Run.ID, TaskID: task.ID, AttemptID: parent.ID})
			if err != nil || !reflect.DeepEqual(frozen, fresh) {
				t.Fatalf("frozen custody replay: %v", err)
			}
		})
	}
}

func TestReviewExecutionProfileConfiguredDrainingUnavailableStatic(t *testing.T) {
	cfg := declaredCoordinatorSettings(t.TempDir())
	w := cfg.Workers["homelab"]
	w.AcceptBacklog = false
	cfg.Workers["homelab"] = w
	c, err := workerruntime.NewConfiguredAdmissionCatalog(cfg)
	if err != nil {
		t.Fatal(err)
	}
	raw := strings.Replace(probeCampaignManifest, "project: dev-fleet", "project: dev-fleet\n  ref: "+strings.Repeat("c", 40), 1)
	raw += "    review_requirements:\n      version: 2\n      risk: routine\n      criteria_file: inputs/plan.md\n      required_reviewers: 2\n      min_provider_families: 2\n      members:\n        - {id: a, role: independent, route: t3-primary/opus, required: true, execution: {effort: high, quota_pool: pool-1, max_turns: 32, resources: {preset: build}}}\n        - {id: b, role: independent, route: other/model, required: true, execution: {effort: medium, quota_pool: pool-1, max_turns: 1, resources: {min_cpu_class: low}}}\n"
	m, err := backlog.ParseManifest([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := c.ReviewAdmissionCatalog(context.Background(), "dev-fleet")
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range catalog.AuthoredWorkers {
		for _, p := range w.Providers {
			_ = p // Authored provider presence is not a live readiness observation.
		}
	}
	if err = backlog.ValidateTaskReviewAdmission(context.Background(), m, c); err != nil {
		t.Fatal("draining/unavailable eligible worker refused statically", err)
	}
	// The configured source is detached even after caller changes pool definitions.
	delete(cfg.QuotaPools, "pool-1")
	if err = backlog.ValidateTaskReviewAdmission(context.Background(), m, c); err != nil {
		t.Fatal("configured snapshot aliases settings", err)
	}
	fresh, err := workerruntime.NewConfiguredAdmissionCatalog(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = backlog.ValidateTaskReviewAdmission(context.Background(), m, fresh); err == nil {
		t.Fatal("missing configured quota definition accepted")
	}
}
func executionDeclaredProduction(t *testing.T) (*backlogadmin.Service, *sqlite.Store, coordinatorDeclaredReviewAdmission, sqlite.CoordinatorRecords, string) {
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
	raw += "    review_requirements:\n      version: 2\n      risk: routine\n      criteria_file: inputs/plan.md\n      required_reviewers: 2\n      min_provider_families: 2\n      members:\n        - {id: a, role: independent, route: t3-primary/opus, required: true, execution: {effort: medium, quota_pool: pool-1, max_turns: 7, resources: {preset: light, memory_mb: 256}}}\n        - {id: b, role: independent, route: other/model, required: true, execution: {effort: medium, quota_pool: pool-1, max_turns: 7, resources: {preset: light, memory_mb: 256}}}\n"
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

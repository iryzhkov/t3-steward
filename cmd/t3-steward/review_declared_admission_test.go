package main

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
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

func declaredCoordinatorSettings(storage string) config.BacklogV2 {
	return config.BacklogV2{
		Storage:       config.V2Storage{Bundles: storage},
		Workers:       map[string]config.V2Worker{"homelab": {Address: "homelab", Credential: "ref:homelab", AcceptBacklog: true, Providers: map[string]config.V2Provider{"t3-primary": {Models: []string{"opus"}, QuotaPool: "pool-1"}, "other": {Models: []string{"model"}, QuotaPool: "pool-1"}}}},
		Projects:      map[string]config.V2Project{"dev-fleet": {Repository: probeRepository, SetupProfile: "go", DefaultRef: "main", Workers: []string{"homelab"}}},
		SetupProfiles: map[string]config.V2SetupProfile{"go": {Commands: []string{"true"}, Timeout: config.Duration(time.Minute)}},
		QuotaPools:    map[string]config.V2QuotaPool{"pool-1": {Provider: "fixture", MaxConcurrent: 2}},
		ReviewRoutes:  map[string]config.ReviewRouteMetadata{"t3-primary/opus": {ProviderFamily: "openai", Tier: "executor"}, "other/model": {ProviderFamily: "fixture-other", Tier: "critical"}},
	}
}
func TestReviewDeclarationSubmissionConfiguredFactory(t *testing.T) {
	ctx := context.Background()
	admin, db := probeReadinessService(t, nil, "homelab")
	submissions := probeSubmissions(t, admin, db)
	cfg := declaredCoordinatorSettings(submissions.StorageRoot)
	catalog, err := workerruntime.NewConfiguredAdmissionCatalog(cfg)
	if err != nil {
		t.Fatal(err)
	}
	bundle := probeCampaignFixture(t)
	raw := strings.Replace(probeCampaignManifest, "project: dev-fleet", "project: dev-fleet\n  ref: "+strings.Repeat("c", 40), 1)
	raw += "    review_requirements:\n      version: 1\n      risk: routine\n      criteria_file: inputs/plan.md\n      required_reviewers: 2\n      min_provider_families: 2\n      members:\n        - {id: a, role: independent, route: t3-primary/opus, required: true}\n        - {id: b, role: independent, route: other/model, required: true}\n"
	if err = os.WriteFile(filepath.Join(bundle, "workflow.yaml"), []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	m, err := backlog.LoadManifest(bundle)
	if err != nil {
		t.Fatal(err)
	}
	// Actual coordinator permanent gate must fail without configured authority, before publication.
	if err = (coordinatorPermanentValidator{admin: admin}).ValidatePermanent(ctx, m); err == nil || !strings.Contains(err.Error(), "configured admission") {
		t.Fatalf("unwired review accepted: %v", err)
	}
	observed := probeWorkerSnapshot("homelab")
	observed.Sequence++
	observed.ObservedAt = observed.ObservedAt.Add(time.Nanosecond)
	observed.Inventory.CatalogRevision = "worker-forged-catalog"
	observed.Inventory.Providers = append(observed.Inventory.Providers, domain.WorkerProviderInventory{InstanceID: "other", Models: []string{"model"}, QuotaPoolID: "pool-1", Available: true})
	if err = db.SaveWorkerSnapshot(ctx, observed); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"metadata", "project", "model"} {
		broken := declaredCoordinatorSettings(submissions.StorageRoot)
		switch bad {
		case "metadata":
			delete(broken.ReviewRoutes, "other/model")
		case "project":
			p := broken.Projects["dev-fleet"]
			p.Repository = "bad"
			broken.Projects["dev-fleet"] = p
		case "model":
			broken.Workers["homelab"].Providers["other"] = config.V2Provider{Models: []string{"alias"}, QuotaPool: "pool-1"}
		}
		c, err := workerruntime.NewConfiguredAdmissionCatalog(broken)
		if err != nil {
			t.Fatal(err)
		}
		submissions.Permanent = coordinatorPermanentValidator{admin: admin, reviews: c}
		if _, err = submissions.SubmitDirectory(ctx, backlog.DirectorySubmission{IdempotencyKey: "bad-" + bad, BundleDir: bundle, Principal: "local:1000"}); err == nil {
			t.Fatal("bad configured submission published")
		}
		if probeWorkflowCount(t, db) != 0 {
			t.Fatal("permanent refusal created workflow")
		}
	}
	submissions.Permanent = coordinatorPermanentValidator{admin: admin, reviews: catalog}
	if _, err = submissions.SubmitDirectory(ctx, backlog.DirectorySubmission{IdempotencyKey: "healthy-declaration", BundleDir: bundle, Principal: "local:1000"}); err != nil {
		t.Fatal(err)
	}
	records, err := db.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	task := records.Tasks[0]
	run := records.WorkflowRuns[0]
	a := records.Attempts[0]
	a.Progress = domain.ProgressActive
	a.Control = domain.ControlRunning
	a.ThreadID = "thread"
	a.AssignmentID = "assignment"
	a.Revision = 1
	as := domain.Assignment{ID: "assignment", DispatchToken: "token", AttemptID: a.ID, Project: "dev-fleet", ThreadID: "thread", Epoch: 1, State: domain.AssignmentClaimed, TaskDigest: domain.TaskDigest(task), TaskRevision: task.DefinitionRevision, GraphRevision: run.GraphRevision, Route: domain.ProviderRoute{ProviderInstanceID: "t3-primary", Model: "opus"}}
	if err = db.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{a}, Assignments: []domain.Assignment{as}}); err != nil {
		t.Fatal(err)
	}
	entry, err := newCoordinatorDeclaredReviewAdmission(cfg, db)
	if err != nil {
		t.Fatal(err)
	}
	frozen, err := entry.FreezeDeclared(ctx, backlog.DeclaredAdmissionRequest{RunID: run.ID, TaskID: task.ID, AttemptID: a.ID})
	if err != nil {
		t.Fatal(err)
	}
	if frozen.Authority.Requirements.Members[0].ProviderFamily != "openai" || frozen.Authority.Requirements.Members[1].ProviderFamily != "fixture-other" {
		t.Fatal("configured metadata not used")
	}
	a.Revision++
	if err = db.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{a}}); err != nil {
		t.Fatal(err)
	}
	again, err := entry.FreezeDeclared(ctx, backlog.DeclaredAdmissionRequest{RunID: run.ID, TaskID: task.ID, AttemptID: a.ID})
	if err != nil || !reflect.DeepEqual(frozen, again) {
		t.Fatal("factory refroze original authority", err)
	}

	admin.SetGraphAmendmentSupport(filepath.Join(t.TempDir(), "graph-inputs"), func(w domain.Workflow, task domain.Task) error {
		_, err := entry.service.Projects.Resolve(w, task)
		return err
	})
	admin.SetArtifactOpener(func(ctx context.Context, id string) (domain.Artifact, io.ReadCloser, error) {
		a, file, err := entry.service.Artifacts.Open(ctx, id)
		return a, file, err
	})
	principal := backlogadmin.Principal{ID: "local:1000", Roles: []string{backlogadmin.LocalAdminRole}}
	cloned, err := admin.AmendGraph(ctx, principal, domain.GraphAmendment{ID: "declared-clone", RunID: run.ID, ExpectedRevision: run.GraphRevision, Operation: "clone", Reason: "fixture clone"})
	if err != nil {
		t.Fatal(err)
	}
	if cloned.Graph.Tasks[0].ReviewRequirements.Criteria.RunID != cloned.Run.ID || cloned.Graph.Tasks[0].ReviewRequirements.Criteria.TaskID == task.ID || cloned.Graph.Tasks[0].ReviewRequirements.Criteria.ArtifactID == task.ReviewRequirements.Criteria.ArtifactID {
		t.Fatal("clone retained old criteria custody")
	}
	a.Progress = domain.ProgressFailed
	a.Control = domain.ControlStopped
	a.Revision++
	run.Progress = domain.ProgressFailed
	run.Revision++
	as.State = domain.AssignmentReleased
	if err = db.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{a}, WorkflowRuns: []domain.WorkflowRun{run}, Assignments: []domain.Assignment{as}}); err != nil {
		t.Fatal(err)
	}
	rerun, err := admin.AmendGraph(ctx, principal, domain.GraphAmendment{ID: "declared-rerun", RunID: run.ID, TaskID: task.ID, ExpectedRevision: run.GraphRevision, Operation: "rerun", Reason: "fixture rerun"})
	if err != nil {
		t.Fatal(err)
	}
	rerunTask := rerun.Graph.Tasks[0]
	d := rerunTask.ReviewRequirements
	if rerun.Graph.RerunOf == nil || rerun.Graph.RerunOf.SourceRunID != run.ID || d.Criteria.RunID != rerun.Run.ID || d.Criteria.ArtifactID == task.ReviewRequirements.Criteria.ArtifactID || d.Criteria.SHA256 != task.ReviewRequirements.Criteria.SHA256 {
		t.Fatal("rerun lost/reused forged provenance")
	}
	reloaded, err := db.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var retry domain.Attempt
	for _, candidate := range reloaded.Attempts {
		if candidate.WorkflowRunID == rerun.Run.ID {
			retry = candidate
		}
	}
	retry.Progress = domain.ProgressActive
	retry.Control = domain.ControlRunning
	retry.ThreadID = "rerun-thread"
	retry.AssignmentID = "rerun-assignment"
	custody := domain.Assignment{ID: retry.AssignmentID, DispatchToken: "rerun-token", AttemptID: retry.ID, Epoch: 1, State: domain.AssignmentClaimed, ThreadID: retry.ThreadID, Project: "dev-fleet", TaskDigest: domain.TaskDigest(rerunTask), TaskRevision: rerunTask.DefinitionRevision, GraphRevision: rerun.Run.GraphRevision, Route: as.Route}
	if err = db.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{retry}, Assignments: []domain.Assignment{custody}}); err != nil {
		t.Fatal(err)
	}
	rerunFrozen, err := entry.FreezeDeclared(ctx, backlog.DeclaredAdmissionRequest{RunID: rerun.Run.ID, TaskID: rerunTask.ID, AttemptID: retry.ID})
	if err != nil {
		t.Fatal(err)
	}
	if rerunFrozen.Provenance.Criteria.RunID != rerun.Run.ID || rerunFrozen.Provenance.Criteria.ArtifactID != d.Criteria.ArtifactID {
		t.Fatal("rerun freeze used old artifact authority")
	}
	for _, candidate := range reloaded.Tasks {
		if candidate.ID == task.ID && !reflect.DeepEqual(candidate.ReviewRequirements, task.ReviewRequirements) {
			t.Fatal("rerun changed source policy")
		}
	}
}

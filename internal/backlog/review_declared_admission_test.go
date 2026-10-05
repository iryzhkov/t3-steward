package backlog

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type declarationValidator struct{ catalog AdmissionCatalogSource }

func (v declarationValidator) ValidatePermanent(ctx context.Context, m Manifest) error {
	return ValidateTaskReviewAdmission(ctx, m, v.catalog)
}
func newDeclaredAdmissionFixture(t *testing.T) admissionFixture {
	f := newAdmissionFixture(t)
	rewriteBundleManifest(t, f.source, declaredManifestYAML())
	root := f.service.Artifacts.SubmissionRoot
	ingested, err := (BundleIngester{Store: f.store, StorageRoot: root, Permanent: declarationValidator{f.catalog}}).Ingest(context.Background(), f.source)
	if err != nil {
		t.Fatal(err)
	}
	f.records = ingested.Records
	a := &f.records.Attempts[0]
	a.Revision = 1
	a.Progress = domain.ProgressActive
	a.Control = domain.ControlRunning
	a.ThreadID = "declared-thread"
	a.AssignmentID = "declared-assignment"
	task := f.records.Tasks[0]
	run := f.records.WorkflowRuns[0]
	f.records.Assignments = []domain.Assignment{{ID: a.AssignmentID, DispatchToken: "declared-token", AttemptID: a.ID, Project: "t3-steward", ThreadID: a.ThreadID, Epoch: 1, State: domain.AssignmentClaimed, Route: domain.ProviderRoute{ProviderInstanceID: "codex", Model: "org/sol"}, TaskDigest: domain.TaskDigest(task), TaskRevision: task.DefinitionRevision, GraphRevision: run.GraphRevision}}
	if err = f.store.SaveCoordinatorRecords(context.Background(), f.records); err != nil {
		t.Fatal(err)
	}
	f.request = AdmissionRequest{RunID: run.ID, TaskID: task.ID, AttemptID: a.ID, Policy: declaredPolicy(task.ReviewRequirements)}
	return f
}
func declaredRequest(f admissionFixture) DeclaredAdmissionRequest {
	return DeclaredAdmissionRequest{f.request.RunID, f.request.TaskID, f.request.AttemptID}
}
func TestReviewDeclarationSQLiteReloadFreezeAndOriginalIssue(t *testing.T) {
	f := newDeclaredAdmissionFixture(t)
	ctx := context.Background()
	reopened, err := sqlite.OpenMigrated(f.store.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	f.service.Store = reopened
	loaded, err := reopened.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var task domain.Task
	for _, v := range loaded.Tasks {
		if v.ID == f.request.TaskID {
			task = v
		}
	}
	if !reflect.DeepEqual(task.ReviewRequirements, f.records.Tasks[0].ReviewRequirements) {
		t.Fatal("compiled declaration lost in SQLite")
	}
	d := task.ReviewRequirements
	if d.Criteria.SHA256 != admissionDigestBytes([]byte("retained criteria")) || d.Criteria.RunID != f.request.RunID || d.Criteria.TaskID != task.ID || d.RoundLimit != 2 {
		t.Fatal("compiled provenance/default lost")
	}
	first, err := f.service.FreezeDeclared(ctx, declaredRequest(f))
	if err != nil {
		t.Fatal(err)
	}
	if first.Authority.DeclarationDigest != domain.TaskDigest(task) {
		t.Fatal("task digest not frozen")
	}
	// Saved authority is reused even after catalog changes and valid parent revision advance.
	a := f.records.Attempts[0]
	a.Revision++
	if err = reopened.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{a}}); err != nil {
		t.Fatal(err)
	}
	f.catalog.catalog.Classifications = nil
	next, err := f.service.FreezeDeclared(ctx, declaredRequest(f))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, next) || next.Authority.Parent.IssuedRevision != 1 {
		t.Fatal("refroze later issued revision")
	}
	task.ReviewRequirements.Members[0].Role = "fake"
	if next.Authority.Requirements.Members[0].Role == "fake" {
		t.Fatal("returned members alias")
	}
}
func TestReviewDeclarationRejectMalformedAndOrdinaryControls(t *testing.T) {
	base := declaredManifestYAML()
	cases := map[string]string{
		"unknown family":        strings.Replace(base, "id: one,", "provider_family: forged, id: one,", 1),
		"unknown tier":          strings.Replace(base, "id: one,", "tier: critical, id: one,", 1),
		"unknown digest":        strings.Replace(base, "risk: routine", "policy_digest: fake\n      risk: routine", 1),
		"branch":                strings.Replace(base, strings.Repeat("c", 40), "main", 1),
		"default source":        strings.Replace(base, ", ref: "+strings.Repeat("c", 40), "", 1),
		"malformed full source": strings.Replace(base, strings.Repeat("c", 40), strings.Repeat("z", 40), 1),
		"criteria undeclared":   strings.Replace(base, "inputs: [inputs/criteria.md]", "inputs: []", 1),
		"criteria escape":       strings.ReplaceAll(base, "inputs/criteria.md", "../criteria.md"),
		"criteria manifest":     strings.ReplaceAll(base, "inputs/criteria.md", "workflow.yaml"),
		"bad version":           strings.Replace(base, "version: 1", "version: 2", 1),
		"bad risk":              strings.Replace(base, "risk: routine", "risk: fake", 1),
		"round ceiling":         strings.Replace(base, "risk: routine", "risk: routine\n      round_limit: 3", 1),
		"negative round":        strings.Replace(base, "risk: routine", "risk: routine\n      round_limit: -1", 1),
		"count":                 strings.Replace(base, "required_reviewers: 2", "required_reviewers: 3", 1),
		"diversity waiver":      strings.Replace(base, "min_provider_families: 2", "min_provider_families: 1", 1),
		"duplicate member":      strings.Replace(base, "id: two", "id: one", 1),
		"empty role":            strings.Replace(base, "role: independent", "role: ''", 1),
		"wildcard route":        strings.Replace(base, "codex/org/sol", "codex/*", 1),
		"required mismatch":     strings.Replace(base, "required: true", "required: false", 1),
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseManifest([]byte(raw)); err == nil {
				t.Fatal("malformed declaration accepted")
			}
		})
	}
	ordinary := "version: 2\nname: ordinary\nenvironment: {project: t3-steward}\ntasks: {inspect: {prompt_file: prompts/inspect.md}}\n"
	for _, raw := range []string{ordinary, strings.Replace(ordinary, "project: t3-steward", "project: t3-steward, ref: main", 1), strings.Replace(ordinary, "project: t3-steward", "project: t3-steward, type: fresh", 1)} {
		m, err := ParseManifest([]byte(raw))
		if err != nil {
			t.Fatal(err)
		}
		if err = ValidateTaskReviewAdmission(context.Background(), m, nil); err != nil {
			t.Fatal("ordinary failed without catalog", err)
		}
	}
}
func TestReviewDeclarationIngestFailClosedFilesAndCopies(t *testing.T) {
	f := newAdmissionFixture(t)
	rewriteBundleManifest(t, f.source, declaredManifestYAML())
	ctx := context.Background()
	if _, err := (BundleIngester{Store: f.store, StorageRoot: f.service.Artifacts.SubmissionRoot}).Ingest(ctx, f.source); err == nil || !strings.Contains(err.Error(), "configured") {
		t.Fatalf("missing adapter accepted %v", err)
	}
	for _, kind := range []string{"missing", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			bundle := validBundle(t)
			rewriteBundleManifest(t, bundle, declaredManifestYAML())
			if kind == "symlink" {
				outside := filepath.Join(t.TempDir(), "criteria.md")
				if err := os.WriteFile(outside, []byte("bad"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.MkdirAll(filepath.Join(bundle, "inputs"), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, filepath.Join(bundle, "inputs/criteria.md")); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := LoadManifest(bundle); err == nil {
				t.Fatal("unsafe criteria accepted")
			}
		})
	}
	d := newDeclaredAdmissionFixture(t).records.Tasks[0]
	digest := domain.TaskDigest(d)
	cloned := cloneDAGState(DAGState{Tasks: []domain.Task{d}})
	planned := clonePlanningTask(d)
	cloned.Tasks[0].ReviewRequirements.Members[0].Route = "forged/model"
	planned.ReviewRequirements.Criteria.SHA256 = strings.Repeat("0", 64)
	if domain.TaskDigest(d) != digest {
		t.Fatal("deep-copy mutation changed source")
	}
	if domain.TaskDigest(planned) == digest {
		t.Fatal("criteria bytes missing from digest")
	}
}
func TestReviewDeclarationConfiguredSelectedPolicyAndDrift(t *testing.T) {
	for _, kind := range []string{"missing declaration", "criteria identity", "member", "assignment digest", "metadata", "route", "project", "risk critical"} {
		t.Run(kind, func(t *testing.T) {
			f := newDeclaredAdmissionFixture(t)
			switch kind {
			case "missing declaration":
				f.records.Tasks[0].ReviewRequirements = nil
			case "criteria identity":
				f.records.Tasks[0].ReviewRequirements.Criteria.RunID = "foreign"
			case "member":
				f.records.Tasks[0].ReviewRequirements.Members[0].Route = "unauthorized/model"
			case "assignment digest":
				f.records.Assignments[0].TaskDigest = "wrong"
			case "metadata":
				f.catalog.catalog.Classifications[1].ProviderFamily = "openai"
			case "route":
				f.catalog.catalog.AuthoredWorkers[0].Providers[1].Models = nil
			case "project":
				f.catalog.catalog.AuthoredWorkers[0].Projects = nil
			case "risk critical":
				f.records.Tasks[0].ReviewRequirements.Risk = "risky"
				f.catalog.catalog.Classifications[1].Tier = "executor"
			}
			if err := f.store.SaveCoordinatorRecords(context.Background(), f.records); err != nil {
				t.Fatal(err)
			}
			if _, err := f.service.FreezeDeclared(context.Background(), declaredRequest(f)); err == nil {
				t.Fatal("forged/drifted authority admitted")
			}
		})
	}
}

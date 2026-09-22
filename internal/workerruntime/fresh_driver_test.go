package workerruntime

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFreshPackageDriverLifecycle(t *testing.T) {
	ctx := context.Background()
	pkg := testPackage()
	pkg.Environment.Type = backlog.EnvironmentFresh
	pkg.Environment.Repository = ""
	pkg.Environment.Ref = ""
	pkg.Environment.T3Project = ""
	pkg.StaticInputs = []workerproto.ArtifactObject{testArtifact("input", "inputs/nested/context.txt", "context")}
	freshThrough := pkg.CreatedAt.Add(24 * time.Hour)
	pkg.Context = &domain.ProjectContext{
		Version: domain.ProjectContextVersion, Revision: "context-1", Status: domain.ProjectContextAccepted,
		Objective: "complete with only package context", Authority: []string{"coordinator:run"},
		Budget: "one attempt", Outputs: []string{"result.txt"}, RequiredReferences: []string{"input-ref"},
		References: []domain.ProjectContextReference{{
			ID: "input-ref", Kind: domain.ContextReferenceGit, URI: "https://example.invalid/repo",
			Revision: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Status: domain.ProjectContextPinned, Authority: "coordinator",
			Binding: &domain.ProjectContextArtifactBinding{ArtifactID: pkg.StaticInputs[0].ID, Path: pkg.StaticInputs[0].Path, SHA256: pkg.StaticInputs[0].SHA256},
		}},
		Decisions:       []domain.ProjectContextDecision{{ID: "accepted", Status: "accepted", Summary: "use retained input", Authority: "review-gate"}},
		CheckpointDelta: []string{"accepted branch unchanged"},
		CodeLocations:   []domain.ProjectContextLocation{{Path: "internal/workerruntime/fresh_driver_test.go", Revision: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},
		InputLocations:  []domain.ProjectContextLocation{{Path: pkg.StaticInputs[0].Path, Revision: pkg.StaticInputs[0].SHA256}},
		Setup:           []string{"true"}, Checks: []string{"test -f .t3/context/index.json"}, CapabilityRefs: []string{"input-ref"},
		Freshness: domain.ProjectContextFreshness{ObservedAt: pkg.CreatedAt, FreshThrough: &freshThrough},
	}
	pkg.RequiredCapabilities = []string{workerproto.PackageCapabilityProjectContext}
	manifest, err := workerproto.BuildExecutionPackageManifest(pkg)
	if err != nil {
		t.Fatal(err)
	}
	if err := workerproto.ValidateExecutionPackageManifest(manifest, 8<<20); err != nil {
		t.Fatal(err)
	}
	changed := manifest
	changed.Package.Environment.Type = "git"
	if err := workerproto.ValidateExecutionPackageManifest(changed, 8<<20); err == nil {
		t.Fatal("type tampering accepted")
	}
	catalog, err := backlog.NewProjectCatalog([]backlog.ProjectDefinition{{Name: "steward", Type: backlog.EnvironmentFresh, SetupProfile: "go"}}, []backlog.SetupProfile{{Name: "go", Commands: []string{"true"}, Timeout: time.Minute}})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	control := &recordingT3{}
	publisher := &recordingPublisher{}
	makeDriver := func() *LocalDriver {
		driver, err := NewLocalDriver(LocalDriver{
			Config:    LocalDriverConfig{CatalogRevision: "catalog-1", ArtifactRoot: filepath.Join(root, "artifacts"), RunsRoot: filepath.Join(root, "runs")},
			Catalog:   catalog,
			Workspace: backlog.WorkspacePreparer{GitBinary: "/must-not-call-git", Processes: successfulProcessRunner{}},
			Source:    mapArtifactSource{pkg.Prompt.ID: []byte("prompt"), "input": []byte("context")},
			Publisher: publisher, T3: control,
		})
		if err != nil {
			t.Fatal(err)
		}
		return driver
	}
	driver := makeDriver()
	workspace, err := driver.Prepare(ctx, pkg)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(workspace, ".t3", "inputs", "nested", "context.txt"))
	if err != nil || string(data) != "context" {
		t.Fatalf("input=%q %v", data, err)
	}
	index, err := os.ReadFile(filepath.Join(workspace, filepath.FromSlash(domain.ProjectContextFile)))
	if err != nil || !strings.Contains(string(index), `"objective": "complete with only package context"`) {
		t.Fatalf("project context=%q %v", index, err)
	}
	if _, err := os.Stat(filepath.Join(workspace, ".git")); !os.IsNotExist(err) {
		t.Fatal("created Git repository")
	}
	restarted := makeDriver()
	inspected, exists, err := restarted.InspectWorkspace(ctx, pkg)
	if err != nil || !exists || inspected != workspace {
		t.Fatalf("restart inspection=%s %v %v", inspected, exists, err)
	}
	if err := restarted.CreateThread(ctx, pkg, workspace); err != nil {
		t.Fatal(err)
	}
	if len(control.managedProjects) != 1 || len(control.created) != 1 || control.created[0].WorktreePath != workspace {
		t.Fatal("managed project dispatch missing")
	}
	if err := restarted.Cleanup(ctx, pkg, workspace); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(workspace); !os.IsNotExist(err) {
		t.Fatal("owned workspace retained")
	}
}

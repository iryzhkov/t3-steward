package workerruntime

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"os"
	"path/filepath"
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
	pkg.StaticInputs = []workerproto.ArtifactObject{testArtifact("input", "context.txt", "context")}
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
	data, err := os.ReadFile(filepath.Join(workspace, ".t3", "inputs", "context.txt"))
	if err != nil || string(data) != "context" {
		t.Fatalf("input=%q %v", data, err)
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

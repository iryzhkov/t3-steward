package workerruntime

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type dependencyNamespaceSetupRunner struct{}

func (dependencyNamespaceSetupRunner) Run(ctx context.Context, request backlog.ProcessRequest) (backlog.ProcessResult, error) {
	command := exec.CommandContext(ctx, request.Program, request.Args...)
	command.Dir = request.Dir
	command.Stdout = request.Log
	command.Stderr = request.Log
	return backlog.ProcessResult{}, command.Run()
}
func (dependencyNamespaceSetupRunner) Kill(string) error { return nil }

func TestLocalDriverUsesRecordedDependencyNamespaceInsteadOfOpaqueTaskID(t *testing.T) {
	repository, commit := makeGitRepository(t)
	pkg := testPackage()
	pkg.Environment.Repository = "https://example.com/steward.git"
	pkg.Environment.Ref = commit
	object := testArtifact("handoff", "dependencies/producer/handoff.md", "handoff")
	pkg.Dependencies = []workerproto.DependencyInput{{TaskID: "task-opaque-producer-id", Artifacts: []workerproto.ArtifactObject{object}}}
	catalog, err := backlog.NewProjectCatalog([]backlog.ProjectDefinition{{Name: "steward", Repository: pkg.Environment.Repository, DefaultRef: commit, T3ProjectTemplate: "development", SetupProfile: "go"}}, []backlog.SetupProfile{{Name: "go", Commands: []string{"test -f .t3/dependencies/producer/handoff.md"}, Timeout: time.Minute}})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	driver, err := NewLocalDriver(LocalDriver{
		Config:  LocalDriverConfig{CatalogRevision: "catalog-1", ArtifactRoot: filepath.Join(root, "artifacts"), RunsRoot: filepath.Join(root, "runs")},
		Catalog: catalog, Workspace: backlog.WorkspacePreparer{Cache: staticRepositoryCache{path: repository}, Processes: dependencyNamespaceSetupRunner{}},
		Source: mapArtifactSource{pkg.Prompt.ID: []byte("prompt"), object.ID: []byte("handoff")}, Publisher: &recordingPublisher{}, T3: &recordingT3{},
	})
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := driver.Prepare(context.Background(), pkg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = removeReadOnlyTree(root) })
	data, err := os.ReadFile(filepath.Join(workspace, ".t3", filepath.FromSlash(object.Path)))
	if err != nil || string(data) != "handoff" {
		t.Fatalf("recorded dependency path=%q error=%v", data, err)
	}
}

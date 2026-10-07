package workerruntime

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestDependencyIntegrityRepairsMissingAndCorruptMaterialization(t *testing.T) {
	for _, damage := range []string{"missing", "digest", "dangling"} {
		t.Run(damage, func(t *testing.T) {
			pkg := testPackage()
			object := testArtifact("handoff", "dependencies/producer/handoff.md", "verified handoff")
			pkg.Dependencies = []workerproto.DependencyInput{{TaskID: "producer", Artifacts: []workerproto.ArtifactObject{object}}}
			root := t.TempDir()
			workspace := filepath.Join(root, "workspace")
			if err := os.MkdirAll(filepath.Join(workspace, ".t3"), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("../../dependencies", filepath.Join(workspace, ".t3", "dependencies")); err != nil {
				t.Fatal(err)
			}
			if damage != "dangling" {
				if err := os.MkdirAll(filepath.Join(root, "dependencies", "producer"), 0700); err != nil {
					t.Fatal(err)
				}
			}
			if damage == "digest" {
				if err := os.WriteFile(filepath.Join(root, "dependencies", "producer", "handoff.md"), []byte("corrupt"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			driver := &LocalDriver{Config: LocalDriverConfig{ArtifactRoot: filepath.Join(root, "artifacts")}, Source: mapArtifactSource{object.ID: []byte("verified handoff")}}
			driver.Workspace.StorageRoot = driver.Config.ArtifactRoot
			if err := driver.ensureDependencyIntegrity(context.Background(), pkg, workspace); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = removeReadOnlyTree(root) })
			got, err := os.ReadFile(filepath.Join(workspace, ".t3", "dependencies", "producer", "handoff.md"))
			if err != nil || string(got) != "verified handoff" {
				t.Fatalf("input=%q error=%v", got, err)
			}
		})
	}
}

func TestDependencyIntegrityRepairsMissingDeclaredCommit(t *testing.T) {
	workspace, commit := makeGitRepository(t)
	// Keep the fixture's input tree adjacent to its checkout.
	root := t.TempDir()
	consumer := filepath.Join(root, "workspace")
	if output, err := exec.Command("git", "clone", "--no-local", workspace, consumer).CombinedOutput(); err != nil {
		t.Fatalf("clone: %v %s", err, output)
	}
	pkg := testPackage()
	store := backlog.CampaignRefStore{Root: filepath.Join(root, "refs")}
	provenance, err := store.Publish(context.Background(), backlog.PublishCommitRequest{
		WorkflowRunID: pkg.Identity.WorkflowRunID, TaskID: "producer", Name: "implementation",
		Repository: pkg.Environment.Repository, WorkspaceDir: workspace, Base: commit,
	}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(provenance)
	if err != nil {
		t.Fatal(err)
	}
	object := testArtifact("commit", "dependencies/producer/implementation", string(raw))
	pkg.Dependencies = []workerproto.DependencyInput{{TaskID: "producer", Artifacts: []workerproto.ArtifactObject{object}}}
	if err := os.MkdirAll(filepath.Join(consumer, ".t3"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../../dependencies", filepath.Join(consumer, ".t3", "dependencies")); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "dependencies", "producer"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "dependencies", "producer", "implementation"), raw, 0400); err != nil {
		t.Fatal(err)
	}
	driver := &LocalDriver{Config: LocalDriverConfig{ArtifactRoot: filepath.Join(root, "artifacts")}, Source: mapArtifactSource{object.ID: raw}}
	driver.Workspace.CampaignRefs = store
	if err := driver.ensureDependencyIntegrity(context.Background(), pkg, consumer); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = removeReadOnlyTree(root) })
	output, err := exec.Command("git", "-C", consumer, "rev-parse", "--verify", provenance.Ref+"^{commit}").Output()
	if err != nil || strings.TrimSpace(string(output)) != commit {
		t.Fatalf("commit ref=%s error=%v", output, err)
	}
}

func TestDependencyIntegrityReportsUnavailableArtifact(t *testing.T) {
	pkg := testPackage()
	object := testArtifact("handoff", "dependencies/producer/handoff.md", "verified handoff")
	pkg.Dependencies = []workerproto.DependencyInput{{TaskID: "producer", Artifacts: []workerproto.ArtifactObject{object}}}
	root := t.TempDir()
	workspace := filepath.Join(root, "workspace")
	if err := os.MkdirAll(filepath.Join(workspace, ".t3"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../../dependencies", filepath.Join(workspace, ".t3", "dependencies")); err != nil {
		t.Fatal(err)
	}
	driver := &LocalDriver{Config: LocalDriverConfig{ArtifactRoot: filepath.Join(root, "artifacts")}, Source: mapArtifactSource{}}
	driver.Workspace.StorageRoot = driver.Config.ArtifactRoot
	err := driver.ensureDependencyIntegrity(context.Background(), pkg, workspace)
	var integrity *backlog.DependencyIntegrityError
	if !errors.As(err, &integrity) || integrity.Artifact != "handoff.md" || integrity.Producer != "producer" || !strings.Contains(err.Error(), "handoff.md") {
		t.Fatalf("error=%v", err)
	}
}

func TestRecoveredWorkspaceNeverStartsWithDanglingDependencies(t *testing.T) {
	pkg := testPackage()
	object := testArtifact("handoff", "dependencies/producer/handoff.md", "verified handoff")
	pkg.Dependencies = []workerproto.DependencyInput{{TaskID: "producer", Artifacts: []workerproto.ArtifactObject{object}}}
	root := t.TempDir()
	workspace := filepath.Join(root, "workspace")
	if err := os.MkdirAll(filepath.Join(workspace, ".t3"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../../dependencies", filepath.Join(workspace, ".t3", "dependencies")); err != nil {
		t.Fatal(err)
	}
	control := &recordingT3{}
	driver := &LocalDriver{Config: LocalDriverConfig{ArtifactRoot: filepath.Join(root, "artifacts")}, Source: mapArtifactSource{pkg.Prompt.ID: []byte("prompt")}, T3: control}
	driver.Workspace.StorageRoot = driver.Config.ArtifactRoot
	if _, err := driver.cacheArtifact(context.Background(), pkg.Prompt, pkg.Limits.MaxArtifactBytes); err != nil {
		t.Fatal(err)
	}
	err := driver.CreateThread(context.Background(), pkg, workspace)
	var integrity *backlog.DependencyIntegrityError
	if !errors.As(err, &integrity) {
		t.Fatalf("startup error=%v, want dependency integrity failure", err)
	}
	if len(control.created) != 0 {
		t.Fatal("agent started with dangling dependency directory")
	}
}

package workerruntime

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

const metadataSentinelBytes = "original host bytes"

func metadataSentinel(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "host-sentinel")
	if err := os.WriteFile(path, []byte(metadataSentinelBytes), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o640); err != nil {
		t.Fatal(err)
	}
	return path
}

func assertMetadataSentinelUnchanged(t *testing.T, path string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != metadataSentinelBytes || info.Mode().Perm() != 0o640 {
		t.Fatalf("host sentinel = %q mode %#o, want %q mode 0640", got, info.Mode().Perm(), metadataSentinelBytes)
	}
}

// R-13: the identity record and its self-ignoring .gitignore are written into
// a repository checkout before containment exists, so a tracked symlink at
// either name, or at the identity directory itself, must not redirect the
// write to a host file.
func TestLocalDriverTaskIdentityRefusesRepositorySymlinks(t *testing.T) {
	for _, name := range []string{"task.env", ".gitignore"} {
		t.Run(name, func(t *testing.T) {
			workspace := t.TempDir()
			if err := os.Mkdir(filepath.Join(workspace, domain.TaskIdentityDir), 0o700); err != nil {
				t.Fatal(err)
			}
			target := metadataSentinel(t)
			if err := os.Symlink(target, filepath.Join(workspace, domain.TaskIdentityDir, name)); err != nil {
				t.Fatal(err)
			}
			err := (&LocalDriver{}).writeTaskIdentity(testPackage(), workspace)
			assertMetadataSentinelUnchanged(t, target)
			if err == nil || !strings.Contains(err.Error(), "symbolic link") || !strings.Contains(err.Error(), name) {
				t.Fatalf("error = %v, want a refusal naming %s as a symbolic link", err, name)
			}
			// A refused exclusion leaves no record behind that git would
			// then pick up.
			if name == ".gitignore" {
				if _, err := os.Lstat(filepath.Join(workspace, filepath.FromSlash(domain.TaskIdentityFile))); !os.IsNotExist(err) {
					t.Fatalf("the identity record was written without its exclusion: %v", err)
				}
			}
		})
	}
	t.Run("directory", func(t *testing.T) {
		workspace := t.TempDir()
		outside := t.TempDir()
		if err := os.Symlink(outside, filepath.Join(workspace, domain.TaskIdentityDir)); err != nil {
			t.Fatal(err)
		}
		err := (&LocalDriver{}).writeTaskIdentity(testPackage(), workspace)
		if err == nil || !strings.Contains(err.Error(), "symbolic link") {
			t.Fatalf("error = %v, want a refusal of the linked identity directory", err)
		}
		if entries, _ := os.ReadDir(outside); len(entries) != 0 {
			t.Fatalf("identity material landed outside the workspace: %v", entries)
		}
	})
}

// The project context index is the other metadata file the worker writes into
// a checkout before containment: a tracked .t3/context symlink must not move
// it to a host directory.
func TestLocalDriverProjectContextRefusesALinkedDirectory(t *testing.T) {
	workspace := t.TempDir()
	outside := t.TempDir()
	if err := os.Mkdir(filepath.Join(workspace, ".t3"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(workspace, ".t3", "context")); err != nil {
		t.Fatal(err)
	}
	pkg := projectContextPackage(t)
	err := (&LocalDriver{}).writeProjectContext(pkg, workspace)
	if err == nil || !strings.Contains(err.Error(), "symbolic link") {
		t.Fatalf("error = %v, want a refusal of the linked context directory", err)
	}
	if entries, _ := os.ReadDir(outside); len(entries) != 0 {
		t.Fatalf("project context landed outside the workspace: %v", entries)
	}

	// Without the link, the index is materialized exactly as before.
	clean := t.TempDir()
	if err := (&LocalDriver{}).writeProjectContext(pkg, clean); err != nil {
		t.Fatal(err)
	}
	if err := verifyProjectContextFile(pkg, clean); err != nil {
		t.Fatal(err)
	}
}

func projectContextPackage(t *testing.T) workerproto.ExecutionPackage {
	t.Helper()
	pkg := testPackage()
	pkg.Environment.Type = backlog.EnvironmentFresh
	pkg.Environment.Repository = ""
	pkg.Environment.Ref = ""
	pkg.Environment.T3Project = ""
	pkg.StaticInputs = []workerproto.ArtifactObject{testArtifact("input", "inputs/nested/context.txt", "context")}
	freshThrough := pkg.CreatedAt.Add(24 * time.Hour)
	pkg.Context = &domain.ProjectContext{
		Version: domain.ProjectContextVersion, Revision: "context-1", Status: domain.ProjectContextAccepted,
		Objective: "materialize metadata", Authority: []string{"coordinator:run"},
		Budget: "one attempt", Outputs: []string{"result.txt"}, RequiredReferences: []string{"input-ref"},
		References: []domain.ProjectContextReference{{
			ID: "input-ref", Kind: domain.ContextReferenceGit, URI: "https://example.invalid/repo",
			Revision: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Status: domain.ProjectContextPinned, Authority: "coordinator",
			Binding: &domain.ProjectContextArtifactBinding{ArtifactID: pkg.StaticInputs[0].ID, Path: pkg.StaticInputs[0].Path, SHA256: pkg.StaticInputs[0].SHA256},
		}},
		Decisions:      []domain.ProjectContextDecision{{ID: "accepted", Status: "accepted", Summary: "use retained input", Authority: "review-gate"}},
		InputLocations: []domain.ProjectContextLocation{{Path: pkg.StaticInputs[0].Path, Revision: pkg.StaticInputs[0].SHA256}},
		Setup:          []string{"true"}, Checks: []string{"test -f .t3/context/index.json"}, CapabilityRefs: []string{"input-ref"},
		Freshness: domain.ProjectContextFreshness{ObservedAt: pkg.CreatedAt, FreshThrough: &freshThrough},
	}
	pkg.RequiredCapabilities = []string{workerproto.PackageCapabilityProjectContext}
	if _, err := projectContextContent(pkg); err != nil {
		t.Fatalf("fixture package is invalid: %v", err)
	}
	return pkg
}

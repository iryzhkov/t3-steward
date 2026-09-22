package workerruntime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestInspectWorkspaceRefusesProjectContextTampering(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(string) error
	}{
		{"deleted", func(path string) error { return os.Remove(path) }},
		{"truncated", func(path string) error {
			if err := os.Chmod(path, 0o644); err != nil {
				return err
			}
			if err := os.WriteFile(path, nil, 0o444); err != nil {
				return err
			}
			return os.Chmod(path, 0o444)
		}},
		{"replaced", func(path string) error {
			if err := os.Chmod(path, 0o644); err != nil {
				return err
			}
			if err := os.WriteFile(path, []byte("{}\n"), 0o444); err != nil {
				return err
			}
			return os.Chmod(path, 0o444)
		}},
		{"mode", func(path string) error { return os.Chmod(path, 0o644) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pkg := testPackage()
			pkg.Environment.Repository = "https://example.invalid/repo"
			input := testArtifact("context-input", "inputs/context.txt", "input")
			pkg.StaticInputs = []workerproto.ArtifactObject{input}
			fresh := *pkg.ExpiresAt
			pkg.Context = &domain.ProjectContext{
				Version: domain.ProjectContextVersion, Revision: "context-1", Status: domain.ProjectContextAccepted,
				Objective: "cold start", Authority: []string{"gate-decision:accepted"}, Budget: "one attempt",
				Outputs: []string{"result.txt"}, RequiredReferences: []string{"input"},
				References: []domain.ProjectContextReference{{
					ID: "input", Kind: domain.ContextReferenceGit, URI: "https://example.invalid/repo",
					Revision: strings.Repeat("a", 40), Status: domain.ProjectContextPinned, Authority: "coordinator",
					Binding: &domain.ProjectContextArtifactBinding{ArtifactID: input.ID, Path: input.Path, SHA256: input.SHA256},
				}},
				Setup: []string{"true"}, Checks: []string{"true"},
				Freshness: domain.ProjectContextFreshness{ObservedAt: pkg.CreatedAt, FreshThrough: &fresh},
			}
			pkg.RequiredCapabilities = []string{workerproto.PackageCapabilityProjectContext}
			root := t.TempDir()
			catalog, err := backlog.NewProjectCatalog(
				[]backlog.ProjectDefinition{{Name: "steward", Type: backlog.EnvironmentGit, Repository: "https://example.invalid/repo", DefaultRef: "main", T3ProjectTemplate: "development", SetupProfile: "go"}},
				[]backlog.SetupProfile{{Name: "go", Commands: []string{"true"}, Timeout: pkg.Limits.PrepareTimeout}},
			)
			if err != nil {
				t.Fatal(err)
			}
			driver, err := NewLocalDriver(LocalDriver{
				Config:    LocalDriverConfig{CatalogRevision: "catalog-1", ArtifactRoot: filepath.Join(root, "artifacts"), RunsRoot: filepath.Join(root, "runs"), DryRun: true},
				Catalog:   catalog,
				Source:    mapArtifactSource{pkg.Prompt.ID: []byte("prompt"), input.ID: []byte("input")},
				Publisher: &recordingPublisher{},
			})
			if err != nil {
				t.Fatal(err)
			}
			workspace, err := driver.Prepare(context.Background(), pkg)
			if err != nil {
				t.Fatal(err)
			}
			index := filepath.Join(workspace, filepath.FromSlash(domain.ProjectContextFile))
			if err := tc.mutate(index); err != nil {
				t.Fatal(err)
			}
			if _, exists, err := driver.InspectWorkspace(context.Background(), pkg); err == nil || exists {
				t.Fatalf("tampered context accepted: exists=%v err=%v", exists, err)
			}
		})
	}
}

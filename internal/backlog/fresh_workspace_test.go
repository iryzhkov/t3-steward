package backlog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestFreshManifestAndCatalog(t *testing.T) {
	raw := "version: 2\nname: fresh\nenvironment:\n  project: scratch\n  type: fresh\ntasks: {}\n"
	m, err := ParseManifest([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := NewProjectCatalog([]ProjectDefinition{{Name: "scratch", Type: EnvironmentFresh, SetupProfile: "empty"}}, []SetupProfile{{Name: "empty", Commands: []string{"true"}, Timeout: time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	workflow := domain.Workflow{ID: "workflow", Project: "scratch", Environment: domain.ExecutionEnvironment{Type: m.Environment.Type, Scope: m.Environment.Scope}}
	task := domain.Task{ID: "task", WorkflowID: "workflow"}
	env, err := catalog.Resolve(workflow, task)
	if err != nil || env.Type != EnvironmentFresh || env.Repository != "" || env.Ref != "" {
		t.Fatalf("resolve=%+v %v", env, err)
	}
	for _, extra := range []string{"  ref: main\n", "  scope: workflow\n"} {
		if _, err := ParseManifest([]byte(strings.Replace(raw, "tasks:", extra+"tasks:", 1))); err == nil {
			t.Fatal("invalid fresh declaration accepted")
		}
	}
	workflow.Environment.Type = EnvironmentGit
	if _, err := catalog.Resolve(workflow, task); err == nil {
		t.Fatal("type mismatch accepted")
	}
	for _, project := range []ProjectDefinition{
		{Name: "scratch", Type: EnvironmentFresh, Repository: "https://example.com/repo", SetupProfile: "empty"},
		{Name: "scratch", Type: EnvironmentFresh, DefaultRef: "main", SetupProfile: "empty"},
	} {
		if _, err := NewProjectCatalog([]ProjectDefinition{project}, []SetupProfile{{Name: "empty", Commands: []string{"true"}, Timeout: time.Second}}); err == nil {
			t.Fatal("fresh Git metadata accepted")
		}
	}
}

func TestFreshWorkspaceCustodyAndFailedPreparation(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[fail], func(t *testing.T) {
			root, storage := t.TempDir(), t.TempDir()
			t.Cleanup(func() { _ = removeIngestedTree(root) })
			request := workspaceRequest("", "", workspaceTask("task", "task"), "attempt")
			request.Environment.Type = EnvironmentFresh
			request.Environment.Setup.Commands = []string{"test ! -e .git && test \"$(cat .t3/inputs/input.txt)\" = data"}
			if fail {
				request.Environment.Setup.Commands = append(request.Environment.Setup.Commands, "exit 9")
			}
			request.InputArtifacts = []domain.Artifact{storedInputArtifact(t, storage, "run-1", "input.txt", "data")}
			p := WorkspacePreparer{RunsRoot: root, StorageRoot: storage, GitBinary: "/must-not-run-git", Processes: testProcessRunner{}}
			prepared, err := p.Prepare(context.Background(), request)
			if fail {
				var pe *PreparationError
				if !errors.As(err, &pe) || pe.LogPath == "" {
					t.Fatalf("failure=%v", err)
				}
				if _, err := os.Stat(filepath.Join(root, "run-1", "task", "attempt")); !os.IsNotExist(err) {
					t.Fatal("failed workspace published")
				}
				assertNoPreparationStages(t, filepath.Join(root, "run-1", "task"))
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if prepared.Commit != "" || prepared.CacheReused {
				t.Fatal("fresh workspace fabricated Git identity")
			}
			if _, err := os.Stat(filepath.Join(prepared.WorkspaceDir, ".git")); !os.IsNotExist(err) {
				t.Fatal("fresh workspace contains Git")
			}
			info, err := os.Stat(filepath.Join(prepared.InputsDir, "input.txt"))
			if err != nil || info.Mode().Perm()&0222 != 0 {
				t.Fatal("input not immutable")
			}
			if _, err := p.Prepare(context.Background(), request); err == nil {
				t.Fatal("duplicate preparation overwritten")
			}
		})
	}
}

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

// Reconstructs the review reproduction using production finalization. Both a
// different workspace HEAD and dirty tracked content can pass verification
// while the declared commit fails it; neither is same-commit evidence.
func TestReviewVerificationMustBindDeclaredCommit(t *testing.T) {
	for _, dirty := range []bool{false, true} {
		t.Run(fmt.Sprintf("dirty=%t", dirty), func(t *testing.T) {
			ctx := context.Background()
			f := loadCampaignResultFixture(t, "accepted")
			dir, storage := t.TempDir(), t.TempDir()
			git := func(args ...string) string {
				t.Helper()
				c := exec.Command("git", append([]string{"-C", dir}, args...)...)
				c.Env = append(os.Environ(), "GIT_CONFIG_COUNT=2", "GIT_CONFIG_KEY_0=gc.autoDetach", "GIT_CONFIG_VALUE_0=false", "GIT_CONFIG_KEY_1=maintenance.autoDetach", "GIT_CONFIG_VALUE_1=false")
				out, err := c.CombinedOutput()
				if err != nil {
					t.Fatalf("git %v: %s: %v", args, out, err)
				}
				return strings.TrimSpace(string(out))
			}
			git("init")
			git("config", "user.name", "Review")
			git("config", "user.email", "review@example.invalid")
			git("config", "commit.gpgsign", "false")
			git("config", "core.hooksPath", "/dev/null")
			git("commit", "--allow-empty", "-m", "base")
			base := git("rev-parse", "HEAD")
			file := filepath.Join(dir, "source.txt")
			if err := os.WriteFile(file, []byte("bad\n"), 0600); err != nil {
				t.Fatal(err)
			}
			git("add", "source.txt")
			git("commit", "-m", "candidate fails verification")
			candidate := git("rev-parse", "HEAD")
			git("branch", "candidate")
			if err := os.WriteFile(file, []byte("good\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if !dirty {
				git("commit", "-am", "workspace passes verification")
			}
			p := &f.Workflow.Tasks[1]
			p.Task.Outputs = p.Task.Outputs[:1]
			p.Attempt.ReviewGate = nil
			p.Task.Outputs[0].Commit.Revision = "candidate"
			p.Task.Verification = []string{"grep -qx good source.txt"}
			finalizer := backlog.AttemptFinalizer{StorageRoot: storage, CampaignRefs: backlog.CampaignRefStore{Root: t.TempDir()}}
			result, err := finalizer.Finalize(ctx, backlog.AttemptFinalization{Task: p.Task, Attempt: *p.Attempt, WorkspaceDir: dir, ExplicitSuccess: true, Repository: "test-repository", BaseCommit: base})
			defer filepath.Walk(storage, func(path string, info os.FileInfo, err error) error {
				if err == nil && info.IsDir() {
					return os.Chmod(path, 0700)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if result.Completion.Failure != "" || !result.Completion.VerificationPassed {
				t.Fatalf("finalize: %+v", result.Completion)
			}
			p.Artifacts = nil
			for _, a := range result.Artifacts {
				raw, err := os.ReadFile(filepath.Join(storage, filepath.FromSlash(a.StoragePath)))
				if err != nil {
					t.Fatal(err)
				}
				f.Artifacts[a.ID] = json.RawMessage(raw)
				p.Artifacts = append(p.Artifacts, backlogadmin.Artifact{Metadata: backlogadmin.ArtifactMetadata{ID: a.ID, WorkflowRunID: a.WorkflowRunID, TaskID: a.TaskID, AttemptID: a.AttemptID, Kind: a.Kind, Name: a.Name}})
			}
			doc := buildCampaignResult(ctx, f.Workflow, f.open)
			if got := doc.Tasks[0].Commits[0].Commit; got != candidate {
				t.Fatalf("unexpected candidate: %s", got)
			}
			var out bytes.Buffer
			renderCampaignResult(&out, doc)
			t.Log(out.String())
			git("checkout", "--force", "candidate")
			if err := exec.Command("git", "-C", dir, "diff", "--exit-code").Run(); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("grep", "-qx", "good", "source.txt")
			cmd.Dir = dir
			if err := cmd.Run(); err == nil {
				t.Fatal("candidate unexpectedly passed")
			}
			if doc.Acceptance.Accepted || !strings.Contains(doc.Acceptance.Reason, "not bound to declared commit") {
				t.Fatalf("reported same-commit acceptance without binding: %+v", doc.Acceptance)
			}
		})
	}
}

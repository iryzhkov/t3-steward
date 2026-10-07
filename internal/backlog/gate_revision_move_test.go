package backlog

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// A gate command moved the declared revision to a failing commit after the
// pre-command binding check. The gate attested HEAD's tree, and finalization
// then published the moved revision: an ungated tree under passing evidence.
func TestGateDeclaredRevisionMovedDuringGateIsNotPublished(t *testing.T) {
	for _, revision := range []string{"work", "refs/heads/work"} {
		t.Run(revision, func(t *testing.T) {
			dir := h2GateRepository(t)
			base := gateBindingGit(t, dir, "rev-parse", "HEAD")
			writeTestFile(t, dir, "source.txt", "bad")
			h2Commit(t, dir)
			bad := gateBindingGit(t, dir, "rev-parse", "HEAD")
			gateBindingGit(t, dir, "checkout", "-q", "--detach", base)
			gateBindingGit(t, dir, "branch", "work", base)
			req := h2GateRequest(dir, "moving-revision")
			req.Task.Gate = &domain.TaskGate{Commands: []string{fmt.Sprintf("grep -qx source source.txt && git update-ref refs/heads/work %s", bad)}, Timeout: 5 * time.Second}
			req.Task.Outputs = []domain.ArtifactDeclaration{{Name: "handoff", Commit: &domain.CommitOutput{Revision: revision}}}
			req.Repository, req.BaseCommit = dir, base
			storage := t.TempDir()
			refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "refs")}
			result, err := (AttemptFinalizer{StorageRoot: storage, CampaignRefs: refs, Processes: &directRunner{}}).Finalize(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			cleanupImmutable(t, result.StorageDir)
			report := h2ReadGate(t, storage, result)
			if report.Passed || result.Completion.VerificationPassed {
				t.Fatalf("gate passed although declared revision moved during it: passed=%v verification=%v", report.Passed, result.Completion.VerificationPassed)
			}
			if report.Failure == nil || !strings.Contains(report.Failure.Reason, "moved during the gate") {
				t.Fatalf("gate failure does not name the moved revision: %+v", report.Failure)
			}
			for _, a := range result.Artifacts {
				if a.Name != "handoff" {
					continue
				}
				var p CommitProvenance
				if err := json.Unmarshal(readStoredArtifact(t, storage, a), &p); err != nil {
					t.Fatal(err)
				}
				t.Fatalf("published declared commit %s after a failed gate (bad=%s)", p.Commit, bad)
			}
		})
	}
}

// A gate that rewrites HEAD without changing its tree still passes for a task
// that publishes no commit, as before the binding; with a declared commit it
// fails, since the published commit would not be the one gated.
func TestGateHeadRewriteWithSameTreeFailsOnlyWithDeclaredCommit(t *testing.T) {
	for _, declared := range []bool{false, true} {
		t.Run(fmt.Sprint("declared=", declared), func(t *testing.T) {
			dir := h2GateRepository(t)
			base := gateBindingGit(t, dir, "rev-parse", "HEAD")
			req := h2GateRequest(dir, "amend")
			req.Task.Gate = &domain.TaskGate{Commands: []string{"git -c user.name=t -c user.email=t@t commit -q --amend --no-edit --allow-empty --date=2020-01-01T00:00:00"}, Timeout: 5 * time.Second}
			if declared {
				req.Task.Outputs = []domain.ArtifactDeclaration{{Name: "handoff", Commit: &domain.CommitOutput{}}}
			}
			req.Repository, req.BaseCommit = dir, base
			storage := t.TempDir()
			refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "refs")}
			result, err := (AttemptFinalizer{StorageRoot: storage, CampaignRefs: refs, Processes: &directRunner{}}).Finalize(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			cleanupImmutable(t, result.StorageDir)
			if moved := gateBindingGit(t, dir, "rev-parse", "HEAD"); moved == base {
				t.Fatal("gate command did not rewrite HEAD")
			}
			report := h2ReadGate(t, storage, result)
			if report.Passed == declared || result.Completion.VerificationPassed == declared {
				t.Fatalf("declared=%v: gate passed=%v verification=%v failure=%+v", declared, report.Passed, result.Completion.VerificationPassed, report.Failure)
			}
		})
	}
}

// Publish runs Git in the producer's workspace on the host, after the gate. A
// workspace pre-push hook ran there and could replace the published commit;
// a url.*.insteadOf entry could send the push elsewhere.
func TestCampaignRefPublishIgnoresWorkspaceHooksAndRedirects(t *testing.T) {
	ctx := context.Background()
	repository := newGitFixture(t)
	commit := gitOutput(t, repository, "rev-parse", "HEAD")
	marker := filepath.Join(t.TempDir(), "hook-ran")
	hook := filepath.Join(gitOutput(t, repository, "rev-parse", "--absolute-git-dir"), "hooks", "pre-push")
	writeHook := exec.Command("sh", "-c", `mkdir -p "$(dirname "$1")" && printf '#!/bin/sh\ntouch %s\n' "$2" > "$1" && chmod +x "$1"`, "sh", hook, marker)
	if out, err := writeHook.CombinedOutput(); err != nil {
		t.Fatalf("install hook: %s %v", out, err)
	}
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
	request := PublishCommitRequest{
		WorkflowRunID: "run-1", TaskID: "task-producer", Name: "handoff",
		Repository: repository, WorkspaceDir: repository, Base: commit, ExpectedCommit: commit,
	}
	if _, err := refs.Publish(ctx, request, nil); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("workspace pre-push hook ran during publication")
	}

	decoy := filepath.Join(t.TempDir(), "decoy.git")
	gitRun(t, repository, "init", "-q", "--bare", decoy)
	gitRun(t, repository, "config", "url."+decoy+".insteadOf", filepath.Join(refs.Root, "campaigns.git"))
	request.Name = "redirected"
	if p, err := refs.Publish(ctx, request, nil); err == nil {
		t.Fatalf("publication redirected by workspace config reported success: %+v", p)
	}
	if _, err := refs.Resolve("run-1", "task-producer", "redirected"); err == nil {
		t.Fatal("redirected publication left a provenance record")
	}
}

// gateBackgroundChild returns a gate command that passes and leaves a child
// running action once releaseGateChild signals that the gate has finished,
// then creating done.
func gateBackgroundChild(action, done string) string {
	child := fmt.Sprintf(`(until [ -e %q ]; do sleep 0.01; done; %s; touch %q) >/dev/null 2>&1 </dev/null &`, done+".gate", action, done)
	return "grep -qx source source.txt && { " + child + " }"
}

// releaseGateChild is an afterGate hook: it lets the child of
// gateBackgroundChild act and waits for it, so the child's action always
// lands after the gate's own checks and before capture.
func releaseGateChild(t *testing.T, done string) func() {
	return func() {
		if err := os.WriteFile(done+".gate", nil, 0o600); err != nil {
			t.Fatal(err)
		}
		waitForFile(t, done)
	}
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("background child did not finish: %s", path)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A child left by a gate command rewrote a tracked declared file output after
// the gate's checks; the passing gate attested one tree and the captured
// output came from another.
func TestGateBackgroundChildCannotRewriteTrackedOutput(t *testing.T) {
	dir := h2GateRepository(t)
	storage := t.TempDir()
	done := filepath.Join(t.TempDir(), "done")
	req := h2GateRequest(dir, "background-output")
	rewrite := `i=0; while [ $i -lt 400 ]; do echo bad > .s.tmp && mv .s.tmp source.txt; i=$((i+1)); done`
	req.Task.Gate = &domain.TaskGate{Commands: []string{gateBackgroundChild(rewrite, done)}, Timeout: 5 * time.Second}
	req.Task.Outputs = []domain.ArtifactDeclaration{{Name: "source.txt"}}
	result, err := (AttemptFinalizer{StorageRoot: storage, Processes: &directRunner{}, afterGate: releaseGateChild(t, done)}).Finalize(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	waitForFile(t, done)
	cleanupImmutable(t, result.StorageDir)
	for _, a := range result.Artifacts {
		if a.Name != "source.txt" {
			continue
		}
		got := strings.TrimSpace(string(readStoredArtifact(t, storage, a)))
		if got != "source" && result.Completion.VerificationPassed {
			t.Fatalf("verification passed with captured source.txt=%q, not the gated tree's content", got)
		}
		if got != "source" && !strings.Contains(result.Completion.Failure, "changed after the gate") {
			t.Fatalf("failure does not name the changed output: %q", result.Completion.Failure)
		}
		return
	}
	t.Fatalf("no output captured: %+v", result.Completion)
}

// A child left by a gate command moved the declared revision after the gate's
// checks; publication is pinned to the gated commit and refuses it.
func TestGateBackgroundChildCannotMoveDeclaredRevision(t *testing.T) {
	dir := h2GateRepository(t)
	base := gateBindingGit(t, dir, "rev-parse", "HEAD")
	writeTestFile(t, dir, "source.txt", "bad")
	h2Commit(t, dir)
	bad := gateBindingGit(t, dir, "rev-parse", "HEAD")
	gateBindingGit(t, dir, "checkout", "-q", "--detach", base)
	gateBindingGit(t, dir, "branch", "work", base)
	storage := t.TempDir()
	done := filepath.Join(t.TempDir(), "done")
	req := h2GateRequest(dir, "background-ref")
	req.Task.Gate = &domain.TaskGate{Commands: []string{gateBackgroundChild("git update-ref refs/heads/work "+bad, done)}, Timeout: 5 * time.Second}
	req.Task.Outputs = []domain.ArtifactDeclaration{{Name: "handoff", Commit: &domain.CommitOutput{Revision: "work"}}}
	req.Repository, req.BaseCommit = dir, base
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "refs")}
	result, err := (AttemptFinalizer{StorageRoot: storage, CampaignRefs: refs, Processes: &directRunner{}, afterGate: releaseGateChild(t, done)}).Finalize(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	waitForFile(t, done)
	cleanupImmutable(t, result.StorageDir)
	if p, err := refs.Resolve(req.Attempt.WorkflowRunID, req.Task.ID, "handoff"); err == nil && p.Commit != base {
		t.Fatalf("published %s, not the gated commit %s", p.Commit, base)
	}
}

// Publication is pinned to the gated commit, so a declared revision that
// moves after the gate's own checks still cannot publish another commit.
func TestCampaignRefPublishRefusesCommitOtherThanGated(t *testing.T) {
	ctx := context.Background()
	repository := newGitFixture(t)
	gated := gitOutput(t, repository, "rev-parse", "HEAD")
	gitRun(t, repository, "branch", "work")
	writeGitFile(t, repository, "version.txt", "ungated\n")
	gitRun(t, repository, "commit", "-qam", "ungated")
	gitRun(t, repository, "update-ref", "refs/heads/work", "HEAD")
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
	request := PublishCommitRequest{
		WorkflowRunID: "run-1", TaskID: "task-producer", Name: "handoff",
		Repository: repository, WorkspaceDir: repository, Base: gated,
		Revision: "work", ExpectedCommit: gated,
	}
	if p, err := refs.Publish(ctx, request, nil); err == nil || !strings.Contains(err.Error(), "not the gated commit "+gated) {
		t.Fatalf("published %+v for a moved revision, err=%v", p, err)
	}
	if _, err := refs.Resolve("run-1", "task-producer", "handoff"); err == nil {
		t.Fatal("refused publication left a provenance record")
	}
	gitRun(t, repository, "update-ref", "refs/heads/work", gated)
	if p, err := refs.Publish(ctx, request, nil); err != nil || p.Commit != gated {
		t.Fatalf("gated revision publish = %+v, %v", p, err)
	}
}

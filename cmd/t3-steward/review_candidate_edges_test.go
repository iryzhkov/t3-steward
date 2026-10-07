package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
)

func TestReviewCandidateBundleMultiplePrerequisites(t *testing.T) {
	f := newCandidateFixture(t)
	candidateGit(t, "checkout", "-qb", "right", f.base)
	candidateGit(t, "commit", "--allow-empty", "-qm", "right")
	right := candidateGit(t, "rev-parse", "HEAD")
	candidateGit(t, "checkout", "--detach", f.head)
	candidateGit(t, "merge", "--no-ff", "-qm", "merge", right)
	candidateGit(t, "push", "-q", "origin", f.head+":refs/heads/left", right+":refs/heads/right")
	path := filepath.Join(f.root, "merge.bundle")
	candidateGit(t, "bundle", "create", path, "HEAD", "^"+f.head, "^"+right)
	dir, err := candidateBuild(t, f, "--bundle", path)
	if err != nil {
		t.Fatal(err)
	}
	b := candidateLoad(t, dir)
	if b.Manifest.Review.HeadCommit != candidateGit(t, "rev-parse", "HEAD") {
		t.Fatal("merge head")
	}
	if err := backlog.ValidateReviewManifest(b.Manifest); err != nil {
		t.Fatal(err)
	}
	other := reviewInputTempDir(t)
	candidateGit(t, "clone", "-q", f.remote, other)
	candidateGit(t, "-C", other, "checkout", "--detach", b.Manifest.Environment.Ref)
	mounted := filepath.Join(other, ".t3", "inputs")
	if err := os.MkdirAll(mounted, 0700); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "merge.bundle"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mounted, "merge.bundle"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	for _, task := range b.Manifest.Tasks {
		prompt, err := os.ReadFile(filepath.Join(dir, task.PromptFile))
		if err != nil {
			t.Fatal(err)
		}
		commands := []string{"set -e"}
		for _, line := range strings.Split(string(prompt), "\n") {
			if strings.HasPrefix(line, "git fetch ") || strings.HasPrefix(line, "git checkout ") {
				commands = append(commands, line)
			}
		}
		cmd := exec.Command("sh", "-c", strings.Join(commands, "\n"))
		cmd.Dir = other
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("exact prompt failed: %v %s", err, output)
		}
		if got := candidateGit(t, "-C", other, "rev-parse", "HEAD"); got != b.Manifest.Review.HeadCommit {
			t.Fatal("wrong checkout", got)
		}
	}
}
func TestReviewCandidateBundlePrerequisiteWithoutDefault(t *testing.T) {
	f := newCandidateFixture(t)
	f.catalog[0].DefaultRef = ""
	path := filepath.Join(f.root, "probe.git")
	candidateGit(t, "bundle", "create", path, "HEAD", "^"+f.base)
	dir, err := candidateBuild(t, f, "--bundle", path)
	if err != nil {
		t.Fatal(err)
	}
	b := candidateLoad(t, dir)
	if b.Manifest.Environment.Ref != f.base {
		t.Fatal("prerequisite not used")
	}
}
func TestReviewCandidateOlderCoordinatorRefusal(t *testing.T) {
	h := newTaskRunHarness()
	h.projects = reviewCatalog()
	h.release = "0.11.0-rc.116"
	cli := reviewCLI{task: h.cli()}
	for _, flag := range []string{"--commit", "--bundle"} {
		a, err := parseReviewArgs([]string{"--project", "scratch", flag, "candidate", "--reviewer", "a/full", "--reviewer", "b/full", "--no-notify"})
		if err != nil {
			t.Fatal(err)
		}
		err = cli.run(context.Background(), a)
		if err == nil || !strings.Contains(err.Error(), "0.11.0-rc.117") {
			t.Fatalf("old coordinator %s: %v", flag, err)
		}
	}
}
func TestReviewCandidateShellQuote(t *testing.T) {
	value := ".t3/inputs/quote' and space.bundle"
	quoted := reviewShellQuote(value)
	// Test the command with an actual shell, without executing candidate code.
	raw, err := exec.Command("sh", "-c", "printf '%s' "+quoted).Output()
	if err != nil || string(raw) != value {
		t.Fatalf("quoted path: %q %v", raw, err)
	}
}

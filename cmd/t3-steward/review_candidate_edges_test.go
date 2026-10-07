package main

import (
	"context"
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

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/campaign"
	"github.com/iryzhkov/t3-steward/internal/pinnedinput"
)

type candidateFixture struct {
	root, remote, base, head string
	catalog                  []backlogadmin.Project
}

func candidateGit(t *testing.T, args ...string) string {
	t.Helper()
	c := exec.Command("git", args...)
	c.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
	raw, err := c.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, raw)
	}
	return strings.TrimSpace(string(raw))
}
func newCandidateFixture(t *testing.T) candidateFixture {
	t.Helper()
	f := candidateFixture{root: reviewInputTempDir(t), remote: filepath.Join(reviewInputTempDir(t), "remote.git"), catalog: reviewCatalog()}
	candidateGit(t, "init", "--bare", "-q", f.remote)
	t.Chdir(f.root)
	candidateGit(t, "init", "-q")
	candidateGit(t, "config", "user.name", "Fixture")
	candidateGit(t, "config", "user.email", "fixture@example.invalid")
	candidateGit(t, "remote", "add", "origin", f.remote)
	if err := os.WriteFile("code.txt", []byte("before\n"), 0600); err != nil {
		t.Fatal(err)
	}
	candidateGit(t, "add", "code.txt")
	candidateGit(t, "commit", "-qm", "base")
	f.base = candidateGit(t, "rev-parse", "HEAD")
	candidateGit(t, "push", "-q", "origin", "HEAD:refs/heads/main")
	if err := os.WriteFile("code.txt", []byte("after\n"), 0600); err != nil {
		t.Fatal(err)
	}
	candidateGit(t, "add", "code.txt")
	candidateGit(t, "commit", "-qm", "candidate")
	f.head = candidateGit(t, "rev-parse", "HEAD")
	f.catalog[0].Type, f.catalog[0].Repository, f.catalog[0].DefaultRef = "git", f.remote, f.base
	return f
}
func candidateBuild(t *testing.T, f candidateFixture, flags ...string) (string, error) {
	t.Helper()
	args := append([]string{"--project", "scratch", "--reviewer", "a/full", "--reviewer", "b/full", "--no-notify"}, flags...)
	a, err := parseReviewArgs(args)
	if err != nil {
		return "", err
	}
	return buildReviewCampaign(a, f.catalog, time.Now())
}
func candidateLoad(t *testing.T, dir string) *campaign.Campaign {
	t.Helper()
	t.Cleanup(func() { os.RemoveAll(dir) })
	b, err := campaign.Load(dir, campaign.DefaultLimits)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func TestReviewCandidateCommit(t *testing.T) {
	f := newCandidateFixture(t)
	if dir, err := candidateBuild(t, f, "--commit", "HEAD"); err == nil || !strings.Contains(err.Error(), "push the commit or use --bundle") {
		os.RemoveAll(dir)
		t.Fatalf("unpushed candidate: %v", err)
	}
	candidateGit(t, "push", "-q", "origin", "HEAD:refs/heads/candidate")
	for _, withBase := range []bool{false, true} {
		flags := []string{"--commit", "HEAD"}
		if withBase {
			flags = append(flags, "--base", "HEAD~1")
		}
		dir, err := candidateBuild(t, f, flags...)
		if err != nil {
			t.Fatal(err)
		}
		b := candidateLoad(t, dir)
		if b.Manifest.Environment.Ref != f.head || b.Manifest.Review.HeadCommit != f.head {
			t.Fatal("candidate not pinned")
		}
		_, err = os.Stat(filepath.Join(dir, "review.diff"))
		if withBase {
			if err != nil || b.Manifest.Review.BaseCommit != f.base {
				t.Fatal("base/diff not pinned", err)
			}
			raw, err := os.ReadFile(filepath.Join(dir, "review.diff"))
			if err != nil || !strings.Contains(string(raw), "+after") {
				t.Fatal("missing candidate diff", err)
			}
		} else if !os.IsNotExist(err) || b.Manifest.Review.BaseCommit != "" {
			t.Fatal("commit-only generated a diff")
		}
	}
	// A stale tracking ref must not make a deleted remote candidate reachable.
	candidateGit(t, "push", "-q", "origin", ":refs/heads/candidate")
	if dir, err := candidateBuild(t, f, "--commit", "HEAD"); err == nil {
		os.RemoveAll(dir)
		t.Fatal("stale remote ref accepted")
	}
}
func TestReviewCandidateBundle(t *testing.T) {
	f := newCandidateFixture(t)
	path := filepath.Join(f.root, "export.bundle")
	// Same single-head/base shape as campaign commit export.
	candidateGit(t, "branch", "exported", f.head)
	candidateGit(t, "bundle", "create", path, "refs/heads/exported", "^"+f.base)
	dir, err := candidateBuild(t, f, "--bundle", path)
	if err != nil {
		t.Fatal(err)
	}
	b := candidateLoad(t, dir)
	if b.Manifest.Environment.Ref != f.base || b.Manifest.Review.BaseCommit != f.base || b.Manifest.Review.HeadCommit != f.head {
		t.Fatal("bundle identities not pinned")
	}
	original, _ := os.ReadFile(path)
	snap, err := os.ReadFile(filepath.Join(dir, "export.bundle"))
	if err != nil || string(snap) != string(original) {
		t.Fatal("bundle bytes not snapshotted", err)
	}
	for _, task := range b.Manifest.Tasks {
		raw, err := os.ReadFile(filepath.Join(dir, task.PromptFile))
		if err != nil {
			t.Fatal(err)
		}
		prompt := string(raw)
		for _, want := range []string{"git fetch", ".t3/inputs/export.bundle", f.head, "git checkout --detach"} {
			if !strings.Contains(prompt, want) {
				t.Fatalf("prompt missing %q: %s", want, prompt)
			}
		}
	}
	// Execute the exact instructed fetch in a different checkout with only base.
	other := reviewInputTempDir(t)
	candidateGit(t, "clone", "-q", f.remote, other)
	candidateGit(t, "-C", other, "fetch", "--no-tags", "--", filepath.Join(dir, "export.bundle"), f.head)
	candidateGit(t, "-C", other, "checkout", "--detach", f.head)
	if got := candidateGit(t, "-C", other, "rev-parse", "HEAD"); got != f.head {
		t.Fatal(got)
	}
	// No prerequisite: prepare from the project's default ref.
	full := filepath.Join(f.root, "full.bundle")
	candidateGit(t, "bundle", "create", full, "refs/heads/exported")
	dir, err = candidateBuild(t, f, "--bundle", full)
	if err != nil {
		t.Fatal(err)
	}
	b = candidateLoad(t, dir)
	if b.Manifest.Environment.Ref != f.base || b.Manifest.Review.HeadCommit != f.head {
		t.Fatal("full bundle ref")
	}
}
func TestReviewCandidateBundleRefusals(t *testing.T) {
	f := newCandidateFixture(t)
	for _, kind := range []string{"invalid", "multiple", "oversized", "unrelated", "missing-prerequisite", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(f.root, kind+".bundle")
			switch kind {
			case "invalid":
				if err := os.WriteFile(path, []byte("invalid"), 0600); err != nil {
					t.Fatal(err)
				}
			case "oversized":
				if err := os.WriteFile(path, make([]byte, pinnedinput.MaxFileBytes+1), 0600); err != nil {
					t.Fatal(err)
				}
			case "multiple":
				candidateGit(t, "branch", "another", f.base)
				candidateGit(t, "bundle", "create", path, "HEAD", "refs/heads/another")
			case "unrelated", "missing-prerequisite":
				other := reviewInputTempDir(t)
				candidateGit(t, "init", "-q", other)
				candidateGit(t, "-C", other, "config", "user.name", "Other")
				candidateGit(t, "-C", other, "config", "user.email", "other@example.invalid")
				candidateGit(t, "-C", other, "commit", "--allow-empty", "-qm", "unrelated")
				base := candidateGit(t, "-C", other, "rev-parse", "HEAD")
				candidateGit(t, "-C", other, "commit", "--allow-empty", "-qm", "more")
				args := []string{"-C", other, "bundle", "create", path, "HEAD"}
				if kind == "missing-prerequisite" {
					args = append(args, "^"+base)
				}
				candidateGit(t, args...)
			case "symlink":
				source := filepath.Join(f.root, "valid.bundle")
				candidateGit(t, "bundle", "create", source, "HEAD", "^"+f.base)
				if err := os.Symlink(source, path); err != nil {
					t.Fatal(err)
				}
			}
			dir, err := candidateBuild(t, f, "--bundle", path)
			if err == nil {
				os.RemoveAll(dir)
				t.Fatal("accepted", kind)
			}
			if kind == "oversized" && !strings.Contains(err.Error(), "1048576") && !strings.Contains(err.Error(), "1 MiB") {
				t.Fatal("unclear size refusal", err)
			}
		})
	}
}
func TestReviewCandidateFlagsAndHelp(t *testing.T) {
	modes := []string{"--commit", "--bundle", "--diff", "--diff-file"}
	for _, mode := range []string{"--commit", "--bundle"} {
		if _, err := parseReviewArgs([]string{mode, "x", "--project", "p"}); err != nil {
			t.Fatal(err)
		}
	}
	for i, left := range modes {
		for _, right := range modes[i+1:] {
			if _, err := parseReviewArgs([]string{"--project", "p", left, "x", right, "y"}); err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
				t.Fatalf("%s %s: %v", left, right, err)
			}
		}
	}
	if _, err := parseReviewArgs([]string{"--base", "HEAD"}); err == nil || !strings.Contains(err.Error(), "--commit") {
		t.Fatal("orphan base", err)
	}
	for _, flag := range []string{"commit", "bundle", "base"} {
		if _, err := parseReviewTaskArgs([]string{"--task", "current", "--" + flag, "x"}); err == nil || !strings.Contains(err.Error(), "--"+flag+" is not accepted") {
			t.Fatalf("task flag %s: %v", flag, err)
		}
		if !strings.Contains(reviewUsage, "--"+flag) {
			t.Fatal("help missing", flag)
		}
	}
	if !strings.Contains(reviewUsage, "mutually exclusive") {
		t.Fatal("help missing exclusion")
	}
}

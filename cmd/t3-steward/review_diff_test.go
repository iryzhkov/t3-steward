package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/campaign"
)

func TestReviewDiffPinsBothCommitsAndOmitsDirtyFiles(t *testing.T) {
	root := t.TempDir()
	// macOS exposes its temporary directory through /var -> /private/var.
	// Reproduce that layout on Linux without weakening input symlink refusal.
	tempRoot := t.TempDir()
	canonical, err := filepath.EvalSymlinks(tempRoot)
	if err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(t.TempDir(), "temp-alias")
	if err := os.Symlink(canonical, alias); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TMPDIR", alias)
	t.Chdir(root)
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
		raw, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v %s", args, err, raw)
		}
		return strings.TrimSpace(string(raw))
	}
	git("init", "-q")
	git("config", "user.name", "Review fixture")
	git("config", "user.email", "fixture@example.invalid")
	git("remote", "add", "origin", "https://example.invalid/review.git")
	file := filepath.Join(root, "code.txt")
	if err := os.WriteFile(file, []byte("before\n"), 0600); err != nil {
		t.Fatal(err)
	}
	git("add", ".")
	git("commit", "-qm", "before")
	base := git("rev-parse", "HEAD")
	if err := os.WriteFile(file, []byte("committed\n"), 0600); err != nil {
		t.Fatal(err)
	}
	git("add", ".")
	git("commit", "-qm", "after")
	head := git("rev-parse", "HEAD")
	if err := os.WriteFile(file, []byte("dirty secret\n"), 0600); err != nil {
		t.Fatal(err)
	}
	catalog := reviewCatalog()
	catalog[0].Type = "git"
	catalog[0].Repository = "https://example.invalid/review.git"
	catalog[0].DefaultRef = "HEAD"
	a, err := parseReviewArgs([]string{"--project", "scratch", "--diff", "HEAD~1..HEAD", "--reviewer", "a/full", "--reviewer", "b/full", "--no-notify"})
	if err != nil {
		t.Fatal(err)
	}
	dir, err := buildReviewCampaign(a, catalog, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	bundle, err := campaign.Load(dir, campaign.DefaultLimits)
	if err != nil {
		t.Fatal(err)
	}
	r := bundle.Manifest.Review
	if r.BaseCommit != base || r.HeadCommit != head || bundle.Manifest.Environment.Ref != head {
		t.Fatal("refs not pinned")
	}
	diff, err := os.ReadFile(filepath.Join(dir, "review.diff"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(diff), "+committed") || strings.Contains(string(diff), "dirty secret") {
		t.Fatalf("diff used working tree: %s", diff)
	}
	if err := os.WriteFile(file, []byte(strings.Repeat("oversized line\n", 100000)), 0600); err != nil {
		t.Fatal(err)
	}
	git("add", ".")
	git("commit", "-qm", "oversized")
	_, err = buildReviewCampaign(a, catalog, time.Now())
	if err == nil || !strings.Contains(err.Error(), "1 MiB") {
		t.Fatalf("oversized diff refusal: %v", err)
	}
}

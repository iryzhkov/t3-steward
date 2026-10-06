package main

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/campaign"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReviewRefusesHandWrittenSharedScope(t *testing.T) {
	a, _ := parseReviewArgs([]string{"--project", "scratch", "--reviewer", "a/full", "--reviewer", "b/full", "--no-notify"})
	catalog := reviewCatalog()
	catalog[0].Type = "git"
	catalog[0].Repository = "https://example.invalid/review.git"
	catalog[0].DefaultRef = "main"
	dir, err := buildReviewCampaign(a, catalog, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, campaign.ManifestFileName)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw = []byte(strings.Replace(string(raw), "scope: task", "scope: workflow", 1))
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	_, err = campaign.Load(dir, campaign.DefaultLimits)
	if err == nil || !strings.Contains(err.Error(), "environment.scope") {
		t.Fatalf("shared scope refusal: %v", err)
	}
}

func TestReviewOldCoordinatorRefusesBeforeCatalog(t *testing.T) {
	h := newTaskRunHarness()
	h.release = "0.11.0-rc.103"
	cli := reviewCLI{task: h.cli()}
	a, _ := parseReviewArgs([]string{"--project", "steward", "--reviewer", "a/full", "--no-notify"})
	err := cli.run(context.Background(), a)
	want := "coordinator does not support review rounds (needs 0.11.0-rc.104 or later)"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("old coordinator refusal: %v", err)
	}
	for _, q := range h.queries {
		if q == "projects" {
			t.Fatal("queried catalog before compatibility refusal")
		}
	}
}

func TestReviewRegisterOnlyNamesItsRefusal(t *testing.T) {
	a, _ := parseReviewArgs([]string{"--project", "scratch", "--reviewer", "a/full", "--reviewer", "b/full", "--no-notify"})
	dir, err := buildReviewCampaign(a, reviewCatalog(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	store, err := sqlitetest.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	_, err = (backlog.BundleIngester{RegisterOnly: true, Store: store, StorageRoot: t.TempDir()}).Ingest(context.Background(), dir)
	if err == nil || !strings.Contains(err.Error(), "register-only refuses review rounds") {
		t.Fatalf("register refusal: %v", err)
	}
}

func TestReviewJudgeNeedsExactlySwarmTasks(t *testing.T) {
	a, _ := parseReviewArgs([]string{"--project", "scratch", "--reviewer", "a/full", "--swarm", "security,tests", "--swarm-model", "a/cheap", "--judge", "b/full", "--no-notify"})
	catalog := reviewCatalog()
	catalog[0].Type = "git"
	catalog[0].Repository = "https://example.invalid/review.git"
	catalog[0].DefaultRef = "main"
	dir, err := buildReviewCampaign(a, catalog, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	b, err := campaign.Load(dir, campaign.DefaultLimits)
	if err != nil {
		t.Fatal(err)
	}
	task := b.Manifest.Tasks["judge"]
	task.Needs = []string{"swarm-security", "swarm-security"}
	b.Manifest.Tasks["judge"] = task
	err = backlog.ValidateReviewManifest(b.Manifest)
	if err == nil {
		t.Fatal("duplicate judge needs accepted, omitted tests lens")
	}
}

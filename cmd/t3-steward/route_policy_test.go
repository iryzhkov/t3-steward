package main

import (
	"context"
	"errors"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/campaign"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const m13Policy = "schema: route-policy/v1\nroles:\n  - name: execute\n    candidates:\n      - {route: claude/opus, effort: high, tier: standard}\n      - {route: codex/sol, effort: medium, tier: standard}\n"

func TestM13PolicyStrictAndOrdered(t *testing.T) {
	p, err := parseRoutePolicy([]byte(m13Policy))
	if err != nil {
		t.Fatal(err)
	}
	if p.Roles[0].Candidates[0].Route != "claude/opus" {
		t.Fatal("order lost")
	}
	for _, raw := range []string{strings.Replace(m13Policy, "route-policy/v1", "route-policy/v2", 1), strings.Replace(m13Policy, "high", "max", 1), m13Policy + "unknown: true\n", strings.Replace(m13Policy, "codex/sol", "claude/opus", 1), m13Policy + "---\n" + m13Policy} {
		if _, err := parseRoutePolicy([]byte(raw)); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
}
func TestM13ValidityIsNotAvailability(t *testing.T) {
	p, _ := parseRoutePolicy([]byte(m13Policy))
	workers := []backlogadmin.Worker{{Providers: []backlogadmin.WorkerProviderAuthorization{{Instance: "claude", Models: []string{"opus"}}, {Instance: "codex", Models: []string{"sol"}}}}}
	if err := validatePolicyCatalog(p, workers); err != nil {
		t.Fatal(err)
	}
	project := backlogadmin.Project{Name: "p", Workers: []backlogadmin.ProjectWorker{{Worker: "w", Ready: true, Advertises: true, Routes: []backlogadmin.ProjectRoute{{Instance: "codex", Model: "sol"}}}}}
	sel, err := selectPolicyRoute(p, "execute", "", "", "", project, nil)
	if err != nil {
		t.Fatal(err)
	}
	if sel.Route != "codex/sol" || sel.Effort != "medium" {
		t.Fatalf("%+v", sel)
	}
	if _, err := selectPolicyRoute(p, "missing", "", "", "", project, nil); err == nil {
		t.Fatal("unknown role accepted")
	}
	if _, err := selectPolicyRoute(p, "execute", "claude/opus", "", "", project, nil); err == nil {
		t.Fatal("explicit pin widened")
	}
	if _, err := selectPolicyRoute(p, "execute", "", "max", "", project, nil); err == nil {
		t.Fatal("max accepted")
	}
	project.Workers[0].Ready = false
	if _, err := selectPolicyRoute(p, "execute", "", "", "", project, nil); err == nil {
		t.Fatal("offline eligible")
	}
}
func TestM13ExplicitEffortAndSnapshot(t *testing.T) {
	p, _ := parseRoutePolicy([]byte(m13Policy))
	project := backlogadmin.Project{Workers: []backlogadmin.ProjectWorker{{Ready: true, Routes: []backlogadmin.ProjectRoute{{Instance: "claude", Model: "opus"}}}}}
	sel, err := selectPolicyRoute(p, "execute", "claude/opus", "low", "", project, nil)
	if err != nil {
		t.Fatal(err)
	}
	if sel.Effort != "low" || sel.Reason != "explicit model override" {
		t.Fatalf("%+v", sel)
	}
	snap, err := policyInputs(nil, p, []policySelection{sel})
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := snap.Write(dir); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "route-policy.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != m13Policy {
		t.Fatal("snapshot changed")
	}
}
func TestM13FlagsAndMissingPolicy(t *testing.T) {
	a, err := parseTaskRunArgs([]string{"--role", "execute", "--effort", "high", "--policy-file", "x", "--", "hello"})
	if err != nil || a.role != "execute" {
		t.Fatalf("%+v %v", a, err)
	}
	if _, err := parseTaskRunArgs([]string{"--effort", "max", "--", "hello"}); err == nil {
		t.Fatal("max accepted")
	}
	r, err := parseReviewArgs([]string{"--role", "critical-review", "--policy-file", "x", "--effort", "medium", "--no-notify"})
	if err != nil || r.role != "critical-review" {
		t.Fatalf("%+v %v", r, err)
	}
	c := taskRunCLI{policyPath: filepath.Join(t.TempDir(), "missing")}
	if _, err := c.loadPolicy(context.Background(), "", "execute"); err == nil || !strings.Contains(err.Error(), "policy") {
		t.Fatalf("%v", err)
	}
}

func TestM13ConstraintsApplyToExplicitPins(t *testing.T) {
	raw := strings.Replace(m13Policy, "    candidates:", "    constraints: {tiers: [standard], exclude_provider_families: [claude]}\n    candidates:", 1)
	p, err := parseRoutePolicy([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	project := backlogadmin.Project{Name: "p", Workers: []backlogadmin.ProjectWorker{{Ready: true, Routes: []backlogadmin.ProjectRoute{{Instance: "claude", Model: "opus", ProviderFamily: "claude", Tier: "executor"}, {Instance: "codex", Model: "sol", ProviderFamily: "openai", Tier: "critical"}}}}}
	if _, err := selectPolicyRoute(p, "execute", "claude/opus", "", "", project, nil); err == nil {
		t.Fatal("pin bypassed excluded family")
	}
	if _, err := selectPolicyRoute(p, "execute", "codex/sol", "", "", project, nil); err == nil {
		t.Fatal("pin bypassed catalog tier constraint")
	}
	if _, err := selectPolicyRoute(p, "execute", "", "", "", project, nil); err == nil {
		t.Fatal("candidate declaration overrode catalog tier")
	}
}
func TestM13ReviewRoleDoesNotWeakenRound(t *testing.T) {
	raw := "schema: route-policy/v1\nroles:\n - name: review\n   candidates:\n    - {route: a/full, effort: high, tier: standard}\n    - {route: b/full, effort: medium, tier: standard}\n"
	p, err := parseRoutePolicy([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	project := reviewCatalog()[0]
	project.Workers[0].Ready = true
	project.Workers[0].Advertises = true
	a := reviewArgs{role: "review", policy: p, risk: "routine", deadline: time.Hour, noNotify: true, project: "scratch"}
	if _, err := resolveReviewPolicy(a, project); err == nil {
		t.Fatal("single role fanned out or bypassed diversity")
	}
	a.swarm = "security"
	a.swarmModels = []string{"a/cheap"}
	a.judge = "a/full"
	chosen, err := resolveReviewPolicy(a, project)
	if err != nil {
		t.Fatal(err)
	}
	if len(chosen.reviewers) != 1 || chosen.reviewers[0] != "b/full" || chosen.judge != "a/full" {
		t.Fatalf("%+v", chosen)
	}
	dir, err := buildReviewCampaign(chosen, []backlogadmin.Project{project}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	bundle, err := campaign.Load(dir, campaign.DefaultLimits)
	if err != nil {
		t.Fatal(err)
	}
	independent := bundle.Manifest.Tasks["independent-1"]
	if len(independent.Needs) != 0 || len(independent.InputsFrom) != 0 || independent.Routes[0].Options["effort"] != "medium" {
		t.Fatalf("%+v", independent)
	}
	if len(bundle.Manifest.Inputs) != 2 {
		t.Fatalf("provenance missing: %+v", bundle.Manifest.Inputs)
	}
	a.reviewers = []string{"a/full"} // explicit cannot be replaced with b/full to make the round pass
	pinned, err := resolveReviewPolicy(a, project)
	if err != nil {
		t.Fatal(err)
	}
	if pinned.reviewers[0] != "a/full" {
		t.Fatal("explicit reviewer widened")
	}
	if dir, err := buildReviewCampaign(pinned, []backlogadmin.Project{project}, time.Now()); err == nil {
		os.RemoveAll(dir)
		t.Fatal("explicit role bypassed diversity")
	}
}
func TestM13TaskRetainsFrozenProvenanceAndRefusesCatalogFailure(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	raw := strings.ReplaceAll(strings.ReplaceAll(m13Policy, "claude/opus", "t3-primary/opus"), "codex/sol", "t3-primary/claude-haiku-4-5")
	path := filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	h := newTaskRunHarness()
	c := h.cli()
	c.policyPath = path
	originalQuery := c.query
	failCatalog := false
	c.query = func(ctx context.Context, q backlogadmin.Query) (backlogadmin.Response, error) {
		if q.Kind == backlogadmin.QueryWorkers {
			if failCatalog {
				return backlogadmin.Response{}, errors.New("old coordinator")
			}
			return backlogadmin.Response{Workers: []backlogadmin.Worker{{Providers: []backlogadmin.WorkerProviderAuthorization{{Instance: "t3-primary", Models: []string{"opus", "claude-haiku-4-5"}}}}}}, nil
		}
		return originalQuery(ctx, q)
	}
	args := []string{"--role", "execute", "--project", "steward", "--ref", "main", "--no-notify", "--json", "--", "hello"}
	if err := c.run(context.Background(), args); err != nil {
		t.Fatal(err)
	}
	manifest, files := h.manifest(t)
	if manifest.Routes[0].Options["effort"] != "high" {
		t.Fatal("effort not sent")
	}
	if files["route-policy.yaml"] != raw || !strings.Contains(files["route-selection.json"], "route-selection/v1") {
		t.Fatalf("missing retained provenance: %+v", files)
	}
	record := h.record(t)
	if record.Selection == nil || record.Selection.PolicyDigest == "" || record.Route.Effort != "high" {
		t.Fatalf("%+v", record)
	}
	h.stdout.Reset()
	if err := c.run(context.Background(), args); err != nil {
		t.Fatal(err)
	}
	if !h.record(t).Replayed {
		t.Fatal("replay lost")
	}
	before := len(h.archives)
	failCatalog = true
	h.stdout.Reset()
	if err := c.run(context.Background(), args); err == nil {
		t.Fatal("catalog failure submitted")
	}
	if len(h.archives) != before {
		t.Fatal("submitted after refusal")
	}
}
func TestM13NoPolicyMixedCoordinatorExplicit(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	h := newTaskRunHarness()
	h.projectsErr = errors.New("old catalog")
	if err := h.run("--project", "steward", "--model", "t3-primary/opus", "--ref", "main", "--no-notify", "--json", "--", "hello"); err != nil {
		t.Fatal(err)
	}
	if len(h.archives) != 1 {
		t.Fatal("explicit mixed caller refused")
	}
	h.stdout.Reset()
	if err := h.run("--role", "execute", "--project", "steward", "--ref", "main", "--no-notify", "--", "hello"); err == nil {
		t.Fatal("missing policy accepted")
	}
	if len(h.archives) != 1 {
		t.Fatal("submitted after missing policy")
	}
}

func TestM13PoolWindows(t *testing.T) {
	now := time.Now()
	short := now.Add(time.Hour)
	weekly := now.Add(7 * 24 * time.Hour)
	pool := domain.QuotaPool{ID: "p", ProviderInstanceIDs: []string{"codex"}}
	states := []domain.BucketState{{Key: domain.BucketKey{ProviderInstanceID: "codex", Window: "five-hour"}, UsedPercent: 25, ResetsAt: &short, ObservedAt: now}, {Key: domain.BucketKey{ProviderInstanceID: "codex", Window: "weekly"}, UsedPercent: 90, ResetsAt: &weekly, ObservedAt: now}}
	windows := modelsPoolWindows(pool, states, now, time.Hour)
	if len(windows) != 2 {
		t.Fatalf("%+v", windows)
	}
	for _, w := range windows {
		if w.UsedPercent == 90 && (w.ResetsAt == nil || !w.ResetsAt.Equal(weekly)) {
			t.Fatal("weekly percent paired with short reset")
		}
	}
}
